/*
 * Copyright (C) 2012-2018 Vasily Tarasov
 * Copyright (C) 2012-2014 Geoff Kuenning
 * Copyright (C) 2012-2014 Sonam Mandal
 * Copyright (C) 2012-2014 Karthikeyani Palanisami
 * Copyright (C) 2012-2014 Philip Shilane
 * Copyright (C) 2012-2014 Sagar Trehan
 * Copyright (C) 2012-2018 Erez Zadok
 * Copyright (c) 2016-2017 Vinothkumar Raja
 * Copyright (c) 2017-2017 Nidhi Panpalia
 * Copyright (c) 2017-2018 Noopur Maheshwari
 * Copyright (c) 2018-2018 Rahul Rane
 * Copyright (c) 2012-2018 Stony Brook University
 * Copyright (c) 2012-2018 The Research Foundation for SUNY
 * This file is released under the GPL.
 */

#include <linux/errno.h>
#include "persistent-data/dm-btree.h"
#include "persistent-data/dm-space-map.h"
#include "persistent-data/dm-space-map-disk.h"
#include "persistent-data/dm-block-manager.h"
#include "persistent-data/dm-transaction-manager.h"

#include "dm-dedup-hybrid.h"
#include "dm-dedup-backend.h"
#include "dm-dedup-kvstore.h"

#define EMPTY_ENTRY 0xFF
#define DELETED_ENTRY 0x6B
#define HASHTABLE_OVERPROV	(10)
#define UINT32_MAX	(4294967295U)

#define METADATA_BSIZE 4096
#define METADATA_MAXLOCKS 5
#define METADATA_SUPERBLOCK_LOCATION 0
#define PRIVATE_DATA_SIZE 16 /* physical and logical block counter */
#define DM_DEDUP_MAGIC 0x44447570 /* Hex value for "DDUP" */
#define DM_DEDUP_VERSION 1
#define SUPERBLOCK_CSUM_XOR 189575
struct metadata {
	/* Space Map */
	u32 *smap;
	u64 smax;
	u64 allocptr;

	struct dm_block_manager *meta_bm;
	struct dm_transaction_manager *tm;
	struct dm_space_map *data_sm;
	struct dm_space_map *meta_sm;

	/*
	 * XXX: Currently we support only one linear and one sparse KVS.
	 */
	struct kvstore_hybrid_linear *kvs_linear;
	struct kvstore_hybrid_sparse *kvs_sparse;

	u8 private_data[PRIVATE_DATA_SIZE];

};

struct kvstore_hybrid_linear {
	struct kvstore ckvs;
	u32 kmax;
	char *store;
};

struct kvstore_hybrid_sparse {
	struct kvstore ckvs;
	u32 entry_size;
	struct dm_btree_info info;
	u64 root;

	/*
	 * We will put max limit for linear probing.  We are maintaining two
	 * values for that.  First one indicates current max value for linear
	 * probing and second is hard limit until which linear probing is
	 * allowed.
	 */
	u32 lpc_cur;
	u32 lpc_max;

};

enum superblock_flags {
	CLEAN_SHUTDOWN /* on disk flag to mark clean shutdown */
};

#define SPACE_MAP_ROOT_SIZE 128

struct metadata_superblock {
	__le32 csum; /* Checksum of superblock except for this field. */
	__le64 magic; /* Magic number to check against */
	__le32 version; /* Metadata root version */
	__le32 flags; /* General purpose flags */
	__le64 blocknr;	/* This block number, dm_block_t. */
	__u8 uuid[16]; /* UUID of device (Not used) */
	__u8 lpc_last; /* Stores current limit on linear probing */
	/* Metadata space map */
	__u8 metadata_space_map_root[SPACE_MAP_ROOT_SIZE];
	__u8 data_space_map_root[SPACE_MAP_ROOT_SIZE]; /* Data space map */
	//__le64 lbn_pbn_root; /* lbn pbn btree root. */
	__le64 hash_pbn_root; /* hash pbn btree root. */
	__le32 data_block_size;	/* In bytes */
	__le32 metadata_block_size; /* In bytes */
	__u8 private_data[PRIVATE_DATA_SIZE]; /* Dmdedup counters */
	__le64 metadata_nr_blocks;/* Number of metadata blocks used. */
} __packed;

/*
 * It initializes the root of linear and sparse cow btrees and also
 * in case sparse hybrid restores last set max linear probing value
 * from superblock stored in metadata device.
 *
 * Return -ERR code on failure.
 * return 0 on success.
 */
static int __begin_transaction(struct metadata *md)
{
	int r;
	struct metadata_superblock *disk_super;
	struct dm_block *sblock;

	r = dm_bm_read_lock(md->meta_bm, METADATA_SUPERBLOCK_LOCATION,
			    NULL, &sblock);
	if (r)
		return r;

	disk_super = dm_block_data(sblock);

	// if (md->kvs_linear)
	// 	md->kvs_linear->root = le64_to_cpu(disk_super->lbn_pbn_root);

	if (md->kvs_sparse) {
		md->kvs_sparse->root = le64_to_cpu(disk_super->hash_pbn_root);
		md->kvs_sparse->lpc_cur = disk_super->lpc_last;
	}

	memcpy(md->private_data, disk_super->private_data, PRIVATE_DATA_SIZE);

	dm_bm_unlock(sblock);

	return r;
}

/*
 * It stores the current state of metadata device into superblock and write it
 * to disk.
 *
 * Returns -ERR on failure.
 * Returns 0 on success.
 */
static int __commit_transaction(struct metadata *md, bool clean_shutdown_flag)
{
	int r = 0;
	size_t metadata_len, data_len;
	struct metadata_superblock *disk_super;
	struct dm_block *sblock;

	BUILD_BUG_ON(sizeof(struct metadata_superblock) > 512);

	r = dm_sm_commit(md->data_sm);
	if (r < 0)
		goto out;

	r = dm_tm_pre_commit(md->tm);
	if (r < 0)
		goto out;

	r = dm_sm_root_size(md->meta_sm, &metadata_len);
	if (r < 0)
		goto out;

	r = dm_sm_root_size(md->data_sm, &data_len);
	if (r < 0)
		goto out;

	r = dm_bm_write_lock(md->meta_bm, METADATA_SUPERBLOCK_LOCATION,
			     NULL, &sblock);
	if (r)
		goto out;

	disk_super = dm_block_data(sblock);

	/* if destroy flag is set, set the bit 1 otherwise 0 */
	if (clean_shutdown_flag)
		disk_super->flags |= (1 << CLEAN_SHUTDOWN);
	else
		disk_super->flags &= ~(1 << CLEAN_SHUTDOWN);

	// if (md->kvs_linear)
	// 	disk_super->lbn_pbn_root = cpu_to_le64(md->kvs_linear->root);

	if (md->kvs_sparse) {
		disk_super->hash_pbn_root = cpu_to_le64(md->kvs_sparse->root);
		disk_super->lpc_last = md->kvs_sparse->lpc_cur;
	}

	r = dm_sm_copy_root(md->meta_sm,
			    &disk_super->metadata_space_map_root, metadata_len);

	if (r < 0)
		goto out_locked;

	r = dm_sm_copy_root(md->data_sm, &disk_super->data_space_map_root,
			    data_len);
	if (r < 0)
		goto out_locked;

	memcpy(disk_super->private_data, md->private_data, PRIVATE_DATA_SIZE);

	disk_super->csum = cpu_to_le32(dm_bm_checksum(&disk_super->flags,
					sizeof(struct metadata_superblock)
					- sizeof(__le32),
					SUPERBLOCK_CSUM_XOR));

	r = dm_tm_commit(md->tm, sblock);

	goto out;

out_locked:
	dm_bm_unlock(sblock);

out:
	return r;
}

/* It initializes super block fields. */
static int write_initial_superblock(struct metadata *md)
{
	int r;
	size_t meta_len, data_len;
	struct dm_block *sblock;
	struct metadata_superblock *disk_super;

	r = dm_sm_root_size(md->meta_sm, &meta_len);
	if (r < 0)
		return r;

	r = dm_sm_root_size(md->data_sm, &data_len);
	if (r < 0)
		return r;

	r = dm_sm_commit(md->data_sm);
	if (r < 0)
		return r;

	r = dm_tm_pre_commit(md->tm);
	if (r < 0)
		return r;

	r = dm_bm_write_lock_zero(md->meta_bm, METADATA_SUPERBLOCK_LOCATION,
				  NULL, &sblock);
	if (r < 0)
		return r;

	disk_super = dm_block_data(sblock);

	r = dm_sm_copy_root(md->meta_sm, &disk_super->metadata_space_map_root,
			    meta_len);
	if (r < 0)
		goto bad_locked;

	r = dm_sm_copy_root(md->data_sm, &disk_super->data_space_map_root,
			    data_len);
	if (r < 0)
		goto bad_locked;

	disk_super->magic = cpu_to_le32(DM_DEDUP_MAGIC);
	disk_super->version = DM_DEDUP_VERSION;

	disk_super->data_block_size = cpu_to_le32(METADATA_BSIZE);
	disk_super->metadata_block_size = cpu_to_le32(METADATA_BSIZE);

	disk_super->blocknr = cpu_to_le64(dm_block_location(sblock));

	/* set the clean shutdown flag to 0 */
	disk_super->flags &= ~(1 << CLEAN_SHUTDOWN);

	return dm_tm_commit(md->tm, sblock);

bad_locked:
	dm_bm_unlock(sblock);
	return r;
}

/*
 * It checks if first block of superblock is zeroed out or not. If found
 * zeroed out result is filled with true otherwise false.
 *
 * Returns -ERR code on error scenario.
 * Returns 0 on successful execution.
 */
static int superblock_all_zeroes(struct dm_block_manager *bm, bool *result)
{
	int r;
	unsigned int i;
	struct dm_block *b;
	__le64 *data_le, zero = cpu_to_le64(0);
	unsigned int sb_block_size = dm_bm_block_size(bm) / sizeof(__le64);

	/*
	 * We can't use a validator here - it may be all zeroes.
	 */
	r = dm_bm_read_lock(bm, METADATA_SUPERBLOCK_LOCATION, NULL, &b);
	if (r)
		return r;

	data_le = dm_block_data(b);
	*result = true;
	for (i = 0; i < sb_block_size; i++) {
		if (data_le[i] != zero) {
			*result = false;
			break;
		}
	}

	dm_bm_unlock(b);
	return 0;
}

/*
 * It verifies superblock various fields set with correct
 * values or not.
 *
 * Returns -ERR on failure.
 * Returns 0 on success.
 */
static int verify_superblock(struct dm_block_manager *bm)
{
	int r;
	struct metadata_superblock *disk_super;
	struct dm_block *sblock;
	__le32 csum_le;

	r = dm_bm_read_lock(bm, METADATA_SUPERBLOCK_LOCATION,
			    NULL, &sblock);
	if (r)
		goto out;

	disk_super = dm_block_data(sblock);

	csum_le = cpu_to_le32(dm_bm_checksum(&disk_super->flags,
					     sizeof(struct metadata_superblock)
					     - sizeof(__le32),
					     SUPERBLOCK_CSUM_XOR));

	if (csum_le != disk_super->csum) {
		DMERR("Superblock checksum verification failed");
		goto bad_sb;
	}

	if (le64_to_cpu(disk_super->magic) != DM_DEDUP_MAGIC) {
		DMERR("Magic number mismatch");
		goto bad_sb;
	}

	if (disk_super->version != DM_DEDUP_VERSION) {
		DMERR("Version number mismatch");
		/*
		 * XXX: handle version upgrade in future if possible
		 */
		goto bad_sb;
	}

	if (le32_to_cpu(disk_super->data_block_size) != METADATA_BSIZE) {
		DMERR("Data block size mismatch");
		goto bad_sb;
	}

	if (le32_to_cpu(disk_super->metadata_block_size) != METADATA_BSIZE) {
		DMERR("Metadata block size mismatch");
		goto bad_sb;
	}

	/* if clean shutdown flag is not set return error */
	if (!(disk_super->flags & (1 << CLEAN_SHUTDOWN)))
		DMWARN("Possible data Inconsistency. Run dmdedup_corruption_check tool");

	goto unlock_superblock;

bad_sb:
	r = -1;

unlock_superblock:
	dm_bm_unlock(sblock);

out:
	return r;
}

static struct metadata *init_meta_hybrid(void *input_param, bool *unformatted)
{
	int ret;
	u64 smap_size, tmp;
	struct metadata *md;
	struct dm_block_manager *meta_bm;
	struct dm_space_map *meta_sm;
	struct dm_space_map *data_sm = NULL;
	struct dm_transaction_manager *tm;
	struct init_param_hybrid *p =
				(struct init_param_hybrid *)input_param;

	DMINFO("Initializing hybrid backend");

	*unformatted = true;

	md = kzalloc(sizeof(*md), GFP_NOIO);
	if (!md)
		return ERR_PTR(-ENOMEM);

	meta_bm = dm_block_manager_create(p->metadata_bdev, METADATA_BSIZE,
					  METADATA_MAXLOCKS);
	if (IS_ERR(meta_bm)) {
		md = (struct metadata *)meta_bm;
		goto badbm;
	}

	ret = superblock_all_zeroes(meta_bm, unformatted);
	if (ret) {
		md = ERR_PTR(ret);
		goto badtm;
	}

	if (!*unformatted) {
		struct dm_block *sblock;
		struct metadata_superblock *disk_super;

		DMINFO("Reconstruct DDUP device");

		ret = verify_superblock(meta_bm);
		if (ret < 0) {
			DMERR("superblock verification failed");
			/* XXX: handle error */
		}

		md->meta_bm = meta_bm;

		ret = dm_bm_read_lock(meta_bm, METADATA_SUPERBLOCK_LOCATION,
				      NULL, &sblock);
		if (ret < 0) {
			DMERR("could not read_lock superblock");
			/* XXX: handle error */
		}

		disk_super = dm_block_data(sblock);

		ret = dm_tm_open_with_sm(meta_bm, METADATA_SUPERBLOCK_LOCATION,
					 disk_super->metadata_space_map_root,
					 sizeof(
					 disk_super->metadata_space_map_root),
					 &md->tm, &md->meta_sm);
		if (ret < 0) {
			DMERR("could not open_with_sm superblock");
			/* XXX: handle error */
		}

		md->data_sm = dm_sm_disk_open(md->tm,
					      disk_super->data_space_map_root,
					      sizeof(
					      disk_super->data_space_map_root));
		if (IS_ERR(md->data_sm)) {
			DMERR("dm_disk_open failed");
			/*XXX: handle error */
		}

		dm_bm_unlock(sblock);

		goto begin_trans;
	}

	ret = dm_tm_create_with_sm(meta_bm, METADATA_SUPERBLOCK_LOCATION,
				   &tm, &meta_sm);
	if (ret < 0) {
		md = ERR_PTR(ret);
		goto badtm;
	}

	data_sm = dm_sm_disk_create(tm, p->blocks);
	if (IS_ERR(data_sm)) {
		md = (struct metadata *)data_sm;
		goto badsm;
	}

	md->meta_bm = meta_bm;
	md->tm = tm;
	md->meta_sm = meta_sm;
	md->data_sm = data_sm;

	ret = write_initial_superblock(md);
	if (ret < 0) {
		md = ERR_PTR(ret);
		goto badwritesuper;
	}

begin_trans:
	ret = __begin_transaction(md);
	if (ret < 0) {
		md = ERR_PTR(ret);
		goto badwritesuper;
	}

	smap_size = p->blocks * sizeof(uint32_t);

	md->smap = vmalloc(smap_size);
	if (!md->smap) {
		kfree(md);
		return ERR_PTR(-ENOMEM);
	}

	tmp = smap_size;
	(void)do_div(tmp, (1024 * 1024));
	DMINFO("Space allocated for pbn reference count map: %llu.%06llu MB",
	       tmp, smap_size - (tmp * (1024 * 1024)));

	memset(md->smap, 0, smap_size);

	md->smax = p->blocks;
	md->allocptr = 0;
	md->kvs_linear = NULL;
	md->kvs_sparse = NULL;

	return md;

badwritesuper:
	dm_sm_destroy(data_sm);
badsm:
	dm_tm_destroy(tm);
	dm_sm_destroy(meta_sm);
badtm:
	dm_block_manager_destroy(meta_bm);
badbm:
	kfree(md);
	return md;
}

static void exit_meta_hybrid(struct metadata *md)
{
	int ret;

	bool clean_shutdown_flag = true;
	ret = __commit_transaction(md, clean_shutdown_flag);
	if (ret < 0)
		DMWARN("%s: __commit_transaction() failed, error = %d.",
		       __func__, ret);

	dm_sm_destroy(md->data_sm);
	dm_tm_destroy(md->tm);
	dm_sm_destroy(md->meta_sm);
	dm_block_manager_destroy(md->meta_bm);

	if (md->smap)
		vfree(md->smap);
	
	if (md->kvs_linear) {
		if (md->kvs_linear->store)
			vfree(md->kvs_linear->store);
		kfree(md->kvs_linear);
	}
	kfree(md->kvs_sparse);

	kfree(md);
}

static int flush_meta_hybrid(struct metadata *md)
{
	int r;

	bool clean_shutdown_flag = false;
	r = __commit_transaction(md, clean_shutdown_flag);
	if (r < 0)
		return r;

	r = __begin_transaction(md);

	return r;
}

/********************************************************
 *		Space Management Functions		*
 ********************************************************/

static uint64_t next_head(u64 current_head, u64 smax)
{
	current_head += 1;
	return dm_sector_div64(current_head, smax);
}

static int alloc_data_block_hybrid(struct metadata *md, uint64_t *blockn)
{
	u64 head, tail;

	head = md->allocptr;
	tail = md->allocptr;

	do {
		if (!md->smap[head]) {
			md->smap[head] = 1;
			*blockn = head;
			md->allocptr = next_head(head, md->smax);
			return 0;
		}

		head = next_head(head, md->smax);

	} while (head != tail);

	return -ENOSPC;
	//return dm_sm_new_block(md->data_sm, blockn);
}

static int inc_refcount_hybrid(struct metadata *md, uint64_t blockn)
{
	if (blockn >= md->smax)
		return -ERANGE;

	if (md->smap[blockn] != UINT32_MAX)
		md->smap[blockn]++;
	else
		return -E2BIG;

	return 0;
	//return dm_sm_inc_block(md->data_sm, blockn);
}

static int dec_refcount_hybrid(struct metadata *md, uint64_t blockn)
{
	if (blockn >= md->smax)
		return -ERANGE;

	if (md->smap[blockn])
		md->smap[blockn]--;
	else
		return -EFAULT;

	return 0;
	//return dm_sm_dec_block(md->data_sm, blockn);
}

static int get_refcount_hybrid(struct metadata *md, uint64_t blockn)
{
	if (blockn >= md->smax)
		return -ERANGE;

	return md->smap[blockn];
	// u32 refcount;
	// int r;

	// r = dm_sm_get_count(md->data_sm, blockn, &refcount);
	// if (r < 0)
	// 	return r;

	// return (int)refcount;
}

/*
 * This function checks if an entry is marked as deleted
 * (tombstone) or not.  We check if every byte in the
 * entry holds the value of DELETED_ENTRY.
 */
bool is_deleted_entry_hybrid(const char *ptr, uint32_t length)
{
	int i = 0;

	while ((i < length) && (ptr[i] == DELETED_ENTRY))
		i++;

	return i == length;
}
bool is_empty_entry_hybrid(unsigned char *ptr, int length)
{
	int i = 0;

	while ((i < length) && (ptr[i] == EMPTY_ENTRY))
		i++;

	return i == length;
}
/*********************************************************
 *		Linear KVS Functions			 *
 *********************************************************/
/*
 * It deletes key from btree.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int kvs_delete_linear_hybrid(struct kvstore *kvs,
			       void *key, int32_t ksize)
{
	u64 idx;
	char *ptr;
	struct kvstore_hybrid_linear *kvhybrid = NULL;

	kvhybrid = container_of(kvs, struct kvstore_hybrid_linear, ckvs);

	if (ksize != kvs->ksize)
		return -EINVAL;

	idx = *((uint64_t *)key);

	if (idx > kvhybrid->kmax)
		return -ERANGE;

	ptr = kvhybrid->store + kvs->vsize * idx;

	if (is_empty_entry_hybrid(ptr, kvs->vsize))
		return -ENODEV;

	memset(ptr, EMPTY_ENTRY, kvs->vsize);
	return 0;
}

/*
 * 0 - on success
 * -ENODATA - if entry not found
 * <0 - error on lookup
 */
static int kvs_lookup_linear_hybrid(struct kvstore *kvs, void *key,
				      s32 ksize, void *value, int32_t *vsize)
{
	u64 idx;
	char *ptr;
	int r = -ENODATA;
	struct kvstore_hybrid_linear *kvhybrid = NULL;

	kvhybrid = container_of(kvs, struct kvstore_hybrid_linear, ckvs);

	if (ksize != kvs->ksize)
		return -EINVAL;

	idx = *((uint64_t *)key);

	if (idx > kvhybrid->kmax)
		return -ERANGE;

	ptr = kvhybrid->store + kvs->vsize * idx;

	if (is_empty_entry_hybrid(ptr, kvs->vsize))
		return r;

	memcpy(value, ptr, kvs->vsize);
	*vsize = kvs->vsize;

	return 0;
}

/* Inserts key into cow btree.
 *
 * Returns -ERR code in failure.
 * Reurns 0 on success.
 */
static int kvs_insert_linear_hybrid(struct kvstore *kvs, void *key,
			       s32 ksize, void *value,
			       int32_t vsize)
{
	u64 idx;
	char *ptr;
	struct kvstore_hybrid_linear *kvhybrid = NULL;

	kvhybrid = container_of(kvs, struct kvstore_hybrid_linear, ckvs);

	if (ksize != kvs->ksize)
		return -EINVAL;

	if (vsize != kvs->vsize)
		return -EINVAL;

	idx = *((uint64_t *)key);

	if (idx > kvhybrid->kmax)
		return -ERANGE;

	ptr = kvhybrid->store + kvs->vsize * idx;

	memcpy(ptr, value, kvs->vsize);

	return 0;
}

static struct kvstore *kvs_create_linear_hybrid(struct metadata *md,
						  u32 ksize, uint32_t vsize,
						  u32 kmax,
						  bool unformatted)
{
	struct kvstore_hybrid_linear *kvs;
	u64 kvstore_size, tmp;

	if (!vsize || !ksize || !kmax)
		return ERR_PTR(-ENOTSUPP);

	/* Currently only 64bit keys are supported */
	if (ksize != 8)
		return ERR_PTR(-ENOTSUPP);

	/* We do not support two or more KVSs at the moment */
	if (md->kvs_linear)
		return ERR_PTR(-EBUSY);

	kvs = kmalloc(sizeof(*kvs), GFP_NOIO);
	if (!kvs)
		return ERR_PTR(-ENOMEM);

	kvstore_size = (kmax + 1) * vsize;
	kvs->store = vmalloc(kvstore_size);
	if (!kvs->store) {
		kfree(kvs);
		return ERR_PTR(-ENOMEM);
	}

	tmp = kvstore_size;
	(void)do_div(tmp, (1024 * 1024));
	DMINFO("Space allocated for linear key value store: %llu.%06llu MB",
	       tmp, kvstore_size - (tmp * (1024 * 1024)));

	memset(kvs->store, EMPTY_ENTRY, kvstore_size);

	kvs->ckvs.ksize = ksize;
	kvs->ckvs.vsize = vsize;
	kvs->kmax = kmax;
	kvs->ckvs.kvs_insert = kvs_insert_linear_hybrid;
	kvs->ckvs.kvs_lookup = kvs_lookup_linear_hybrid;
	kvs->ckvs.kvs_delete = kvs_delete_linear_hybrid;
	kvs->ckvs.kvs_iterate = NULL;

	return &(kvs->ckvs);
}

/********************************************************
 *		Sparse KVS Functions			*
 ********************************************************/

/*
 * It deletes the exact entry whose keyval is provided as
 * an input. No lookup is done here.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int kvs_delete_entry(struct kvstore_hybrid_sparse *kvhybrid,
			    char *cur_entry, char *next_entry,
			    u64 cur_key_val, int ret_next)
{
	int r;

	if (ret_next == 0 &&
	    memcmp(cur_entry, next_entry, sizeof(cur_key_val)) == 0) {
		/* There is a next key and it is a linearly probed one. */
		memset(cur_entry, DELETED_ENTRY, kvhybrid->entry_size);
			       __dm_bless_for_disk(&cur_key_val);

		r = dm_btree_insert(&(kvhybrid->info), kvhybrid->root,
				    &cur_key_val, cur_entry, &(kvhybrid->root));
		// DMWARN("Marked as tombstone for keyval = %lld", cur_key_val);
	} else {
		/*
		 * There is a next key and it is not a linearly probed one.
		 * OR
		 * There is no next key.
		 */
		r = dm_btree_remove(&(kvhybrid->info),
				    kvhybrid->root,
				    &cur_key_val,
				    &(kvhybrid->root));
		/* DMWARN("Performed actual deletion for keyval = %lld",
		       cur_key_val); */
	}
	return r;
}

static int kvs_delete_sparse_hybrid(struct kvstore *kvs,
				      void *key, int32_t ksize)
{
	char *cur_entry, *next_entry = NULL;
	u64 key_val, cur_key_val;
	int r = 0;
	struct kvstore_hybrid_sparse *kvhybrid = NULL;

	kvhybrid = container_of(kvs, struct kvstore_hybrid_sparse, ckvs);

	if (ksize != kvs->ksize)
		return -EINVAL;

	cur_entry = kmalloc(kvhybrid->entry_size, GFP_NOIO);
	if (!cur_entry)
		return -ENOMEM;

	key_val = (*(uint64_t *)key);

	r = dm_btree_lookup(&(kvhybrid->info), kvhybrid->root, &key_val, cur_entry);

	if (r == -ENODATA) {
		return -ENODEV;
	}
	while (r == 0) {
		cur_key_val = key_val;
		key_val++;

		next_entry = kmalloc(kvhybrid->entry_size, GFP_NOIO);
		if (!next_entry)
			return -ENOMEM;

		r = dm_btree_lookup(&(kvhybrid->info),
				     kvhybrid->root,
				     &key_val, next_entry);

		if (!memcmp(cur_entry, key, ksize)) {
			/* Key found. */
			r = kvs_delete_entry(kvhybrid, cur_entry, next_entry,
					     cur_key_val, r);
			// DMWARN("Deleted key successfully");
			goto out;
		} else if (r == 0) {
			/* Key not found but there is a next key. */
			cur_entry = next_entry;
		} else {
			break;
		}
	}
out:
	kfree(cur_entry);
	kfree(next_entry);
	return r;
}

/*
 * 0 - not found or even after hitting limit for max linear
 * probing but we could not find an entry.
 * 1 - found
 * < 0 - error on lookup
 */
static int kvs_lookup_sparse_hybrid(struct kvstore *kvs, void *key,
				      s32 ksize, void *value, int32_t *vsize)
{
	char *entry;
	u64 key_val;
	int i, r = -ENODATA;
	struct kvstore_hybrid_sparse *kvhybrid = NULL;

	kvhybrid = container_of(kvs, struct kvstore_hybrid_sparse, ckvs);

	if (ksize != kvs->ksize)
		return -EINVAL;

	entry = kmalloc(kvhybrid->entry_size, GFP_NOIO);
	if (!entry)
		return -ENOMEM;

	key_val = (*(uint64_t *)key);
	/*
	 * In case of linear probing we need to iterate only till current set
	 * lpc_max.
	 */
	/*
	 * XXX:Need to put lock around whole code since multiple threads
	 * might be accessing this limit.
	 */
	for (i = 0; i <= kvhybrid->lpc_cur; i++) {
		r = dm_btree_lookup(&(kvhybrid->info), kvhybrid->root, &key_val,
		entry);
		/* if entry not found in btree */
		if (r == -ENODATA) {
			kfree(entry);
			return r;
		} else if (r == 0) {
			/* If entry is found but only first 8 bytes are matched. */
			if (!memcmp(entry, key, ksize)) {
				memcpy(value, entry + ksize, kvs->vsize);
				kfree(entry);
				return 0;
			}
			DMWARN("kvs_lookup_sparse_hybrid: hash collision for "
			"key :%llu %s", key_val, entry);
			key_val++;
		} else {
			/* Error in finding an entry. */
			kfree(entry);
			return r;
		}
	}
	kfree(entry);
	return r;
}

/*
 * It tries to insert key into cow btree. In case of collision linear
 * probing is done until it hits max limit.
 *
 * Returns -ERR code on failure.
 * Returns 0 on success.
 */
static int kvs_insert_sparse_hybrid(struct kvstore *kvs, void *key,
			       s32 ksize, void *value, int32_t vsize)
{
	char *entry;
	u64 key_val;
	int i, r;
	struct kvstore_hybrid_sparse *kvhybrid = NULL;

	kvhybrid = container_of(kvs, struct kvstore_hybrid_sparse, ckvs);

	if (ksize != kvs->ksize)
		return -EINVAL;

	if (vsize != kvs->vsize)
		return -EINVAL;

	entry = kmalloc(kvhybrid->entry_size, GFP_NOIO);
	if (!entry)
		return -ENOMEM;

	key_val = (*(uint64_t *)key);

	for (i = 0; i <= kvhybrid->lpc_max; i++) {
		r = dm_btree_lookup(&(kvhybrid->info), kvhybrid->root, &key_val,
		entry);
		if (r == -ENODATA ||
			is_deleted_entry_hybrid(entry, kvhybrid->entry_size)) {
			memcpy(entry, key, ksize);
			memcpy(entry + ksize, value, vsize);
			__dm_bless_for_disk(&key_val);
			r = dm_btree_insert(&(kvhybrid->info), kvhybrid->root,
			&key_val, entry, &(kvhybrid->root));
			kfree(entry);
			if (i > kvhybrid->lpc_cur) {
				/*
				 * TODO: Need to put locks around it since
				 * multiple threads might read/write this
				 * variable.
				 */
				DMINFO("Changing linear probing to %d", i);
				kvhybrid->lpc_cur = i;
			}
			return 0;
		} else if (r >= 0) {
			DMINFO("Collision detected for key: %s",(char *)key);
			key_val++;
		} else {
			kfree(entry);
			return r;
		}
	}
	DMINFO("Linear probing hard limit hit for insert hence"
	"changing current max to hard limit :%d", kvhybrid->lpc_max);
	/* XXX: Need to hold lock on variable */
	kvhybrid->lpc_cur = kvhybrid->lpc_max;
	kfree(entry);
	return -ENOSPC;

}

static int kvs_iterate_sparse_hybrid(struct kvstore *kvs,
				       int (*fn)(void *key, int32_t ksize,
						 void *value, s32 vsize,
						 void *data),
					void *dc)
{
	struct kvstore_hybrid_sparse *kvhybrid = NULL;
	char *entry, *key = NULL, *value = NULL;
	int r = 0;
	dm_block_t lowest, highest;

	kvhybrid = container_of(kvs, struct kvstore_hybrid_sparse, ckvs);

	entry = kmalloc(kvs->ksize + kvs->vsize, GFP_NOIO);
	if (!entry)
		goto out;

	key = kmalloc(kvs->ksize, GFP_NOIO);
	if (!key)
		goto out;

	value = kmalloc(kvs->vsize, GFP_NOIO);
	if (!value)
		goto out;

	/* Get the lowest and highest keys in the key value store */
	r = dm_btree_find_lowest_key(&(kvhybrid->info), kvhybrid->root, &lowest);
	if (r <= 0)
		goto out;

	r = dm_btree_find_highest_key(&(kvhybrid->info), kvhybrid->root, &highest);
	if (r <= 0)
		goto out;

	while (lowest <= highest) {
		/* Get the next entry entry in the kvs store */
		r = dm_btree_lookup_next(&(kvhybrid->info), kvhybrid->root,
					 &lowest, &lowest, (void *)entry);

		lowest++;
		/*
		 * Do not iterate over entries that are marked as deleted
		 */
		if (r || is_deleted_entry_hybrid(entry, kvs->ksize + kvs->vsize))
			continue;

		/* Split the key and value separately */
		memcpy(key, entry, kvs->ksize);
		memcpy(value, (void *)(entry + kvs->ksize), kvs->vsize);

		/* Call the cleanup callback function */
		r = fn((void *)key, kvs->ksize, (void *)value,
		       kvs->vsize, (void *)dc);
		if (r)
			goto out;
	}

out:
	kfree(value);
	kfree(key);
	kfree(entry);

	return r;
}

static struct kvstore *kvs_create_sparse_hybrid(struct metadata *md,
						  u32 ksize, uint32_t vsize,
						  u32 knummax,
						  bool unformatted, u32 type)
{
	struct kvstore_hybrid_sparse *kvs;
	int r;

	if (!vsize || !ksize)
		return ERR_PTR(-ENOTSUPP);

	/* We do not support two or more KVSs at the moment */
	if (md->kvs_sparse)
		return ERR_PTR(-EBUSY);

	kvs = kmalloc(sizeof(*kvs), GFP_NOIO);
	if (!kvs)
		return ERR_PTR(-ENOMEM);

	kvs->ckvs.vsize = vsize;
	kvs->ckvs.ksize = ksize;
	kvs->entry_size = vsize + ksize;

	kvs->info.tm = md->tm;
	kvs->info.levels = 1;
	kvs->info.value_type.context = NULL;
	kvs->info.value_type.size = kvs->entry_size;
	kvs->info.value_type.inc = NULL;
	kvs->info.value_type.dec = NULL;
	kvs->info.value_type.equal = NULL;
	kvs->lpc_max = MAX_LINEAR_PROBING_LIMIT;
	kvs->lpc_cur = 0;

	if (!unformatted) {
		kvs->ckvs.kvs_insert = kvs_insert_sparse_hybrid;
		kvs->ckvs.kvs_lookup = kvs_lookup_sparse_hybrid;
		kvs->ckvs.kvs_delete = kvs_delete_sparse_hybrid;
		kvs->ckvs.kvs_iterate = kvs_iterate_sparse_hybrid;

		md->kvs_sparse = kvs;
		__begin_transaction(md);
	} else {
		r = dm_btree_empty(&(kvs->info), &(kvs->root), 2);
		if (r < 0) {
			kvs = ERR_PTR(r);
			goto badtree;
		}

		/* I think this should be moved below the 4 lines below */
		flush_meta_hybrid(md);

		kvs->ckvs.kvs_insert = kvs_insert_sparse_hybrid;
		kvs->ckvs.kvs_lookup = kvs_lookup_sparse_hybrid;
		kvs->ckvs.kvs_delete = kvs_delete_sparse_hybrid;
		kvs->ckvs.kvs_iterate = kvs_iterate_sparse_hybrid;

		md->kvs_sparse = kvs;
	}

	return &(kvs->ckvs);

badtree:
	kfree(kvs);
	return (struct kvstore *)kvs;
}

int get_private_data_hybrid(struct metadata *md, void **data, uint32_t size)
{
	if (size > sizeof(md->private_data))
		return -1;

	memcpy(*data, md->private_data, size);
	return 0;
}

int set_private_data_hybrid(struct metadata *md, void *data, uint32_t size)
{
	if (size > sizeof(md->private_data))
		return -1;

	memcpy(md->private_data, data, size);
	return 0;
}

struct dm_block_manager {
	struct dm_bufio_client *bufio;
	bool read_only:1;
};

void* get_bufio_client_hybrid(struct metadata *md)
{
	struct dm_block_manager* bm = md->meta_bm;
	return (void*)(bm->bufio);
};

struct metadata_ops metadata_ops_hybrid = {
	.init_meta = init_meta_hybrid,
	.exit_meta = exit_meta_hybrid,
	.kvs_create_linear = kvs_create_linear_hybrid,
	.kvs_create_sparse = kvs_create_sparse_hybrid,

	.alloc_data_block = alloc_data_block_hybrid,
	.inc_refcount = inc_refcount_hybrid,
	.dec_refcount = dec_refcount_hybrid,
	.get_refcount = get_refcount_hybrid,
	.set_refcount = NULL,

	.flush_meta = flush_meta_hybrid,

	.flush_bufio_cache = NULL,
	.get_bufio_client = get_bufio_client_hybrid,
	.get_private_data = get_private_data_hybrid,
	.set_private_data = set_private_data_hybrid,
	.change_hash_pbn_root = NULL,
};
