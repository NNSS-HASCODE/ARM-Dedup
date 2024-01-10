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

#include <linux/vmalloc.h>
#include <linux/kdev_t.h>
#include <linux/kernel.h>
#include <linux/sched.h>
#include <linux/sched/signal.h>
#include <linux/pid.h>
#include <linux/kthread.h>
#include <linux/mutex.h>

#include "dm-dedup-target.h"
#include "dm-dedup-rw.h"
#include "dm-dedup-hash.h"
#include "dm-dedup-backend.h"
#include "dm-dedup-ram.h"
#include "dm-dedup-cbt.h"
#include "dm-dedup-xremap.h"
#include "dm-dedup-hybrid.h"
#include "dm-dedup-kvstore.h"
#include "dm-dedup-check.h"

#define MAX_DEV_NAME_LEN (64)

#define MIN_IOS 64

#ifdef TV_U32

#define TV_TYPE 0x80000000
#define TV_VER  0x7fffffff
#define TV_MAX  0x7fffffff
#define TV_BIT  31

#else

#define TV_TYPE 0x80
#define TV_VER  0x7f
#define TV_MAX  0x7f
#define TV_BIT  7

#endif

#define MIN_DATA_DEV_BLOCK_SIZE (4 * 1024)
#define MAX_DATA_DEV_BLOCK_SIZE (1024 * 1024)

struct on_disk_stats {
	u64 physical_block_counter;
	u64 logical_block_counter;
};

/*
 * All incoming requests are packed in the dedup_work structure
 * for further processing by the workqueue thread.
 */
struct dedup_work {
	struct work_struct worker;
	struct dedup_config *config;
	struct bio *bio;
};

struct remap_or_discard_work {
    struct work_struct worker;
    struct dedup_config *config;
    u64 pbn;
    u64 lbn;
    int temp;
    int flag;
};

enum backend {
	BKND_INRAM,
	BKND_COWBTREE,
	BKND_XREMAP,
	BKND_HYBRID
};

#define LIST_SIZE	2
#define PRIVATE_DATA_SIZE 16
struct dm_bufio_client {
	struct mutex lock;

	struct list_head lru[LIST_SIZE];
	unsigned long n_buffers[LIST_SIZE];

	struct block_device *bdev;
	unsigned block_size;
	s8 sectors_per_block_bits;
	void (*alloc_callback)(struct dm_buffer *);
	void (*write_callback)(struct dm_buffer *);

	struct kmem_cache *slab_buffer;
	struct kmem_cache *slab_cache;
	struct dm_io_client *dm_io;

	struct list_head reserved_buffers;
	unsigned need_reserved_buffers;

	unsigned minimum_buffers;

	struct rb_root buffer_tree;
	wait_queue_head_t free_buffer_wait;

	sector_t start;

	int async_write_error;
	unsigned long long cntio;
	unsigned long long cntbio;
	unsigned long long cntbio_read;
	unsigned long long cntbio_write;
	unsigned long long cntbio_sort[6];
	unsigned long long cntbio_sort_r[6];
	unsigned long long tmp_io_time;
	unsigned long long total_io_time;
	int rw;
	struct list_head client_list;
	struct shrinker shrinker;
};

struct dm_buffer {
	struct rb_node node;
	struct list_head lru_list;
	sector_t block;
	void *data;
	unsigned char data_mode;		/* DATA_MODE_* */
	unsigned char list_mode;		/* LIST_* */
	blk_status_t read_error;
	blk_status_t write_error;
	unsigned hold_count;
	unsigned long state;
	unsigned long last_accessed;
	unsigned dirty_start;
	unsigned dirty_end;
	unsigned write_start;
	unsigned write_end;
	struct dm_bufio_client *c;
	struct list_head write_list;
	void (*end_io)(struct dm_buffer *, blk_status_t);
#ifdef CONFIG_DM_DEBUG_BLOCK_STACK_TRACING
#define MAX_STACK 10
	struct stack_trace stack_trace;
	unsigned long stack_entries[MAX_STACK];
#endif
};

struct rd_work {
    struct dedup_config *config;
    u64 pbn;
    u64 lbn;
    int temp;
    int flag;
};

struct rd_queue {
    int front, rear;
    struct rd_work data[10000];
};

struct mutex queue_lock;

struct rd_queue rd_queue;

static void init_queue(void) {
    rd_queue.front = 0;
    rd_queue.rear = 0;
}

static bool queue_is_empty(void) {
    /* return (rd_queue.front == rd_queue.rear); */
    bool ret;
    mutex_lock(&queue_lock);
    ret = (rd_queue.front == rd_queue.rear);
    mutex_unlock(&queue_lock);
    return ret;
}

static void queue_push(struct dedup_config *config, u64 pbn, u64 lbn, int temp, int flag) {
    rd_queue.data[rd_queue.rear].config = config;
    rd_queue.data[rd_queue.rear].pbn = pbn;
    rd_queue.data[rd_queue.rear].lbn = lbn;
    rd_queue.data[rd_queue.rear].temp = temp;
    rd_queue.data[rd_queue.rear].flag = flag;
    mutex_lock(&queue_lock);
    rd_queue.rear = (rd_queue.rear + 1) % 10000;
    mutex_unlock(&queue_lock);
}

static struct rd_work queue_pop(void) {
    struct rd_work rd_work;
    rd_work = rd_queue.data[rd_queue.front];
    mutex_lock(&queue_lock);
    rd_queue.front = (rd_queue.front + 1) % 10000;
    mutex_unlock(&queue_lock);
    return rd_work;
}

static inline unsigned long read_tsc(void) {
    unsigned long var;
    unsigned int hi, lo;

    asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
    var = ((unsigned long long int) hi << 32) | lo;
    
    return var;
}

static void calc_tsc(struct dedup_config *dc, int period, int type) {
    unsigned long var, t;
    unsigned int hi, lo;

	asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
    var = ((unsigned long long int) hi << 32) | lo;
	if(!dc->enable_time_stats)
		return;
	switch (type)
	{
	case PERIOD_START:
		dc->tmp_period_time[period] = var;
		break;
	case PERIOD_END:
		if(dc->tmp_period_time[period]) {
			t = var - dc->tmp_period_time[period];
			dc->total_period_time[period] += t;
		}
		dc->tmp_period_time[period] = 0;
		break;
	default:
		break;
	}
	
    return;
}

static int calculate_tarSSD(struct dedup_config *dc, u64 lpn) {
	sector_t align_size = dc->ssd_num, tmp = 0;
	tmp = sector_div(lpn, align_size);
	return tmp;
}

static u64 calculate_entry_offset(struct dedup_config *dc, u64 lpn, int target) {
	u64 offset =0;
	if(dc->remote_len)
		offset = lpn % (dc->remote_len);
	if(dc->raid_mode && dc->remote_len) {
		u64 tmp;
		int pd_idx;
		int ssdnum = dc->ssd_num;
		u64 len = dc->remote_len / ssdnum * (ssdnum - 1);
		offset = lpn % len;
		tmp = offset % (ssdnum - 1);
		offset = offset / (ssdnum - 1) * ssdnum + (offset % (ssdnum - 1));
		pd_idx = (ssdnum - 1) - (target % ssdnum);
		if(tmp >= pd_idx)
			offset += 1;
	}
	return offset;
}

static sector_t remap_tarSSD(struct dedup_config *dc, u64 lpn, int origin, int target) {
	calc_tsc(dc, PERIOD_MAP, PERIOD_START);
	if(origin <= target) {
		lpn += (target - origin);
	}
	else {
		lpn -= (origin - target);
	}
	calc_tsc(dc, PERIOD_MAP, PERIOD_END);
	return lpn;
}

/* Initializes bio. */
static void bio_zero_endio(struct bio *bio)
{
	zero_fill_bio(bio);
	bio->bi_status = BLK_STS_OK;
	bio_endio(bio);
}

/* Returns the logical block number for the bio. */
static uint64_t bio_lbn(struct dedup_config *dc, struct bio *bio)
{
	sector_t lbn = bio->bi_iter.bi_sector;

	sector_div(lbn, dc->sectors_per_block);

	return lbn;
}

/* Entry point to the generic block layer. */
static void do_io_remap_device(struct dedup_config *dc, struct bio *bio)
{
	bio_set_dev(bio, dc->data_dev->bdev);
	generic_make_request(bio);
}

/*
 * Updates the sector indice, given the pbn and offset calculation, and
 * enters the generic block layer.
 */
static void do_io(struct dedup_config *dc, struct bio *bio, uint64_t pbn)
{
	int offset;

	offset = sector_div(bio->bi_iter.bi_sector, dc->sectors_per_block);
	bio->bi_iter.bi_sector = (sector_t)pbn * dc->sectors_per_block + offset;

	do_io_remap_device(dc, bio);
}

/*
 * Gets the pbn from the LBN->PBN entry and performs io request.
 * If corruption check is enabled, it prepares the check_io
 * structure for FEC and then performs io request.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int handle_read_xremap(struct dedup_config *dc, struct bio *bio)
{
	u64 lbn;
	t_v tv;
	struct bio *clone;
	int ref, t;
	
	clone = bio;
	lbn = bio_lbn(dc, bio);

	/* get the tv pair in LBN->tv store for incoming lbn */
	ref = dc->mdops->get_refcount(dc->bmd, lbn);
	tv.type = (ref & TV_TYPE) != 0;
	tv.ver = (ref & TV_VER);
	t = calculate_tarSSD(dc, lbn);
	//DMINFO("     [ENODATA=%d][lbn=%llu][t_v=%x][type=%d][ver=%d]", r==0, lbn, r, tv.type, tv.ver);

	if (tv.type == 0 && tv.ver == 0) {
		/* unable to find the entry in LBN->tv store */
		bio_zero_endio(bio);
	} else if (tv.type == 1 && t != tv.ver) {
		/* entry found in the LBN->tv store and is a remoteread*/
		lbn = remap_tarSSD(dc, lbn, t, tv.ver);
		clone->bi_opf = (clone->bi_opf & (~REQ_OP_MASK)) | REQ_OP_REMOTEREAD | REQ_NOMERGE;
		clone->bi_read_hint = calculate_entry_offset(dc, lbn, tv.ver);
		//DMINFO("     [t=%d][v=%d][lbn=%llu][op=%x]", t, tv.ver, lbn, bio->bi_opf);
		calc_tsc(dc, PERIOD_IO, PERIOD_START);
		do_io(dc, clone, lbn);
		calc_tsc(dc, PERIOD_IO, PERIOD_END);
	} else {
		calc_tsc(dc, PERIOD_IO, PERIOD_START);
		do_io(dc, clone, lbn);
		calc_tsc(dc, PERIOD_IO, PERIOD_END);
	}

	return 0;
}

/*
 * Gets the pbn from the LBN->PBN entry and performs io request.
 * If corruption check is enabled, it prepares the check_io
 * structure for FEC and then performs io request.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int handle_read(struct dedup_config *dc, struct bio *bio)
{
	u64 lbn;
	u32 vsize;
	struct lbn_pbn_value lbnpbn_value;
	struct check_io *io;
	struct bio *clone;
	int r;

	lbn = bio_lbn(dc, bio);

	/* get the pbn in LBN->PBN store for incoming lbn */
	r = dc->kvs_lbn_pbn->kvs_lookup(dc->kvs_lbn_pbn, (void *)&lbn,
			sizeof(lbn), (void *)&lbnpbn_value, &vsize);
	//DMINFO("     [ENODATA=%d][lbn=%llu][pbn=%llu]", r==-ENODATA, lbn, lbnpbn_value.pbn);

	if (r == -ENODATA) {
		/* unable to find the entry in LBN->PBN store */
		bio_zero_endio(bio);
	} else if (r == 0) {
		/* entry found in the LBN->PBN store */

		/* if corruption check not enabled directly do io request */
		if (!dc->check_corruption) {
			clone = bio;
			goto read_no_fec;
		}

		/* Prepare check_io structure to be later used for FEC */
		io = kmalloc(sizeof(struct check_io), GFP_NOIO);
		io->dc = dc;
		io->pbn = lbnpbn_value.pbn;
		io->lbn = lbn;
		io->base_bio = bio;

		/*
		 * Prepare bio clone to handle disk read
		 * clone is created so that we can have our own endio
		 * where we call bio_endio on original bio
		 * after corruption checks are done
		 */
		clone = bio_clone_fast(bio, GFP_NOIO, &dc->bs);
		if (!clone) {
			r = -ENOMEM;
			goto out_clone_fail;
		}

		/*
		 * Store the check_io structure in bio's private field
		 * used as indirect argument when disk read is finished
		 */
		clone->bi_end_io = dedup_check_endio;
		clone->bi_private = io;

read_no_fec:
		calc_tsc(dc, PERIOD_IO, PERIOD_START);
		do_io(dc, clone, lbnpbn_value.pbn);
		calc_tsc(dc, PERIOD_IO, PERIOD_END);
	} else {
		goto out;
	}

	r = 0;
	goto out;

out_clone_fail:
	kfree(io);

out:
	return r;

}

static int garbage_collect(struct dedup_config *dc);
/*
 * Allocates pbn_new and increments the logical and physical block
 * counters. Note that it also increments refcount internally.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
int allocate_block(struct dedup_config *dc, uint64_t *pbn_new)
{
	int r;

	calc_tsc(dc, PERIOD_IO, PERIOD_START);
	r = dc->mdops->alloc_data_block(dc->bmd, pbn_new);
	calc_tsc(dc, PERIOD_IO, PERIOD_END);

	if (!r) {
		dc->logical_block_counter++;
		dc->physical_block_counter++;
	} else {
		r = garbage_collect(dc);
		if(r)
			return r;
		calc_tsc(dc, PERIOD_FUA, PERIOD_START);
		r = dc->mdops->flush_meta(dc->bmd);
		calc_tsc(dc, PERIOD_FUA, PERIOD_END);
		if (r < 0)
			DMERR("Failed to flush the metadata to disk.");
		DMINFO("garbage_collect is trigged while allocating.");
		dc->writes_after_flush = 0;
		r = dc->mdops->alloc_data_block(dc->bmd, pbn_new);
	}

	return r;
}

/*
 * Allocates pbn_new and performs write io.
 * Inserts the new LBN->PBN entry.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int alloc_pbnblk_and_insert_lbn_pbn(struct dedup_config *dc,
					   u64 *pbn_new,
					   struct bio *bio, uint64_t lbn)
{
	int r = 0;
	struct lbn_pbn_value lbnpbn_value;

	r = allocate_block(dc, pbn_new);
	if (r < 0) {
		r = -EIO;
		return r;
	}

	lbnpbn_value.pbn = *pbn_new;
	calc_tsc(dc, PERIOD_IO, PERIOD_START);
	do_io(dc, bio, *pbn_new);
	calc_tsc(dc, PERIOD_IO, PERIOD_END);

	calc_tsc(dc, PERIOD_L2P, PERIOD_START);
	r = dc->kvs_lbn_pbn->kvs_insert(dc->kvs_lbn_pbn, (void *)&lbn,
					sizeof(lbn), (void *)&lbnpbn_value,
					sizeof(lbnpbn_value));
	calc_tsc(dc, PERIOD_L2P, PERIOD_END);
	calc_tsc(dc, PERIOD_REF, PERIOD_START);
	if (r < 0)
		dc->mdops->dec_refcount(dc->bmd, *pbn_new);
	calc_tsc(dc, PERIOD_REF, PERIOD_END);
	return r;
}

/*
 * Internal function to handle write when lbn-pbn entry is not
 * present. It creates a new lbn-pbn mapping and insert given
 * hash for this new pbn in hash-pbn mapping. Then increments
 * refcount for this new pbn.
 *
 * Returns -ERR code on failure.
 * Returns 0 on success.
 */
static int __handle_no_lbn_pbn(struct dedup_config *dc,
			       struct bio *bio, uint64_t lbn, u8 *hash)
{
	int r, ret;
	u64 pbn_new;
	struct hash_pbn_value hashpbn_value;

	/* Create a new lbn-pbn mapping for given lbn */
	r = alloc_pbnblk_and_insert_lbn_pbn(dc, &pbn_new, bio, lbn);
	if (r < 0)
		goto out;

	calc_tsc(dc, PERIOD_FP, PERIOD_START);
	/* Inserts new hash-pbn mapping for given hash. */
	hashpbn_value.pbn = pbn_new;
	r = dc->kvs_hash_pbn->kvs_insert(dc->kvs_hash_pbn, (void *)hash,
					 dc->crypto_key_size,
					 (void *)&hashpbn_value,
					 sizeof(hashpbn_value));
	calc_tsc(dc, PERIOD_FP, PERIOD_END);
	if (r < 0)
		goto kvs_insert_err;

	calc_tsc(dc, PERIOD_REF, PERIOD_START);
	/* Increments refcount for new pbn entry created. */
	r = dc->mdops->inc_refcount(dc->bmd, pbn_new);
	calc_tsc(dc, PERIOD_REF, PERIOD_END);
	if (r < 0)
		goto inc_refcount_err;

	/* On all successful steps increment new write count. */
	dc->newwrites++;
	goto out;

/* Error handling code path */
inc_refcount_err:
	/* Undo actions taken in hash-pbn kvs insert. */
	ret = dc->kvs_hash_pbn->kvs_delete(dc->kvs_hash_pbn,
					   (void *)hash, dc->crypto_key_size);
	if (ret < 0)
		DMERR("Error in deleting previously created hash pbn entry.");
kvs_insert_err:
	/* Undo actions taken in alloc_pbnblk_and_insert_lbn_pbn. */
	ret = dc->kvs_lbn_pbn->kvs_delete(dc->kvs_lbn_pbn,
					  (void *)&lbn, sizeof(lbn));
	if (ret < 0)
		DMERR("Error in deleting previously created lbn pbn entry.");
	ret = dc->mdops->dec_refcount(dc->bmd, pbn_new);
	if (ret < 0)
		DMERR("ERROR in decrementing previously incremented refcount.");
out:
	return r;
}

/*
 * Internal function to handle write when lbn-pbn mapping is present.
 * It creates a block for new pbn and inserts lbn-pbn(new) mapping.
 * Decrements old pbn refcount and inserts new hash-pbn entry followed
 * by incrementing refcount of new pbn.
 *
 * Returns -ERR code on failure.
 * Returns 0 on success.
 */
static int __handle_has_lbn_pbn(struct dedup_config *dc,
				struct bio *bio, uint64_t lbn, u8 *hash,
				u64 pbn_old)
{
	int r, ret;
	u64 pbn_new;
	struct hash_pbn_value hashpbn_value;

	/* Allocates a new block for new pbn and inserts lbn-pbn lapping. */
	r = alloc_pbnblk_and_insert_lbn_pbn(dc, &pbn_new, bio, lbn);
	if (r < 0)
		goto out;

	calc_tsc(dc, PERIOD_FP, PERIOD_START);
	/* Inserts new hash-pbn entry for given hash. */
	hashpbn_value.pbn = pbn_new;
	r = dc->kvs_hash_pbn->kvs_insert(dc->kvs_hash_pbn, (void *)hash,
					 dc->crypto_key_size,
					 (void *)&hashpbn_value,
					 sizeof(hashpbn_value));
	calc_tsc(dc, PERIOD_FP, PERIOD_END);
	if (r < 0)
		goto kvs_insert_err;

	calc_tsc(dc, PERIOD_REF, PERIOD_START);
	/* Increments refcount of new pbn. */
	r = dc->mdops->inc_refcount(dc->bmd, pbn_new);
	if (r < 0)
		goto inc_refcount_err;

	/* Decrements refcount for old pbn and decrement logical block cnt. */
	r = dc->mdops->dec_refcount(dc->bmd, pbn_old);
	if (r < 0)
		goto dec_refcount_err;
	dc->logical_block_counter--;
	if(1 == dc->mdops->get_refcount(dc->bmd, pbn_old))
		dc->invalid_fp++;
	calc_tsc(dc, PERIOD_REF, PERIOD_END);

	/* On all successful steps increment overwrite count. */
	dc->overwrites++;
	goto out;

/* Error handling code path. */
dec_refcount_err:
	/* Undo actions taken while incrementing refcount of new pbn. */
	ret = dc->mdops->dec_refcount(dc->bmd, pbn_new);
	if (ret < 0)
		DMERR("Error in decrementing previously incremented refcount.");

inc_refcount_err:
	ret = dc->kvs_hash_pbn->kvs_delete(dc->kvs_hash_pbn, (void *)hash,
					   dc->crypto_key_size);
	if (ret < 0)
		DMERR("Error in deleting previously inserted hash pbn entry");

kvs_insert_err:
	/* Undo actions taken in alloc_pbnblk_and_insert_lbn_pbn. */
	ret = dc->kvs_lbn_pbn->kvs_delete(dc->kvs_lbn_pbn, (void *)&lbn,
					  sizeof(lbn));
	if (ret < 0)
		DMERR("Error in deleting previously created lbn pbn entry");

	ret = dc->mdops->dec_refcount(dc->bmd, pbn_new);
	if (ret < 0)
		DMERR("ERROR in decrementing previously incremented refcount.");
out:
	return r;
}

static int issue_discard(struct dedup_config *dc, u64 lpn, int id);
static int issue_remap(struct dedup_config *dc, u64 lpn1, u64 lpn2, int last);
/*
 * Handles write io when Hash->PBN entry is not found.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int handle_write_no_hash_xremap(struct dedup_config *dc,
				struct bio *bio, uint64_t lbn, u8 *hash)
{
	int r,ref, t;
	t_v tv;
	u64 lbn2;
	struct hash_pbn_value_x hashpbn_value_x;
	t = calculate_tarSSD(dc, lbn);
	ref = dc->mdops->get_refcount(dc->bmd, lbn);
	tv.type = (ref & TV_TYPE) != 0;
	tv.ver = (ref & TV_VER);
	
	if(tv.type == 1){
		if(t != tv.ver) { // used mapped to another device
			lbn2 = remap_tarSSD(dc, lbn, t, tv.ver);
			calc_tsc(dc, PERIOD_IO, PERIOD_START);
            queue_push(dc, 0, lbn2, t, 0);
			calc_tsc(dc, PERIOD_IO, PERIOD_END);
			bio->bi_iter.bi_ssdno = t;
		}
		r = dc->mdops->set_refcount(dc->bmd, lbn, 0);
	}

	if(tv.type == 0 && tv.ver < TV_MAX)
		r = dc->mdops->inc_refcount(dc->bmd, lbn);
	r = dc->mdops->get_refcount(dc->bmd, lbn);
	tv.type = (r & TV_TYPE) != 0;
	tv.ver = (r & TV_VER);

	/*if(tv.ver >= TV_MAX) {
		dc->gc_needed = 1;
	}*/
	
	hashpbn_value_x.tv.type = tv.type;
	hashpbn_value_x.tv.ver = tv.ver;
	hashpbn_value_x.pbn = lbn;
	calc_tsc(dc, PERIOD_FP, PERIOD_START);
	r = dc->kvs_hash_pbn->kvs_delete(dc->kvs_hash_pbn, (void *)hash, dc->crypto_key_size);
	r = dc->kvs_hash_pbn->kvs_insert(dc->kvs_hash_pbn, (void *)hash,
					 dc->crypto_key_size,
					 (void *)&hashpbn_value_x,
					 sizeof(hashpbn_value_x));
	calc_tsc(dc, PERIOD_FP, PERIOD_END);
	if (r < 0)
		return r;
	calc_tsc(dc, PERIOD_IO, PERIOD_START);
	do_io_remap_device(dc, bio);
	calc_tsc(dc, PERIOD_IO, PERIOD_END);
	if (dc->enable_time_stats)
		dc->uniqwrites++;
	return r;
}

/*
 * Handles write io when Hash->PBN entry is not found.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int handle_write_no_hash(struct dedup_config *dc,
				struct bio *bio, uint64_t lbn, u8 *hash)
{
	int r;
	u32 vsize;
	struct lbn_pbn_value lbnpbn_value;

	calc_tsc(dc, PERIOD_L2P, PERIOD_START);
	r = dc->kvs_lbn_pbn->kvs_lookup(dc->kvs_lbn_pbn, (void *)&lbn,
					sizeof(lbn), (void *)&lbnpbn_value,
					&vsize);
	calc_tsc(dc, PERIOD_L2P, PERIOD_END);
	if (r == -ENODATA) {
		/* No LBN->PBN mapping entry */
		r = __handle_no_lbn_pbn(dc, bio, lbn, hash);
	} else if (r == 0) {
		/* LBN->PBN mappings exist */
		r = __handle_has_lbn_pbn(dc, bio, lbn, hash, lbnpbn_value.pbn);
	}
	if (r == 0 && dc->enable_time_stats)
		dc->uniqwrites++;
	return r;
}

/*
 * Internal function to handle write when hash-pbn entry is present,
 * but lbn-pbn entry is not present.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int __handle_no_lbn_pbn_with_hash(struct dedup_config *dc,
					 struct bio *bio, uint64_t lbn,
					 u64 pbn_this,
					 struct lbn_pbn_value lbnpbn_value)
{
	int r = 0, ret;

	calc_tsc(dc, PERIOD_REF, PERIOD_START);
	/* Increments refcount of this passed pbn */
	r = dc->mdops->inc_refcount(dc->bmd, pbn_this);
	if (r < 0)
		goto out;
	if(2 == dc->mdops->get_refcount(dc->bmd, pbn_this)){
		dc->invalid_fp--;
	}
	calc_tsc(dc, PERIOD_REF, PERIOD_END);

	lbnpbn_value.pbn = pbn_this;

	calc_tsc(dc, PERIOD_L2P, PERIOD_START);
	/* Insert lbn->pbn_this entry */
	r = dc->kvs_lbn_pbn->kvs_insert(dc->kvs_lbn_pbn, (void *)&lbn,
					sizeof(lbn), (void *)&lbnpbn_value,
					sizeof(lbnpbn_value));
	calc_tsc(dc, PERIOD_L2P, PERIOD_END);
	if (r < 0)
		goto kvs_insert_error;

	dc->logical_block_counter++;

	bio->bi_status = BLK_STS_OK;
	bio_endio(bio);
	dc->newwrites++;
	goto out;

kvs_insert_error:
	/* Undo actions taken while incrementing refcount of this pbn. */
	ret = dc->mdops->dec_refcount(dc->bmd, pbn_this);
	if (ret < 0)
		DMERR("Error in decrementing previously incremented refcount.");
out:
	return r;
}

/*
 * Internal function to handle write when both hash-pbn entry and lbn-pbn
 * entry is present.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int __handle_has_lbn_pbn_with_hash(struct dedup_config *dc,
					  struct bio *bio, uint64_t lbn,
					  u64 pbn_this,
					  struct lbn_pbn_value lbnpbn_value)
{
	int r = 0, ret;
	struct lbn_pbn_value this_lbnpbn_value;
	u64 pbn_old;

	pbn_old = lbnpbn_value.pbn;

	/* special case, overwrite same LBN/PBN with same data */
	if (pbn_this == pbn_old)
		goto out;

	calc_tsc(dc, PERIOD_REF, PERIOD_START);
	/* Increments refcount of this passed pbn */
	r = dc->mdops->inc_refcount(dc->bmd, pbn_this);
	calc_tsc(dc, PERIOD_REF, PERIOD_END);
	if (r < 0)
		goto out;

	this_lbnpbn_value.pbn = pbn_this;

	calc_tsc(dc, PERIOD_L2P, PERIOD_START);
	/* Insert lbn->pbn_this entry */
	r = dc->kvs_lbn_pbn->kvs_insert(dc->kvs_lbn_pbn, (void *)&lbn,
					sizeof(lbn),
					(void *)&this_lbnpbn_value,
					sizeof(this_lbnpbn_value));
	calc_tsc(dc, PERIOD_L2P, PERIOD_END);
	if (r < 0)
		goto kvs_insert_err;

	calc_tsc(dc, PERIOD_REF, PERIOD_START);
	/* Decrement refcount of old pbn */
	r = dc->mdops->dec_refcount(dc->bmd, pbn_old);
	if (r < 0)
		goto dec_refcount_err;
	if(1 == dc->mdops->get_refcount(dc->bmd, pbn_old))
		dc->invalid_fp++;
	calc_tsc(dc, PERIOD_REF, PERIOD_END);

	goto out;	/* all OK */

dec_refcount_err:
	/* Undo actions taken while decrementing refcount of old pbn */
	/* Overwrite lbn->pbn_this entry with lbn->pbn_old entry */
	ret = dc->kvs_lbn_pbn->kvs_insert(dc->kvs_lbn_pbn, (void *)&lbn,
				    	  sizeof(lbn), (void *)&lbnpbn_value,
					  sizeof(lbnpbn_value));
	if (ret < 0)
		DMERR("Error in overwriting lbn->pbn_this [%llu] with"
		      " lbn-pbn_old entry [%llu].", this_lbnpbn_value.pbn,
		      lbnpbn_value.pbn);

kvs_insert_err:
	ret = dc->mdops->dec_refcount(dc->bmd, pbn_this);
	if (ret < 0)
		DMERR("Error in decrementing previously incremented refcount.");
out:
	if (r == 0) {
		bio->bi_status = BLK_STS_OK;
		bio_endio(bio);
		dc->overwrites++;
	}

	return r;
}

/*
 * Returns 1 on collision
 */
static int check_collision(struct dedup_config *dc, u64 lpn, int oldno) {
	//int i;
	t_v tv;
	u64 base = 0;
	u64 num = dc->pblocks;
	//u64 percent = 1;
	u64 len = dc->remote_len;
	int val = 0, tmp = 0;
	
	if(len == 0) {
		return (oldno != calculate_tarSSD(dc, lpn));
	}
	if(oldno == calculate_tarSSD(dc, lpn)) {
		return 0;
	}
	base = calculate_entry_offset(dc, lpn, oldno);
	if (dc->raid_mode) {
		int pd_idx, raid_disk;
		raid_disk = dc->ssd_num;
		pd_idx = (raid_disk - 1) - (base % raid_disk);
		if (pd_idx == oldno) {
			return 1;
		}
	}
	while (base < num)
	{
		if(base == lpn) {
			base += len;
			continue;
		}
			
		val = dc->mdops->get_refcount(dc->bmd, base);
		tv.type = (val & TV_TYPE) != 0;
		tv.ver = (val & TV_VER);
		tmp = calculate_tarSSD(dc, base);
		if((tv.type) && (tmp != tv.ver) && (oldno == tv.ver))
			return 1;
		base += len;
	}
	return 0;
}

/*
 * Handles write io when Hash->PBN entry is found.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int handle_write_with_hash_xremap(struct dedup_config *dc, struct bio *bio,
				  u64 lbn, u8 *final_hash,
				  struct hash_pbn_value_x hashpbn_value_x)
{
	int r, ref, tmp;
	t_v old_tv, cur_tv, lbn_tv;
	u32 val;
	//struct lbn_pbn_value lbnpbn_value;
	u64 pbn_this, lbn2;

	pbn_this = hashpbn_value_x.pbn;//lpn remap to
	old_tv.type = hashpbn_value_x.tv.type;
	old_tv.ver = hashpbn_value_x.tv.ver;
	
	ref = dc->mdops->get_refcount(dc->bmd, pbn_this);
	cur_tv.type = (ref & TV_TYPE) != 0;
	cur_tv.ver = (ref & TV_VER);
	
	ref = dc->mdops->get_refcount(dc->bmd, lbn);
	lbn_tv.type = (ref & TV_TYPE) != 0;
	lbn_tv.ver = (ref & TV_VER);
	
	if (cur_tv.type == 1 || cur_tv.ver >= TV_MAX || cur_tv.ver != old_tv.ver) {//fp is invalid
		//r = dc->kvs_hash_pbn->kvs_delete(dc->kvs_hash_pbn, (void *)final_hash, dc->crypto_key_size);
		r = handle_write_no_hash_xremap(dc, bio, lbn, final_hash);
		if(dc->enable_time_stats)
			dc->hit_wrong_fp++;
	} else {
		//Re-write the same data to the same pos
		if(pbn_this == lbn) {
			bio->bi_status = BLK_STS_OK;
			bio_endio(bio);
			if (dc->enable_time_stats) {
				dc->dupwrites++;
				dc->hit_right_fp++;
			}
			return 0;
		}
		
		cur_tv.type = 1;
		cur_tv.ver = calculate_tarSSD(dc, pbn_this);
		if(check_collision(dc, lbn, cur_tv.ver)){//collision happen
			//r = dc->kvs_hash_pbn->kvs_delete(dc->kvs_hash_pbn, (void *)final_hash, dc->crypto_key_size);
			r = handle_write_no_hash_xremap(dc, bio, lbn, final_hash);
			if(dc->enable_time_stats)
				dc->hit_corrupt_fp++;
			return r;
		}

		val = (cur_tv.type << TV_BIT) | cur_tv.ver;
		r = dc->mdops->set_refcount(dc->bmd, lbn, val);
		tmp = calculate_tarSSD(dc, lbn);
		
		calc_tsc(dc, PERIOD_IO, PERIOD_START);
		if(lbn_tv.type == 0) { //the lbn writed an unique data
			if(lbn_tv.ver > 0 &&  tmp != cur_tv.ver){// had data & updated to a different device
                queue_push(dc, 0, lbn, tmp, 0);
			}
            queue_push(dc, pbn_this, lbn, tmp, 1);
		}
		else {	// the lbn writed an duplicate data
			if(cur_tv.ver != lbn_tv.ver){ // updated to a different device
				lbn2 = remap_tarSSD(dc, lbn, tmp, lbn_tv.ver);
                queue_push(dc, 0, lbn2, tmp, 0);
			}
            queue_push(dc, pbn_this, lbn, lbn_tv.ver, 1);
		}
		calc_tsc(dc, PERIOD_IO, PERIOD_END);
		
		bio->bi_status = BLK_STS_OK;
		bio_endio(bio);
		if (dc->enable_time_stats)
			dc->dupwrites++;
		if(dc->enable_time_stats)
			dc->hit_right_fp++;
	}
	
	return r;
}

/*
 * Handles write io when Hash->PBN entry is found.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int handle_write_with_hash(struct dedup_config *dc, struct bio *bio,
				  u64 lbn, u8 *final_hash,
				  struct hash_pbn_value hashpbn_value)
{
	int r;
	u32 vsize;
	struct lbn_pbn_value lbnpbn_value;
	u64 pbn_this;

	pbn_this = hashpbn_value.pbn;
	calc_tsc(dc, PERIOD_L2P, PERIOD_START);
	r = dc->kvs_lbn_pbn->kvs_lookup(dc->kvs_lbn_pbn, (void *)&lbn,
					sizeof(lbn), (void *)&lbnpbn_value, &vsize);
	calc_tsc(dc, PERIOD_L2P, PERIOD_END);

	if (r == -ENODATA) {
		/* No LBN->PBN mapping entry */
		r = __handle_no_lbn_pbn_with_hash(dc, bio, lbn, pbn_this,
						  lbnpbn_value);
	} else if (r == 0) {
		/* LBN->PBN mapping entry exists */
		r = __handle_has_lbn_pbn_with_hash(dc, bio, lbn, pbn_this,
						   lbnpbn_value);
	}
	if (r == 0 && dc->enable_time_stats)
		dc->dupwrites++;
	return r;
}

/*
 * Performs a lookup for Hash->PBN entry.
 * If entry is not found, it invokes handle_write_no_hash.
 * If entry is found, it invokes handle_write_with_hash.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int handle_write(struct dedup_config *dc, struct bio *bio)
{
	u64 lbn;
	u8 hash[MAX_DIGEST_SIZE];
	struct hash_pbn_value hashpbn_value;
	struct hash_pbn_value_x hashpbn_value_x;
	u32 vsize;
	struct bio *new_bio = NULL;
	int r;

	/* If there is a data corruption make the device read-only */
	if (dc->corrupted_blocks > dc->fec_fixed)
		return -EIO;

	dc->writes++;

	/* Read-on-write handling */
	if (bio->bi_iter.bi_size < dc->block_size) {
		dc->reads_on_writes++;
		new_bio = prepare_bio_on_write(dc, bio);
		if (!new_bio || IS_ERR(new_bio))
			return -ENOMEM;
		bio = new_bio;
	}

	lbn = bio_lbn(dc, bio);

	calc_tsc(dc, PERIOD_HASH, PERIOD_START);
	r = compute_hash_bio(dc->desc_table, bio, hash);
	calc_tsc(dc, PERIOD_HASH, PERIOD_END);
	if (r)
		return r;
	calc_tsc(dc, PERIOD_FP, PERIOD_START);
	if (!strcmp(dc->backend_str, "xremap"))
		r = dc->kvs_hash_pbn->kvs_lookup(dc->kvs_hash_pbn, hash,
					 dc->crypto_key_size,
					 &hashpbn_value_x, &vsize);
	else
		r = dc->kvs_hash_pbn->kvs_lookup(dc->kvs_hash_pbn, hash,
					 dc->crypto_key_size,
					 &hashpbn_value, &vsize);
	calc_tsc(dc, PERIOD_FP, PERIOD_END);
	dc->gc_needed = 0;

	if (r == -ENODATA) {
		if(dc->enable_time_stats)
			dc->hit_none_fp++;
		dc->inserted_fp++;
		if (!strcmp(dc->backend_str, "xremap"))
			r = handle_write_no_hash_xremap(dc, bio, lbn, hash);
		else
			r = handle_write_no_hash(dc, bio, lbn, hash);
	}
	else if (r == 0) {
		if (!strcmp(dc->backend_str, "xremap"))
			r = handle_write_with_hash_xremap(dc, bio, lbn, hash,
					   hashpbn_value_x);
		else
			r = handle_write_with_hash(dc, bio, lbn, hash,
					   hashpbn_value);
	}
	if (r < 0)
		return r;

	if (!strcmp(dc->backend_str, "xremap")) {
		if(dc->gc_needed || dc->inserted_fp >= dc->gc_threhold) {
			r = garbage_collect(dc);
			calc_tsc(dc, PERIOD_FUA, PERIOD_START);
			r = dc->mdops->flush_meta(dc->bmd);
			calc_tsc(dc, PERIOD_FUA, PERIOD_END);
			DMINFO("garbage_collect is trigged. gc_needed = %llu, lbn = %llu", dc->gc_needed, lbn);
			dc->writes_after_flush = 0;
			dc->gc_needed = 0;
			return 0;
		}
	} else {
		if(dc->invalid_fp >= dc->gc_threhold) {
			r = garbage_collect(dc);
			calc_tsc(dc, PERIOD_FUA, PERIOD_START);
			r = dc->mdops->flush_meta(dc->bmd);
			calc_tsc(dc, PERIOD_FUA, PERIOD_END);
			DMINFO("garbage_collect is trigged.");
			dc->writes_after_flush = 0;
			return 0;
		}
	}
	
	dc->writes_after_flush++;
	if ((dc->flushrq > 0 && dc->writes_after_flush >= dc->flushrq) ||
	    (bio->bi_opf & (REQ_PREFLUSH | REQ_FUA))) {
		calc_tsc(dc, PERIOD_FUA, PERIOD_START);
		r = dc->mdops->flush_meta(dc->bmd);
		calc_tsc(dc, PERIOD_FUA, PERIOD_END);
		if (r < 0)
			return r;
		dc->writes_after_flush = 0;
	} else if(dc->flushrq < 0) {
		int flushrq = - dc->flushrq;
		unsigned long x, t = read_tsc();
		x = (t - dc->time_last_flush) / 1900000;
		if(x >= flushrq) {
			calc_tsc(dc, PERIOD_FUA, PERIOD_START);
			r = dc->mdops->flush_meta(dc->bmd);
			calc_tsc(dc, PERIOD_FUA, PERIOD_END);
			if (r < 0)
				return r;
			dc->writes_after_flush = 0;
			dc->time_last_flush = t;
		}
	}

	return 0;
}

/*
 * Handles discard request by clearing LBN-PBN mapping and
 * decrementing refcount of pbn. If refcount reaches one that
 * means only hash-pbn mapping is present which will be cleaned
 * up at garbage collection time.
 *
 * Returns -ERR on failure
 * Returns 0 on success
 */
static int handle_discard(struct dedup_config *dc, struct bio *bio)
{
	u64 lbn, pbn_val;
	u32 vsize;
	struct lbn_pbn_value lbnpbn_value;
	int r, ret;

	lbn = bio_lbn(dc, bio);
	DMWARN("Discard request received for LBN :%llu", lbn);

	/* Get the pbn from LBN->PBN store for requested LBN. */
	r = dc->kvs_lbn_pbn->kvs_lookup(dc->kvs_lbn_pbn, (void *)&lbn,
					sizeof(lbn), (void *)&lbnpbn_value,
					&vsize);
	if (r == -ENODATA) {
		/*
 		 * Entry not present in LBN->PBN store hence need to forward
 		 * the discard request to underlying block layer without
 		 * remapping with pbn.
 		 */
		DMWARN("Discard request received for lbn [%llu] whose LBN-PBN entry"
		" is not present.", lbn);
		do_io_remap_device(dc, bio);
		goto out;
	}
	if (r < 0)
		goto out;

	/* Entry found in the LBN->PBN store */
	pbn_val = lbnpbn_value.pbn;

	/*
	 * Decrement pbn's refcount. If the refcount reaches one then forward discard
	 * request to underlying block device.
	 */
	if (dc->mdops->get_refcount(dc->bmd, pbn_val) > 1) {
		r = dc->kvs_lbn_pbn->kvs_delete(dc->kvs_lbn_pbn,
						(void *)&lbn,
						sizeof(lbn));
		if (r < 0) {
			DMERR("Failed to delete LBN-PBN entry for pbn_val :%llu",
				pbn_val);
			goto out;
		}
		r = dc->mdops->dec_refcount(dc->bmd, pbn_val);
		if (r < 0) {
			/*
 			 * If could not decrement refcount then need to revert
 			 * above deletion of lbn-pbn mapping.
 			 */
			ret = dc->kvs_lbn_pbn->kvs_insert(dc->kvs_lbn_pbn,
							(void *)&lbn,
							sizeof(lbn),
							(void *)&lbnpbn_value,
							sizeof(lbnpbn_value));
			goto out;
		}

		dc->physical_block_counter -= 1;
	}
	/*
 	 * If refcount reaches 1 then forward discard request to underlying
 	 * block layer else end bio request.
 	 */
	if (dc->mdops->get_refcount(dc->bmd, pbn_val) == 1) {
		do_io(dc, bio, pbn_val);
	} else {
		bio->bi_status = BLK_STS_OK;
		bio_endio(bio);
	}
out:
	return r;
}

/*
 * Processes block io requests and propagates negative error
 * code to block io status (BLK_STS_*).
 */
static void process_bio(struct dedup_config *dc, struct bio *bio)
{
	int r;

	if(dc->enable_time_stats) {
		dc->usr_total_cnt += 1;
	}
	if (bio->bi_opf & (REQ_PREFLUSH | REQ_FUA) && !bio_sectors(bio)) {
		calc_tsc(dc, PERIOD_FUA, PERIOD_START);
		r = dc->mdops->flush_meta(dc->bmd);
		calc_tsc(dc, PERIOD_FUA, PERIOD_END);
		if (r == 0)
			dc->writes_after_flush = 0;
		do_io_remap_device(dc, bio);
		return;
	}
	if (bio_op(bio) == REQ_OP_DISCARD) {
		//DMINFO("DISCA:[bi_sector=0x%llx][bi_size=%u][bi_vcnt=%hu]", (unsigned long long)bio->bi_iter.bi_sector, bio->bi_iter.bi_size, bio->bi_vcnt);
		r = handle_discard(dc, bio);
		return;
	}

	switch (bio_data_dir(bio)) {
	case READ:
		//DMINFO("READ :[bi_sector=0x%llx][bi_size=%u][bi_vcnt=%hu][blk_name=%s]", (unsigned long long)bio->bi_iter.bi_sector, bio->bi_iter.bi_size, bio->bi_vcnt, bio->bi_disk->disk_name);
		//for (i = 0; i < bio->bi_vcnt; ++i)
		//    DMINFO("[bv_page=0x%p][bv_len=%u][bv_offset=%u]", bio->bi_io_vec[i].bv_page, bio->bi_io_vec[i].bv_len, bio->bi_io_vec[i].bv_offset);
		//DMINFO("\n");
		calc_tsc(dc, PERIOD_READ, PERIOD_START);
		if (!strcmp(dc->backend_str, "xremap")) {
			r = handle_read_xremap(dc, bio);
		}
		else
			r = handle_read(dc, bio);
		calc_tsc(dc, PERIOD_READ, PERIOD_END);
		if(dc->enable_time_stats) {
			dc->usr_reads_cnt += 1;
		}
		break;
	case WRITE:
		//DMINFO("WRITE:[bi_sector=0x%llx][bi_size=%u][bi_vcnt=%hu]", (unsigned long long)bio->bi_iter.bi_sector, bio->bi_iter.bi_size, bio->bi_vcnt);
		//for (i = 0; i < bio->bi_vcnt; ++i)
		//    DMINFO("[bv_page=0x%p][bv_len=%u][bv_offset=%u]", bio->bi_io_vec[i].bv_page, bio->bi_io_vec[i].bv_len, bio->bi_io_vec[i].bv_offset);
		//DMINFO("\n");
		calc_tsc(dc, PERIOD_WRITE, PERIOD_START);
		r = handle_write(dc, bio);
		calc_tsc(dc, PERIOD_WRITE, PERIOD_END);
		if(dc->enable_time_stats) {
			dc->usr_write_cnt += 1;
		}
	}

	if (r < 0) {
		switch (r) {
		case -EWOULDBLOCK:
			bio->bi_status = BLK_STS_AGAIN;
			break;
		case -EINVAL:
		case -EIO:
			bio->bi_status = BLK_STS_IOERR;
			break;
		case -ENODATA:
			bio->bi_status = BLK_STS_MEDIUM;
			break;
		case -ENOMEM:
			bio->bi_status = BLK_STS_RESOURCE;
			break;
		case -EPERM:
			bio->bi_status = BLK_STS_PROTECTION;
			break;
		}
		bio_endio(bio);
	}
}

/*
 * Main function for all work pool threads that process the block io
 * operation.
 */
static void do_work(struct work_struct *ws)
{
	struct dedup_work *data = container_of(ws, struct dedup_work, worker);
	struct dedup_config *dc = (struct dedup_config *)data->config;
	struct bio *bio = (struct bio *)data->bio;

	mempool_free(data, dc->dedup_work_pool);

	calc_tsc(dc, PERIOD_TOTAL, PERIOD_START);
	process_bio(dc, bio);
	calc_tsc(dc, PERIOD_TOTAL, PERIOD_END);
}

/*
 * Defers block io operations by enqueuing them in the work pool
 * queue.
 */
static void dedup_defer_bio(struct dedup_config *dc, struct bio *bio)
{
	struct dedup_work *data;

	data = mempool_alloc(dc->dedup_work_pool, GFP_NOIO);
	if (!data) {
		bio->bi_status = BLK_STS_RESOURCE;
		bio_endio(bio);
		return;
	}

	data->bio = bio;
	data->config = dc;

	INIT_WORK(&(data->worker), do_work);

	queue_work(dc->workqueue, &(data->worker));
}

/*
 * Wrapper function for dedup_defer_bio.
 *
 * Returns DM_MAPIO_SUBMITTED.
 */
static int dm_dedup_map(struct dm_target *ti, struct bio *bio)
{
	dedup_defer_bio(ti->private, bio);

	return DM_MAPIO_SUBMITTED;
}

static int handle_rd_request(void * para) {
    while(!kthread_should_stop()) {
        if(!queue_is_empty()) {
            struct rd_work rd_work;
            rd_work = queue_pop();
            if(rd_work.flag)
                issue_remap(rd_work.config, rd_work.pbn, rd_work.lbn, rd_work.temp);
            else 
                issue_discard(rd_work.config, rd_work.lbn, rd_work.temp);
        }
        else {
            msleep(100);
            cond_resched();
        }
    }
    return 0;
}

struct dedup_args {
	struct dm_target *ti;

	struct dm_dev *meta_dev;

	struct dm_dev *data_dev;
	u64 data_size;

	u32 block_size;

	char hash_algo[CRYPTO_ALG_NAME_LEN];

	enum backend backend;
	char backend_str[MAX_BACKEND_NAME_LEN];

	int flushrq;
	int gc_rate;

	u64 ssd_num;
	u64 collision_rate;
	u64 gc_size;
	u64 raid_mode;

	bool corruption_flag;
};

/*
 * Parses metadata device.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int parse_meta_dev(struct dedup_args *da, struct dm_arg_set *as,
			  char **err)
{
	int r;

	r = dm_get_device(da->ti, dm_shift_arg(as),
			  dm_table_get_mode(da->ti->table), &da->meta_dev);
	if (r)
		*err = "Error opening metadata device";

	return r;
}

/*
 * Parses data device.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int parse_data_dev(struct dedup_args *da, struct dm_arg_set *as,
			  char **err)
{
	int r;

	r = dm_get_device(da->ti, dm_shift_arg(as),
			  dm_table_get_mode(da->ti->table), &da->data_dev);
	if (r)
		*err = "Error opening data device";
	else
		da->data_size = i_size_read(da->data_dev->bdev->bd_inode);

	return r;
}

/*
 * Parses block size.
 *
 * Returns -EINVAL in failure.
 * Returns 0 on success.
 */
static int parse_block_size(struct dedup_args *da, struct dm_arg_set *as,
			    char **err)
{
	u32 block_size;

	if (kstrtou32(dm_shift_arg(as), 10, &block_size) ||
	    !block_size ||
		block_size < MIN_DATA_DEV_BLOCK_SIZE ||
		block_size > MAX_DATA_DEV_BLOCK_SIZE ||
		!is_power_of_2(block_size)) {
		*err = "Invalid data block size";
		return -EINVAL;
	}

	if (block_size > da->data_size) {
		*err = "Data block size is larger than the data device";
		return -EINVAL;
	}

	da->block_size = block_size;

	return 0;
}

/*
 * Checks for a recognized hash algorithm.
 *
 * Returns -EINVAL in failure.
 * Returns 0 on success.
 */
static int parse_hash_algo(struct dedup_args *da, struct dm_arg_set *as,
			   char **err)
{
	strlcpy(da->hash_algo, dm_shift_arg(as), CRYPTO_ALG_NAME_LEN);

	if (!crypto_has_alg(da->hash_algo, 0, CRYPTO_ALG_ASYNC)) {
		*err = "Unrecognized hash algorithm";
		return -EINVAL;
	}

	return 0;
}

/*
 * Checks for a supported metadata backend.
 *
 * Returns -EINVAL in failure.
 * Returns 0 on success.
 */
static int parse_backend(struct dedup_args *da, struct dm_arg_set *as,
			 char **err)
{
	char backend[MAX_BACKEND_NAME_LEN];

	strlcpy(backend, dm_shift_arg(as), MAX_BACKEND_NAME_LEN);

	if (!strcmp(backend, "inram")) {
		da->backend = BKND_INRAM;
	} else if (!strcmp(backend, "cowbtree")) {
		da->backend = BKND_COWBTREE;
	} else if (!strcmp(backend, "xremap")) {
		da->backend = BKND_XREMAP;
	} else if (!strcmp(backend, "hybrid")) {
		da->backend = BKND_HYBRID;
	} else {
		*err = "Unsupported metadata backend";
		return -EINVAL;
	}

	strlcpy(da->backend_str, backend, MAX_BACKEND_NAME_LEN);

	return 0;
}

/*
 * Checks for a valid flushrq value.
 *
 * Returns -EINVAL in failure.
 * Returns 0 on success.
 */
static int parse_flushrq(struct dedup_args *da, struct dm_arg_set *as,
			 char **err)
{
	if (kstrtoint(dm_shift_arg(as), 10, &da->flushrq)) {
		*err = "Invalid flushrq value";
		return -EINVAL;
	}

	return 0;
}

/*
 * Checks for a valid corruption flag value.
 *
 * Returns -EINVAL in failure.
 * Returns 0 on success.
 */
static int parse_corruption_flag(struct dedup_args *da, struct dm_arg_set *as,
			 char **err)
{
	bool corruption_flag;

        if (kstrtobool(dm_shift_arg(as), &corruption_flag)) {
                *err = "Invalid corruption flag value";
                return -EINVAL;
        }

        da->corruption_flag = corruption_flag;

        return 0;

}

/*
 * Checks for a valid gc rate.
 *
 * Returns -EINVAL in failure.
 * Returns 0 on success.
 */
static int parse_gc_rate(struct dedup_args *da, struct dm_arg_set *as,
			 char **err)
{
	if (kstrtoint(dm_shift_arg(as), 10, &da->gc_rate)) {
		*err = "Invalid gc rate";
		return -EINVAL;
	}

	return 0;
}

/*
 * Parses SSD number.
 *
 * Returns -EINVAL in failure.
 * Returns 0 on success.
 */
static int parse_ssd_num(struct dedup_args *da, struct dm_arg_set *as,
			    char **err)
{
	u32 ssd_num;
	if (kstrtou32(dm_shift_arg(as), 10, &ssd_num)) {
		*err = "Invalid SSD number";
		return -EINVAL;
	}
	da->ssd_num = ssd_num;
	return 0;
}

/*
 * Parses Collision Rate
 *
 * Returns -EINVAL in failure.
 * Returns 0 on success.
 */
static int parse_collision_rate(struct dedup_args *da, struct dm_arg_set *as,
			    char **err)
{
	u32 collision_rate;
	if (kstrtou32(dm_shift_arg(as), 10, &collision_rate)) {
		*err = "Invalid collision rate";
		return -EINVAL;
	}
	da->collision_rate = collision_rate;
	return 0;
}

/*
 * Parses GC Size
 *
 * Returns -EINVAL in failure.
 * Returns 0 on success.
 */
static int parse_gc_size(struct dedup_args *da, struct dm_arg_set *as,
			    char **err)
{
	u32 gc_size;
	if (kstrtou32(dm_shift_arg(as), 10, &gc_size)) {
		*err = "Invalid collision rate";
		return -EINVAL;
	}
	da->gc_size = gc_size;
	return 0;
}

/*
 * Parses Raid Mode
 *
 * Returns -EINVAL in failure.
 * Returns 0 on success.
 */
static int parse_raid_mode(struct dedup_args *da, struct dm_arg_set *as,
			    char **err)
{
	u32 raid_mode;
	if (kstrtou32(dm_shift_arg(as), 10, &raid_mode)) {
		*err = "Invalid collision rate";
		return -EINVAL;
	}
	da->raid_mode = raid_mode;
	return 0;
}

/*
 * Wrapper function for all parse functions.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int parse_dedup_args(struct dedup_args *da, int argc,
			    char **argv, char **err)
{
	struct dm_arg_set as;
	int r;

	if (argc < 12) {
		*err = "Insufficient args";
		return -EINVAL;
	}

	if (argc > 12) {
		*err = "Too many args";
		return -EINVAL;
	}

	as.argc = argc;
	as.argv = argv;

	r = parse_meta_dev(da, &as, err);
	if (r)
		return r;

	r = parse_data_dev(da, &as, err);
	if (r)
		return r;

	r = parse_block_size(da, &as, err);
	if (r)
		return r;

	r = parse_hash_algo(da, &as, err);
	if (r)
		return r;

	r = parse_backend(da, &as, err);
	if (r)
		return r;

	r = parse_flushrq(da, &as, err);
	if (r)
		return r;

	r = parse_corruption_flag(da, &as, err);
	if (r)
		return r;

	r = parse_gc_rate(da, &as, err);
	if (r)
		return r;

	r = parse_ssd_num(da, &as, err);
	if (r)
		return r;

	r = parse_collision_rate(da, &as, err);
	if (r)
		return r;

	r = parse_gc_size(da, &as, err);
	if (r)
		return r;

	r = parse_raid_mode(da, &as, err);
	if (r)
		return r;

	return 0;
}

/*
 * Decrements metadata and data device's use count
 * and removes them if necessary.
 */
static void destroy_dedup_args(struct dedup_args *da)
{
	if (da->meta_dev)
		dm_put_device(da->ti, da->meta_dev);

	if (da->data_dev)
		dm_put_device(da->ti, da->data_dev);
}

/*
 * Dmdedup constructor.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int dm_dedup_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
	struct dedup_args da;
	struct dedup_config *dc;
	struct workqueue_struct *wq;

	struct init_param_inram iparam_inram;
	struct init_param_cowbtree iparam_cowbtree;
	struct init_param_xremap iparam_xremap;
	struct init_param_hybrid iparam_hybrid;
	void *iparam = NULL;
	struct metadata *md = NULL;

	sector_t data_size;
	int r, i;
	int crypto_key_size;

	struct on_disk_stats d;
	struct on_disk_stats *data = &d;
	u64 logical_block_counter = 0;
	u64 physical_block_counter = 0;

	mempool_t *dedup_work_pool = NULL;
	mempool_t *check_work_pool = NULL;

	bool unformatted;

	memset(&da, 0, sizeof(struct dedup_args));
	da.ti = ti;

	r = parse_dedup_args(&da, argc, argv, &ti->error);
	if (r)
		goto out;

	dc = kzalloc(sizeof(*dc), GFP_KERNEL);
	if (!dc) {
		ti->error = "Error allocating memory for dedup config";
		r = -ENOMEM;
		goto out;
	}

	/* Do we need to add BIOSET_NEED_RESCURE in the flags passed in bioset_create as well? */
	r = bioset_init(&dc->bs, MIN_IOS, 0, BIOSET_NEED_BVECS);
	if (r) {
		ti->error = "failed to create bioset";
		r = -ENOMEM;
		goto bad_bs;
	}

	wq = create_singlethread_workqueue("dm-dedup");
	if (!wq) {
		ti->error = "failed to create workqueue";
		r = -ENOMEM;
		goto bad_bs;
	}

    mutex_init(&queue_lock);
    init_queue();

    dc->rd = kthread_run(handle_rd_request, NULL, "dm-dedup-rd");

	dedup_work_pool = mempool_create_kmalloc_pool(MIN_DEDUP_WORK_IO,
						      sizeof(struct dedup_work));

	if (!dedup_work_pool) {
		ti->error = "failed to create dedup mempool";
		r = -ENOMEM;
		goto bad_dedup_mempool;
	}

	check_work_pool = mempool_create_kmalloc_pool(MIN_DEDUP_WORK_IO,
						sizeof(struct check_work));

	if (!check_work_pool) {
		ti->error = "failed to create fec mempool";
		r = -ENOMEM;
		goto bad_check_mempool;
	}

	dc->io_client = dm_io_client_create();
	if (IS_ERR(dc->io_client)) {
		ti->error = "failed to create dm_io_client";
		r = PTR_ERR(dc->io_client);
		goto bad_io_client;
	}
	

	dc->block_size = da.block_size;
	dc->sectors_per_block = to_sector(da.block_size);
	data_size = ti->len;
	(void)sector_div(data_size, dc->sectors_per_block);
	dc->lblocks = data_size;
	dc->raid_mode = da.raid_mode;
	dc->raid_id = (dc->raid_mode) ? -2 : -1;

	data_size = i_size_read(da.data_dev->bdev->bd_inode) >> SECTOR_SHIFT;
	(void)sector_div(data_size, dc->sectors_per_block);
	dc->pblocks = data_size;

	/* Meta-data backend specific part */
	switch (da.backend) {
	case BKND_INRAM:
		dc->mdops = &metadata_ops_inram;
		iparam_inram.blocks = dc->pblocks;
		iparam = &iparam_inram;
		break;
	case BKND_COWBTREE:
		dc->mdops = &metadata_ops_cowbtree;
		iparam_cowbtree.blocks = dc->pblocks;
		iparam_cowbtree.metadata_bdev = da.meta_dev->bdev;
		iparam = &iparam_cowbtree;
		break;
	case BKND_XREMAP:
		dc->mdops = &metadata_ops_xremap;
		iparam_xremap.blocks = dc->pblocks;
		iparam_xremap.metadata_bdev = da.meta_dev->bdev;
		iparam = &iparam_xremap;
		break;
	case BKND_HYBRID:
		dc->mdops = &metadata_ops_hybrid;
		iparam_hybrid.blocks = dc->pblocks;
		iparam_hybrid.metadata_bdev = da.meta_dev->bdev;
		iparam = &iparam_hybrid;
		break;
	}

	strcpy(dc->backend_str, da.backend_str);

	md = dc->mdops->init_meta(iparam, &unformatted);
	if (IS_ERR(md)) {
		ti->error = "failed to initialize backend metadata";
		r = PTR_ERR(md);
		goto bad_metadata_init;
	}

	dc->desc_table = desc_table_init(da.hash_algo);
	if (IS_ERR(dc->desc_table)) {
		ti->error = "failed to initialize crypto API";
		r = PTR_ERR(dc->desc_table);
		goto bad_metadata_init;
	}

	crypto_key_size = get_hash_digestsize(dc->desc_table);

	switch (da.backend) {
		case BKND_XREMAP:
			dc->kvs_hash_pbn = dc->mdops->kvs_create_sparse(md, crypto_key_size,
				sizeof(struct hash_pbn_value_x),
				dc->pblocks, unformatted, 0);
			break;
		default:
			dc->kvs_hash_pbn = dc->mdops->kvs_create_sparse(md, crypto_key_size,
				sizeof(struct hash_pbn_value),
				dc->pblocks, unformatted, 0);
	}
	dc->gc_threhold = 1ULL * dc->pblocks * da.gc_rate / 100;
	if (IS_ERR(dc->kvs_hash_pbn)) {
		ti->error = "failed to create sparse KVS";
		r = PTR_ERR(dc->kvs_hash_pbn);
		goto bad_kvstore_init;
	}

	dc->kvs_lbn_pbn = dc->mdops->kvs_create_linear(md, 8,
			sizeof(struct lbn_pbn_value), dc->lblocks, unformatted);
	if (IS_ERR(dc->kvs_lbn_pbn)) {
		ti->error = "failed to create linear KVS";
		r = PTR_ERR(dc->kvs_lbn_pbn);
		goto bad_kvstore_init;
	}

	r = dc->mdops->flush_meta(md);
	if (r < 0) {
		ti->error = "failed to flush metadata";
		goto bad_kvstore_init;
	}

	if (!unformatted && dc->mdops->get_private_data) {
		r = dc->mdops->get_private_data(md, (void **)&data,
				sizeof(struct on_disk_stats));
		if (r < 0) {
			ti->error = "failed to get private data from superblock";
			goto bad_kvstore_init;
		}

		logical_block_counter = data->logical_block_counter;
		physical_block_counter = data->physical_block_counter;
	}

	dc->data_dev = da.data_dev;
	dc->metadata_dev = da.meta_dev;

	dc->workqueue = wq;
	dc->dedup_work_pool = dedup_work_pool;
	dc->check_work_pool = check_work_pool;

	dc->bmd = md;

	dc->logical_block_counter = logical_block_counter;
	dc->physical_block_counter = physical_block_counter;

	dc->gc_counter = 0;
	dc->writes = 0;
	dc->dupwrites = 0;
	dc->uniqwrites = 0;
	dc->reads_on_writes = 0;
	dc->overwrites = 0;
	dc->newwrites = 0;
	dc->gc_count = 0;
	dc->gc_fp_count = 0;
	dc->gc_cur_size = 0;
	dc->gc_last_fp = 0;
	dc->hit_none_fp = 0;
	dc->hit_right_fp = 0;
	dc->hit_wrong_fp = 0;
	dc->hit_corrupt_fp = 0;
	dc->usr_total_cnt = 0;
	dc->usr_write_cnt = 0;
	dc->usr_reads_cnt = 0;

	dc->gc_type = 0;
	dc->invalid_fp = 0;
	dc->inserted_fp = 0;

	dc->ssd_num = da.ssd_num;
	dc->remote_len = dc->pblocks * da.collision_rate / (da.ssd_num * 100);
	dc->gc_size = da.gc_size;

	dc->enable_time_stats = 0;
	for(i = 0; i < PREIOD_NUM; ++i) {
		dc->tmp_period_time[i] = 0;
		dc->total_period_time[i] = 0;
	}
	dc->check_corruption = da.corruption_flag;
	dc->fec = false;
	dc->fec_fixed = 0;
	dc->corrupted_blocks = 0;

	strcpy(dc->crypto_alg, da.hash_algo);
	dc->crypto_key_size = crypto_key_size;

	dc->flushrq = da.flushrq;
	dc->writes_after_flush = 0;
	dc->time_last_flush = read_tsc();

	r = dm_set_target_max_io_len(ti, dc->sectors_per_block);
	if (r)
		goto bad_kvstore_init;

	ti->num_flush_bios = 1;
	ti->flush_supported = true;
	ti->discards_supported = true;
	ti->num_discard_bios = 1;
	ti->private = dc;
	for(i = 0; i < dc->ssd_num; ++i) {
		blkdev_issue_discard(dc->data_dev->bdev, i*8, 8, GFP_NOIO, 0, dc->raid_id, dc->remote_len);
	}
	return 0;

bad_kvstore_init:
	desc_table_deinit(dc->desc_table);
bad_metadata_init:
	if (md && !IS_ERR(md))
		dc->mdops->exit_meta(md);
	dm_io_client_destroy(dc->io_client);
bad_io_client:
	mempool_destroy(check_work_pool);
bad_check_mempool:
	mempool_destroy(dedup_work_pool);
bad_dedup_mempool:
	destroy_workqueue(wq);
bad_bs:
	kfree(dc);
out:
	destroy_dedup_args(&da);
	return r;
}


/* Dmdedup destructor. */
static void dm_dedup_dtr(struct dm_target *ti)
{
	struct dedup_config *dc = ti->private;
	struct on_disk_stats data;
	int ret;

	if (dc->mdops->set_private_data) {
		data.physical_block_counter = dc->physical_block_counter;
		data.logical_block_counter = dc->logical_block_counter;

		ret = dc->mdops->set_private_data(dc->bmd, &data,
				sizeof(struct on_disk_stats));
		if (ret < 0)
			DMERR("Failed to set the private data in superblock.");
	}

	ret = dc->mdops->flush_meta(dc->bmd);
	if (ret < 0)
		DMERR("Failed to flush the metadata to disk.");

	flush_workqueue(dc->workqueue);
	destroy_workqueue(dc->workqueue);
    kthread_stop(dc->rd);

	mempool_destroy(dc->dedup_work_pool);

	dc->mdops->exit_meta(dc->bmd);

	dm_io_client_destroy(dc->io_client);

	dm_put_device(ti, dc->data_dev);
	dm_put_device(ti, dc->metadata_dev);
	desc_table_deinit(dc->desc_table);

	kfree(dc);
}

/* Gives Dmdedup status. */
static void dm_dedup_status(struct dm_target *ti, status_type_t status_type,
			    unsigned int status_flags, char *result, unsigned int maxlen)
{
	struct dedup_config *dc = ti->private;
	struct dm_bufio_client *c = (struct dm_bufio_client *)(dc->mdops->get_bufio_client(dc->bmd));
	u64 meta_io_cnt;
	u64 fp_io_cnt;
	u64 mapping_io_cnt;
	u64 refcount_io_cnt;
	u64 others_io_cnt;
	u64 persist_io_cnt;
	u64 data_total_block_count;
	u64 data_used_block_count;
	u64 data_free_block_count;
	u64 data_actual_block_count;
	int sz = 0;

	switch (status_type) {
	case STATUSTYPE_INFO:
		meta_io_cnt = c->cntbio;
		fp_io_cnt = c->cntbio_sort[2] + c->cntbio_sort_r[2];
		mapping_io_cnt = c->cntbio_sort[1] + c->cntbio_sort_r[1];
		refcount_io_cnt = c->cntbio_sort[3] + c->cntbio_sort_r[3];
		refcount_io_cnt += c->cntbio_sort[4] + c->cntbio_sort_r[4];
		others_io_cnt = c->cntbio_sort[0] + c->cntbio_sort_r[0];
		persist_io_cnt = c->cntbio_sort[5] + c->cntbio_sort_r[5];
		
		data_used_block_count = dc->physical_block_counter;
		data_actual_block_count = dc->logical_block_counter;
		data_total_block_count = dc->pblocks;
		data_free_block_count = data_total_block_count - data_used_block_count;	
		dc->total_period_time[PERIOD_OTHER] = dc->total_period_time[PERIOD_WRITE] - dc->total_period_time[PERIOD_HASH];
		dc->total_period_time[PERIOD_MIO] = c->total_io_time;
		dc->total_period_time[PERIOD_META] = dc->total_period_time[PERIOD_READ] + dc->total_period_time[PERIOD_WRITE] - dc->total_period_time[PERIOD_HASH] - \
				dc->total_period_time[PERIOD_MIO] - dc->total_period_time[PERIOD_MAP] - dc->total_period_time[PERIOD_IO] ;

		DMEMIT("usr_total_cnt:%llu,usr_write_cnt:%llu,usr_reads_cnt:%llu,", dc->usr_total_cnt, dc->usr_write_cnt, dc->usr_reads_cnt);
		DMEMIT("meta_io_cnt:%llu,fp_io_cnt:%llu,mapping_io_cnt:%llu,refcount_io_cnt:%llu,others_io_cnt:%llu,persist_io_cnt:%llu,",
				meta_io_cnt, fp_io_cnt, mapping_io_cnt, refcount_io_cnt, others_io_cnt, persist_io_cnt);
		DMEMIT("write_meta_io_cnt:%llu,write_fp_io_cnt:%llu,write_mapping_io_cnt:%llu,write_refcount_io_cnt:%llu,write_others_io_cnt:%llu,write_persist_io_cnt:%llu,",
				c->cntbio_write, c->cntbio_sort[2], c->cntbio_sort[1], c->cntbio_sort[3] + c->cntbio_sort[4], c->cntbio_sort[0], c->cntbio_sort[5]);
		DMEMIT("read_meta_io_cnt:%llu,read_fp_io_cnt:%llu,read_mapping_io_cnt:%llu,read_refcount_io_cnt:%llu,read_others_io_cnt:%llu,read_persist_io_cnt:%llu,",
				c->cntbio_read, c->cntbio_sort_r[2], c->cntbio_sort_r[1], c->cntbio_sort_r[3] + c->cntbio_sort_r[4], c->cntbio_sort_r[0], c->cntbio_sort_r[5]);
		DMEMIT("time_total:%llu, time_read:%llu, time_write:%llu, ",
				dc->total_period_time[PERIOD_TOTAL], dc->total_period_time[PERIOD_READ], dc->total_period_time[PERIOD_WRITE]);
		DMEMIT("time_hash:%llu, time_other:%llu, time_fp:%llu, time_l2p:%llu, time_ref:%llu, time_fua:%llu, time_io:%llu, time_gc:%llu, time_map:%llu, ",
				dc->total_period_time[PERIOD_HASH], dc->total_period_time[PERIOD_OTHER], dc->total_period_time[PERIOD_FP], dc->total_period_time[PERIOD_L2P],
				dc->total_period_time[PERIOD_REF], dc->total_period_time[PERIOD_FUA], dc->total_period_time[PERIOD_IO], dc->total_period_time[PERIOD_GC],
				dc->total_period_time[PERIOD_MAP]);
		DMEMIT("gc_count:%llu,gc_fp_count:%llu, ", dc->gc_count, dc->gc_fp_count);
		DMEMIT("hit_right_fp:%llu,hit_wrong_fp:%llu,hit_corrupt_fp:%llu,hit_none_fp:%llu, ", dc->hit_right_fp, dc->hit_wrong_fp, dc->hit_corrupt_fp, dc->hit_none_fp);
		DMEMIT("invalid_fp:%llu,inserted_fp:%llu, ", dc->invalid_fp, dc->inserted_fp);
		DMEMIT("totalwrite:%llu,uniqwrites:%llu,dupwrites:%llu, ", dc->usr_write_cnt, dc->uniqwrites, dc->dupwrites);
		DMEMIT("cycle_total:%llu, cycle_fp:%llu, cycle_meta_process:%llu, cycle_meta_io:%llu, cycle_raid_map:%llu", dc->total_period_time[PERIOD_WRITE] + dc->total_period_time[PERIOD_READ],
				dc->total_period_time[PERIOD_HASH], dc->total_period_time[PERIOD_META], dc->total_period_time[PERIOD_MIO], dc->total_period_time[PERIOD_MAP]);
		break;
	case STATUSTYPE_TABLE:
		DMEMIT("%s %s %u %s %s %d",
		       dc->metadata_dev->name, dc->data_dev->name, dc->block_size,
			dc->crypto_alg, dc->backend_str, dc->flushrq);
	}
}

/*
 * Cleans up Hash->PBN entry.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int cleanup_hash_pbn(void *key, int32_t ksize, void *value,
			    s32 vsize, void *data)
{
	int r = 0;
	u64 pbn_val = 0;
	struct hash_pbn_value hashpbn_value = *((struct hash_pbn_value *)value);
	struct dedup_config *dc = (struct dedup_config *)data;

	BUG_ON(!data);

	pbn_val = hashpbn_value.pbn;

	if (dc->mdops->get_refcount(dc->bmd, pbn_val) == 1) {
		r = dc->kvs_hash_pbn->kvs_delete(dc->kvs_hash_pbn,
							key, ksize);
		if (r < 0)
			goto out;
		r = dc->mdops->dec_refcount(dc->bmd, pbn_val);
		if (r < 0)
			goto out_dec_refcount;

		issue_discard(dc, pbn_val, calculate_tarSSD(dc, pbn_val));
		dc->physical_block_counter -= 1;
		dc->gc_counter++;
		dc->gc_fp_count++;
		dc->invalid_fp--;
		dc->gc_cur_size++;
		if (dc->gc_cur_size >= dc->gc_size)
			r = 1;
	}

	goto out;

out_dec_refcount:
	dc->kvs_hash_pbn->kvs_insert(dc->kvs_hash_pbn, key,
			ksize, (void *)&hashpbn_value,
			sizeof(hashpbn_value));
out:
	return r;
}

/*
 * Cleans up Hash->PBN entry.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int cleanup_hash_pbn_x(void *key, int32_t ksize, void *value,
			    s32 vsize, void *data)
{
	int r = 0, ref = 0;
	u64 pbn_val = 0;
	t_v old_tv, cur_tv;
	struct hash_pbn_value_x hashpbn_value_x = *((struct hash_pbn_value_x *)value);
	struct dedup_config *dc = (struct dedup_config *)data;

	BUG_ON(!data);

	pbn_val = hashpbn_value_x.pbn;
	old_tv.type = hashpbn_value_x.tv.type;
	old_tv.ver = hashpbn_value_x.tv.ver;
	ref = dc->mdops->get_refcount(dc->bmd, pbn_val);
	cur_tv.type = (ref & TV_TYPE) != 0;
	cur_tv.ver = (ref & TV_VER);

	if (cur_tv.type == 1 || cur_tv.ver  != old_tv.ver) {
		r = dc->kvs_hash_pbn->kvs_delete(dc->kvs_hash_pbn,
							key, ksize);
		if (r < 0)
			goto out;

		dc->physical_block_counter -= 1;
		dc->gc_counter++;
		dc->gc_fp_count++;
		dc->inserted_fp--;
	}
	else if (cur_tv.type == 0 && cur_tv.ver != 1 && cur_tv.ver == old_tv.ver) {
		dc->mdops->set_refcount(dc->bmd, pbn_val, 1);
		hashpbn_value_x.tv.type = 0;
		hashpbn_value_x.tv.ver = 1;
		r = dc->kvs_hash_pbn->kvs_delete(dc->kvs_hash_pbn, key, ksize);
		r = dc->kvs_hash_pbn->kvs_insert(dc->kvs_hash_pbn,
							key, ksize, (void*)(&hashpbn_value_x), vsize);
	}
out:
	return r;
}

/*
 * Performs garbage collection.
 * Iterates over all Hash->PBN entries and cleans up
 * hashes if the refcount of block is 1.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int garbage_collect(struct dedup_config *dc)
{
	int err = 0;

	BUG_ON(!dc);

	dc->gc_cur_size = 0;
	calc_tsc(dc, PERIOD_GC, PERIOD_START);
	if (!strcmp(dc->backend_str, "xremap")) {
		err = dc->kvs_hash_pbn->kvs_iterate(dc->kvs_hash_pbn,
			&cleanup_hash_pbn_x, (void *)dc);
	}
	else {
	/* Cleanup hashes if the refcount of block == 1 */
		err = dc->kvs_hash_pbn->kvs_iterate(dc->kvs_hash_pbn,
			&cleanup_hash_pbn, (void *)dc);
	}
	dc->gc_count++;
	calc_tsc(dc, PERIOD_GC, PERIOD_END);
	dc->gc_cur_size = 0;
	return err;
}

/*
 * Performs discard request.
 * Issue a discard request and submit it.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int issue_discard(struct dedup_config *dc, u64 lpn, int id)
{
	u32 id1, id2, isremote;
	u64 entry_offset;
	int err = 0;
	sector_t dev_start = lpn * 8, dev_end = 8;
	id1 = calculate_tarSSD(dc, lpn);
	id2 = id;

	BUG_ON(!dc);
	//DMINFO("[LBN1=%lx][ID1=%d][ID2=%d]", dev_start, id1, id2);
	if (!strcmp(dc->backend_str, "xremap")) {
		entry_offset = calculate_entry_offset(dc, lpn, id1);
		isremote = (id1 != id2) ? 1 : 0;
	}
	else {
	 	entry_offset = 0;
		isremote = 0;
	}
	err = blkdev_issue_discard(dc->data_dev->bdev, dev_start,
			dev_end, GFP_NOIO, 0, isremote, entry_offset);
	if (err) {
		DMERR("Error in issue discard: %d.", err);
		return err;
	}
	return 0;
}


static int issue_begin_end(struct dedup_config *dc, u64 lpn, int id)
{
	struct dm_bufio_client *c;
	u32 id1, id2;
	int err = 0;
	sector_t dev_start = lpn * 8, dev_end = 8;
	id1 = dc->raid_id;
	id2 = id;

	BUG_ON(!dc);
	//DMINFO("[LBN1=%lx][ID1=%d][ID2=%d]", dev_start, id1, id2);
	c = (struct dm_bufio_client *)(dc->mdops->get_bufio_client(dc->bmd));
	c->total_io_time = 0;
	err = blkdev_issue_discard(dc->data_dev->bdev, dev_start,
			dev_end, GFP_NOIO, 0, id1, id2);
	if (err) {
		DMERR("Error in issue notice: %d.", err);
		return err;
	}
	return 0;
}
/*
 * Performs remap request.
 * Issue a remap request and submit it.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int issue_remap(struct dedup_config *dc, u64 lpn1, u64 lpn2, int last)
{
	u32 id1, id2, isremote, isupdate;
	u64 entry_offset;
	int err = 0;
	
	sector_t dev_start = lpn1 * 8, dev_end = 8, dev_s2 = lpn2 * 8;
	id1 = calculate_tarSSD(dc, lpn1);
	id2 = calculate_tarSSD(dc, lpn2);
	entry_offset = calculate_entry_offset(dc, lpn2, id1);

	BUG_ON(!dc);

	//DMINFO("[LBN1=%lx][LBN2=%lx][ID1=%d][ID2=%d][ID3=%d]", dev_start, dev_s2, id1, id2, last);
	isremote = (id1 != id2) ? 1 : 0;
	isupdate = (last != id2) ? 1 : 0;
	err = blkdev_issue_remap(dc->data_dev->bdev, dev_start, dev_s2, isremote, entry_offset, last,
		dev_end, GFP_NOIO, 0);
	if (err) {
		DMERR("Error in issue remap: %d.", err);
		return err;
	}
	return 0;
}

static uint64_t hexStr2int(char* s, int len) {
	int i = 0;
	char c = '0';
    uint64_t x = 0;
    for(i = 0; i < len; ++i) {
        c = s[i];
        x <<= 4;
        if(c >='0' && c <= '9')
            x += (c - '0');
        else if(c >= 'a' && c <= 'f')
            x += (c - 'a' + 10);
        else if(c >= 'A' && c <= 'F')
            x += (c - 'A' + 10);
    }
	return x;
}

static uint64_t intStr2int(char* s, int len) {
	int i = 0;
	char c = '0';
    uint64_t x = 0;
    for(i = 0; i < len; ++i) {
        c = s[i];
        x *= 10;
        if(c >='0' && c <= '9')
            x += (c - '0');
    }
	return x;
}
/*
 * Gives Debug messages for garbage collection.
 * Also, enables and disables corruption check and
 * FEC flags.
 *
 * Returns -ERR code in failure.
 * Returns 0 on success.
 */
static int dm_dedup_message(struct dm_target *ti,
			    unsigned int argc, char **argv,
			    char *result, unsigned maxlen)
{
	int r = 0, i = 0;
	uint64_t lpn1 = 0, lpn2 = 0;

	struct dedup_config *dc = ti->private;
	BUG_ON(!dc);

	if (!strcasecmp(argv[0], "garbage_collect")) {
		r = garbage_collect(dc);
		if (r < 0)
			DMERR("Error in performing garbage_collect: %d.", r);
	} else if (!strcasecmp(argv[0], "notice")) {
		dc->enable_time_stats = dc->enable_time_stats ? 0 : 1;
		if (argc != 2) {
			DMINFO("Incomplete message: Usage notice <begin/end>");
			r = -EINVAL;
        } else if (!strcasecmp(argv[1], "begin")){
			//lpn1 = intStr2int(argv[2], strlen(argv[2]));
			//DMINFO("lpn = %llx", (unsigned long long)lpn1);
			for(i = 0; i < dc->ssd_num; ++i) {
				r = issue_begin_end(dc, i, -1);
			}
		} else if (!strcasecmp(argv[1], "end")){
			for(i = 0; i < dc->ssd_num; ++i) {
				r = issue_begin_end(dc, i, -2);
			}
		}
		if (r)
			DMERR("Error in issue begin_end: %d.", r);
	} else if (!strcasecmp(argv[0], "gc_setting")) {
		if (argc != 2) {
			DMINFO("Incomplete message: Usage gc_setting <type> <percent/constant>");
			r = -EINVAL;
        } else if (!strcasecmp(argv[1], "p")){
			lpn1 = intStr2int(argv[2], strlen(argv[2]));
			dc->gc_threhold = dc->pblocks * lpn1 / 100;
		} else if (!strcasecmp(argv[1], "c")){
			lpn1 = intStr2int(argv[2], strlen(argv[2]));
			dc->gc_threhold = lpn1;
		}
		if (r)
			DMERR("Error in issue begin_end: %d.", r);
	} else if (!strcasecmp(argv[0], "remap")) {
		if (argc != 3) {
			DMINFO("Incomplete message: Usage remap <LPN1> <LPN2>");
			r = -EINVAL;
        } else {
			lpn1 = hexStr2int(argv[1], strlen(argv[1]));
			lpn2 = hexStr2int(argv[2], strlen(argv[2]));
			DMINFO("lpn1 = %llx , lpn2 = %llx", (unsigned long long)lpn1, (unsigned long long)lpn2);
		}
		r = issue_remap(dc, lpn1, lpn2, 0);
		if (r)
			DMERR("Error in issue remap: %d.", r);
	} else if (!strcasecmp(argv[0], "drop_bufio_cache")) {
		if (dc->mdops->flush_bufio_cache)
			dc->mdops->flush_bufio_cache(dc->bmd);
		else
			r = -ENOTSUPP;
	} else if (!strcasecmp(argv[0], "corruption")) {
                if (argc != 2) {
                        DMINFO("Incomplete message: Usage corruption <0,1,2>:"
				"0 - disable all corruption check flags, "
				"1 - Enable corruption check, "
				"2 - Enable FEC flag  (also enable corruption check if disabled)");
                        r = -EINVAL;
                } else if (!strcasecmp(argv[1], "1")) {
                        dc->check_corruption = true;
                        dc->fec = false;
                } else if (!strcasecmp(argv[1], "2")) {
                        dc->check_corruption = true;
                        dc->fec = true;
                } else if (!strcasecmp(argv[1], "0")) {
                        dc->fec = false;
                        dc->check_corruption = false;
                } else {
                        r = -EINVAL;
                }
	} else {
		r = -EINVAL;
	}

	return r;
}

static struct target_type dm_dedup_target = {
	.name = "dedup",
	.version = {1, 0, 0},
	.module = THIS_MODULE,
	.ctr = dm_dedup_ctr,
	.dtr = dm_dedup_dtr,
	.map = dm_dedup_map,
	.message = dm_dedup_message,
	.status = dm_dedup_status,
};

static int __init dm_dedup_init(void)
{
	return dm_register_target(&dm_dedup_target);
}

static void __exit dm_dedup_exit(void)
{
	dm_unregister_target(&dm_dedup_target);
}

module_init(dm_dedup_init);
module_exit(dm_dedup_exit);

MODULE_DESCRIPTION(DM_NAME " target for data deduplication");
MODULE_LICENSE("GPL");
