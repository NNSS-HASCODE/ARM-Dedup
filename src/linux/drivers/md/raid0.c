/*
   raid0.c : Multiple Devices driver for Linux
	     Copyright (C) 1994-96 Marc ZYNGIER
	     <zyngier@ufr-info-p7.ibp.fr> or
	     <maz@gloups.fdn.fr>
	     Copyright (C) 1999, 2000 Ingo Molnar, Red Hat

   RAID-0 management functions.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   You should have received a copy of the GNU General Public License
   (for example /usr/src/linux/COPYING); if not, write to the Free
   Software Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
*/

#include <linux/atomic.h>
#include "linux/printk.h"
#include <linux/stddef.h>
#include <linux/mm.h>
#include <linux/mm_types.h>
#include <asm/string_64.h>
#include <linux/bio.h>
#include <linux/gfp.h>
#include <linux/types.h>
#include <linux/blkdev.h>
#include <linux/seq_file.h>
#include <linux/module.h>
#include <linux/slab.h>
#include <trace/events/block.h>
#include "md.h"
#include "raid0.h"
#include "raid5.h"

static int raid_mode = 5;

static void calc_tsc(struct r0conf *conf, int period, int type) {
    unsigned long var, t;
    unsigned int hi, lo;

	asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
    var = ((unsigned long long int) hi << 32) | lo;
	if(!conf->enable_time_stats)
		return;
	switch (type)
	{
	case PERIOD_START:
		conf->tmp_period_time[period] = var;
		break;
	case PERIOD_END:
		if(conf->tmp_period_time[period]) {
			t = var - conf->tmp_period_time[period];
			conf->total_period_time[period] += t;
		}
		conf->tmp_period_time[period] = 0;
		break;
	default:
		break;
	}
	
    return;
}

#define UNSUPPORTED_MDDEV_FLAGS		\
	((1L << MD_HAS_JOURNAL) |	\
	 (1L << MD_JOURNAL_CLEAN) |	\
	 (1L << MD_FAILFAST_SUPPORTED) |\
	 (1L << MD_HAS_PPL) |		\
	 (1L << MD_HAS_MULTIPLE_PPLS))

static int raid0_congested(struct mddev *mddev, int bits)
{
	struct r0conf *conf = mddev->private;
	struct md_rdev **devlist = conf->devlist;
	int raid_disks = conf->strip_zone[0].nb_dev;
	int i, ret = 0;

	for (i = 0; i < raid_disks && !ret ; i++) {
		struct request_queue *q = bdev_get_queue(devlist[i]->bdev);

		ret |= bdi_congested(q->backing_dev_info, bits);
	}
	return ret;
}

/*
 * inform the user of the raid configuration
*/
static void dump_zones(struct mddev *mddev)
{
	int j, k;
	sector_t zone_size = 0;
	sector_t zone_start = 0;
	char b[BDEVNAME_SIZE];
	struct r0conf *conf = mddev->private;
	int raid_disks = conf->strip_zone[0].nb_dev;
	pr_debug("md: RAID0 configuration for %s - %d zone%s\n",
		 mdname(mddev),
		 conf->nr_strip_zones, conf->nr_strip_zones==1?"":"s");
	for (j = 0; j < conf->nr_strip_zones; j++) {
		char line[200];
		int len = 0;

		for (k = 0; k < conf->strip_zone[j].nb_dev; k++)
			len += snprintf(line+len, 200-len, "%s%s", k?"/":"",
					bdevname(conf->devlist[j*raid_disks
							       + k]->bdev, b));
		pr_debug("md: zone%d=[%s]\n", j, line);

		zone_size  = conf->strip_zone[j].zone_end - zone_start;
		pr_debug("      zone-offset=%10lluKB, device-offset=%10lluKB, size=%10lluKB\n",
			(unsigned long long)zone_start>>1,
			(unsigned long long)conf->strip_zone[j].dev_start>>1,
			(unsigned long long)zone_size>>1);
		zone_start = conf->strip_zone[j].zone_end;
	}
}

/*
 * Input: a 'big' sector number,
 * Output: index of the data and parity disk, and the sector # in them.
 */
sector_t r5_compute_sector(struct mddev *mddev, struct r0conf *conf, sector_t r_sector, int *dd_idx, int is_pd)
{
	sector_t stripe, stripe2;
	sector_t chunk_number;
	unsigned int chunk_offset;
	int pd_idx;
	sector_t new_sector;
	int sectors_per_chunk = mddev->chunk_sectors;
	int raid_disks = conf->strip_zone[0].nb_dev;
	int data_disks = raid_disks - 1;

	/* First compute the information on this sector */

	/*
	 * Compute the chunk number and the sector offset inside the chunk
	 */
	chunk_offset = sector_div(r_sector, sectors_per_chunk);
	chunk_number = r_sector;

	/*
	 * Compute the stripe number
	 */
	stripe = chunk_number;
	*dd_idx = sector_div(stripe, data_disks);
	stripe2 = stripe;
	/*
	 * Select the parity disk based on the user selected algorithm.
	 */
	pd_idx = -1;
	pd_idx = data_disks - sector_div(stripe2, raid_disks);
	*dd_idx = (pd_idx + 1 + *dd_idx) % raid_disks;
	if (is_pd)
		*dd_idx = pd_idx;
	/*
	 * Finally, compute the new sector number
	 */
	new_sector = (sector_t)stripe * sectors_per_chunk + chunk_offset;
	return new_sector;
}

static int create_strip_zones(struct mddev *mddev, struct r0conf **private_conf)
{
	int i, c, err;
	sector_t curr_zone_end, sectors;
	struct md_rdev *smallest, *rdev1, *rdev2, *rdev, **dev;
	struct strip_zone *zone;
	int cnt;
	char b[BDEVNAME_SIZE];
	char b2[BDEVNAME_SIZE];
	struct r0conf *conf = kzalloc(sizeof(*conf), GFP_KERNEL);
	unsigned short blksize = 512;

	*private_conf = ERR_PTR(-ENOMEM);
	if (!conf)
		return -ENOMEM;
	rdev_for_each(rdev1, mddev) {
		pr_debug("md/raid0:%s: looking at %s\n",
			 mdname(mddev),
			 bdevname(rdev1->bdev, b));
		c = 0;

		/* round size to chunk_size */
		sectors = rdev1->sectors;
		sector_div(sectors, mddev->chunk_sectors);
		rdev1->sectors = sectors * mddev->chunk_sectors;

		blksize = max(blksize, queue_logical_block_size(
				      rdev1->bdev->bd_disk->queue));

		rdev_for_each(rdev2, mddev) {
			pr_debug("md/raid0:%s:   comparing %s(%llu)"
				 " with %s(%llu)\n",
				 mdname(mddev),
				 bdevname(rdev1->bdev,b),
				 (unsigned long long)rdev1->sectors,
				 bdevname(rdev2->bdev,b2),
				 (unsigned long long)rdev2->sectors);
			if (rdev2 == rdev1) {
				pr_debug("md/raid0:%s:   END\n",
					 mdname(mddev));
				break;
			}
			if (rdev2->sectors == rdev1->sectors) {
				/*
				 * Not unique, don't count it as a new
				 * group
				 */
				pr_debug("md/raid0:%s:   EQUAL\n",
					 mdname(mddev));
				c = 1;
				break;
			}
			pr_debug("md/raid0:%s:   NOT EQUAL\n",
				 mdname(mddev));
		}
		if (!c) {
			pr_debug("md/raid0:%s:   ==> UNIQUE\n",
				 mdname(mddev));
			conf->nr_strip_zones++;
			pr_debug("md/raid0:%s: %d zones\n",
				 mdname(mddev), conf->nr_strip_zones);
		}
	}
	pr_debug("md/raid0:%s: FINAL %d zones\n",
		 mdname(mddev), conf->nr_strip_zones);
	/*
	 * now since we have the hard sector sizes, we can make sure
	 * chunk size is a multiple of that sector size
	 */
	if ((mddev->chunk_sectors << 9) % blksize) {
		pr_warn("md/raid0:%s: chunk_size of %d not multiple of block size %d\n",
			mdname(mddev),
			mddev->chunk_sectors << 9, blksize);
		err = -EINVAL;
		goto abort;
	}

	err = -ENOMEM;
	conf->strip_zone = kcalloc(conf->nr_strip_zones,
				   sizeof(struct strip_zone),
				   GFP_KERNEL);
	if (!conf->strip_zone)
		goto abort;
	conf->devlist = kzalloc(array3_size(sizeof(struct md_rdev *),
					    conf->nr_strip_zones,
					    mddev->raid_disks),
				GFP_KERNEL);
	if (!conf->devlist)
		goto abort;

	/* The first zone must contain all devices, so here we check that
	 * there is a proper alignment of slots to devices and find them all
	 */
	zone = &conf->strip_zone[0];
	cnt = 0;
	smallest = NULL;
	dev = conf->devlist;
	err = -EINVAL;
	rdev_for_each(rdev1, mddev) {
		int j = rdev1->raid_disk;

		if (mddev->level == 10) {
			/* taking over a raid10-n2 array */
			j /= 2;
			rdev1->new_raid_disk = j;
		}

		if (mddev->level == 1) {
			/* taiking over a raid1 array-
			 * we have only one active disk
			 */
			j = 0;
			rdev1->new_raid_disk = j;
		}

		if (j < 0) {
			pr_warn("md/raid0:%s: remove inactive devices before converting to RAID0\n",
				mdname(mddev));
			goto abort;
		}
		if (j >= mddev->raid_disks) {
			pr_warn("md/raid0:%s: bad disk number %d - aborting!\n",
				mdname(mddev), j);
			goto abort;
		}
		if (dev[j]) {
			pr_warn("md/raid0:%s: multiple devices for %d - aborting!\n",
				mdname(mddev), j);
			goto abort;
		}
		dev[j] = rdev1;

		if (!smallest || (rdev1->sectors < smallest->sectors))
			smallest = rdev1;
		cnt++;
	}
	if (cnt != mddev->raid_disks) {
		pr_warn("md/raid0:%s: too few disks (%d of %d) - aborting!\n",
			mdname(mddev), cnt, mddev->raid_disks);
		goto abort;
	}
	zone->nb_dev = cnt;
	zone->zone_end = smallest->sectors * cnt;
	conf->max_sector = (smallest->sectors) * (cnt - 1);

	curr_zone_end = zone->zone_end;

	/* now do the other zones */
	for (i = 1; i < conf->nr_strip_zones; i++)
	{
		int j;

		zone = conf->strip_zone + i;
		dev = conf->devlist + i * mddev->raid_disks;

		pr_debug("md/raid0:%s: zone %d\n", mdname(mddev), i);
		zone->dev_start = smallest->sectors;
		smallest = NULL;
		c = 0;

		for (j=0; j<cnt; j++) {
			rdev = conf->devlist[j];
			if (rdev->sectors <= zone->dev_start) {
				pr_debug("md/raid0:%s: checking %s ... nope\n",
					 mdname(mddev),
					 bdevname(rdev->bdev, b));
				continue;
			}
			pr_debug("md/raid0:%s: checking %s ..."
				 " contained as device %d\n",
				 mdname(mddev),
				 bdevname(rdev->bdev, b), c);
			dev[c] = rdev;
			c++;
			if (!smallest || rdev->sectors < smallest->sectors) {
				smallest = rdev;
				pr_debug("md/raid0:%s:  (%llu) is smallest!.\n",
					 mdname(mddev),
					 (unsigned long long)rdev->sectors);
			}
		}

		zone->nb_dev = c;
		sectors = (smallest->sectors - zone->dev_start) * c;
		pr_debug("md/raid0:%s: zone->nb_dev: %d, sectors: %llu\n",
			 mdname(mddev),
			 zone->nb_dev, (unsigned long long)sectors);

		curr_zone_end += sectors;
		zone->zone_end = curr_zone_end;

		pr_debug("md/raid0:%s: current zone start: %llu\n",
			 mdname(mddev),
			 (unsigned long long)smallest->sectors);
	}

	pr_debug("md/raid0:%s: done.\n", mdname(mddev));
	*private_conf = conf;

	return 0;
abort:
	kfree(conf->strip_zone);
	kfree(conf->devlist);
	kfree(conf);
	*private_conf = ERR_PTR(err);
	return err;
}

/* Find the zone which holds a particular offset
 * Update *sectorp to be an offset in that zone
 */
static struct strip_zone *find_zone(struct r0conf *conf,
				    sector_t *sectorp)
{
	int i;
	struct strip_zone *z = conf->strip_zone;
	sector_t sector = *sectorp;

	for (i = 0; i < conf->nr_strip_zones; i++)
		if (sector < z[i].zone_end) {
			if (i)
				*sectorp = sector - z[i-1].zone_end;
			return z + i;
		}
	BUG();
}

/*
 * remaps the bio to the target device. we separate two flows.
 * power 2 flow and a general flow for the sake of performance
*/
static struct md_rdev *map_sector(struct mddev *mddev, struct strip_zone *zone,
				sector_t sector, sector_t *sector_offset, int is_pd)
{
	unsigned int sect_in_chunk;
	sector_t chunk;
	struct r0conf *conf = mddev->private;
	int raid_disks = conf->strip_zone[0].nb_dev;
	unsigned int chunk_sects = mddev->chunk_sectors;
	int dd_idx;

	if (raid_mode && sector < conf->max_sector) {
		*sector_offset = r5_compute_sector(mddev, conf, sector, &dd_idx, is_pd);
		return conf->devlist[dd_idx];
	}
	if (is_power_of_2(chunk_sects)) {
		int chunksect_bits = ffz(~chunk_sects);
		/* find the sector offset inside the chunk */
		sect_in_chunk  = sector & (chunk_sects - 1);
		sector >>= chunksect_bits;
		/* chunk in zone */
		chunk = *sector_offset;
		/* quotient is the chunk in real device*/
		sector_div(chunk, zone->nb_dev << chunksect_bits);
	} else{
		sect_in_chunk = sector_div(sector, chunk_sects);
		chunk = *sector_offset;
		sector_div(chunk, chunk_sects * zone->nb_dev);
	}
	/*
	*  position the bio over the real device
	*  real sector = chunk in device + starting of zone
	*	+ the position in the chunk
	*/
	*sector_offset = (chunk * chunk_sects) + sect_in_chunk;
	return conf->devlist[(zone - conf->strip_zone)*raid_disks
			     + sector_div(sector, zone->nb_dev)];
}

static sector_t raid0_size(struct mddev *mddev, sector_t sectors, int raid_disks)
{
	sector_t array_sectors = 0;
	struct md_rdev *rdev;

	WARN_ONCE(sectors || raid_disks,
		  "%s does not support generic reshape\n", __func__);

	rdev_for_each(rdev, mddev)
		array_sectors += (rdev->sectors &
				  ~(sector_t)(mddev->chunk_sectors-1));

	return array_sectors;
}

static void raid0_free(struct mddev *mddev, void *priv);

static int raid0_run(struct mddev *mddev)
{
	struct r0conf *conf;
	int ret;
	int i;
	struct workqueue_struct *wq = NULL;
	mempool_t *work_pool = NULL;

	if (mddev->chunk_sectors == 0) {
		pr_warn("md/raid0:%s: chunk size must be set.\n", mdname(mddev));
		return -EINVAL;
	}
	if (md_check_no_bitmap(mddev))
		return -EINVAL;

	if(raid_mode) {
		wq = create_singlethread_workqueue("md-r5");
		if (!wq) {
			return -ENOMEM;
		}
		work_pool = mempool_create_kmalloc_pool(256, sizeof(struct r5_check_work));
		if (!work_pool) {
			destroy_workqueue(wq);
			return -ENOMEM;
		}
	}

	/* if private is not null, we are here after takeover */
	if (mddev->private == NULL) {
		ret = create_strip_zones(mddev, &conf);
		if (ret < 0)
			return ret;
		mddev->private = conf;
	}
	conf = mddev->private;
	conf->enable_time_stats = 0;
	for(i = 0; i < RAID_NUM; ++i) {
		conf->tmp_period_time[i] = 0;
		conf->total_period_time[i] = 0;
	}
	if(raid_mode) {
		conf->workqueue = wq;
		conf->work_pool = work_pool;
		pr_debug("md/raid0:%s: work related done.\n", mdname(mddev));
	}
	conf->io_count = 0;
	conf->page_count = 0;
	conf->io_count_free = 0;
	conf->page_count_free = 0;
	conf->remote_parity = NULL;
	atomic_set(&(conf->user_reads), 0);
	atomic_set(&(conf->user_write), 0);
	atomic_set(&(conf->meta_reads), 0);
	atomic_set(&(conf->meta_write), 0);
	atomic_set(&(conf->remote_parity_used), 0);
	if (mddev->queue) {
		struct md_rdev *rdev;
		bool discard_supported = true;

		blk_queue_max_hw_sectors(mddev->queue, mddev->chunk_sectors);
		blk_queue_max_write_same_sectors(mddev->queue, mddev->chunk_sectors);
		blk_queue_max_write_zeroes_sectors(mddev->queue, mddev->chunk_sectors);
		blk_queue_max_discard_sectors(mddev->queue, UINT_MAX);

		blk_queue_io_min(mddev->queue, mddev->chunk_sectors << 9);
		blk_queue_io_opt(mddev->queue,
				 (mddev->chunk_sectors << 9) * mddev->raid_disks);

		rdev_for_each(rdev, mddev) {
			disk_stack_limits(mddev->gendisk, rdev->bdev,
					  rdev->data_offset << 9);
			if (blk_queue_discard(bdev_get_queue(rdev->bdev)))
				discard_supported = true;
		}
		if (!discard_supported)
			blk_queue_flag_clear(QUEUE_FLAG_DISCARD, mddev->queue);
		else
			blk_queue_flag_set(QUEUE_FLAG_DISCARD, mddev->queue);
	}

	/* calculate array device size */
	md_set_array_sectors(mddev, raid0_size(mddev, 0, 0));

	pr_debug("md/raid0:%s: md_size is %llu sectors.\n",
		 mdname(mddev),
		 (unsigned long long)mddev->array_sectors);

	if (mddev->queue) {
		/* calculate the max read-ahead size.
		 * For read-ahead of large files to be effective, we need to
		 * readahead at least twice a whole stripe. i.e. number of devices
		 * multiplied by chunk size times 2.
		 * If an individual device has an ra_pages greater than the
		 * chunk size, then we will not drive that device as hard as it
		 * wants.  We consider this a configuration error: a larger
		 * chunksize should be used in that case.
		 */
		int stripe = mddev->raid_disks *
			(mddev->chunk_sectors << 9) / PAGE_SIZE;
		if (mddev->queue->backing_dev_info->ra_pages < 2* stripe)
			mddev->queue->backing_dev_info->ra_pages = 2* stripe;
	}

	dump_zones(mddev);

	ret = md_integrity_register(mddev);

	return ret;
}

static void raid0_free(struct mddev *mddev, void *priv)
{
	struct r0conf *conf = priv;

	if(conf->workqueue)
		destroy_workqueue(conf->workqueue);
	if(conf->work_pool)
		mempool_destroy(conf->work_pool);
	if(conf->remote_parity) {
		vfree(conf->remote_parity);
	}
	kfree(conf->strip_zone);
	kfree(conf->devlist);
	kfree(conf);
}

void page_xor(struct page* dst, struct page* src, size_t size) {
	unsigned int i;
	unsigned long* dst_ptr = page_address(dst);
	unsigned long* src_ptr = page_address(src);

	// Calculate the number of elements (unsigned long) to XOR.
	size_t num_elements = size / sizeof(unsigned long);

	// Perform the XOR operation.
	for (i = 0; i < num_elements; i++) {
		dst_ptr[i] ^= src_ptr[i];
	}
}

void r5_valid_write_endio(struct bio *bio) {
	struct r5_check_io *io;

	io = bio->bi_private;
	bio_free_pages(bio);
	bio_put(bio);
	io->conf->io_count_free += 1;
	io->conf->page_count_free += 1;
	kfree(io);
}

static int r5_valid_write(struct r5_check_io* io) {
	struct bio *write_bio;
	write_bio = bio_alloc(GFP_KERNEL, 1);
	if (!write_bio) {
		if(io->page) 
			__free_page(io->page);
		kfree(io);
		pr_err("Failed to allocate bio\n");
		return -ENOMEM;
	}
	
	bio_set_dev(write_bio, io->dev->bdev);
	if(io->is_remote) {
		write_bio->bi_opf = REQ_OP_REMOTEWRITE;
		write_bio->bi_read_hint = io->entry_offset;
	}
	else {
		write_bio->bi_opf = REQ_OP_WRITE;
	}
	write_bio->bi_iter.bi_sector = io->sector;
	write_bio->bi_end_io = r5_valid_write_endio;
	write_bio->bi_private = io;
	if (bio_add_page(write_bio, io->page, PAGE_SIZE, 0) <= 0) {
		bio_put(write_bio);
		if(io->page) 
			__free_page(io->page);
		kfree(io);
		return -ENOSPC;
	}

	generic_make_request(write_bio);
	return 0;
}

static void issue_work_write(struct work_struct *ws) {
	struct r5_check_work *data = container_of(ws, struct r5_check_work, worker);
	struct r5_check_io *io = (struct r5_check_io *)data->io;

	mempool_free(data, io->conf->work_pool);

	r5_valid_write(io);
}

void r5_valid_read_endio(struct bio *bio) {
	struct r5_check_io *io;
	struct r5_check_work *data;

	io = bio->bi_private;
	if(io->page && io->bio_page) {
		page_xor(io->page, io->bio_page, PAGE_SIZE);
	}
	bio_free_pages(bio);
	bio_put(bio);
	
	data = mempool_alloc(io->conf->work_pool, GFP_NOIO);
	if (!data) {
		if(io->page)
			__free_page(io->page);
		kfree(io);
		return;
	}
	data->io = io;
	INIT_WORK(&(data->worker), issue_work_write);
	queue_work(io->conf->workqueue, &(data->worker));
}

static int r5_valid_read(struct r5_check_io* io) {
	struct bio *read_bio;
	struct page *page;
	read_bio = bio_alloc(GFP_KERNEL, 1);
	if (!read_bio) {
		if(io->page) 
			__free_page(io->page);
		kfree(io);
		pr_err("Failed to allocate bio\n");
		return -ENOMEM;
	}
	page = alloc_page(GFP_KERNEL);
	if (!page) {
		if(io->page) 
			__free_page(io->page);
		kfree(io);
		pr_err("Failed to allocate page\n");
		bio_put(read_bio);
		return -ENOMEM;
	}
	
	bio_set_dev(read_bio, io->dev->bdev);
	if(io->is_remote) {
		read_bio->bi_opf = REQ_OP_REMOTEREAD;
		read_bio->bi_read_hint = io->entry_offset;
	}
	else {
		read_bio->bi_opf = REQ_OP_READ;
	}
	read_bio->bi_iter.bi_sector = io->sector;
	read_bio->bi_end_io = r5_valid_read_endio;
	read_bio->bi_private = io;
	if (bio_add_page(read_bio, page, PAGE_SIZE, 0) <= 0) {
		bio_put(read_bio);
		__free_page(page);
		if(io->page)
			__free_page(io->page);
		kfree(io);
		return -ENOSPC;
	}
	io->bio_page = page;
	generic_make_request(read_bio);
	return 0;
}

static void issue_work_read(struct work_struct *ws) {
	struct r5_check_work *data = container_of(ws, struct r5_check_work, worker);
	struct r5_check_io *io = (struct r5_check_io *)data->io;

	mempool_free(data, io->conf->work_pool);

	r5_valid_read(io);
}

void r5_read_old_endio(struct bio *bio) {
	struct r5_check_io *io;
	struct r5_check_work *data;

	io = bio->bi_private;
	if(io->page && io->bio_page) {
		page_xor(io->page, io->bio_page, PAGE_SIZE);
	}
	bio_free_pages(bio);
	bio_put(bio);
	
	data = mempool_alloc(io->conf->work_pool, GFP_NOIO);
	if (!data) {
		if(io->page)
			__free_page(io->page);
		kfree(io);
		return;
	}
	data->io = io;
	INIT_WORK(&(data->worker), issue_work_read);
	queue_work(io->conf->workqueue, &(data->worker));
}

static int r5_read_old(sector_t sector, struct block_device * bdev, struct r5_check_io* io) {
	struct bio *read_bio;
	struct page *page;
	read_bio = bio_alloc(GFP_KERNEL, 1);
	if (!read_bio) {
		if(io->page) 
			__free_page(io->page);
		kfree(io);
		pr_err("Failed to allocate bio\n");
		return -ENOMEM;
	}
	page = alloc_page(GFP_KERNEL);
	if (!page) {
		if(io->page) 
			__free_page(io->page);
		kfree(io);
		pr_err("Failed to allocate page\n");
		bio_put(read_bio);
		return -ENOMEM;
	}
	
	bio_set_dev(read_bio, bdev);
	if(io->is_remote) {
		read_bio->bi_opf = REQ_OP_REMOTEREAD;
		read_bio->bi_read_hint = io->entry_offset;
	}
	else {
		read_bio->bi_opf = REQ_OP_READ;
	}
	read_bio->bi_iter.bi_sector = sector;
	read_bio->bi_end_io = r5_read_old_endio;
	read_bio->bi_private = io;
	if (bio_add_page(read_bio, page, PAGE_SIZE, 0) <= 0) {
		if(io->page) 
			__free_page(io->page);
		kfree(io);
		bio_put(read_bio);
		__free_page(page);
		return -ENOSPC;
	}
	io->bio_page = page;
	generic_make_request(read_bio);
	return 0;
}

/*
 * Is io distribute over 1 or more chunks ?
*/
static inline int is_io_in_chunk_boundary(struct mddev *mddev,
			unsigned int chunk_sects, struct bio *bio)
{
	if (likely(is_power_of_2(chunk_sects))) {
		return chunk_sects >=
			((bio->bi_iter.bi_sector & (chunk_sects-1))
					+ bio_sectors(bio));
	} else{
		sector_t sector = bio->bi_iter.bi_sector;
		return chunk_sects >= (sector_div(sector, chunk_sects)
						+ bio_sectors(bio));
	}
}

static void raid0_handle_discard(struct mddev *mddev, struct bio *bio)
{
	struct r0conf *conf = mddev->private;
	struct strip_zone *zone;
	sector_t start = bio->bi_iter.bi_sector;
	sector_t end;
	unsigned int stripe_size;
	sector_t first_stripe_index, last_stripe_index;
	sector_t start_disk_offset;
	unsigned int start_disk_index;
	sector_t end_disk_offset;
	unsigned int end_disk_index;
	unsigned int disk;

	zone = find_zone(conf, &start);

	if (bio_end_sector(bio) > zone->zone_end) {
		struct bio *split = bio_split(bio,
			zone->zone_end - bio->bi_iter.bi_sector, GFP_NOIO,
			&mddev->bio_set);
		bio_chain(split, bio);
		generic_make_request(bio);
		bio = split;
		end = zone->zone_end;
	} else
		end = bio_end_sector(bio);

	if (zone != conf->strip_zone)
		end = end - zone[-1].zone_end;

	/* Now start and end is the offset in zone */
	stripe_size = zone->nb_dev * mddev->chunk_sectors;

	first_stripe_index = start;
	sector_div(first_stripe_index, stripe_size);
	last_stripe_index = end;
	sector_div(last_stripe_index, stripe_size);

	start_disk_index = (int)(start - first_stripe_index * stripe_size) /
		mddev->chunk_sectors;
	start_disk_offset = ((int)(start - first_stripe_index * stripe_size) %
		mddev->chunk_sectors) +
		first_stripe_index * mddev->chunk_sectors;
	end_disk_index = (int)(end - last_stripe_index * stripe_size) /
		mddev->chunk_sectors;
	end_disk_offset = ((int)(end - last_stripe_index * stripe_size) %
		mddev->chunk_sectors) +
		last_stripe_index * mddev->chunk_sectors;

	if(raid_mode && bio->bi_iter.bi_sector < conf->max_sector && (bio->bi_iter.bi_ssdno != -1 && bio->bi_iter.bi_ssdno != -2)) {
		struct page *page;
		struct md_rdev *tmp_dev, *tmp_dev2;
		struct r5_check_io *io;
		sector_t bio_sector = bio->bi_iter.bi_sector;
		sector_t sector, sector2, s1, s2;
		page = alloc_page(GFP_KERNEL);
		if (!page) {
			pr_err("Failed to allocate page\n");
			return;
		}
		memset(page_address(page), 0, PAGE_SIZE);
		sector = bio_sector;
		tmp_dev = map_sector(mddev, zone, sector, &sector, 0); //[local/remote]: oldData's dev
		s1 = sector + zone->dev_start + tmp_dev->data_offset; //[local/remote]: oldData's sector
		sector2 = bio_sector;
		tmp_dev2 = map_sector(mddev, zone, sector2, &sector2, 1); //[local]: parity's dev
		s2 = sector2 + zone->dev_start + tmp_dev2->data_offset; //[local]: parity's sector

		io = kmalloc(sizeof(struct r5_check_io), GFP_NOIO);
		io->conf = conf;
		io->page = page;
		io->is_remote = bio->bi_iter.bi_ssdno;
		io->entry_offset = bio->bi_iter.bi_ssdno_2;
		io->sector = s2;
		if(io->is_remote) {
			int pd_idx, raid_disk;
			raid_disk = conf->strip_zone[0].nb_dev;
			pd_idx = (raid_disk - 1) - (io->entry_offset % raid_disk);
			io->dev = conf->devlist[pd_idx];
		}
		else {
			io->dev = tmp_dev2;
		}
		io->conf->io_count += 1;
		io->conf->page_count += 1;
	
		calc_tsc(conf, RAID_IO_DISCARD, PERIOD_START);
		r5_read_old(s1, tmp_dev->bdev, io);
		calc_tsc(conf, RAID_IO_DISCARD, PERIOD_END);
	}

	if((bio->bi_iter.bi_ssdno == -1 || bio->bi_iter.bi_ssdno == -2) && bio->bi_iter.bi_ssdno_2 != -1 && bio->bi_iter.bi_ssdno_2 != -2) {
		int i;
		atomic_t *t;
		sector_t sector, s1;
		struct md_rdev *tmp_dev;
		sector_t bio_sector = bio->bi_iter.bi_sector;
		u64 size = bio->bi_iter.bi_ssdno_2 * sizeof(atomic_t);
		
		sector = bio_sector;
		tmp_dev = map_sector(mddev, zone, sector, &sector, 0);
		s1 = sector + zone->dev_start + tmp_dev->data_offset;
		if(conf->data_start == 0) {
			conf->data_start = s1;
		}
		if(conf->remote_parity == NULL && bio->bi_iter.bi_ssdno_2) {
			conf->remote_parity = vmalloc(size);
			t = conf->remote_parity;
			for (i = 0; i < bio->bi_iter.bi_ssdno_2; ++i, ++t) {
				atomic_set(t, 0);
			}
		}
		pr_debug("md/raid0:%s: sector:%lu map to %lu\n", mdname(mddev), bio_sector, s1);
	}

	if(raid_mode && bio->bi_iter.bi_sector < conf->max_sector) {
		struct bio *discard_bio = NULL;
		struct md_rdev *tmp_dev;
		sector_t sector, s1;
		sector_t bio_sector = bio->bi_iter.bi_sector;
		
		sector = bio_sector;
		tmp_dev = map_sector(mddev, zone, sector, &sector, 0);
		s1 = sector + zone->dev_start + tmp_dev->data_offset;
		
		if(bio->bi_iter.bi_ssdno == 1 && conf->remote_parity) {
			u64 offset = bio->bi_iter.bi_ssdno_2;
			atomic_t *t = conf->remote_parity + offset;
			if(atomic_dec_and_test(t))
				atomic_dec(&(conf->remote_parity_used));
		}
		calc_tsc(conf, RAID_IO_DISCARD, PERIOD_START);
		if (__blkdev_issue_discard(tmp_dev->bdev,s1,8, GFP_NOIO, 
			0, &discard_bio, bio->bi_iter.bi_ssdno, bio->bi_iter.bi_ssdno_2) || !discard_bio) 
		{
			bio_endio(bio);
			return;
		}
		bio_chain(discard_bio, bio);
		bio_clone_blkcg_association(discard_bio, bio);
		if (mddev->gendisk)
			trace_block_bio_remap(bdev_get_queue(tmp_dev->bdev),
				discard_bio, disk_devt(mddev->gendisk),
				bio->bi_iter.bi_sector);
		generic_make_request(discard_bio);
		bio_endio(bio);
		calc_tsc(conf, RAID_IO_DISCARD, PERIOD_END);
		return;
	}

	for (disk = 0; disk < zone->nb_dev; disk++) {
		sector_t dev_start, dev_end;
		struct bio *discard_bio = NULL;
		struct md_rdev *rdev;

		if (disk < start_disk_index)
			dev_start = (first_stripe_index + 1) *
				mddev->chunk_sectors;
		else if (disk > start_disk_index)
			dev_start = first_stripe_index * mddev->chunk_sectors;
		else
			dev_start = start_disk_offset;

		if (disk < end_disk_index)
			dev_end = (last_stripe_index + 1) * mddev->chunk_sectors;
		else if (disk > end_disk_index)
			dev_end = last_stripe_index * mddev->chunk_sectors;
		else
			dev_end = end_disk_offset;

		if (dev_end <= dev_start)
			continue;

		rdev = conf->devlist[(zone - conf->strip_zone) *
			conf->strip_zone[0].nb_dev + disk];
		calc_tsc(conf, RAID_IO_DISCARD, PERIOD_START);
		if (__blkdev_issue_discard(rdev->bdev,
			dev_start + zone->dev_start + rdev->data_offset,
			dev_end - dev_start, GFP_NOIO, 0, &discard_bio, bio->bi_iter.bi_ssdno, bio->bi_iter.bi_ssdno_2) ||
		    !discard_bio)
			continue;
		bio_chain(discard_bio, bio);
		bio_clone_blkcg_association(discard_bio, bio);
		if (mddev->gendisk)
			trace_block_bio_remap(bdev_get_queue(rdev->bdev),
				discard_bio, disk_devt(mddev->gendisk),
				bio->bi_iter.bi_sector);
		generic_make_request(discard_bio);
		calc_tsc(conf, RAID_IO_DISCARD, PERIOD_END);
	}
	bio_endio(bio);
}

static void raid0_handle_remap(struct mddev *mddev, struct bio *bio)
{
	struct r0conf *conf = mddev->private;
	struct strip_zone *zone;
	sector_t start = bio->bi_iter.bi_sector;
	sector_t end;
	unsigned int stripe_size;
	sector_t first_stripe_index, last_stripe_index;
	sector_t start_disk_offset;
	unsigned int start_disk_index;
	sector_t end_disk_offset;
	unsigned int end_disk_index;

	sector_t start2 = bio->bi_iter.bi_sector_2;
	sector_t end2;
	sector_t first_stripe_index2, last_stripe_index2;
	sector_t start_disk_offset2;
	unsigned int start_disk_index2;

	unsigned int disk;

	zone = find_zone(conf, &start);

	if (bio_end_sector(bio) > zone->zone_end) {
		struct bio *split = bio_split(bio,
			zone->zone_end - bio->bi_iter.bi_sector, GFP_NOIO,
			&mddev->bio_set);
		bio_chain(split, bio);
		generic_make_request(bio);
		bio = split;
		end = zone->zone_end;
	} else
		end = bio_end_sector(bio);

	if (zone != conf->strip_zone)
		end = end - zone[-1].zone_end;

	/* Now start and end is the offset in zone */
	stripe_size = zone->nb_dev * mddev->chunk_sectors;

	first_stripe_index = start;
	sector_div(first_stripe_index, stripe_size);
	last_stripe_index = end;
	sector_div(last_stripe_index, stripe_size);

	end2 = bio->bi_iter.bi_sector_2 + (bio->bi_iter.bi_size >> 9);
	first_stripe_index2 = start2;
	sector_div(first_stripe_index2, stripe_size);
	last_stripe_index2 = end2;
	sector_div(last_stripe_index2, stripe_size);

	start_disk_index = (int)(start - first_stripe_index * stripe_size) /
		mddev->chunk_sectors;
	start_disk_offset = ((int)(start - first_stripe_index * stripe_size) %
		mddev->chunk_sectors) +
		first_stripe_index * mddev->chunk_sectors;
	end_disk_index = (int)(end - last_stripe_index * stripe_size) /
		mddev->chunk_sectors;
	end_disk_offset = ((int)(end - last_stripe_index * stripe_size) %
		mddev->chunk_sectors) +
		last_stripe_index * mddev->chunk_sectors;

	start_disk_index2 = (int)(start2 - first_stripe_index2 * stripe_size) /
		mddev->chunk_sectors;
	start_disk_offset2 = ((int)(start2 - first_stripe_index2 * stripe_size) %
		mddev->chunk_sectors) +
		first_stripe_index2 * mddev->chunk_sectors;
	
	if(raid_mode && bio->bi_iter.bi_sector < conf->max_sector && (bio->bi_iter.bi_ssdno != -1 && bio->bi_iter.bi_ssdno != -2)) {
		struct page *page;
		struct md_rdev *tmp_dev, *tmp_dev2, *tmp_dev3;
		struct r5_check_io *io;
		sector_t bio_sector = bio->bi_iter.bi_sector;
		sector_t bio_sector_2 = bio->bi_iter.bi_sector_2;
		sector_t sector, sector2, sector3, s1, s2, s3;
		page = alloc_page(GFP_KERNEL);
		if (!page) {
			pr_err("Failed to allocate page\n");
			return;
		}
		memset(page_address(page), 0, PAGE_SIZE);
		sector = bio_sector;
		tmp_dev = map_sector(mddev, zone, sector, &sector, 0); //[loacal/remote]: newData's dev; [remote]: oldData's dev
		s1 = sector + zone->dev_start + tmp_dev->data_offset; //[loacal/remote]: newData's sector; [remote]: oldData's sector
		sector2 = bio_sector_2;
		tmp_dev2 = map_sector(mddev, zone, sector2, &sector2, 0); //[local]: oldData's dev
		s2 = sector2 + zone->dev_start + tmp_dev2->data_offset; //[local]: oldData's sector
		sector3 = bio_sector_2;
		tmp_dev3 = map_sector(mddev, zone, sector3, &sector3, 1); //[local]: parity's dev
		s3 = sector3 + zone->dev_start + tmp_dev3->data_offset; //[local]: parity's sector
		
		io = kmalloc(sizeof(struct r5_check_io), GFP_NOIO);
		io->conf = conf;
		io->page = page;
		io->is_remote = bio->bi_iter.bi_ssdno;
		io->entry_offset = bio->bi_iter.bi_ssdno_2;
		io->conf->io_count += 1;
		io->conf->page_count += 1;
		if(io->is_remote) {
			int pd_idx, raid_disk;
			raid_disk = conf->strip_zone[0].nb_dev;
			pd_idx = (raid_disk - 1) - (io->entry_offset % raid_disk);
			
			io->dev = conf->devlist[pd_idx];
			io->sector = s1;
			calc_tsc(conf, RAID_IO_REMAP, PERIOD_START);
			r5_read_old(s1, tmp_dev->bdev, io);
			calc_tsc(conf, RAID_IO_REMAP, PERIOD_END);
		}
		else {
			io->dev = tmp_dev3;
			io->sector = s3;
			calc_tsc(conf, RAID_IO_REMAP, PERIOD_START);
			r5_read_old(s2, tmp_dev2->bdev, io);
			calc_tsc(conf, RAID_IO_REMAP, PERIOD_END);
		}
	}
	
	if(raid_mode && bio->bi_iter.bi_sector < conf->max_sector) {
		struct bio *discard_bio = NULL;
		struct md_rdev *tmp_dev, *tmp_dev2;
		sector_t sector, sector2, s1, s2;
		sector_t bio_sector = bio->bi_iter.bi_sector;
		sector_t bio_sector2 = bio->bi_iter.bi_sector_2;
		
		sector = bio_sector;
		tmp_dev = map_sector(mddev, zone, sector, &sector, 0);
		s1 = sector + zone->dev_start + tmp_dev->data_offset;
		sector2 = bio_sector2;
		tmp_dev2 = map_sector(mddev, zone, sector2, &sector2, 0);
		s2 = sector2 + zone->dev_start + tmp_dev->data_offset;
		
		if(bio->bi_iter.bi_ssdno == 1 && conf->remote_parity) {
			u64 offset = bio->bi_iter.bi_ssdno_2;
			atomic_t *t = conf->remote_parity + offset;
			if(atomic_read(t) == 0)
				atomic_inc(&(conf->remote_parity_used));
			atomic_inc(t);
		}
		calc_tsc(conf, RAID_IO_REMAP, PERIOD_START);
		if (__blkdev_issue_remap(tmp_dev->bdev, s1, bio->bi_iter.bi_ssdno ? bio->bi_iter.bi_sector_2 : s2,
			bio->bi_iter.bi_ssdno, bio->bi_iter.bi_ssdno_2, bio->bi_iter.bi_ssdno_3,
			8, GFP_NOIO, 0, &discard_bio) || !discard_bio) 
		{
			bio_endio(bio);
			return;
		}
		bio_chain(discard_bio, bio);
		bio_clone_blkcg_association(discard_bio, bio);
		if (mddev->gendisk)
			trace_block_bio_remap(bdev_get_queue(tmp_dev->bdev),
				discard_bio, disk_devt(mddev->gendisk),
				bio->bi_iter.bi_sector);
		generic_make_request(discard_bio);
		bio_endio(bio);
		calc_tsc(conf, RAID_IO_REMAP, PERIOD_END);
		return;
	}

	for (disk = 0; disk < zone->nb_dev; disk++) {
		sector_t dev_start, dev_end;
		sector_t dev_start2;
		struct bio *discard_bio = NULL;
		struct md_rdev *rdev;

		if (disk < start_disk_index)
			dev_start = (first_stripe_index + 1) *
				mddev->chunk_sectors;
		else if (disk > start_disk_index)
			dev_start = first_stripe_index * mddev->chunk_sectors;
		else
			dev_start = start_disk_offset;

		if (disk < end_disk_index)
			dev_end = (last_stripe_index + 1) * mddev->chunk_sectors;
		else if (disk > end_disk_index)
			dev_end = last_stripe_index * mddev->chunk_sectors;
		else
			dev_end = end_disk_offset;

		if (dev_end <= dev_start)
			continue;

		dev_start2 = start_disk_offset2;

		rdev = conf->devlist[(zone - conf->strip_zone) *
			conf->strip_zone[0].nb_dev + disk];
		calc_tsc(conf, RAID_IO_REMAP, PERIOD_START);
		if (__blkdev_issue_remap(rdev->bdev,
			dev_start + zone->dev_start + rdev->data_offset,
			bio->bi_iter.bi_ssdno ? bio->bi_iter.bi_sector_2 : dev_start2 + zone->dev_start + rdev->data_offset,
			bio->bi_iter.bi_ssdno, bio->bi_iter.bi_ssdno_2, bio->bi_iter.bi_ssdno_3,
			dev_end - dev_start, GFP_NOIO, 0, &discard_bio) ||
		    !discard_bio)
			continue;
		bio_chain(discard_bio, bio);
		bio_clone_blkcg_association(discard_bio, bio);
		if (mddev->gendisk)
			trace_block_bio_remap(bdev_get_queue(rdev->bdev),
				discard_bio, disk_devt(mddev->gendisk),
				bio->bi_iter.bi_sector);
		generic_make_request(discard_bio);
		calc_tsc(conf, RAID_IO_REMAP, PERIOD_END);
	}
	bio_endio(bio);
}


static bool raid0_make_request(struct mddev *mddev, struct bio *bio)
{
	struct strip_zone *zone;
	struct md_rdev *tmp_dev;
	struct md_rdev *tmp_dev2;
	struct r0conf *conf;
	sector_t bio_sector;
	sector_t sector;
	sector_t sector2;
	unsigned chunk_sects;
	unsigned sectors;
	struct r5_check_io *io;
	struct page *page, *page2;

	/* printk(KERN_INFO "[op=%d][bi_sector=0x%llx][bi_size=%u][chunksector=%d][blk_name=%s]\n", bio->bi_opf, \
			(unsigned long long)bio->bi_iter.bi_sector, bio->bi_iter.bi_size, mddev->chunk_sectors, bio->bi_disk->disk_name); */

	conf = mddev->private;
	
	calc_tsc(conf, RAID_RDWR, PERIOD_START);

	if (unlikely(bio->bi_opf & REQ_PREFLUSH)) {
		md_flush_request(mddev, bio);
		return true;
	}

	if (unlikely((bio_op(bio) == REQ_OP_DISCARD))) {
		calc_tsc(conf, RAID_DISCARD, PERIOD_START);
		if (bio->bi_iter.bi_ssdno == -1 || bio->bi_iter.bi_ssdno == -2) {
			int i;
			switch (bio->bi_iter.bi_ssdno_2) {
			case -1:
				conf->enable_time_stats = 1;
				for(i = 0; i < RAID_NUM; ++i) {
					conf->tmp_period_time[i] = 0;
					conf->total_period_time[i] = 0;
				}
				atomic_set(&(conf->user_reads), 0);
				atomic_set(&(conf->user_write), 0);
				atomic_set(&(conf->meta_reads), 0);
				atomic_set(&(conf->meta_write), 0);
				atomic_set(&(conf->remote_parity_used), 0);
				break;
			case -2:
				conf->enable_time_stats = 0;
				break;
			default:
				break;
			}
		}
		raid0_handle_discard(mddev, bio);
		calc_tsc(conf, RAID_DISCARD, PERIOD_END);
		return true;
	}

	if (unlikely((bio_op(bio) == REQ_OP_DEDUPWRITE))) {
		calc_tsc(conf, RAID_REMAP, PERIOD_START);
		raid0_handle_remap(mddev, bio);
		calc_tsc(conf, RAID_REMAP, PERIOD_END);
		return true;
	}

	bio_sector = bio->bi_iter.bi_sector;
	sector = bio_sector;
	chunk_sects = mddev->chunk_sectors;

	sectors = chunk_sects -
		(likely(is_power_of_2(chunk_sects))
		 ? (sector & (chunk_sects-1))
		 : sector_div(sector, chunk_sects));

	/* Restore due to sector_div */
	sector = bio_sector;

	if (sectors < bio_sectors(bio)) {
		struct bio *split = bio_split(bio, sectors, GFP_NOIO,
					      &mddev->bio_set);
		bio_chain(split, bio);
		generic_make_request(bio);
		bio = split;
	}

	zone = find_zone(mddev->private, &sector);
	tmp_dev = map_sector(mddev, zone, sector, &sector, 0);
	bio_set_dev(bio, tmp_dev->bdev);
	bio->bi_iter.bi_sector = sector + zone->dev_start +
		tmp_dev->data_offset;

	if(bio_op(bio) == REQ_OP_WRITE) {
		if(bio->bi_iter.bi_sector < conf->data_start)
			atomic_inc(&(conf->meta_write));
		else
		 	atomic_inc(&(conf->user_write));
	}
	else if(bio_op(bio) == REQ_OP_READ) {
		if(bio->bi_iter.bi_sector < conf->data_start)
			atomic_inc(&(conf->meta_reads));
		else
		 	atomic_inc(&(conf->user_reads));
	}
	
	if(raid_mode && (bio_op(bio) == REQ_OP_WRITE) && (bio_sector < conf->max_sector) && (bio->bi_iter.bi_size != 0)) {
		struct bio_vec bv;
		struct bvec_iter iter;
		page = alloc_page(GFP_KERNEL);
		if (!page) {
			pr_err("Failed to allocate page.\n");
			return -ENOMEM;
		}
		bio_for_each_segment(bv, bio, iter) {
			page2 = bv.bv_page;
			if(page2)
				memcpy(page_address(page), page_address(page2), PAGE_SIZE);
        	break;
		}

		sector2 = bio_sector;
		tmp_dev2 = map_sector(mddev, zone, sector2, &sector2, 1);
		
		io = kmalloc(sizeof(struct r5_check_io), GFP_NOIO);
		io->conf = conf;
		io->dev = tmp_dev2;
		io->page = page;
		io->sector = sector2 + zone->dev_start + tmp_dev2->data_offset;
		io->is_remote = 0;
		io->entry_offset = 0;
		io->conf->io_count++;
		io->conf->page_count++;
	
		calc_tsc(conf, RAID_IO_RDWR, PERIOD_START);
		r5_read_old(bio->bi_iter.bi_sector, tmp_dev->bdev, io);
		calc_tsc(conf, RAID_IO_RDWR, PERIOD_END);
	}

	calc_tsc(conf, RAID_IO_RDWR, PERIOD_START);
	if (mddev->gendisk)
		trace_block_bio_remap(bio->bi_disk->queue, bio,
				disk_devt(mddev->gendisk), bio_sector);
	mddev_check_writesame(mddev, bio);
	mddev_check_write_zeroes(mddev, bio);
	generic_make_request(bio);
	calc_tsc(conf, RAID_IO_RDWR, PERIOD_END);

	calc_tsc(conf, RAID_RDWR, PERIOD_END);

	return true;
}

static void raid0_status(struct seq_file *seq, struct mddev *mddev)
{
	struct r0conf *conf = mddev->private;
	u64 total = conf->total_period_time[RAID_RDWR] + conf->total_period_time[RAID_REMAP] + conf->total_period_time[RAID_DISCARD];
	u64 total_io = conf->total_period_time[RAID_IO_RDWR] + conf->total_period_time[RAID_IO_REMAP] + conf->total_period_time[RAID_IO_DISCARD];
	u64 process = total - total_io;
	seq_printf(seq, " %dk chunks", mddev->chunk_sectors / 2);
	seq_printf(seq, "\ntotal cycles: %llu, process cycles: %llu, io cycles: %llu\n", total, process, total_io);
	seq_printf(seq, "io_cnt:%lld, io_cnt_free:%lld;", conf->io_count, conf->io_count_free);
	seq_printf(seq, "page_cnt:%lld, page_cnt_free:%lld,", conf->page_count, conf->page_count_free);
	seq_printf(seq, "user_reads:%d, user_writes:%d,meta_reads:%d, meta_writes:%d, remote_parity_used: %d", \
		atomic_read(&(conf->user_reads)), atomic_read(&(conf->user_write)), \
		atomic_read(&(conf->meta_reads)), atomic_read(&(conf->meta_write)), atomic_read(&(conf->remote_parity_used)));
	return;
}

static void *raid0_takeover_raid45(struct mddev *mddev)
{
	struct md_rdev *rdev;
	struct r0conf *priv_conf;

	if (mddev->degraded != 1) {
		pr_warn("md/raid0:%s: raid5 must be degraded! Degraded disks: %d\n",
			mdname(mddev),
			mddev->degraded);
		return ERR_PTR(-EINVAL);
	}

	rdev_for_each(rdev, mddev) {
		/* check slot number for a disk */
		if (rdev->raid_disk == mddev->raid_disks-1) {
			pr_warn("md/raid0:%s: raid5 must have missing parity disk!\n",
				mdname(mddev));
			return ERR_PTR(-EINVAL);
		}
		rdev->sectors = mddev->dev_sectors;
	}

	/* Set new parameters */
	mddev->new_level = 0;
	mddev->new_layout = 0;
	mddev->new_chunk_sectors = mddev->chunk_sectors;
	mddev->raid_disks--;
	mddev->delta_disks = -1;
	/* make sure it will be not marked as dirty */
	mddev->recovery_cp = MaxSector;
	mddev_clear_unsupported_flags(mddev, UNSUPPORTED_MDDEV_FLAGS);

	create_strip_zones(mddev, &priv_conf);

	return priv_conf;
}

static void *raid0_takeover_raid10(struct mddev *mddev)
{
	struct r0conf *priv_conf;

	/* Check layout:
	 *  - far_copies must be 1
	 *  - near_copies must be 2
	 *  - disks number must be even
	 *  - all mirrors must be already degraded
	 */
	if (mddev->layout != ((1 << 8) + 2)) {
		pr_warn("md/raid0:%s:: Raid0 cannot takeover layout: 0x%x\n",
			mdname(mddev),
			mddev->layout);
		return ERR_PTR(-EINVAL);
	}
	if (mddev->raid_disks & 1) {
		pr_warn("md/raid0:%s: Raid0 cannot takeover Raid10 with odd disk number.\n",
			mdname(mddev));
		return ERR_PTR(-EINVAL);
	}
	if (mddev->degraded != (mddev->raid_disks>>1)) {
		pr_warn("md/raid0:%s: All mirrors must be already degraded!\n",
			mdname(mddev));
		return ERR_PTR(-EINVAL);
	}

	/* Set new parameters */
	mddev->new_level = 0;
	mddev->new_layout = 0;
	mddev->new_chunk_sectors = mddev->chunk_sectors;
	mddev->delta_disks = - mddev->raid_disks / 2;
	mddev->raid_disks += mddev->delta_disks;
	mddev->degraded = 0;
	/* make sure it will be not marked as dirty */
	mddev->recovery_cp = MaxSector;
	mddev_clear_unsupported_flags(mddev, UNSUPPORTED_MDDEV_FLAGS);

	create_strip_zones(mddev, &priv_conf);
	return priv_conf;
}

static void *raid0_takeover_raid1(struct mddev *mddev)
{
	struct r0conf *priv_conf;
	int chunksect;

	/* Check layout:
	 *  - (N - 1) mirror drives must be already faulty
	 */
	if ((mddev->raid_disks - 1) != mddev->degraded) {
		pr_err("md/raid0:%s: (N - 1) mirrors drives must be already faulty!\n",
		       mdname(mddev));
		return ERR_PTR(-EINVAL);
	}

	/*
	 * a raid1 doesn't have the notion of chunk size, so
	 * figure out the largest suitable size we can use.
	 */
	chunksect = 64 * 2; /* 64K by default */

	/* The array must be an exact multiple of chunksize */
	while (chunksect && (mddev->array_sectors & (chunksect - 1)))
		chunksect >>= 1;

	if ((chunksect << 9) < PAGE_SIZE)
		/* array size does not allow a suitable chunk size */
		return ERR_PTR(-EINVAL);

	/* Set new parameters */
	mddev->new_level = 0;
	mddev->new_layout = 0;
	mddev->new_chunk_sectors = chunksect;
	mddev->chunk_sectors = chunksect;
	mddev->delta_disks = 1 - mddev->raid_disks;
	mddev->raid_disks = 1;
	/* make sure it will be not marked as dirty */
	mddev->recovery_cp = MaxSector;
	mddev_clear_unsupported_flags(mddev, UNSUPPORTED_MDDEV_FLAGS);

	create_strip_zones(mddev, &priv_conf);
	return priv_conf;
}

static void *raid0_takeover(struct mddev *mddev)
{
	/* raid0 can take over:
	 *  raid4 - if all data disks are active.
	 *  raid5 - providing it is Raid4 layout and one disk is faulty
	 *  raid10 - assuming we have all necessary active disks
	 *  raid1 - with (N -1) mirror drives faulty
	 */

	if (mddev->bitmap) {
		pr_warn("md/raid0: %s: cannot takeover array with bitmap\n",
			mdname(mddev));
		return ERR_PTR(-EBUSY);
	}
	if (mddev->level == 4)
		return raid0_takeover_raid45(mddev);

	if (mddev->level == 5) {
		if (mddev->layout == ALGORITHM_PARITY_N)
			return raid0_takeover_raid45(mddev);

		pr_warn("md/raid0:%s: Raid can only takeover Raid5 with layout: %d\n",
			mdname(mddev), ALGORITHM_PARITY_N);
	}

	if (mddev->level == 10)
		return raid0_takeover_raid10(mddev);

	if (mddev->level == 1)
		return raid0_takeover_raid1(mddev);

	pr_warn("Takeover from raid%i to raid0 not supported\n",
		mddev->level);

	return ERR_PTR(-EINVAL);
}

static void raid0_quiesce(struct mddev *mddev, int quiesce)
{
}

static struct md_personality raid0_personality=
{
	.name		= "raid0",
	.level		= 0,
	.owner		= THIS_MODULE,
	.make_request	= raid0_make_request,
	.run		= raid0_run,
	.free		= raid0_free,
	.status		= raid0_status,
	.size		= raid0_size,
	.takeover	= raid0_takeover,
	.quiesce	= raid0_quiesce,
	.congested	= raid0_congested,
};

static int __init raid0_init (void)
{
	return register_md_personality (&raid0_personality);
}

static void raid0_exit (void)
{
	unregister_md_personality (&raid0_personality);
}

module_init(raid0_init);
module_exit(raid0_exit);
module_param(raid_mode, int, 0644);
MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("RAID0 (striping) personality for MD");
MODULE_ALIAS("md-personality-2"); /* RAID0 */
MODULE_ALIAS("md-raid0");
MODULE_ALIAS("md-level-0");
