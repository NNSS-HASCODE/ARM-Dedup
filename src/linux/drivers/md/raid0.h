/* SPDX-License-Identifier: GPL-2.0 */
#ifndef _RAID0_H
#define _RAID0_H

#include <linux/kernel.h>
#include <linux/types.h>
#include <linux/workqueue.h>
#include <linux/mempool.h>

enum {
	RAID_RDWR = 0,
	RAID_REMAP,
	RAID_DISCARD,
	RAID_IO_RDWR,
	RAID_IO_REMAP,
	RAID_IO_DISCARD,
	RAID_NUM
};

enum {
	PERIOD_START = 0,
	PERIOD_END
};

struct strip_zone {
	sector_t zone_end;	/* Start of the next zone (in sectors) */
	sector_t dev_start;	/* Zone offset in real dev (in sectors) */
	int	 nb_dev;	/* # of devices attached to the zone */
};

struct r0conf {
	struct strip_zone	*strip_zone;
	struct md_rdev		**devlist; /* lists of rdevs, pointed to
					    * by strip_zone->dev */
	int			nr_strip_zones;
	struct workqueue_struct *workqueue;
	mempool_t *work_pool;
	u64 max_sector;
	u64 enable_time_stats;
	u64 tmp_period_time[RAID_NUM];
	u64 total_period_time[RAID_NUM];
	long long int io_count;
	long long int page_count;
	long long int io_count_free;
	long long int page_count_free;
	sector_t data_start;
	atomic_t user_reads;
	atomic_t user_write;
	atomic_t meta_reads;
	atomic_t meta_write;
	atomic_t remote_parity_used;
	atomic_t *remote_parity;
};

struct r5_check_io {
	struct r0conf* conf;
	struct md_rdev *dev;
	struct page *page;
	struct page *bio_page;
	sector_t sector;
	int is_remote;
	u64 entry_offset;
};

struct r5_check_work {
	struct work_struct worker;
	struct r5_check_io *io;
};

#endif
