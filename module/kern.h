/**
 * kern.h - Common definitions for Walb kernel code.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_KERN_H_KERNEL
#define WALB_KERN_H_KERNEL

#include "check_kernel.h"

#include <linux/workqueue.h>
#include <linux/bio.h>
#include <linux/spinlock.h>
#include <linux/kernel.h>
#include <linux/blkdev.h>
#include <linux/mutex.h>

#include "walb/common.h"
#include "walb/print.h"
#include "walb/logger.h"
#include "walb/check.h"
#include "walb/log_device.h"
#include "walb/sector.h"
#include "walb/ioctl.h"
#include "checkpoint.h"
#include "util.h"

/**
 * Walb device major.
 */
extern int walb_major_;

/**
 * Workqueues.
 */
extern struct workqueue_struct *wq_normal_;
extern struct workqueue_struct *wq_nrt_;
extern struct workqueue_struct *wq_unbound_;
extern struct workqueue_struct *wq_misc_;

/**
 * If non-zero, data IOs will be sorted for better performance.
 */
extern unsigned int is_sort_data_io_;

/**
 * Executable binary path for error notification.
 */
#define EXEC_PATH_ON_ERROR_LEN 256
extern char exec_path_on_error_[];

/**
 * Non-zero if you want write IOs to fail in order not to
 * overflow the ring buffer.
 */
extern unsigned int is_error_before_overflow_;

/*
 * Minor number and partition management.
 */
#define WALB_MINORS	  16
#define WALB_MINORS_SHIFT  4
#define DEVNUM(kdevnum) ((MINOR(kdev_t_to_nr(kdevnum)) >> MINOR_SHIFT)

/**
 * Lsid indicators.
 *
 * latest:
 *   This is used to generate new logpack.
 * flush:
 *   This is to remember the latest lsid of
 *   log flush request executed.
 * completed:
 *   All logpacks with lsid < completed
 *   have been written to the log device.
 * permanent:
 *   ALl logpacks with lsid < permanent
 *   have been permanent in the log device.
 * written:
 *   All logpacks with lsid < written
 *   have been written to the data device.
 * prev_written:
 *   Previously synced written to the superblock.
 *   You do not need to sync superblock
 *   while written == prev_written.
 * oldest:
 *   All logpacks with lsid < oldest
 *   on the log device can be overwritten.
 *
 * Property 1
 *   oldest <= prev_written <= written
 *   <= permanent <= completed <= latest.
 * Property 2
 *   permanent <= flush <= latest.
 */
struct lsid_set
{
	u64 latest;
	u64 flush;
	u64 completed;
	u64 permanent;
	u64 written;
	u64 prev_written;
	u64 oldest;
};

enum {
	/* Write always fails if set. */
	WALB_STATE_READ_ONLY = 0,

	/* All IOs must fail during finalization if set. */
	WALB_STATE_FINALIZE,

	/* Overflow state if set. */
	WALB_STATE_OVERFLOW,
};

/**
 * The internal representation of walb and walblog device.
 */
struct walb_dev
{
	/* Wrapper device id. */
	dev_t devt;

	/* See WALB_STATE_XXX */
	unsigned long flags;

	/* Size of underlying devices. [logical block] */
	u64 ldev_size;
	u64 ddev_size;
	u64 size;
	spinlock_t size_lock;

	/* You can get physical sector size [byte] with
	   bdev_physical_block_size(bdev).

	   Those of underlying log device and data device
	   must be the same.

	   This may be 512 or 4096.
	*/
	u16 physical_bs;

	/* Underlying block devices */
	struct block_device *ldev; /* log device */
	struct block_device *ddev; /* data device */

	/*
	 * chunk sectors [logical block].
	 * if chunk_sectors > 0:
	 *   (1) bio size must not exceed the size.
	 *   (2) bio must not cross over multiple chunks.
	 * else:
	 *   no limitation.
	 *
	 * This is for some drivers like md-raid5.
	 */
	unsigned int ldev_chunk_sectors;
	unsigned int ddev_chunk_sectors;

	/*
	 * Super sector of log device.
	 * The lock must be held to access the lsuper0 while the device is online.
	 */
	spinlock_t lsuper0_lock;
	struct sector_data *lsuper0;

	/* To avoid lock lsuper0 during request processing. */
	u64 ring_buffer_off;
	u64 ring_buffer_size;

	/* Log checksum salt.
	   This is used for logpack header and log data. */
	u32 log_checksum_salt;

	/* Lsids and its lock.
	   Each variable must be accessed with lsid_lock held. */
	spinlock_t lsid_lock;
	struct lsid_set lsids;

	/*
	 * For wrapper device.
	 */
	struct request_queue *queue;
	struct gendisk *gd;
	atomic_t n_users; /* number of users */

	/*
	 * For wrapper log device.
	 */
	struct request_queue *log_queue;
	struct gendisk *log_gd;
	atomic_t log_n_users;

	/*
	 * For checkpointing.
	 */
	struct checkpoint_data cpd;

	/* Maximum logpack size [physical block].
	   This will be used for logpack size
	   not to be too long
	   This will avoid decrease of
	   sequential write performance. */
	unsigned int max_logpack_pb;

	/* Log flush size interval must not exceed this value
	   [physical blocks]. */
	unsigned int log_flush_interval_pb;

	/* Log flush time interval must not exceed this value [jiffies]. */
	unsigned int log_flush_interval_jiffies;

	/* max_pending_sectors < pending_sectors
	   we must stop the queue. */
	unsigned int max_pending_sectors;

	/* min_pending_sectors > pending_sectors
	   we can restart the queue. */
	unsigned int min_pending_sectors;

	/* queue stopped period must not exceed this value. */
	unsigned int queue_stop_timeout_jiffies;

	/* If you prefer small response to large throughput,
	   set n_pack_bulk smaller. */
	unsigned int n_pack_bulk;

	/* If you use IO-scheduling-sensitive storage for the data device,
	 * you should set larger n_io_bulk value.
	 * For example, HDD with little cache.
	 * This must not be so large because we use insertion sort. */
	unsigned int n_io_bulk;

	/* for sysfs. */
	bool support_flush;
	bool support_fua;
	bool support_discard;

	/*
	 * For freeze/melt.
	 */
	struct mutex freeze_lock;
	u8 freeze_state;
	struct delayed_work freeze_dwork;

	/*
	 * Sysfs entry.
	 */
	struct kobject kobj;

	/*
	 * For deferred destory task.
	 */
	struct work_struct destroy_task;

	/*
	 * For IOcore.
	 */
	void *private_data;
};

/*******************************************************************************
 * Macro definition.
 *******************************************************************************/

/* (struct gendisk *) --> (struct walb_dev *) */
#define get_wdev_from_disk(disk) ((struct walb_dev *)(disk)->private_data)

#define wdev_minor(wdev) MINOR((wdev)->devt)

#define WLOGe(wdev, fmt, args...) LOGe("%u: " fmt, wdev_minor(wdev), ##args)
#define WLOGw(wdev, fmt, args...) LOGw("%u: " fmt, wdev_minor(wdev), ##args)
#define WLOGn(wdev, fmt, args...) LOGn("%u: " fmt, wdev_minor(wdev), ##args)
#define WLOGi(wdev, fmt, args...) LOGi("%u: " fmt, wdev_minor(wdev), ##args)
#define WLOGd(wdev, fmt, args...) LOGd("%u: " fmt, wdev_minor(wdev), ##args)
#define WLOG_(wdev, fmt, args...)

/*******************************************************************************
 * Static inline functions.
 *******************************************************************************/

/**
 * Get walb device from request queue.
 */
static inline struct walb_dev* get_wdev_from_queue(struct request_queue *q)
{
	struct walb_dev *wdev;

	ASSERT(q);
	wdev = (struct walb_dev *)q->queuedata;
	return wdev;
}

/**
 * Get walb device from checkpoint data.
 */
static inline struct walb_dev* get_wdev_from_checkpoint_data(
	struct checkpoint_data *cpd)
{
	struct walb_dev *wdev;
	ASSERT(cpd);
	wdev = (struct walb_dev *)container_of(cpd, struct walb_dev, cpd);
	return wdev;
}

/**
 * Check there is no permanent log or not.
 *
 * wdev->lsid_lock must be held.
 */
static inline bool is_permanent_log_empty(struct lsid_set *lsids)
{
	ASSERT(lsids);
	ASSERT(lsids->oldest <= lsids->permanent);
	return lsids->oldest == lsids->permanent;
}

static inline bool is_wdev_dying(struct walb_dev *wdev)
{
	return !wdev
		|| test_bit(WALB_STATE_FINALIZE, &wdev->flags)
		|| !wdev->private_data;
}

/*******************************************************************************
 * Prototypes defined in walb.c
 *******************************************************************************/

struct walb_dev* prepare_wdev(
	unsigned int minor, dev_t ldevt, dev_t ddevt,
	struct walb_start_param *param);
void finalize_wdev(struct walb_dev *wdev);
void destroy_wdev(struct walb_dev *wdev);
void task_destroy_wdev(struct work_struct *task);
bool register_wdev(struct walb_dev *wdev);
void unregister_wdev(struct walb_dev *wdev);

#endif /* WALB_KERN_H_KERNEL */
