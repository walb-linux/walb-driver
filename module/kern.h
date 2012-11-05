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
extern struct workqueue_struct *wq_logpack_;
extern struct workqueue_struct *wq_io_;
extern struct workqueue_struct *wq_ol_;
extern struct workqueue_struct *wq_misc_;

/*
 * Minor number and partition management.
 */
#define WALB_MINORS	  16
#define WALB_MINORS_SHIFT  4
#define DEVNUM(kdevnum) ((MINOR(kdev_t_to_nr(kdevnum)) >> MINOR_SHIFT)

#if 0
/**
 * WalB IOcore interface.
 */
struct walb_iocore_operations
{
	bool (*initialize)(struct walb_dev *wdev);
	void (*finalize)(struct walb_dev *wdev);
	void (*make_request)(struct walb_dev *wdev, struct bio *bio);
	void (*stop)(struct walb_dev *wdev);
	void (*start)(struct walb_dev *wdev);
};
#endif

/**
 * The internal representation of walb and walblog device.
 */
struct walb_dev
{
	dev_t devt; /* Wrapper device id. */
	
	atomic_t is_read_only; /* Write always fails if true */
	struct list_head list; /* member of all_wdevs_ */
	
	/* Max number of snapshots.
	   This is const after log device is initialized. */
	u32 n_snapshots;
	
	/* Size of underlying devices. [logical block] */
	u64 ldev_size;
	u64 ddev_size;
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
	 */
	unsigned int ldev_chunk_sectors;
	unsigned int ddev_chunk_sectors;

	/* Super sector of log device. */
	spinlock_t lsuper0_lock;
	struct sector_data *lsuper0;

	/* To avoid lock lsuper0 during request processing. */
	u64 ring_buffer_off; 
	u64 ring_buffer_size;

	/*
	 * lsid indicators.
	 * Each variable must be accessed with lsid_lock held.
	 *
	 * latest_lsid:
	 *   This is used to generate new logpack.
	 * oldest_lsid:
	 *   All logpacks with lsid < oldest_lsid
	 *   on the log device can be overwritten.
	 * completed_lsid:
	 *   All logpacks with lsid < completed_lsid
	 *   has been written to the log device.
	 *
	 * oldest_lsid <= written_lsid <= completed_lsid <= latest_lsid.
	 */
	spinlock_t lsid_lock;
	u64 latest_lsid;
	u64 oldest_lsid;
#ifdef WALB_FAST_ALGORITHM
	u64 completed_lsid;
#endif
	
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
	 * written_lsid is contained.
	 */
	struct checkpoint_data cpd;
	
	/*
	 * For snapshotting.
	 */
	struct snapshot_data *snapd;

	/* Maximum logpack size [physical block].
	   This will be used for logpack size
	   not to be too long
	   This will avoid decrease of
	   sequential write performance. */
	unsigned int max_logpack_pb;

#ifdef WALB_FAST_ALGORITHM
	/* max_pending_sectors < pending_sectors
	   we must stop the queue. */
	unsigned int max_pending_sectors; 

	/* min_pending_sectors > pending_sectors
	   we can restart the queue. */	
	unsigned int min_pending_sectors;

	/* queue stopped period must not exceed
	   queue_stop_time_ms. */
	unsigned int queue_stop_timeout_ms; 
#endif
	
	/*
	 * For IOcore.
	 */
	void *private_data;
};

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

/*******************************************************************************
 * Prototypes defined in walb.c
 *******************************************************************************/

struct walb_dev* prepare_wdev(
	unsigned int minor, dev_t ldevt, dev_t ddevt,
	struct walb_start_param *param);
void destroy_wdev(struct walb_dev *wdev);
void register_wdev(struct walb_dev *wdev);
void unregister_wdev(struct walb_dev *wdev);


#endif /* WALB_KERN_H_KERNEL */
