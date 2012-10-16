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

/**
 * The internal representation of walb and walblog device.
 */
struct walb_dev
{
	u64 size;			/* Device size in bytes */
	u8 *data;			/* The data array */
	int users;			/* How many users */
	spinlock_t lock;		/* For queue access. */
	struct request_queue *queue;	/* The device request queue */
	struct gendisk *gd;		/* The gendisk structure */

	atomic_t is_read_only;		/* Write always fails if true */

	struct list_head list; /* member of all_wdevs_ */
	
	/* Max number of snapshots.
	   This is const after log device is initialized. */
	u32 n_snapshots;
	
	/* Size of underlying devices. [logical block] */
	u64 ldev_size;
	u64 ddev_size;
	
	/* You can get sector size with
	   bdev_logical_block_size(bdev) and
	   bdev_physical_block_size(bdev).

	   Those of underlying log device and data device
	   must be same.
	*/
	u16 logical_bs;
	u16 physical_bs;

	/* Wrapper device id. */
	dev_t devt;
	
	/* Underlying block devices */
	struct block_device *ldev;
	struct block_device *ddev;

	/* Latest lsid and its lock. */
	spinlock_t latest_lsid_lock;
	u64 latest_lsid;

	/* Spinlock for lsuper0 access.
	   Irq handler must not lock this.
	   Use spin_lock().
	*/
	spinlock_t lsuper0_lock;
	/* Super sector of log device. */
	struct sector_data *lsuper0;

	/* Oldest lsid to manage log area overflow. */
	spinlock_t oldest_lsid_lock;
	u64 oldest_lsid;

#ifdef WALB_FAST_ALGORITHM
	spinlock_t completed_lsid_lock;
	u64 completed_lsid;
#endif
	
	/*
	 * For wrapper log device.
	 */
	/* spinlock_t log_queue_lock; */
	struct request_queue *log_queue;
	struct gendisk *log_gd;

	/*
	 * For checkpointing.
	 */
	struct checkpoint_data cpd;
	
	/*
	 * For snapshotting.
	 */
	struct snapshot_data *snapd;

	/*
	 * IO driver can use this.
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

struct walb_dev* prepare_wdev(unsigned int minor,
			dev_t ldevt, dev_t ddevt, const char* name);
void destroy_wdev(struct walb_dev *wdev);
void register_wdev(struct walb_dev *wdev);
void unregister_wdev(struct walb_dev *wdev);


#endif /* WALB_KERN_H_KERNEL */
