/**
 * wrapper_blk_walb.h - Definition for wrapper_blk_walb operations.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_WRAPPER_BLK_WALB_H_KERNEL
#define WALB_WRAPPER_BLK_WALB_H_KERNEL

#include "check_kernel.h"
#include <linux/types.h>
#include <linux/blkdev.h>
#include <linux/list.h>
#include <linux/spinlock.h>
#include <linux/mutex.h>
#include "walb/sector.h"
#include "wrapper_blk.h"
#include "treemap.h"

/* Make requrest for wrapper_blk_walb_* modules. */
void wrapper_blk_req_request_fn(struct request_queue *q);

/* Called before register. */
bool pre_register(void);

/* Called before unregister */
void pre_unregister(void);

/* Called after unregister. */
void post_unregister(void);

/**
 * Private data as wrapper_blk_dev.private_data.
 */
struct pdata
{
	struct block_device *ldev; /* underlying log device. */
	struct block_device *ddev; /* underlying data device. */

	spinlock_t lsid_lock;
	u64 latest_lsid; /* latest lsid.
			    This is lsid of next created logpack.
			    lsid_lock must be held. */
	u64 oldest_lsid; /* oldest lsid.
			    All previous logpacks of the logpack with
			    the oldest lsid can be overwritten.
			    lsid_lock must be held. */
	u64 written_lsid; /* written lsid.
			     All previous logpacks of the logpack with
			     the written_lsid have been stored.
			     lsid_lock must be held. */
	
	spinlock_t lsuper0_lock; /* Use spin_lock() and spin_unlock(). */
	struct sector_data *lsuper0; /* lsuper0_lock must be held
					to access the sector image. */

	/* To avoid lock lsuper0 during request processing. */
	u64 ring_buffer_off; 
	u64 ring_buffer_size;
	
	/* bit 0: all write must failed.
	   bit 1: logpack submit task working.
	   bit 2: logpack wait task working. */
	unsigned long flags;

	/* chunk sectors.
	   if chunk_sectors > 0:
	     (1) bio size must not exceed the size.
	     (2) bio must not cross over multiple chunks.
	   else:
	     no limitation. */
	unsigned int ldev_chunk_sectors;
	unsigned int ddev_chunk_sectors;

	spinlock_t logpack_submit_queue_lock;
	struct list_head logpack_submit_queue; /* writepack list.
						  logpack_submit_queue_lock
						  must be held. */
	
	spinlock_t logpack_wait_queue_lock;
	struct list_head logpack_wait_queue; /* writepack list.
						logpack_wait_queue_lock
						must be held. */

#ifdef WALB_OVERLAPPING_SERIALIZE
	/**
	 * All req_entry data may not keep reqe->bio_ent_list.
	 * You must keep address and size information in another way.
	 */
	struct mutex overlapping_data_mutex; /* Use mutex_lock()/mutex_unlock(). */
	struct multimap *overlapping_data; /* key: blk_rq_pos(req),
					      val: pointer to req_entry. */
#endif
	
#ifdef WALB_FAST_ALGORITHM
	/**
	 * All req_entry data must keep
	 * reqe->bio_ent_list while they are stored in the pending_data.
	 */
	struct mutex pending_data_mutex; /* Use mutex_lock()/mutex_unlock(). */
	struct multimap *pending_data; /* key: blk_rq_pos(req),
					  val: pointer to req_entry. */
	unsigned int pending_sectors; /* Number of sectors pending
					 [logical block]. */
	unsigned int max_pending_sectors; /* max_pending_sectors < pending_sectors
					     we must stop the queue. */
	unsigned int min_pending_sectors; /* min_pending_sectors > pending_sectors
					     we can restart the queue. */
	unsigned int queue_stop_timeout_ms; /* queue stopped period must not exceed
					       queue_stop_time_ms. */
	unsigned long queue_restart_jiffies; /* For queue stopped timeout check. */
	bool is_queue_stopped; /* true if queue is stopped. */
#endif
};

/* pdata->flags bit. */
#define PDATA_STATE_READ_ONLY            0
#define PDATA_STATE_SUBMIT_TASK_WORKING 1
#define PDATA_STATE_WAIT_TASK_WORKING   2

/*******************************************************************************
 * Prototypes.
 *******************************************************************************/

/*******************************************************************************
 * Utility functions.
 *******************************************************************************/

static inline struct pdata* pdata_get_from_wdev(struct wrapper_blk_dev *wdev)
{
	return (struct pdata *)wdev->private_data;
}

/**
 * Check two requests are overlapping.
 */
static inline bool is_overlap_req(struct request *req0, struct request *req1)
{
	ASSERT(req0);
	ASSERT(req1);
	ASSERT(req0 != req1);

	return (blk_rq_pos(req0) + blk_rq_sectors(req0) > blk_rq_pos(req1) &&
		blk_rq_pos(req1) + blk_rq_sectors(req1) > blk_rq_pos(req0));
}

/**
 * Check read-only mode.
 */
static inline int is_read_only_mode(struct pdata *pdata)
{
	return test_bit(PDATA_STATE_READ_ONLY, &pdata->flags);
}

/**
 * Set read-only mode.
 */
static inline void set_read_only_mode(struct pdata *pdata)
{
	set_bit(PDATA_STATE_READ_ONLY, &pdata->flags);
}

/**
 * Clear read-only mode.
 */
static inline void clear_read_only_mode(struct pdata *pdata)
{
	clear_bit(PDATA_STATE_READ_ONLY, &pdata->flags);
}

#endif /* WALB_WRAPPER_BLK_WALB_H_KERNEL */
