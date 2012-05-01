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
#include "wrapper_blk.h"

/* Make requrest for wrapper_blk_walb_* modules. */
void wrapper_blk_req_request_fn(struct request_queue *q);

/* Called before register. */
bool pre_register(void);
/* Called after unregister. */
void post_unregister(void);

/**
 * Private data as wrapper_dev.private_data.
 */
struct pdata
{
	struct block_device *ldev; /* underlying log device. */
	struct block_device *ddev; /* underlying data device. */

	u64 next_lsid; /* next lsid.
			  queue lock must be held. */
	
	spinlock_t pending_data_lock; /* Use spin_lock() and spin_unlock(). */
	struct list_head writepack_list; /* list head of writepack.
					    pending_data_lock must be held. */
};

static inline pdata_get_from_wdev(struct wrapper_dev *wdev)
{
	return (struct pdata *)wdev->private_data;
}

/* Uitlity functions */

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

#endif /* WALB_WRAPPER_BLK_WALB_H_KERNEL */
