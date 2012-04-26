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
	struct block_device *ldev;
	struct block_device *ddev;
};

#endif /* WALB_WRAPPER_BLK_WALB_H_KERNEL */
