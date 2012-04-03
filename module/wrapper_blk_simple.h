/**
 * wrapper_blk_simple.h - Definition for wrapper_blk_simple operations.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_WRAPPER_BLK_SIMPLE_H_KERNEL
#define WALB_WRAPPER_BLK_SIMPLE_H_KERNEL

#include "check_kernel.h"
#include <linux/types.h>
#include <linux/blkdev.h>

/* Make requrest for simpl_blk_bio_* modules. */
void wrapper_blk_req_request_fn(struct request_queue *q);

/* Called before register. */
bool pre_register(void);
/* Called after unregister. */
void post_unregister(void);

enum plug_policy {
	PLUG_PER_PLUG,
	PLUG_PER_REQ
};

/* Get policy */
enum plug_policy get_policy(void);

#endif /* WALB_WRAPPER_BLK_SIMPLE_H_KERNEL */
