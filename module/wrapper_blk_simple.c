/**
 * wrapper_blk_simple.c - Simple wrapper block device.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>
#include <linux/blkdev.h>
#include <linux/atomic.h>
#include <linux/list.h>
#include <linux/completion.h>
#include <linux/delay.h>

#include "walb/block_size.h"
#include "wrapper_blk.h"
#include "wrapper_blk_simple.h"

/*******************************************************************************
 * Module variables definition.
 *******************************************************************************/

/* Device size list string. The unit of each size is bytes. */
char *device_str_ = "/dev/simple_blk/0";
/* Minor id start. */
int start_minor_ = 0;

/* Logical block size is 512. */
#define LOGICAL_BLOCK_SIZE 512
/* Physical block size. */
int physical_block_size_ = 4096;

/**
 * Plugging policy.
 * 'plug_per_plug' or 'plug_per_req'.
 */
char *plug_policy_str_ = "plug_per_plug";

/*******************************************************************************
 * Module parameters definition.
 *******************************************************************************/

module_param_named(device_str, device_str_, charp, S_IRUGO);
module_param_named(start_minor, start_minor_, int, S_IRUGO);
module_param_named(pbs, physical_block_size_, int, S_IRUGO);
module_param_named(plug_policy, plug_policy_str_, charp, S_IRUGO);
	
/*******************************************************************************
 * Static data definition.
 *******************************************************************************/

/* Policy. */
enum plug_policy plug_policy_;

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

/* Create private data for wdev. */
static bool create_private_data(struct wrapper_blk_dev *wdev);
/* Destroy private data for ssev. */
static void destroy_private_data(struct wrapper_blk_dev *wdev);
/* Customize wdev after register before start. */
static void customize_wdev(struct wrapper_blk_dev *wdev);

static unsigned int get_minor(unsigned int id);
static bool register_dev(void);
static void unregister_dev(void);
static bool start_dev(void);
static void stop_dev(void);

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/


/* Create private data for wdev. */
static bool create_private_data(struct wrapper_blk_dev *wdev)
{
        struct block_device *bdev;
        unsigned int lbs, pbs;
        
        LOGd("create_private_data called");

        /* open underlying device. */
        bdev = blkdev_get_by_path(
                device_str_, FMODE_READ|FMODE_WRITE|FMODE_EXCL,
                create_private_data);
        if (IS_ERR(bdev)) {
                LOGe("open %s failed.", device_str_);
                return false;
        }
        wdev->private_data = bdev;

        /* capacity */
        wdev->capacity = get_capacity(bdev->bd_disk);
        set_capacity(wdev->gd, wdev->capacity);

        /* Block size */
        lbs = bdev_logical_block_size(bdev);
        pbs = bdev_physical_block_size(bdev);
        
        if (lbs != LOGICAL_BLOCK_SIZE) {
                goto error0;
        }
	wdev->pbs = pbs;
        blk_queue_logical_block_size(wdev->queue, lbs);
        blk_queue_physical_block_size(wdev->queue, pbs);

        blk_queue_stack_limits(wdev->queue, bdev_get_queue(bdev));

        return true;
error0:
        blkdev_put(wdev->private_data, FMODE_READ|FMODE_WRITE|FMODE_EXCL);
        return false;
}

/* Destroy private data for ssev. */
static void destroy_private_data(struct wrapper_blk_dev *wdev)
{
        LOGd("destoroy_private_data called.");

        /* close underlying device. */
        blkdev_put(wdev->private_data, FMODE_READ|FMODE_WRITE|FMODE_EXCL);
}

/* Customize wdev after register before start. */
static void customize_wdev(struct wrapper_blk_dev *wdev)
{
        struct request_queue *q, *uq;
        ASSERT(wdev);
        q = wdev->queue;

        uq = bdev_get_queue(wdev->private_data);
        /* Accept REQ_FLUSH and REQ_FUA. */
        if (uq->flush_flags & REQ_FLUSH) {
                if (uq->flush_flags & REQ_FUA) {
                        LOGn("Supports REQ_FLUSH | REQ_FUA.");
                        blk_queue_flush(q, REQ_FLUSH | REQ_FUA);
                } else {
                        LOGn("Supports REQ_FLUSH.");
                        blk_queue_flush(q, REQ_FLUSH);
                }
        } else {
                LOGn("Not support REQ_FLUSH (but support).");
		blk_queue_flush(q, REQ_FLUSH);
        }

        if (blk_queue_discard(uq)) {
                /* Accept REQ_DISCARD. */
                LOGn("Supports REQ_DISCARD.");
                q->limits.discard_granularity = PAGE_SIZE;
                q->limits.discard_granularity = LOGICAL_BLOCK_SIZE;
                q->limits.max_discard_sectors = UINT_MAX;
                q->limits.discard_zeroes_data = 1;
                queue_flag_set_unlocked(QUEUE_FLAG_DISCARD, q);
                /* queue_flag_set_unlocked(QUEUE_FLAG_SECDISCARD, q); */
        } else {
                LOGn("Not support REQ_DISCARD.");
        }
}

static unsigned int get_minor(unsigned int id)
{
        return (unsigned int)start_minor_ + id;
}

static bool register_dev(void)
{
        unsigned int i = 0;
        u64 capacity = 0;
        bool ret;
        struct wrapper_blk_dev *wdev;

        LOGe("register_dev begin");
        
        /* capacity must be set lator. */
        ret = wdev_register_with_req(get_minor(i), capacity, physical_block_size_,
				wrapper_blk_req_request_fn);
                
        if (!ret) {
                goto error;
        }
        wdev = wdev_get(get_minor(i));
        if (!create_private_data(wdev)) {
                goto error;
        }
        customize_wdev(wdev);

        LOGe("register_dev end");

        return true;
error:
        unregister_dev();
        return false;
}

static void unregister_dev(void)
{
        unsigned int i = 0;
        struct wrapper_blk_dev *wdev;
        
        wdev = wdev_get(get_minor(i));
        wdev_unregister(get_minor(i));
        if (wdev) {
                destroy_private_data(wdev);
		FREE(wdev);
        }
}

static bool start_dev(void)
{
        unsigned int i = 0;

        if (!wdev_start(get_minor(i))) {
                goto error;
        }
        return true;
error:
        stop_dev();
        return false;
}


static void stop_dev(void)
{
        unsigned int i = 0;
        
        wdev_stop(get_minor(i));
}

static void set_policy(void)
{
	if (strcmp(plug_policy_str_, "plug_per_req") == 0) {
		plug_policy_ = PLUG_PER_REQ;
		LOGn("plug_policy: plug_per_req\n");
	} else {
		plug_policy_ = PLUG_PER_PLUG;
		LOGn("plug_policy: plug_per_plug\n");
	}
}

/*******************************************************************************
 * Global function definition.
 *******************************************************************************/

enum plug_policy get_policy(void)
{
	return plug_policy_;
}

/*******************************************************************************
 * Init/exit definition.
 *******************************************************************************/

static int __init wrapper_blk_init(void)
{
	if (!is_valid_pbs(physical_block_size_)) {
		goto error0;
	}

	set_policy();
	
        pre_register();
        
        if (!register_dev()) {
                goto error0;
        }
        if (!start_dev()) {
                goto error1;
        }

        return 0;
#if 0
error2:
        stop_dev();
#endif
error1:
        unregister_dev();
error0:
        return -1;
}

static void wrapper_blk_exit(void)
{
        stop_dev();
        unregister_dev();
        post_unregister();
}

module_init(wrapper_blk_init);
module_exit(wrapper_blk_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Simple block req device for Test");
MODULE_ALIAS("wrapper_blk_req");
