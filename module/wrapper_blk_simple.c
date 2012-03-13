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

#include "block_size.h"
#include "wrapper_blk.h"

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

/*******************************************************************************
 * Module parameters definition.
 *******************************************************************************/

module_param_named(device_str, device_str_, charp, S_IRUGO);
module_param_named(start_minor, start_minor_, int, S_IRUGO);
module_param_named(pbs, physical_block_size_, int, S_IRUGO);

/*******************************************************************************
 * Static data definition.
 *******************************************************************************/

/* Block sizes. */
struct block_sizes blksiz_;

/* Queue name */
#define WQ_IO_NAME "wrapper_blk_simple_io"

/* Workqueue for IO. */
struct workqueue_struct *wq_io_ = NULL;

/* kmem_cache name for request work. */
#define KMEM_CACHE_REQ_NAME "req_work_cache"

/* kmem_cache for request workst. */
struct kmem_cache *req_work_cache_ = NULL;

/* request work struct. */
struct req_work
{
        struct request *req;
        struct wrapper_blk_dev *wdev;
        struct work_struct work;
        /* unsigned int id; */
};

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

/* Make requrest for simpl_blk_bio_* modules. */
static void wrapper_blk_req_request_fn(struct request_queue *q);

/* Called before register. */
static bool pre_register(void);
/* Called after unregister. */
static void post_unregister(void);
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

/* Make requrest for simpl_blk_bio_* modules. */
static void wrapper_blk_req_request_fn(struct request_queue *q)
{
        struct wrapper_blk_dev *wdev = wdev_get_from_queue(q);
        struct request *req;

        req = blk_fetch_request(q);
        while (req) {
                /* LOGd("REQ: %"PRIu64" (%u)\n", (u64)blk_rq_pos(req), blk_rq_bytes(req)); */
                __blk_end_request_all(req, 0);
                req = blk_fetch_request(q);
        }
        
        /* now editing */
}

/* Called before register. */
static bool pre_register(void)
{
        LOGd("pre_register called.");

        /* Prepare kmem_cache. */
        req_work_cache_ = kmem_cache_create(
                KMEM_CACHE_REQ_NAME, sizeof(struct req_work), 0, 0, NULL);
        if (!req_work_cache_) {
                LOGe("failed to create kmem_cache.");
                goto error0;
        }
        
        /* prepare workqueue. */
        wq_io_ = alloc_workqueue(WQ_IO_NAME, WQ_MEM_RECLAIM, 0);
        if (!wq_io_) {
                LOGe("failed to allocate a workqueue.");
                goto error1;
        }

        return true;
error1:
        kmem_cache_destroy(req_work_cache_);
error0:
        return false;
}

/* Called after unregister. */
static void post_unregister(void)
{
        LOGd("post_unregister called.");

        /* finalize workqueue. */
        if (wq_io_) {
                flush_workqueue(wq_io_);
                destroy_workqueue(wq_io_);
        }

        /* Destory kmem_cache. */
        kmem_cache_destroy(req_work_cache_);
}

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
        blksiz_init(&wdev->blksiz, lbs, pbs);
        blk_queue_logical_block_size(wdev->queue, lbs);
        blk_queue_physical_block_size(wdev->queue, pbs);

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
                LOGn("Not support REQ_FLUSH.");
        }

        if (blk_queue_discard(uq)) {
                /* Accept REQ_DISCARD. */
                LOGn("Supports REQ_DISCARD.");
                q->limits.discard_granularity = PAGE_SIZE;
                q->limits.discard_granularity = wdev->blksiz.lbs;
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
        ret = wdev_register_with_req(get_minor(i), capacity, &blksiz_,
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
        if (wdev) {
                destroy_private_data(wdev);
        }
        wdev_unregister(get_minor(i));
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

/*******************************************************************************
 * Init/exit definition.
 *******************************************************************************/

static int __init wrapper_blk_init(void)
{
        blksiz_init(&blksiz_, LOGICAL_BLOCK_SIZE, physical_block_size_);

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
