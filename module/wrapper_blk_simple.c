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

#include "block_size.h"
#include "wrapper_blk.h"

/* #define PERFORMANCE_DEBUG */

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

/* kmem_cache for request work. */
#define KMEM_CACHE_REQ_NAME "req_work_cache"
struct kmem_cache *req_work_cache_ = NULL;

/* request work struct. */
struct req_work
{
        struct request *req;
        struct wrapper_blk_dev *wdev;
        struct work_struct work;
#ifdef PERFORMANCE_DEBUG
        unsigned int id;
#endif
};

/* kmem cache for dbio. */
#define KMEM_CACHE_BIO_ENTRY_NAME "bio_entry_cache"
struct kmem_cache *bio_entry_cache_ = NULL;

/* bio as a list entry. */
struct bio_entry
{
        struct bio *bio;
        struct list_head list;
        struct completion done;
        int error; /* bio error status. */
};

#ifdef PERFORMANCE_DEBUG
static atomic_t id_counter_ = ATOMIC_INIT(0);
#endif

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

/* Create/destroy req_work. */
static struct req_work* create_req_work(
        struct request *req,
        struct wrapper_blk_dev *wdev,
        gfp_t gfp_mask,
        void (*worker)(struct work_struct *work));
static void destroy_req_work(struct req_work *work);

/* bio_entry related. */
static void bio_entry_end_io(struct bio *bio, int error);
static struct bio_entry* create_bio_entry(struct bio *bio, struct block_device *bdev);
static void destroy_bio_entry(struct bio_entry *bioe);

/* Request executing task in paralell. */
static void req_work_task(struct work_struct *work);

/* Forward a request as workqueue task. */
static void forward_request_as_wq_task(struct wrapper_blk_dev *wdev, struct request *req);

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

/**
 * Create a req_work.
 *
 * @req started request.
 *
 * RETURN:
 * NULL if failed.
 * CONTEXT:
 * Any.
 */
static struct req_work* create_req_work(
        struct request *req,
        struct wrapper_blk_dev *wdev,
        gfp_t gfp_mask,
        void (*worker)(struct work_struct *work))
{
        struct req_work *work;

        ASSERT(req);
        ASSERT(wdev);
        ASSERT(req_work_cache_);

        work = kmem_cache_alloc(req_work_cache_, gfp_mask);
        if (!work) {
                goto error0;
        }
        work->req = req;
        work->wdev = wdev;
#ifdef PERFORMANCE_DEBUG
        work->id = atomic_inc_return(&id_counter_);
#endif
        INIT_WORK(&work->work, worker);
        return work;
error0:
        return NULL;
}

/**
 * Destory a bio_work.
 */
static void destroy_req_work(struct req_work *work)
{
        if (work) {
                kmem_cache_free(req_work_cache_, work);
        }
}


/**
 * endio callback for bio_entry.
 */
static void bio_entry_end_io(struct bio *bio, int error)
{
        struct bio_entry *bioe = bio->bi_private;
        ASSERT(bioe);

        LOGd("bio_entry_end_io() begin.\n");
        
        bioe->error = error;
        complete(&bioe->done);

        LOGd("bio_entry_end_io() end.\n");
}

/**
 * Create a bio_entry.
 *
 * @bio original bio.
 * @bdev block device to forward bio.
 */
static struct bio_entry* create_bio_entry(struct bio *bio, struct block_device *bdev)
{
        struct bio_entry *bioe;
        struct bio *biotmp;

        LOGd("create_bio_entry() begin.\n");

        bioe = kmem_cache_alloc(bio_entry_cache_, GFP_NOIO);
        if (!bioe) {
                LOGd("kmem_cache_alloc() failed.");
                goto error0;
        }
        init_completion(&bioe->done);
        bioe->error = 0;

        /* clone bio */
        bioe->bio = NULL;
        biotmp = bio_clone(bio, GFP_NOIO);
        if (!biotmp) {
                LOGd("bio_clone() failed.");
                goto error1;
        }
        biotmp->bi_bdev = bdev;
        biotmp->bi_end_io = bio_entry_end_io;
        biotmp->bi_private = bioe;
        bioe->bio = biotmp;

        LOGd("create_bio_entry() end.\n");
        return bioe;

error1:
        destroy_bio_entry(bioe);
error0:
        LOGd("create_bio_entry() end with error.\n");
        return NULL;
}

/**
 * Destroy a bio_entry.
 */
static void destroy_bio_entry(struct bio_entry *bioe)
{
        LOGd("destroy_bio_entry() begin.\n");
        
        if (!bioe) {
                return;
        }

        if (bioe->bio) {
                LOGd("bio_put %p\n", bioe->bio);
                bio_put(bioe->bio);
        }
        kmem_cache_free(bio_entry_cache_, bioe);

        LOGd("destroy_bio_entry() end.\n");
}


/**
 * Execute a request.
 *
 * CONTEXT:
 *   Non-IRQ.
 *   Request queue lock is not held.
 *   Other tasks may be running concurrently.
 */
static void req_work_task(struct work_struct *work)
{
        struct req_work *req_work = container_of(work, struct req_work, work);
        struct wrapper_blk_dev *wdev = req_work->wdev;
        struct request *req = req_work->req;
        struct block_device *bdev = wdev->private_data;
        struct blk_plug plug;

        struct bio *bio;
        struct bio_entry *bioe, *next;
        struct list_head list;
        int err;
        
        LOGd("req_work_task begin.\n");
        
        INIT_LIST_HEAD(&list);

#if 1
#ifdef PERFORMANCE_DEBUG
        LOGd("REQ %u: %"PRIu64" (%u).\n", req_work->id, (u64)blk_rq_pos(req), blk_rq_bytes(req));
#else
        LOGd("REQ: %"PRIu64" (%u).\n", (u64)blk_rq_pos(req), blk_rq_bytes(req));
#endif
#endif

        /* clone and submit bios. */
        blk_start_plug(&plug);
        __rq_for_each_bio(bio, req) {
        
                /* clone bio */
                bioe = create_bio_entry(bio, bdev);
                if (!bioe) {
                        LOGd("create_bio_entry() failed.\n");
                        goto error0;
                }
                LOGd("list_add_tail\n");
                list_add_tail(&bioe->list, &list);

                /* submit bio */
                LOGd("submit bio %"PRIu64" %u\n", (u64)bio->bi_sector, bio->bi_size);
                generic_make_request(bioe->bio);
        }
        blk_finish_plug(&plug);
        
        /* wait comletion and destroy of all bio_entry s. */
        err = 0;
        list_for_each_entry(bioe, &list, list) {
                wait_for_completion(&bioe->done);
                /* ASSERT(bioe->bio->bi_size == blk_rq_cur_bytes(req)); */
                /* blk_end_request_cur(req, bioe->error); */
                if (bioe->error) { err = bioe->error; }
        }
        /* ASSERT(list_empty(&list)); */
        blk_end_request_all(req, err);

        list_for_each_entry(bioe, &list, list) {
                destroy_bio_entry(bioe);
        }
        /* list_for_each_entry_safe(bioe, next, &list, list) { */
        /*         list_del(&bioe->list); */
        /*         destroy_bio_entry(bioe); */
        /* } */
        
        destroy_req_work(req_work);

        LOGd("req_work_task end.\n");
        return;

error0:
        LOGd("req_work_task error handler.\n");
        list_for_each_entry_safe(bioe, next, &list, list) {
                destroy_bio_entry(bioe);
        }
        blk_end_request_all(req, -EIO);
        LOGd("req_work_task end with error.\n");
}

/**
 * Forward a request as a workqueue task.
 *
 * CONTEXT:
 *   Request queue lock is held.
 */
static void forward_request_as_wq_task(struct wrapper_blk_dev *wdev, struct request *req)
{
        struct req_work *req_work;
        
        LOGd("forward_request_as_wq_task begin.\n");
        
        ASSERT(wdev);
        ASSERT(!(req->cmd_flags & REQ_FUA)); /* Currently REQ_FUA is not supported. */
        ASSERT(!(req->cmd_flags & REQ_FLUSH)); /* REQ_FLUSH must be processed before. */
        
        /* Prepare a task. */
        req_work = create_req_work(req, wdev, GFP_NOIO, req_work_task);
        if (!req_work) {
                LOGd("create_req_work() failed.");
                goto error0;
        }
        /* Enqueue the task. */
        queue_work(wq_io_, &req_work->work);

        LOGd("forward_request_as_wq_task end.\n");
        return;
error0:
        __blk_end_request_all(req, -EIO);
        LOGd("forward_request_as_wq_task end with errors.\n");
}

/**
 * Make requrest callback.
 *
 * CONTEXT: non-IRQ (2.6.39 or later).
 */
static void wrapper_blk_req_request_fn(struct request_queue *q)
{
        struct wrapper_blk_dev *wdev = wdev_get_from_queue(q);
        struct request *req;

        req = blk_fetch_request(q);
        while (req) {
                /* LOGd("REQ: %"PRIu64" (%u)\n", (u64)blk_rq_pos(req), blk_rq_bytes(req)); */

                if (req->cmd_flags & REQ_FLUSH) {
                        flush_workqueue(wq_io_);
                        ASSERT(blk_rq_bytes(req) == 0);
                        __blk_end_request_all(req, 0);
                } else {
                        forward_request_as_wq_task(wdev, req);
                }
                req = blk_fetch_request(q);
        }
}

/* Called before register. */
static bool pre_register(void)
{
        LOGd("pre_register called.");

        /* Prepare kmem_cache for req_work. */
        req_work_cache_ = kmem_cache_create(
                KMEM_CACHE_REQ_NAME, sizeof(struct req_work), 0, 0, NULL);
        if (!req_work_cache_) {
                LOGe("failed to create kmem_cache.");
                goto error0;
        }

        /* Prepare kmem_cache for bio_entry. */
        bio_entry_cache_ = kmem_cache_create(
                KMEM_CACHE_BIO_ENTRY_NAME, sizeof(struct bio_entry), 0, 0, NULL);
        if (!bio_entry_cache_) {
                LOGe("failed to create kmem_cache for bio_entry.");
                goto error1;
        }
        
        /* prepare workqueue. */
        wq_io_ = alloc_workqueue(WQ_IO_NAME, WQ_MEM_RECLAIM, 0);
        /* wq_io_ = create_singlethread_workqueue(WQ_IO_NAME); */
        if (!wq_io_) {
                LOGe("failed to allocate a workqueue.");
                goto error2;
        }

        return true;
#if 0
error3:
        destroy_workqueue(wq_io_);
#endif
error2:
        kmem_cache_destroy(bio_entry_cache_);
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

        /* Destory kmem_cache data. */
        kmem_cache_destroy(bio_entry_cache_);
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
