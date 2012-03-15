/**
 * wrapper_blk_simple_plug_per_plug.c - Simple wrapper block device.
 * 'plug_per_req' means plugging underlying device per request.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/blkdev.h>
#include <linux/atomic.h>
#include <linux/list.h>
#include <linux/completion.h>
#include <linux/delay.h>

#include "block_size.h"
#include "wrapper_blk.h"
#include "wrapper_blk_simple.h"

#define PERFORMANCE_DEBUG

/*******************************************************************************
 * Static data definition.
 *******************************************************************************/

/* Workqueue for request finalizer. */
#define WQ_REQ_FIN_NAME "wq_req_fin"
struct workqueue_struct *wq_req_fin_ = NULL;

/* kmem_cache for request work. */
#define KMEM_CACHE_REQ_FIN_WORK_NAME "req_fin_work_cache"
struct kmem_cache *req_fin_work_cache_ = NULL;

/* request finalization work struct. */
struct req_fin_work
{
        struct work_struct work;
        struct request *req;
        struct wrapper_blk_dev *wdev;
        struct list_head bio_entry_list; /* list head of bio_entry */
#ifdef PERFORMANCE_DEBUG
        unsigned int id;
#endif
};

#ifdef PERFORMANCE_DEBUG
/* for identifier of workqueue tasks. */
static atomic_t wq_id_counter_ = ATOMIC_INIT(0);
#endif

/* kmem cache for dbio. */
#define KMEM_CACHE_BIO_ENTRY_NAME "bio_entry_cache"
struct kmem_cache *bio_entry_cache_ = NULL;

/* bio as a list entry. */
struct bio_entry
{
        struct list_head list; /* list entry */
        struct bio *bio;
        struct completion done;
        unsigned int bi_size; /* keep bi_size at initialization,
                                 because bio->bi_size will be 0 after endio. */
        int error; /* bio error status. */
#ifdef PERFORMANCE_DEBUG
        unsigned int id;
#endif
};

#ifdef PERFORMANCE_DEBUG
/* for identifier of bio_entry. */
static atomic_t bio_entry_id_counter_ = ATOMIC_INIT(0);
#endif

/* number of pending request. */
static atomic_t number_of_pending_req_ = ATOMIC_INIT(0);
/* supports 10000 * 10 = 1,000,000 IOPS */
#define MAX_PENDING_REQUEST 10000
#define POLLING_WAIT_IN_MS 10

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

/* Print request flags for debug. */
static void print_req_flags(struct request *req);

/* Create/destroy req_fin_work. */
static struct req_fin_work* create_req_fin_work(
        struct request *req,
        struct wrapper_blk_dev *wdev,
        gfp_t gfp_mask,
        void (*worker)(struct work_struct *work));
static void destroy_req_fin_work(struct req_fin_work *work);


/* bio_entry related. */
static void bio_entry_end_io(struct bio *bio, int error);
static struct bio_entry* create_bio_entry(struct bio *bio, struct block_device *bdev);
static void destroy_bio_entry(struct bio_entry *bioe);

/* Request finalization task in parallel. */
static void req_fin_work_task(struct work_struct *work);

/* Execute normal request. */
static void exec_normal_req(struct request *req, struct wrapper_blk_dev *wdev);

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * Print request flags for debug.
 */
static void print_req_flags(struct request *req)
{
        LOGd("REQ_FLAGS: "
             "%s%s%s%s%s"
             "%s%s%s%s%s"
             "%s%s%s%s%s"
             "%s%s%s%s%s"
             "%s%s%s%s%s"
             "%s%s%s%s\n", 
             ((req->cmd_flags & REQ_WRITE) ?              "REQ_WRITE" : ""),
             ((req->cmd_flags & REQ_FAILFAST_DEV) ?       " REQ_FAILFAST_DEV" : ""),
             ((req->cmd_flags & REQ_FAILFAST_TRANSPORT) ? " REQ_FAILFAST_TRANSPORT" : ""),
             ((req->cmd_flags & REQ_FAILFAST_DRIVER) ?    " REQ_FAILFAST_DRIVER" : ""),
             ((req->cmd_flags & REQ_SYNC) ?               " REQ_SYNC" : ""),
             ((req->cmd_flags & REQ_META) ?               " REQ_META" : ""),
             ((req->cmd_flags & REQ_PRIO) ?               " REQ_PRIO" : ""),
             ((req->cmd_flags & REQ_DISCARD) ?            " REQ_DISCARD" : ""),
             ((req->cmd_flags & REQ_NOIDLE) ?             " REQ_NOIDLE" : ""),
             ((req->cmd_flags & REQ_RAHEAD) ?             " REQ_RAHEAD" : ""),
             ((req->cmd_flags & REQ_THROTTLED) ?          " REQ_THROTTLED" : ""),
             ((req->cmd_flags & REQ_SORTED) ?             " REQ_SORTED" : ""),
             ((req->cmd_flags & REQ_SOFTBARRIER) ?        " REQ_SOFTBARRIER" : ""),
             ((req->cmd_flags & REQ_FUA) ?                " REQ_FUA" : ""),
             ((req->cmd_flags & REQ_NOMERGE) ?            " REQ_NOMERGE" : ""),
             ((req->cmd_flags & REQ_STARTED) ?            " REQ_STARTED" : ""),
             ((req->cmd_flags & REQ_DONTPREP) ?           " REQ_DONTPREP" : ""),
             ((req->cmd_flags & REQ_QUEUED) ?             " REQ_QUEUED" : ""),
             ((req->cmd_flags & REQ_ELVPRIV) ?            " REQ_ELVPRIV" : ""),
             ((req->cmd_flags & REQ_FAILED) ?             " REQ_FAILED" : ""),
             ((req->cmd_flags & REQ_QUIET) ?              " REQ_QUIET" : ""),
             ((req->cmd_flags & REQ_PREEMPT) ?            " REQ_PREEMPT" : ""),
             ((req->cmd_flags & REQ_ALLOCED) ?            " REQ_ALLOCED" : ""),
             ((req->cmd_flags & REQ_COPY_USER) ?          " REQ_COPY_USER" : ""),
             ((req->cmd_flags & REQ_FLUSH) ?              " REQ_FLUSH" : ""),
             ((req->cmd_flags & REQ_FLUSH_SEQ) ?          " REQ_FLUSH_SEQ" : ""),
             ((req->cmd_flags & REQ_IO_STAT) ?            " REQ_IO_STAT" : ""),
             ((req->cmd_flags & REQ_MIXED_MERGE) ?        " REQ_MIXED_MERGE" : ""),
             ((req->cmd_flags & REQ_SECURE) ?             " REQ_SECURE" : ""));
}

/**
 * Create a req_fin_work.
 *
 * @req started request.
 *
 * RETURN:
 * NULL if failed.
 * CONTEXT:
 * Any.
 */
static struct req_fin_work* create_req_fin_work(
        struct request *req,
        struct wrapper_blk_dev *wdev,
        gfp_t gfp_mask,
        void (*worker)(struct work_struct *work))
{
        struct req_fin_work *work;

        ASSERT(req);
        ASSERT(wdev);
        ASSERT(req_fin_work_cache_);

        work = kmem_cache_alloc(req_fin_work_cache_, gfp_mask);
        if (!work) {
                goto error0;
        }
        work->req = req;
        work->wdev = wdev;
        INIT_LIST_HEAD(&work->bio_entry_list);
        
#ifdef PERFORMANCE_DEBUG
        work->id = atomic_inc_return(&wq_id_counter_);
#endif
        INIT_WORK(&work->work, worker);
        return work;
error0:
        return NULL;
}

/**
 * Destory a bio_work.
 */
static void destroy_req_fin_work(struct req_fin_work *work)
{
        if (work) {
                kmem_cache_free(req_fin_work_cache_, work);
        }
}

/**
 * endio callback for bio_entry.
 */
static void bio_entry_end_io(struct bio *bio, int error)
{
        struct bio_entry *bioe = bio->bi_private;
        ASSERT(bioe);

#ifdef PERFORMANCE_DEBUG
        LOGd("bio_entry_end_io() %u begin.\n", bioe->id);
#else
        LOGd("bio_entry_end_io() begin.\n");
#endif
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
        bioe->bi_size = bio->bi_size;

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

#ifdef PERFORMANCE_DEBUG
        bioe->id = atomic_inc_return(&bio_entry_id_counter_);
#endif
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
 * Finalize the request.
 * (1) wait completion of all bios related the request.
 * (2) notify completion to the block layer.
 * (3) free memories.
 *
 * CONTEXT:
 *   Non-IRQ.
 *   Request queue lock is not held.
 *   Other tasks may be running concurrently.
 */
static void req_fin_work_task(struct work_struct *work)
{
        struct req_fin_work *req_fin_work = container_of(work, struct req_fin_work, work);
        struct request *req = req_fin_work->req;
        struct bio_entry *bioe, *next;
        int remaining = blk_rq_bytes(req);
        
        LOGd("req_fin_work_task begin.\n");
        
        /* wait comletion and destroy of all bio_entry s. */
        list_for_each_entry_safe(bioe, next, &req_fin_work->bio_entry_list, list) {
                wait_for_completion(&bioe->done);
                blk_end_request(req, bioe->error, bioe->bi_size);
                remaining -= bioe->bi_size;
                list_del(&bioe->list);
                destroy_bio_entry(bioe);
        }
        ASSERT(remaining == 0);
        destroy_req_fin_work(req_fin_work);
        atomic_dec(&number_of_pending_req_);
        
        LOGd("req_fin_work_task end.\n");
}

/**
 * Execute a normal request.
 * (1) create request finalize work.
 * (2) clone all bios in the request.
 * (3) submit all cloned bios.
 * (4) enqueue request finalize work.
 *
 * CONTEXT:
 *   Non-IRQ.
 *   Request queue lock is held.
 *   (Other tasks may be running concurrently?)
 */
static void exec_normal_req(struct request *req, struct wrapper_blk_dev *wdev)
{
        struct block_device *bdev = wdev->private_data;
        struct req_fin_work *work;
        struct bio *bio = NULL;
        struct bio_entry *bioe, *next;
        
        LOGd("exec_normal_req begin\n");

        ASSERT(wdev);
        ASSERT(!(req->cmd_flags & REQ_FUA)); /* Currently REQ_FUA is not supported. */
        ASSERT(!(req->cmd_flags & REQ_FLUSH)); /* REQ_FLUSH must be processed before. */
        
        LOGd("REQ: %"PRIu64" (%u).\n", (u64)blk_rq_pos(req), blk_rq_bytes(req));
        
        /* create req_fin_work. */
        work = create_req_fin_work(req, wdev, GFP_NOIO, req_fin_work_task);
        if (!work) {
                LOGe("create_req_fin_work() failed.\n");
                goto error0;
        }
        
        /* clone all bios. */
        __rq_for_each_bio(bio, req) {
                /* clone bio */
                bioe = create_bio_entry(bio, bdev);
                if (!bioe) {
                        LOGd("create_bio_entry() failed.\n");
                        goto error1;
                }
                LOGd("list_add_tail\n");
                list_add_tail(&bioe->list, &work->bio_entry_list);
        }
        LOGd("all bioe is created.\n");

        /* submit all bios. */
        list_for_each_entry(bioe, &work->bio_entry_list, list) {
                bio = bioe->bio;
#ifdef PERFORMANCE_DEBUG
                LOGd("submit bio %u %"PRIu64" %u (rw %lu)\n",
                     bioe->id, (u64)bio->bi_sector, bio->bi_size, bio_rw(bio));
#else
                LOGd("submit bio %"PRIu64" %u (rw %lu)\n",
                     (u64)bio->bi_sector, bio->bi_size, bio_rw(bio));
#endif
                generic_make_request(bio);
        }

        /* enqueue req_fin_work. */
        queue_work(wq_req_fin_, &work->work);

        LOGd("exec_normal_req end.\n");
        return;
error1:
        list_for_each_entry_safe(bioe, next, &work->bio_entry_list, list) {
                list_del(&bioe->list);
                destroy_bio_entry(bioe);
        }
        __blk_end_request_all(req, -EIO);
        atomic_dec(&number_of_pending_req_);
        LOGd("exec_normal_req end.\n");
error0:
        ASSERT(list_empty(&work->bio_entry_list));
        destroy_req_fin_work(work);
}

/*******************************************************************************
 * Global functions definition.
 *******************************************************************************/

/**
 * Make requrest callback.
 *
 * CONTEXT: non-IRQ (2.6.39 or later).
 */
void wrapper_blk_req_request_fn(struct request_queue *q)
{
        struct wrapper_blk_dev *wdev = wdev_get_from_queue(q);
        struct request *req;
        struct blk_plug plug;
        
        blk_start_plug(&plug);
        req = blk_fetch_request(q);
        atomic_inc(&number_of_pending_req_);
        while (req) {
                /* LOGd("REQ: %"PRIu64" (%u)\n", (u64)blk_rq_pos(req), blk_rq_bytes(req)); */
                print_req_flags(req); /* debug */

                if (req->cmd_flags & REQ_FLUSH) {
                        LOGi("REQ_FLUSH request with size %u.\n", blk_rq_bytes(req));
                        blk_finish_plug(&plug);
                        flush_workqueue(wq_req_fin_);
                        ASSERT(blk_rq_bytes(req) == 0);
                        __blk_end_request_all(req, 0);
                        atomic_dec(&number_of_pending_req_);
                        blk_start_plug(&plug);
                } else {
                        exec_normal_req(req, wdev);
                }
                req = blk_fetch_request(q);
                atomic_inc(&number_of_pending_req_);
        }
        blk_finish_plug(&plug);

#if 0
        /* throttling for asynchronouse finalization. */
        while (atomic_read(&number_of_pending_req_) > MAX_PENDING_REQUEST) {
                LOGi("sleep %d ms because %d > MAX_PENDING_REQUEST.\n",
                     POLLING_WAIT_IN_MS, atomic_read(&number_of_pending_req_));
                msleep_interruptible(POLLING_WAIT_IN_MS);
        }
#endif
}

/* Called before register. */
bool pre_register(void)
{
        LOGd("pre_register called.");

        /* Prepare kmem_cache for req_fin_work. */
        req_fin_work_cache_ = kmem_cache_create(
                KMEM_CACHE_REQ_FIN_WORK_NAME, sizeof(struct req_fin_work), 0, 0, NULL);
        if (!req_fin_work_cache_) {
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
        wq_req_fin_ = alloc_workqueue(WQ_REQ_FIN_NAME, WQ_MEM_RECLAIM, 0);
        if (!wq_req_fin_) {
                LOGe("failed to allocate a workqueue.");
                goto error2;
        }

        return true;
#if 0
error3:
        destroy_workqueue(wq_req_fin_);
#endif
error2:
        kmem_cache_destroy(bio_entry_cache_);
error1:
        kmem_cache_destroy(req_fin_work_cache_);
error0:
        return false;
}

/* Called after unregister. */
void post_unregister(void)
{
        LOGd("post_unregister called.");

        /* finalize workqueue. */
        if (wq_req_fin_) {
                flush_workqueue(wq_req_fin_);
                destroy_workqueue(wq_req_fin_);
        }

        /* Destory kmem_cache data. */
        kmem_cache_destroy(bio_entry_cache_);
        kmem_cache_destroy(req_fin_work_cache_);
}

/* end of file. */
