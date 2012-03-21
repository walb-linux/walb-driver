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
#include <linux/spinlock.h>

#include "block_size.h"
#include "wrapper_blk.h"
#include "wrapper_blk_simple.h"

/* #define PERFORMANCE_DEBUG */
/* #define NUMBER_OF_PENDING_REQ */

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
        struct list_head list; /* list entry */
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
#ifdef NUMBER_OF_PENDING_REQ
static atomic_t number_of_pending_req_ = ATOMIC_INIT(0);
/* supports 10000 * 10 = 1,000,000 IOPS */
#define MAX_PENDING_REQUEST 100
#define POLLING_WAIT_IN_MS 10

static atomic_t n_bio_end_io_ = ATOMIC_INIT(0);
static atomic_t n_submit_bio_ = ATOMIC_INIT(0);
#endif

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

/* For executing normal request. */
static struct req_fin_work*
create_cloned_bio_list(struct request *req, struct wrapper_blk_dev *wdev);
static void submit_bio_entry_list(struct list_head *listh);
static void submit_req_fin_work_list(struct list_head *listh);
static void queue_req_fin_work_list(struct list_head *listh);

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
        INIT_LIST_HEAD(&work->list);
        
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
        int uptodate = test_bit(BIO_UPTODATE, &bio->bi_flags);
        ASSERT(bioe);
        ASSERT(bioe->bio == bio);
        ASSERT(uptodate);
        
        /* LOGd("bio_entry_end_io() begin.\n"); */
#ifdef PERFORMANCE_DEBUG
        LOGd("complete bioe_id %u.\n", bioe->id);
        /* LOGd("bio_entry_end_io: in_interrupt: %d\n", in_interrupt()); */
#endif
        bioe->error = error;
        bio_put(bio);
        bioe->bio = NULL;
        complete(&bioe->done);

#ifdef NUMBER_OF_PENDING_REQ
        LOGd("n_bio_end_io: %u\n", atomic_inc_return(&n_bio_end_io_));
#endif
        /* LOGd("bio_entry_end_io() end.\n"); */
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

        /* LOGd("create_bio_entry() begin.\n"); */

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
                LOGe("bio_clone() failed.");
                goto error1;
        }
        biotmp->bi_bdev = bdev;
        biotmp->bi_end_io = bio_entry_end_io;
        biotmp->bi_private = bioe;
        bioe->bio = biotmp;

#ifdef PERFORMANCE_DEBUG
        bioe->id = atomic_inc_return(&bio_entry_id_counter_);
#endif
        /* LOGd("create_bio_entry() end.\n"); */
        return bioe;

error1:
        destroy_bio_entry(bioe);
error0:
        LOGe("create_bio_entry() end with error.\n");
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
                bioe->bio = NULL;
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
#ifdef PERFORMANCE_DEBUG
        unsigned int last_bioe_id = 0;
#endif
        
        LOGd("req_fin_work_task begin.\n");
        
        /* wait comletion and destroy of all bio_entry s. */
        list_for_each_entry_safe(bioe, next, &req_fin_work->bio_entry_list, list) {
#ifdef PERFORMANCE_DEBUG
                LOGd("wait_for_completion bioe_id %u\n", bioe->id);
#endif
                wait_for_completion(&bioe->done);
#ifdef PERFORMANCE_DEBUG
                LOGd("blk_end_request: bioe_id %u\n", bioe->id);
#endif
                blk_end_request(req, bioe->error, bioe->bi_size);
#ifdef PERFORMANCE_DEBUG
                LOGd("done: bioe_id %u\n", bioe->id);
                last_bioe_id = bioe->id;
#endif
                remaining -= bioe->bi_size;
                list_del(&bioe->list);
                destroy_bio_entry(bioe);
        }
        ASSERT(remaining == 0);
        destroy_req_fin_work(req_fin_work);
#ifdef NUMBER_OF_PENDING_REQ
        atomic_dec(&number_of_pending_req_);
        LOGd("dec n_pending_req: %u\n", atomic_read(&number_of_pending_req_));
#endif

#ifdef PERFORMANCE_DEBUG
        LOGd("req_fin_work_task end (last_bioe_id %u).\n", last_bioe_id);
#else
        LOGd("req_fin_work_task end.\n");
#endif
}

/**
 * Create cloned bio list.
 * (1) create request finalize work.
 * (2) clone all bios in the request.
 *
 * This does not submit cloned bios and enqueued task.
 *
 * If an error occurs, __blk_end_request_all(req, -EIO)
 * will be called inside the function.
 *
 * CONTEXT:
 *   Non-IRQ.
 *   Request queue lock is held.
 *   (Other tasks may be running concurrently?)
 * RETURN:
 *   req_fin_work data created in success.
 *   NULL in failure.
 */
static struct req_fin_work*
create_cloned_bio_list(struct request *req, struct wrapper_blk_dev *wdev)
{
        struct block_device *bdev = wdev->private_data;
        struct req_fin_work *work;
        struct bio *bio = NULL;
        struct bio_entry *bioe, *next;
        
        LOGd("create_cloned_bio_list begin\n");

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
                /* LOGd("list_add_tail\n"); */
                list_add_tail(&bioe->list, &work->bio_entry_list);
        }
        LOGd("all bioe is created.\n");

        LOGd("create_cloned_bio_list end\n");
        return work;
error1:
        list_for_each_entry_safe(bioe, next, &work->bio_entry_list, list) {
                list_del(&bioe->list);
                destroy_bio_entry(bioe);
        }
#ifdef NUMBER_OF_PENDING_REQ
        atomic_dec(&number_of_pending_req_);
        LOGd("dec n_pending_req: %u\n", atomic_read(&number_of_pending_req_));
#endif
error0:
        ASSERT(list_empty(&work->bio_entry_list));
        destroy_req_fin_work(work);
        __blk_end_request_all(req, -EIO);
        LOGd("create_cloned_bio_list error\n");
        return NULL;
}

/**
 * Submit all cloned bios.
 *
 * @listh list head of bio_entry.
 */
static void submit_bio_entry_list(struct list_head *listh)
{
        struct bio *bio;
        struct bio_entry *bioe;
        
        /* submit all bios. */
        list_for_each_entry(bioe, listh, list) {
                bio = bioe->bio;
#ifdef PERFORMANCE_DEBUG
                LOGd("submit bio (bioe_id %u) %"PRIu64" %u (rw %lu)\n",
                     bioe->id, (u64)bio->bi_sector, bio->bi_size, bio_rw(bio));
#else
                LOGd("submit bio %"PRIu64" %u (rw %lu)\n",
                     (u64)bio->bi_sector, bio->bi_size, bio_rw(bio));
#endif
                ASSERT(bioe->bio->bi_end_io == bio_entry_end_io);
                generic_make_request(bioe->bio);
#ifdef NUMBER_OF_PENDING_REQ
                LOGd("n_submit_bio: %u\n", atomic_inc_return(&n_submit_bio_));
#endif
        }
}

/**
 * Submit all realted bios in a req_fin_work.
 *
 * @listh list head of req_fin_work.
 */
static void submit_req_fin_work_list(struct list_head *listh)
{
        struct req_fin_work *work;
        struct blk_plug plug;

#ifdef PERFORMANCE_DEBUG
        LOGd("submit_req_fin_work_list begin.\n");
#endif
        blk_start_plug(&plug);
        list_for_each_entry(work, listh, list) {
                submit_bio_entry_list(&work->bio_entry_list);
        }
        blk_finish_plug(&plug);
#ifdef PERFORMANCE_DEBUG
        LOGd("submit_req_fin_work_list end.\n");
#endif
}

/**
 * Enqueue all req_fin_work data in a list.
 *
 * @listh list head of req_fin_work.
 */
static void queue_req_fin_work_list(struct list_head *listh)
{
        struct req_fin_work *work;
        
        list_for_each_entry(work, listh, list) {
                queue_work(wq_req_fin_, &work->work);
        }
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
        struct req_fin_work *work;
        struct list_head listh;

        LOGd("wrapper_blk_req_request_fn: in_interrupt: %lu\n", in_interrupt());
        INIT_LIST_HEAD(&listh);
        req = blk_fetch_request(q);
        while (req) {
                /* LOGd("REQ: %"PRIu64" (%u)\n", (u64)blk_rq_pos(req), blk_rq_bytes(req)); */
                print_req_flags(req); /* debug */

                if (req->cmd_flags & REQ_FLUSH) {
                        LOGd("REQ_FLUSH request with size %u.\n", blk_rq_bytes(req));
                        submit_req_fin_work_list(&listh);
                        queue_req_fin_work_list(&listh);
                        INIT_LIST_HEAD(&listh);
                        flush_workqueue(wq_req_fin_);
                        ASSERT(blk_rq_bytes(req) == 0);
                        __blk_end_request_all(req, 0);
#ifdef NUMBER_OF_PENDING_REQ
                        atomic_dec(&number_of_pending_req_);
                        LOGd("dec n_pending_req: %u\n", atomic_read(&number_of_pending_req_));
#endif
                } else {
                        work = create_cloned_bio_list(req, wdev);
                        if (work) {
                                list_add_tail(&work->list, &listh);
#ifdef NUMBER_OF_PENDING_REQ
                                atomic_inc(&number_of_pending_req_);
                                LOGd("inc n_pending_req: %u\n", atomic_read(&number_of_pending_req_));
#endif
                        }
                        
                }
                req = blk_fetch_request(q);
        }
        submit_req_fin_work_list(&listh);
        queue_req_fin_work_list(&listh);
        INIT_LIST_HEAD(&listh);
        LOGd("wrapper_blk_req_request_fn: end.\n");
        
#if defined(NUMBER_OF_PENDING_REQ) && 0
        /* deprecated code. will be removed. */
        /* throttling for asynchronouse finalization. */
        while (atomic_read(&number_of_pending_req_) > MAX_PENDING_REQUEST) {
                LOGd("sleep %d ms because %d > MAX_PENDING_REQUEST.\n",
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
        /* wq_req_fin_ = create_singlethread_workqueue(WQ_REQ_FIN_NAME); */
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
