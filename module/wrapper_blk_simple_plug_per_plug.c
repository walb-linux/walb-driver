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

/*******************************************************************************
 * Static data definition.
 *******************************************************************************/

/**
 * Main queue to process requests.
 * This should be prepared per device.
 */
#define WQ_REQ_LIST_NAME "wq_req_list"
struct workqueue_struct *wq_req_list_ = NULL;

/**
 * Queue for flush requests.
 */
#define WQ_REQ_FLUSH_NAME "wq_req_flush"
struct workqueue_struct *wq_req_flush_ = NULL;


/**
 * Request list work struct.
 *
 * if flush_req is NULL, req_entry_list can be executed in parallel,
 * else, run flush_req first, then enqueue req_entry_list.
 */
struct req_list_work
{
	struct work_struct work;
	struct list_head list; /* list entry */
	struct wrapper_blk_dev *wdev;
	struct request *flush_req; /* flush request if flush */
	int is_restart_queue; /* If non-zero, the task must restart queue. */
	struct list_head req_entry_list; /* list head of req_entry */
};
/* kmem_cache for req_list_work. */
#define KMEM_CACHE_REQ_LIST_WORK_NAME "req_list_work_cache"
struct kmem_cache *req_list_work_cache_ = NULL;

/**
 * Request entry struct.
 */
struct req_entry
{
	struct list_head list; /* list entry */
	struct request *req;
	struct list_head bio_entry_list; /* list head of bio_entry */
	bool is_submitted; /* true after submitted. */
};
/* kmem cache for dbio. */
#define KMEM_CACHE_REQ_ENTRY_NAME "req_entry_cache"
struct kmem_cache *req_entry_cache_ = NULL;

/* bio as a list entry. */
struct bio_entry
{
	struct list_head list; /* list entry */
	struct bio *bio;
	struct completion done;
	unsigned int bi_size; /* keep bi_size at initialization,
				 because bio->bi_size will be 0 after endio. */
	int error; /* bio error status. */
};
/* kmem cache for dbio. */
#define KMEM_CACHE_BIO_ENTRY_NAME "bio_entry_cache"
struct kmem_cache *bio_entry_cache_ = NULL;


/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

/* Print request flags for debug. */
static void print_req_flags(struct request *req);

/* req_list_work related. */
static struct req_list_work* create_req_list_work(
	struct request *flush_req,
	struct wrapper_blk_dev *wdev, gfp_t gfp_mask,
	void (*worker)(struct work_struct *work));
static void destroy_req_list_work(struct req_list_work *work);

/* req_entry related. */
static struct req_entry* create_req_entry(struct request *req, gfp_t gfp_mask);
static void destroy_req_entry(struct req_entry *reqe);

/* bio_entry related. */
static void bio_entry_end_io(struct bio *bio, int error);
static struct bio_entry* create_bio_entry(
	struct bio *bio, struct block_device *bdev, gfp_t gfp_mask);
static void destroy_bio_entry(struct bio_entry *bioe);

/* Request list work tasks. */
static void req_list_work_task(struct work_struct *work);
static void req_flush_task(struct work_struct *work);

/* Helper functions. */
static bool create_bio_entry_list(struct req_entry *reqe, struct wrapper_blk_dev *wdev);
static void submit_req_entry(struct req_entry *reqe);
static void wait_for_req_entry(struct req_entry *reqe);

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
 * Create a req_list_work.
 *
 * RETURN:
 * NULL if failed.
 * CONTEXT:
 * Any.
 */
static struct req_list_work* create_req_list_work(
	struct request *flush_req,
	struct wrapper_blk_dev *wdev,
	gfp_t gfp_mask,
	void (*worker)(struct work_struct *work))
{
	struct req_list_work *work;

	ASSERT(wdev);
	ASSERT(req_list_work_cache_);

	work = kmem_cache_alloc(req_list_work_cache_, gfp_mask);
	if (!work) {
		goto error0;
	}
	INIT_WORK(&work->work, worker);
	work->wdev = wdev;
	INIT_LIST_HEAD(&work->req_entry_list);
	work->flush_req = flush_req;
	work->is_restart_queue = 0;
        
	return work;
error0:
	return NULL;
}

/**
 * Destory a req_list_work.
 */
static void destroy_req_list_work(struct req_list_work *work)
{
	struct req_entry *reqe, *next;
        
	if (work) {
		list_for_each_entry_safe(reqe, next, &work->req_entry_list, list) {
			list_del(&reqe->list);
			destroy_req_entry(reqe);
		}
#ifdef WALB_DEBUG
		work->flush_req = NULL;
		work->wdev = NULL;
		INIT_LIST_HEAD(&work->req_entry_list);
#endif
		kmem_cache_free(req_list_work_cache_, work);
	}
}

/**
 * Create req_entry struct.
 */
static struct req_entry* create_req_entry(struct request *req, gfp_t gfp_mask)
{
	struct req_entry *reqe;

	reqe = kmem_cache_alloc(req_entry_cache_, gfp_mask);
	if (!reqe) {
		goto error0;
	}
	ASSERT(req);
	reqe->req = req;
	INIT_LIST_HEAD(&reqe->list);
	INIT_LIST_HEAD(&reqe->bio_entry_list);
	reqe->is_submitted = false;
        
	return reqe;
error0:
	return NULL;
}

/**
 * Destroy a req_entry.
 */
static void destroy_req_entry(struct req_entry *reqe)
{
	struct bio_entry *bioe, *next;

	if (reqe) {
		list_for_each_entry_safe(bioe, next, &reqe->bio_entry_list, list) {
			list_del(&bioe->list);
			destroy_bio_entry(bioe);
		}
#ifdef WALB_DEBUG
		reqe->req = NULL;
		INIT_LIST_HEAD(&reqe->list);
		INIT_LIST_HEAD(&reqe->bio_entry_list);
#endif
		kmem_cache_free(req_entry_cache_, reqe);
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
	bioe->error = error;
	bio_put(bio);
	bioe->bio = NULL;
	complete(&bioe->done);
	/* LOGd("bio_entry_end_io() end.\n"); */
}

/**
 * Create a bio_entry.
 *
 * @bio original bio.
 * @bdev block device to forward bio.
 */
static struct bio_entry* create_bio_entry(
	struct bio *bio, struct block_device *bdev, gfp_t gfp_mask)
{
	struct bio_entry *bioe;
	struct bio *biotmp;

	/* LOGd("create_bio_entry() begin.\n"); */

	bioe = kmem_cache_alloc(bio_entry_cache_, gfp_mask);
	if (!bioe) {
		LOGd("kmem_cache_alloc() failed.");
		goto error0;
	}
	init_completion(&bioe->done);
	bioe->error = 0;
	bioe->bi_size = bio->bi_size;

	/* clone bio */
	bioe->bio = NULL;
	biotmp = bio_clone(bio, gfp_mask);
	if (!biotmp) {
		LOGe("bio_clone() failed.");
		goto error1;
	}
	biotmp->bi_bdev = bdev;
	biotmp->bi_end_io = bio_entry_end_io;
	biotmp->bi_private = bioe;
	bioe->bio = biotmp;
        
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
	/* LOGd("destroy_bio_entry() begin.\n"); */
        
	if (!bioe) {
		return;
	}

	if (bioe->bio) {
		LOGd("bio_put %p\n", bioe->bio);
		bio_put(bioe->bio);
		bioe->bio = NULL;
	}
	kmem_cache_free(bio_entry_cache_, bioe);

	/* LOGd("destroy_bio_entry() end.\n"); */
}

/**
 * Create bio_entry list for a request.
 *
 * RETURN:
 *     true if succeed, or false.
 * CONTEXT:
 *     Non-IRQ. Non-atomic.
 */
static bool create_bio_entry_list(struct req_entry *reqe, struct wrapper_blk_dev *wdev)

{
	struct bio_entry *bioe, *next;
	struct bio *bio;
	struct block_device *bdev = wdev->private_data;
        
	ASSERT(reqe);
	ASSERT(reqe->req);
	ASSERT(wdev);
	ASSERT(list_empty(&reqe->bio_entry_list));
        
	/* clone all bios. */
	__rq_for_each_bio(bio, reqe->req) {
		/* clone bio */
		bioe = create_bio_entry(bio, bdev, GFP_NOIO);
		if (!bioe) {
			LOGd("create_bio_entry() failed.\n"); 
			goto error1;
		}
		list_add_tail(&bioe->list, &reqe->bio_entry_list);
	}

	return true;
error1:
	list_for_each_entry_safe(bioe, next, &reqe->bio_entry_list, list) {
		list_del(&bioe->list);
		destroy_bio_entry(bioe);
	}
	ASSERT(list_empty(&reqe->bio_entry_list));
	return false;
}

/**
 * Submit all bios in a bio_entry.
 *
 * @reqe target req_entry.
 */
static void submit_req_entry(struct req_entry *reqe)
{
	struct bio_entry *bioe;
	list_for_each_entry(bioe, &reqe->bio_entry_list, list) {
		generic_make_request(bioe->bio);
	}
	reqe->is_submitted = true;
}

/**
 * Wait for completion and end request.
 *
 * @reqe target req_entry.
 */
static void wait_for_req_entry(struct req_entry *reqe)
{
	struct bio_entry *bioe, *next;
	int remaining;

	ASSERT(reqe);
        
	remaining = blk_rq_bytes(reqe->req);
	list_for_each_entry_safe(bioe, next, &reqe->bio_entry_list, list) {
		wait_for_completion(&bioe->done);
		blk_end_request(reqe->req, bioe->error, bioe->bi_size);
		remaining -= bioe->bi_size;
		list_del(&bioe->list);
		destroy_bio_entry(bioe);
	}
	ASSERT(remaining == 0);
}

/**
 * Execute request list.
 *
 * (1) Clone all bios related each request in the list.
 * (2) Submit them.
 * (3) wait completion of all bios.
 * (4) notify completion to the block layer.
 * (5) free memories.
 *
 * CONTEXT:
 *   Non-IRQ. Non-atomic.
 *   Request queue lock is not held.
 *   Other tasks may be running concurrently.
 */
static void req_list_work_task(struct work_struct *work)
{
	struct req_list_work *rlwork = container_of(work, struct req_list_work, work);
	struct wrapper_blk_dev *wdev = rlwork->wdev;
	struct req_entry *reqe, *next;
	struct blk_plug plug;

	/* LOGd("req_list_work_task begin.\n"); */
        
	ASSERT(rlwork->flush_req == NULL);

	/* prepare and submit */
	blk_start_plug(&plug);
	list_for_each_entry(reqe, &rlwork->req_entry_list, list) {
		if (!create_bio_entry_list(reqe, wdev)) {
			LOGe("create_bio_entry_list failed.\n");
			goto error0;
		}
		submit_req_entry(reqe);
	}
	blk_finish_plug(&plug);

	/* wait completion and end requests. */
	list_for_each_entry_safe(reqe, next, &rlwork->req_entry_list, list) {
		wait_for_req_entry(reqe);
		list_del(&reqe->list);
		destroy_req_entry(reqe);
	}
	/* destroy work struct */
	destroy_req_list_work(rlwork);
	/* LOGd("req_list_work_task end.\n"); */
	return;

error0:
	list_for_each_entry_safe(reqe, next, &rlwork->req_entry_list, list) {
		if (reqe->is_submitted) {
			wait_for_req_entry(reqe);
		} else {
			blk_end_request_all(reqe->req, -EIO);
		}
		list_del(&reqe->list);
		destroy_req_entry(reqe);
	}
	destroy_req_list_work(rlwork);
	LOGd("req_list_work_task error.\n");
}

/**
 * Request flush task.
 */
static void req_flush_task(struct work_struct *work)
{
	struct req_list_work *rlwork = container_of(work, struct req_list_work, work);
	struct request_queue *q = rlwork->wdev->queue;
	int is_restart_queue = rlwork->is_restart_queue;
        
	LOGd("req_flush_task begin.\n");
	ASSERT(rlwork->flush_req);

	/* Flush previous all requests. */
	flush_workqueue(wq_req_list_);
	blk_end_request_all(rlwork->flush_req, 0);

	/* Restart queue if required. */
	if (is_restart_queue) {
		spin_lock(q->queue_lock);
		blk_start_queue(q);
		spin_unlock(q->queue_lock);
	}
        
	if (list_empty(&rlwork->req_entry_list)) {
		destroy_req_list_work(rlwork);
	} else {
		/* Enqueue the following requests */
		rlwork->flush_req = NULL;
		INIT_WORK(&rlwork->work, req_list_work_task);
		queue_work(wq_req_list_, &rlwork->work);
	}
}

/*******************************************************************************
 * Global functions definition.
 *******************************************************************************/

/**
 * Make requrest callback.
 *
 * CONTEXT: in_interrupt(): false, is_atomic(): true.
 */
void wrapper_blk_req_request_fn(struct request_queue *q)
{
	struct wrapper_blk_dev *wdev = wdev_get_from_queue(q);
	struct request *req;
	struct req_entry *reqe;
	struct req_list_work *work;
	struct list_head listh;

	INIT_LIST_HEAD(&listh);

	LOGd("wrapper_blk_req_request_fn: in_interrupt: %lu in_atomic: %d\n",
		in_interrupt(), in_atomic());

	work = create_req_list_work(NULL, wdev, GFP_ATOMIC, req_list_work_task);
	if (!work) { goto error0; }
	req = blk_fetch_request(q);
	while (req) {
		print_req_flags(req); /* debug */

		if (req->cmd_flags & REQ_FLUSH) {
			LOGd("REQ_FLUSH request with size %u.\n", blk_rq_bytes(req));

			list_add_tail(&work->list, &listh);
			work = create_req_list_work(req, wdev, GFP_ATOMIC, req_flush_task);
			if (!work) { __blk_end_request_all(req, -EIO); }
		} else {
			reqe = create_req_entry(req, GFP_ATOMIC);
			if (!reqe) { __blk_end_request_all(req, -EIO); }
			list_add_tail(&reqe->list, &work->req_entry_list);
		}
		req = blk_fetch_request(q);
	}
	list_add_tail(&work->list, &listh);

	list_for_each_entry(work, &listh, list) {
		if (work->flush_req) {
			if (list_is_last(&work->list, &listh)) {
				work->is_restart_queue = true;
				blk_stop_queue(q);
			}
			queue_work(wq_req_flush_, &work->work);
		} else {
			queue_work(wq_req_list_, &work->work);
		}
	}
	INIT_LIST_HEAD(&listh);
	LOGd("wrapper_blk_req_request_fn: end.\n");
	return;
error0:
	while ((req = blk_fetch_request(q)) != NULL) {
		__blk_end_request_all(req, -EIO);
	}
}
        
/* Called before register. */
bool pre_register(void)
{
	LOGd("pre_register called.");

	/* Prepare kmem_cache data. */
	req_list_work_cache_ = kmem_cache_create(
		KMEM_CACHE_REQ_LIST_WORK_NAME,
		sizeof(struct req_list_work), 0, 0, NULL);
	if (!req_list_work_cache_) {
		LOGe("failed to create a kmem_cache.\n");
		goto error0;
	}
	req_entry_cache_ = kmem_cache_create(
		KMEM_CACHE_REQ_ENTRY_NAME,
		sizeof(struct req_entry), 0, 0, NULL);
	if (!req_entry_cache_) {
		LOGe("failed to create a kmem_cache.\n");
		goto error1;
	}
	bio_entry_cache_ = kmem_cache_create(
		KMEM_CACHE_BIO_ENTRY_NAME,
		sizeof(struct bio_entry), 0, 0, NULL);
	if (!bio_entry_cache_) {
		LOGe("failed to create a kmem_cache.\n");
		goto error2;
	}
        
	/* prepare workqueue data. */
	wq_req_list_ = alloc_workqueue(WQ_REQ_LIST_NAME, WQ_MEM_RECLAIM, 0);
	if (!wq_req_list_) { goto error3; }
	wq_req_flush_ = create_singlethread_workqueue(WQ_REQ_FLUSH_NAME);
	if (!wq_req_flush_) { goto error4; }

	return true;

#if 0
error5:
	destroy_workqueue(wq_req_flush_);
#endif
error4:
	destroy_workqueue(wq_req_list_);
error3:
	LOGe("failed to allocate a workqueue.");
	kmem_cache_destroy(bio_entry_cache_);
error2:
	kmem_cache_destroy(req_entry_cache_);
error1:
	kmem_cache_destroy(req_list_work_cache_);
error0:
	return false;
}

/* Called after unregister. */
void post_unregister(void)
{
	LOGd("post_unregister called.");

	/* finalize workqueue data. */
	destroy_workqueue(wq_req_flush_);
	wq_req_flush_ = NULL;
	destroy_workqueue(wq_req_list_);
	wq_req_list_ = NULL;

	/* Destory kmem_cache data. */
	kmem_cache_destroy(bio_entry_cache_);
	bio_entry_cache_ = NULL;
	kmem_cache_destroy(req_entry_cache_);
	req_entry_cache_ = NULL;
	kmem_cache_destroy(req_list_work_cache_);
	req_list_work_cache_ = NULL;
}

/* end of file. */
