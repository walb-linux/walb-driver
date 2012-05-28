/**
 * wrapper_blk_walb_easy.c - WalB block device with Easy Algorithm for test.
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

#include "wrapper_blk.h"
#include "wrapper_blk_walb.h"
#include "sector_io.h"
#include "logpack.h"
#include "walb/walb.h"
#include "walb/block_size.h"

/*******************************************************************************
 * Static data definition.
 *******************************************************************************/

/**
 * Logpack submitter and waiter queue.
 * These should be prepared per walb device.
 * These must be critical path.
 */
#define WQ_LOGPACK_SUBMIT "wq_logpack_submit"
struct workqueue_struct *wq_logpack_submit_ = NULL;
#define WQ_LOGPACK_WAIT "wq_logpack_wait"
struct workqueue_struct *wq_logpack_wait_ = NULL;

/**
 * Queue for various task
 * This should be shared by all walb devices.
 */
#define WQ_NORMAL "wq_normal"
struct workqueue_struct *wq_normal_ = NULL;

/**
 * Logpack list work.
 *
 * if flush_req is NULL, packs in the list can be executed in parallel,
 * else, run flush_req first, then enqueue packs in the list.
 */
struct pack_list_work
{
	struct work_struct work;
	struct wrapper_blk_dev *wdev;
	struct list_head wpack_list; /* list head of writepack. */
};
/* kmem_cache for logpack_list_work. */
#define KMEM_CACHE_PACK_LIST_WORK_NAME "pack_list_work_cache"
struct kmem_cache *pack_list_work_cache_ = NULL;

/**
 * A pack.
 * There are no overlapping requests in a pack.
 */
struct pack
{
	struct list_head list; /* list entry. */
	struct list_head req_ent_list; /* list head of req_entry. */
	bool is_write; /* true if write, or read. */

	/* This is for only write pack. */
	struct sector_data *logpack_header_sector;
};
#define KMEM_CACHE_PACK_NAME "pack_cache"
struct kmem_cache *pack_cache_ = NULL;

/**
 * Request entry struct.
 */
struct req_entry
{
	struct work_struct work; /* used for read/write_req_task. */
	
	struct list_head list; /* list entry */
	struct request *req;
	struct list_head bio_entry_list; /* list head of bio_entry */
	bool is_submitted; /* true after submitted. */

	/* Notification from write_req_task to gc_task.
	   read_req_task does not use this. */
	struct completion done;
};
/* kmem cache for dbio. */
#define KMEM_CACHE_REQ_ENTRY_NAME "req_entry_cache"
struct kmem_cache *req_entry_cache_ = NULL;

/**
 * bio as a list entry.
 */
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
 * Macros definition.
 *******************************************************************************/

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

/* Print request flags for debug. */
static void print_req_flags(struct request *req);

/* pack_list_work related. */
static struct pack_list_work* create_pack_list_work(
	struct wrapper_blk_dev *wdev, gfp_t gfp_mask);
static void destroy_pack_list_work(struct pack_list_work *work);

/* req_entry related. */
static struct req_entry* create_req_entry(struct request *req, gfp_t gfp_mask);
static void destroy_req_entry(struct req_entry *reqe);

/* bio_entry related. */
static void bio_entry_end_io(struct bio *bio, int error);
static struct bio_entry* create_bio_entry(
	struct bio *bio, struct block_device *bdev, gfp_t gfp_mask);
static void destroy_bio_entry(struct bio_entry *bioe);

/* pack related. */
static struct pack* create_pack(bool is_write, gfp_t gfp_mask);
static struct pack* create_writepack(
	gfp_t gfp_mask, unsigned int pbs, u64 logpack_lsid);
static struct pack* create_readpack(gfp_t gfp_mask);
static void destroy_pack(struct pack *pack);
static bool is_overlap_pack_reqe(struct pack *pack, struct req_entry *reqe);

/* helper function. */
static bool writepack_add_req(
	struct list_head *wpack_list, struct pack **wpackp, struct request *req,
	u64 ring_buffer_size, u64 *latest_lsidp, gfp_t gfp_mask);

/* Request pack_list_work tasks. */
static void pack_list_work_task(struct work_struct *work); /* for non-flush, concurrent. */
static void req_flush_task(struct work_struct *work); /* for flush, sequential. */

/* Workqueue tasks. */
static void logpack_list_submit_task(struct work_struct *work);
static void logpack_list_wait_task(struct work_struct *work);
static void logpack_list_gc_task(struct work_struct *work);
static void write_req_task(struct work_struct *work);
static void read_req_task(struct work_struct *work);

/* Helper functions. */
static bool create_bio_entry_list(struct req_entry *reqe, struct wrapper_blk_dev *wdev);
static void submit_req_entry(struct req_entry *reqe);
static void wait_for_req_entry(struct req_entry *reqe);

/* Validator for debug. */
static bool is_valid_prepared_pack(struct pack *pack);
static bool is_valid_pack_list(struct list_head *listh);





/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * Print request flags for debug.
 */
UNUSED
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
 * Create a pack_list_work.
 *
 * RETURN:
 * NULL if failed.
 * CONTEXT:
 * Any.
 */
static struct pack_list_work* create_pack_list_work(
	struct wrapper_blk_dev *wdev, gfp_t gfp_mask)
{
	struct pack_list_work *plwork;

	ASSERT(wdev);
	ASSERT(pack_list_work_cache_);

	plwork = kmem_cache_alloc(pack_list_work_cache_, gfp_mask);
	if (!plwork) {
		goto error0;
	}
	plwork->wdev = wdev;
	INIT_LIST_HEAD(&plwork->wpack_list);
	/* INIT_WORK(&plwork->work, NULL); */
        
	return plwork;
error0:
	return NULL;
}

/**
 * Destory a pack_list_work.
 */
static void destroy_pack_list_work(struct pack_list_work *work)
{
	struct pack *pack, *next;

	if (!work) { return; }
	
	list_for_each_entry_safe(pack, next, &work->wpack_list, list) {
		list_del(&pack->list);
		destroy_pack(pack);
	}
#ifdef WALB_DEBUG
	work->wdev = NULL;
	INIT_LIST_HEAD(&work->wpack_list);
#endif
	kmem_cache_free(pack_list_work_cache_, work);
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
	init_completion(&reqe->done);
	/* You must call INIT_WORK(&reqe->work, func) before using it; */
        
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
	UNUSED int uptodate = test_bit(BIO_UPTODATE, &bio->bi_flags);
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
 * Create a pack.
 */
static struct pack* create_pack(bool is_write, gfp_t gfp_mask)
{
	struct pack *pack;

	/* LOGd("create_bio_entry() begin.\n"); */

	pack = kmem_cache_alloc(pack_cache_, gfp_mask);
	if (!pack) {
		LOGd("kmem_cache_alloc() failed.");
		goto error0;
	}
	INIT_LIST_HEAD(&pack->list);
	INIT_LIST_HEAD(&pack->req_ent_list);
	pack->is_write = is_write;
	
	return pack;
#if 0
error1:
	destory_pack(pack);
#endif
error0:
	LOGe("create_pack() end with error.\n");
	return NULL;
}

/**
 * Create a writepack.
 *
 * @gfp_mask allocation mask.
 * @pbs physical block size in bytes.
 * @logpack_lsid logpack lsid.
 *
 * RETURN:
 *   Allocated and initialized writepack in success, or NULL.
 */
static struct pack* create_writepack(
	gfp_t gfp_mask, unsigned int pbs, u64 logpack_lsid)
{
	struct pack *pack;
	struct walb_logpack_header *lhead;

	ASSERT(logpack_lsid != INVALID_LSID);
	pack = create_pack(true, gfp_mask);
	if (!pack) { goto error0; }
	pack->logpack_header_sector = sector_alloc(pbs, gfp_mask | __GFP_ZERO);
	if (!pack->logpack_header_sector) { goto error1; }

	lhead = get_logpack_header(pack->logpack_header_sector);
	lhead->sector_type = SECTOR_TYPE_LOGPACK;
	lhead->logpack_lsid = logpack_lsid;
	/* lhead->total_io_size = 0; */
	/* lhead->n_records = 0; */
	/* lhead->n_padding = 0; */
	
	return pack;
	
error1:
	destroy_pack(pack);
error0:
	return NULL;
}

/**
 * Create a readpack.
 */
static struct pack* create_readpack(gfp_t gfp_mask)
{
	struct pack *pack;
	
	pack = create_pack(false, gfp_mask);
	if (!pack) { goto error0; }
	pack->logpack_header_sector = NULL;
	return pack;
error0:
	return NULL;
}

/**
 * Destory a pack.
 */
static void destroy_pack(struct pack *pack)
{
	struct req_entry *reqe, *next;
	
	if (!pack) { return; }
	
	list_for_each_entry_safe(reqe, next, &pack->req_ent_list, list) {
		list_del(&reqe->list);
		destroy_req_entry(reqe);
	}
	if (pack->logpack_header_sector) {
		sector_free(pack->logpack_header_sector);
		pack->logpack_header_sector = NULL;
	}
#ifdef WALB_DEBUG
	INIT_LIST_HEAD(&pack->req_ent_list);
#endif
	kmem_cache_free(pack_cache_, pack);
}

/**
 * Check a request in a pack and a request is overlapping.
 */
static bool is_overlap_pack_reqe(struct pack *pack, struct req_entry *reqe)
{
	struct req_entry *tmp_reqe;

	ASSERT(pack);
	ASSERT(reqe);
	ASSERT(reqe->req);
	
	list_for_each_entry(tmp_reqe, &pack->req_ent_list, list) {
		if (is_overlap_req(tmp_reqe->req, reqe->req)) {
			return true;
		}
	}
	return false;
}

/**
 * Add a request entry to a pack.
 *
 * @pack pack to added.
 * @reqe req_entry to add.
 *
 * If an overlapping request exists, add nothing and return false.
 * Else, add the request and return true.
 */
/* DEPRECATED */
/* static bool pack_add_reqe(struct pack *pack, struct req_entry *reqe) */
/* { */
/* 	struct req_entry *tmp_reqe; */
	
/* 	ASSERT(pack); */
/* 	ASSERT(reqe); */
/* 	ASSERT(pack->is_write == (reqe->req->cmd_flags & REQ_WRITE != 0)); */

/* 	/\* Search overlapping requests. *\/ */
/* 	if (is_overlap_pack_reqe(pack, reqe)) { */
/* 		return false; */
/* 	} else { */
/* 		list_add_tail(&reqe->list, &pack->req_ent_list); */
/* 		return true; */
/* 	} */
/* } */

/**
 * Add a request to a writepack.
 */
static bool writepack_add_req(
	struct list_head *wpack_list, struct pack **wpackp, struct request *req,
	u64 ring_buffer_size, u64 *latest_lsidp, gfp_t gfp_mask)
{
	struct req_entry *reqe;
	struct pack *pack;
	bool ret;
	unsigned int pbs;
	struct walb_logpack_header *lhead;

	ASSERT(wpack_list);
	ASSERT(wpackp);
	ASSERT(*wpackp);
	ASSERT(req);
	ASSERT(req->cmd_flags & REQ_WRITE);
	pack = *wpackp;
	ASSERT(pack->is_write);
	ASSERT(pack->logpack_header_sector);
	
	reqe = create_req_entry(req, gfp_mask);
	if (!reqe) { goto error0; }

	pbs = pack->logpack_header_sector->size;
	ASSERT_PBS(pbs);
	lhead = get_logpack_header(pack->logpack_header_sector);
	ASSERT(*latest_lsidp == lhead->logpack_lsid);

	if (req->cmd_flags & REQ_FLUSH) {
		/* Flush request must be the first of the pack. */
		goto newpack;
	}
	if (is_overlap_pack_reqe(pack, reqe)) {
		/* overlap found so create a new pack. */
		goto newpack;
	}
	if (!walb_logpack_header_add_req(
			get_logpack_header(pack->logpack_header_sector),
			req, pbs, ring_buffer_size)) {
		/* logpack header capacity full so create a new pack. */
		goto newpack;
	}

	/* The request is just added to the pack. */
	list_add_tail(&reqe->list, &pack->req_ent_list);
	return true;

newpack:
	list_add_tail(&pack->list, wpack_list);
	*latest_lsidp = get_next_lsid(lhead);
	pack = create_writepack(gfp_mask, pbs, *latest_lsidp);
	if (!pack) { goto error1; }
	*wpackp = pack;

	ret = walb_logpack_header_add_req(
		get_logpack_header(pack->logpack_header_sector),
		req, pbs, ring_buffer_size);
	ASSERT(ret);

	list_add_tail(&reqe->list, &pack->req_ent_list);
	return true;
error1:
	destroy_req_entry(reqe);
error0:
	return false;
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
	struct pdata *pdata = wdev->private_data;
	struct block_device *bdev = pdata->ddev;
        
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
 * Normal pack list execution task.
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
static void pack_list_work_task(struct work_struct *work)
{
	/* now editing */

	
	struct pack_list_work *fwork = container_of(work, struct pack_list_work, work);
	struct wrapper_blk_dev *wdev = fwork->wdev;
	struct req_entry *reqe, *next;
	struct blk_plug plug;

	/* LOGd("pack_list_work_task begin.\n"); */
        
	/* ASSERT(fwork->flush_req == NULL); */


	
/* 	/\* prepare and submit *\/ */
/* 	blk_start_plug(&plug); */
/* 	list_for_each_entry(reqe, &fwork->req_entry_list, list) { */
/* 		if (!create_bio_entry_list(reqe, wdev)) { */
/* 			LOGe("create_bio_entry_list failed.\n"); */
/* 			goto error0; */
/* 		} */
/* 		submit_req_entry(reqe); */
/* 	} */
/* 	blk_finish_plug(&plug); */

/* 	/\* wait completion and end requests. *\/ */
/* 	list_for_each_entry_safe(reqe, next, &fwork->req_entry_list, list) { */
/* 		wait_for_req_entry(reqe); */
/* 		list_del(&reqe->list); */
/* 		destroy_req_entry(reqe); */
/* 	} */
/* 	/\* destroy work struct *\/ */
/* 	destroy_pack_list_work(fwork); */
/* 	/\* LOGd("pack_list_work_task end.\n"); *\/ */
/* 	return; */

/* error0: */
/* 	list_for_each_entry_safe(reqe, next, &fwork->req_entry_list, list) { */
/* 		if (reqe->is_submitted) { */
/* 			wait_for_req_entry(reqe); */
/* 		} else { */
/* 			blk_end_request_all(reqe->req, -EIO); */
/* 		} */
/* 		list_del(&reqe->list); */
/* 		destroy_req_entry(reqe); */
/* 	} */
/* 	destroy_pack_list_work(fwork); */
/* 	LOGd("pack_list_work_task error.\n"); */
}

/**
 * Flush request executing task.
 *
 * CONTEXT:
 *   Non-IRQ. Non-atomic.
 *   Request queue lock is not held.
 *   This task is serialized by the singlethreaded workqueue.
 */
static void req_flush_task(struct work_struct *work)
{
	/* now editing */

	
	struct pack_list_work *fwork = container_of(work, struct pack_list_work, work);
	struct request_queue *q = fwork->wdev->queue;
	unsigned long flags;
        
	LOGd("req_flush_task begin.\n");
	/* ASSERT(fwork->flush_req); */


	
	/* /\* Flush previous all requests. *\/ */
	/* pack_list_workqueue(wq_logpack_submit_); */
	/* blk_end_request_all(fwork->flush_req, 0); */

	/* /\* Restart queue if required. *\/ */
	/* if (must_restart_queue) { */
	/* 	spin_lock_irqsave(q->queue_lock, flags); */
	/* 	ASSERT(blk_queue_stopped(q)); */
	/* 	blk_start_queue(q); */
	/* 	spin_unlock_irqrestore(q->queue_lock, flags); */
	/* } */
        
	/* if (list_empty(&fwork->req_entry_list)) { */
	/* 	destroy_pack_list_work(fwork); */
	/* } else { */
	/* 	/\* Enqueue the following requests *\/ */
	/* 	fwork->flush_req = NULL; */
	/* 	INIT_WORK(&fwork->work, pack_list_work_task); */
	/* 	queue_work(wq_logpack_submit_, &fwork->work); */
	/* } */
	/* LOGd("req_flush_task end.\n"); */
}

/**
 * (1) Complete logpack creation.
 * (2) Submit all logpack-related bio(s).
 * (3) Enqueue logpack_list_wait_task.
 *
 * @work work in a logpack_list_work.
 *
 * CONTEXT:
 *   Workqueue task.
 *   Works are serialized by singlethread workqueue.
 */
static void logpack_list_submit_task(struct work_struct *work)
{
	struct pack_list_work *plwork = container_of(work, struct pack_list_work, work);
	struct wrapper_blk_dev *wdev = plwork->wdev;
	struct req_entry *reqe, *next;
	struct blk_plug plug;

	

	


	
	/* now editing */
}

/**
 *
 * @work work in a logpack_list_work.
 *
 * CONTEXT:
 *   Workqueue task.
 *   Works are serialized by singlethread workqueue.
 */
static void logpack_list_wait_task(struct work_struct *work)
{
	/* now editing */

}

/**
 * Wait all related write requests done and
 * free all related resources.
 *
 * @work work in a logpack_list_work.
 *
 * CONTEXT:
 *   Workqueue task.
 *   Works will be executed in parallel.
 */
static void logpack_list_gc_task(struct work_struct *work)
{
	/* now editing */


}

/**
 * Execute a write request.
 *
 * CONTEXT:
 *   Workqueue task.
 *   Works will be executed in parallel.
 */
static void write_req_task(struct work_struct *work)
{
	/* now editing */

}

/**
 * Execute a read request.
 *
 * CONTEXT:
 *   Workqueue task.
 *   Works will be executed in parallel.
 */
static void read_req_task(struct work_struct *work)
{
	/* now editing */

}


/**
 * Check whether pack is valid.
 *   Assume just created and filled. checksum is not calculated at all.
 *
 * RETURN:
 *   true if valid, or false.
 */
static bool is_valid_prepared_pack(struct pack *pack)
{
	unsigned int idx = 0;
	struct walb_logpack_header *lhead;
	unsigned int pbs;
	struct walb_log_record *lrec;
	unsigned int i;
	struct req_entry *reqe;
	bool is_write;
	u64 total_pb; /* total io size in physical block. */

	CHECK(pack);
	CHECK(pack->logpack_header_sector);
	is_write = pack->is_write;

	lhead = get_logpack_header(pack->logpack_header_sector);
	pbs = pack->logpack_header_sector->size;
	ASSERT_PBS(pbs);
	CHECK(lhead);
	CHECK(is_valid_logpack_header(lhead));

	CHECK(!list_empty(&pack->req_ent_list));
	
	i = 0;
	total_pb = 0;
	list_for_each_entry(reqe, &pack->req_ent_list, list) {

		CHECK(i < lhead->n_records);
		lrec = &lhead->record[i];
		CHECK(lrec);
		CHECK(lrec->is_exist);
		CHECK(!(lrec->is_padding));
		CHECK(reqe->req);

		CHECK(!(reqe->req->cmd_flags & REQ_FLUSH));
		if (is_write) {
			CHECK(reqe->req->cmd_flags & REQ_WRITE);
		} else {
			CHECK(!(reqe->req->cmd_flags & REQ_WRITE));
		}

		CHECK(blk_rq_pos(reqe->req) == (sector_t)lrec->offset);
		CHECK(lhead->logpack_lsid == lrec->lsid - lrec->lsid_local);
		CHECK(blk_rq_sectors(reqe->req) == lrec->io_size);
		total_pb += capacity_pb(pbs, lrec->io_size);
		
		i ++;
		if (lhead->record[i].is_padding) {
			total_pb += capacity_pb(pbs, lrec->io_size);
			i ++;
		}
	}
	CHECK(i == lhead->n_records);
	CHECK(total_pb == lhead->total_io_size);
	return true;
error:
	return false;
}

/**
 * Check whether pack list is valid.
 * This is just for debug.
 *
 * @listh list of struct pack.
 *
 * RETURN:
 *   true if valid, or false.
 */
static bool is_valid_pack_list(struct list_head *listh)
{
	struct pack *pack;
	list_for_each_entry(pack, listh, list) {
		CHECK(is_valid_prepared_pack(pack));
	}
	return true;
error:
	return false;
}


/*******************************************************************************
 * Global functions definition.
 *******************************************************************************/

/**
 * Make requrest callback.
 *
 * CONTEXT:
 *     IRQ: no. ATOMIC: yes.
 *     queue lock is held.
 */
void wrapper_blk_req_request_fn(struct request_queue *q)
{
	struct wrapper_blk_dev *wdev = wdev_get_from_queue(q);
	struct pdata *pdata = pdata_get_from_wdev(wdev);
	struct request *req;
	struct req_entry *reqe;
	struct pack_list_work *plwork;
	struct pack *wpack;
	bool ret;
	u64 latest_lsid, latest_lsid_old;
	
	/* Load latest_lsid */
	spin_lock(&pdata->lsid_lock);
	latest_lsid = pdata->latest_lsid;
	spin_unlock(&pdata->lsid_lock);
	latest_lsid_old = latest_lsid;

	/* Initialize pack_list_work. */
	plwork = create_pack_list_work(wdev, GFP_ATOMIC);
	if (!plwork) { goto error0; }
	/* Create the first writepack. */
	wpack = create_writepack(GFP_ATOMIC, wdev->pbs, latest_lsid);
	if (!wpack) { goto error1; }

	/* Fetch requests and create pack list. */
	while ((req = blk_fetch_request(q)) != NULL) {

		/* print_req_flags(req); */
		if (req->cmd_flags & REQ_WRITE) {
			/* REQ_FLUSH must be here. */
			if (req->cmd_flags & REQ_FLUSH) {
				LOGd("REQ_FLUSH request with size %u.\n", blk_rq_bytes(req));
			}
			ret = writepack_add_req(&plwork->wpack_list, &wpack, req,
						pdata->ring_buffer_size,
						&latest_lsid, GFP_ATOMIC);
			if (!ret) { goto req_error; }
		} else {
			/* Read request */
			reqe = create_req_entry(req, GFP_ATOMIC);
			if (!reqe) { goto req_error; }
			INIT_WORK(&reqe->work, read_req_task);
			queue_work(wq_normal_, &reqe->work);
		}
		continue;
	req_error:
		__blk_end_request_all(req, -EIO);
	}
	ASSERT(get_next_lsid(get_logpack_header(wpack->logpack_header_sector)) == latest_lsid);
	
	/* Enqueue logpack submit work if need. */
	if (!list_empty(&wpack->req_ent_list)) {
		list_add_tail(&wpack->list, &plwork->wpack_list);
	} else {
		destroy_pack(wpack);
		wpack = NULL;
	}
	if (!list_empty(&plwork->wpack_list)) {
		/* Currently all requests are packed and lsid of all writepacks is defined. */
		ASSERT(is_valid_pack_list(&plwork->wpack_list));
		INIT_WORK(&plwork->work, logpack_list_submit_task);
		queue_work(wq_logpack_submit_, &plwork->work);
	} else {
		destroy_pack_list_work(plwork);
		plwork = NULL;
	}
	
	/* Store latest_lsid */
	ASSERT(latest_lsid >= latest_lsid_old);
	spin_lock(&pdata->lsid_lock);
	ASSERT(pdata->latest_lsid == latest_lsid_old);
	pdata->latest_lsid = latest_lsid;
	spin_unlock(&pdata->lsid_lock);
	
	/* LOGd("wrapper_blk_req_request_fn: end.\n"); */
	return;
#if 0
error2:
	destroy_pack(wpack);
#endif
error1:
	destroy_pack_list_work(plwork);
error0:
	while ((req = blk_fetch_request(q)) != NULL) {
		__blk_end_request_all(req, -EIO);
	}
	/* LOGe("wrapper_blk_req_request_fn: error.\n"); */
}

/* Called before register. */
bool pre_register(void)
{
	LOGd("pre_register called.");

	/* Prepare kmem_cache data. */
	pack_list_work_cache_ = kmem_cache_create(
		KMEM_CACHE_PACK_LIST_WORK_NAME,
		sizeof(struct pack_list_work), 0, 0, NULL);
	if (!pack_list_work_cache_) {
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
	pack_cache_ = kmem_cache_create(
		KMEM_CACHE_PACK_NAME,
		sizeof(struct pack), 0, 0, NULL);
	if (pack_cache_) {
		LOGe("failed to create a kmem_cache.\n");
		goto error3;
	}
	
	/* prepare workqueues. */
	wq_logpack_submit_ = create_singlethread_workqueue(WQ_LOGPACK_SUBMIT);
	if (!wq_logpack_submit_) {
		LOGe("failed to allocate a workqueue.");
		goto error4;
	}
	wq_logpack_wait_ = create_singlethread_workqueue(WQ_LOGPACK_WAIT);
	if (!wq_logpack_wait_) {
		LOGe("failed to allocate a workqueue.");
		goto error5;
	}		
	wq_normal_ = alloc_workqueue(WQ_NORMAL, WQ_MEM_RECLAIM, 0);
	if (!wq_normal_) {
		LOGe("failed to allocate a workqueue.");
		goto error6;
	}

	return true;

#if 0
error7:
	destroy_workqueue(wq_normal_);
#endif
error6:
	destroy_workqueue(wq_logpack_wait_);
error5:
	destroy_workqueue(wq_logpack_submit_);
error4:	
	kmem_cache_destroy(pack_cache_);
error3:
	kmem_cache_destroy(bio_entry_cache_);
error2:
	kmem_cache_destroy(req_entry_cache_);
error1:
	kmem_cache_destroy(pack_list_work_cache_);
error0:
	return false;
}

/* Called after unregister. */
void post_unregister(void)
{
	LOGd("post_unregister called.");

	/* finalize workqueue data. */
	destroy_workqueue(wq_normal_);
	wq_normal_ = NULL;
	destroy_workqueue(wq_logpack_wait_);
	wq_logpack_wait_ = NULL;
	destroy_workqueue(wq_logpack_submit_);
	wq_logpack_submit_ = NULL;

	/* Destory kmem_cache data. */
	kmem_cache_destroy(pack_cache_);
	pack_cache_ = NULL;
	kmem_cache_destroy(bio_entry_cache_);
	bio_entry_cache_ = NULL;
	kmem_cache_destroy(req_entry_cache_);
	req_entry_cache_ = NULL;
	kmem_cache_destroy(pack_list_work_cache_);
	pack_list_work_cache_ = NULL;
}

/* end of file. */
