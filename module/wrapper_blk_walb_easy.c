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
 * A write pack.
 * There are no overlapping requests in a pack.
 */
struct pack
{
	struct list_head list; /* list entry. */
	struct list_head req_ent_list; /* list head of req_entry. */

	bool is_fua; /* FUA flag. */
	struct sector_data *logpack_header_sector;
	struct list_head bio_ent_list; /* list head of logpack bio. */

	bool is_logpack_failed; /* true if submittion failed. */
};
#define KMEM_CACHE_PACK_NAME "pack_cache"
struct kmem_cache *pack_cache_ = NULL;

/**
 * Request entry struct.
 */
struct req_entry
{
	struct list_head list; /* list entry */
	struct request *req;
	struct list_head bio_ent_list; /* list head of bio_entry */

	/* Notification from write_req_task to gc_task.
	   read_req_task does not use this. */
	struct completion done;
};
/* kmem cache for dbio. */
#define KMEM_CACHE_REQ_ENTRY_NAME "req_entry_cache"
struct kmem_cache *req_entry_cache_ = NULL;

/**
 * Request entry work struct.
 *
 * This is used for write_req_task/read_req_task.
 */
struct req_entry_work
{
	struct work_struct work;
	struct wrapper_blk_dev *wdev;

	struct req_entry *reqe;
};
#define KMEM_CACHE_REQ_ENTRY_WORK_NAME "req_entry_work_cache"
struct kmem_cache *req_entry_work_cache_ = NULL;

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

/* req_entry_work related. */
static struct req_entry_work* create_req_entry_work(
	struct req_entry *reqe, struct wrapper_blk_dev *wdev, gfp_t gfp_mask);
static void destroy_req_entry_work(struct req_entry_work *reqe_work);

/* bio_entry related. */
static void bio_entry_end_io(struct bio *bio, int error);
static struct bio_entry* create_bio_entry(gfp_t gfp_mask);
static struct bio_entry* create_bio_entry_by_clone(
	struct bio *bio, struct block_device *bdev, gfp_t gfp_mask);
static void destroy_bio_entry(struct bio_entry *bioe);

/* pack related. */
static struct pack* create_pack(gfp_t gfp_mask);
static struct pack* create_writepack(gfp_t gfp_mask, unsigned int pbs, u64 logpack_lsid);
static void destroy_pack(struct pack *pack);
static bool is_overlap_pack_reqe(struct pack *pack, struct req_entry *reqe);

/* helper function. */
static bool writepack_add_req(
	struct list_head *wpack_list, struct pack **wpackp, struct request *req,
	u64 ring_buffer_size, u64 *latest_lsidp, gfp_t gfp_mask);
static bool is_flush_first_req_entry(struct list_head *req_ent_list);

/* Workqueue tasks. */
static void logpack_list_submit_task(struct work_struct *work);
static void logpack_list_wait_task(struct work_struct *work);
static void logpack_list_gc_task(struct work_struct *work);
static void write_req_task(struct work_struct *work);
static void read_req_task(struct work_struct *work);

/* Helper functions for bio_entry list. */
static bool create_bio_entry_list(struct req_entry *reqe, struct block_device *bdev);
static void submit_bio_entry_list(struct req_entry *reqe);
static void wait_for_bio_entry_list(struct req_entry *reqe, bool is_end_request);

/* Validator for debug. */
static bool is_valid_prepared_pack(struct pack *pack);
UNUSED static bool is_valid_pack_list(struct list_head *pack_list);

/* Logpack related functions. */
static void logpack_calc_checksum(
	struct walb_logpack_header *lhead,
	unsigned int pbs, struct list_head *req_ent_list);
static struct bio_entry* logpack_submit_lhead(
	struct walb_logpack_header *lhead, bool is_flush, bool is_fua,
	unsigned int pbs, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size);
static bool logpack_submit_req(
	struct request *req, u64 lsid, bool is_fua,
	struct list_head *bio_ent_list,
	unsigned int pbs, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size);
static struct bio_entry* logpack_submit_bio(
	struct bio *bio, bool is_fua, unsigned int pbs,
	struct block_device *ldev,
	u64 ldev_offset, unsigned int bio_offset);
static struct bio_entry* logpack_submit_flush(struct block_device *bdev, bool is_fua);
static bool logpack_submit(
	struct walb_logpack_header *lhead, bool is_fua,
	struct list_head *req_ent_list, struct list_head *bio_ent_list,
	unsigned int pbs, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size);


static bool logpack_wait(struct pack *wpack);
static void enqueue_datapack_tasks(struct pack *wpack, struct wrapper_blk_dev *wdev);
static void logpack_end_err(struct request_queue *q, struct pack *wpack);
static void logpack_end_err_wo_lock(struct pack *wpack);



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
	INIT_LIST_HEAD(&reqe->bio_ent_list);
	init_completion(&reqe->done);
        
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
		list_for_each_entry_safe(bioe, next, &reqe->bio_ent_list, list) {
			list_del(&bioe->list);
			destroy_bio_entry(bioe);
		}
#ifdef WALB_DEBUG
		reqe->req = NULL;
		INIT_LIST_HEAD(&reqe->list);
		INIT_LIST_HEAD(&reqe->bio_ent_list);
#endif
		kmem_cache_free(req_entry_cache_, reqe);
	}
}

/**
 * Create a req_entry_work.
 */
static struct req_entry_work* create_req_entry_work(
	struct req_entry *reqe, struct wrapper_blk_dev *wdev, gfp_t gfp_mask)
{
	struct req_entry_work *reqe_work;
	ASSERT(reqe);
	ASSERT(wdev);
	
	reqe_work = kmem_cache_alloc(req_entry_work_cache_, gfp_mask);
	if (!reqe_work) { goto error0; }
	reqe_work->wdev = wdev;
	reqe_work->reqe = reqe;
	return reqe_work;
	
error0:
	return NULL;
}

/**
 * Destory a req_entry_work.
 */
static void destroy_req_entry_work(struct req_entry_work *reqe_work)
{
	if (!reqe_work) { return; }

	if (reqe_work->reqe) {
		destroy_req_entry(reqe_work->reqe);
	}
	kmem_cache_free(req_entry_work_cache_, reqe_work);
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
 * Internal bio and bi_size will be set NULL.
 */
static struct bio_entry* create_bio_entry(gfp_t gfp_mask)
{
	struct bio_entry *bioe;

	bioe = kmem_cache_alloc(bio_entry_cache_, gfp_mask);
	if (!bioe) {
		LOGd("kmem_cache_alloc() failed.");
		goto error0;
	}
	init_completion(&bioe->done);
	bioe->error = 0;
	bioe->bio = NULL;
	bioe->bi_size = 0;
	return bioe;

error0:
	LOGe("create_bio_entry() end with error.\n");
	return NULL;
}

/**
 * Create a bio_entry by clone.
 *
 * @bio original bio.
 * @bdev block device to forward bio.
 */
static struct bio_entry* create_bio_entry_by_clone(
	struct bio *bio, struct block_device *bdev, gfp_t gfp_mask)
{
	struct bio_entry *bioe;
	struct bio *biotmp;

	bioe = create_bio_entry(gfp_mask);
	if (!bioe) { goto error0; }
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
	LOGe("create_bio_entry_by_clone() end with error.\n");
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
static struct pack* create_pack(gfp_t gfp_mask)
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
	pack->is_fua = false;
	pack->is_logpack_failed = false;
	
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
	pack = create_pack(gfp_mask);
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
 * Check first request entry is flush.
 *
 * @req_ent_list req_entry list. Never empty.
 *
 * RETURN:
 *   true if the first req_entry is flush request, or false.
 */
static bool is_flush_first_req_entry(struct list_head *req_ent_list)
{
	struct req_entry *reqe;
	ASSERT(!list_empty(req_ent_list));
	
	reqe = list_first_entry(req_ent_list, struct req_entry, list);
	ASSERT(reqe);
	ASSERT(reqe->req);
	if (reqe->req->cmd_flags == REQ_FLUSH) {
		return true;
	} else {
		return false;
	}
}

/**
 * Create bio_entry list for a request.
 *
 * RETURN:
 *     true if succeed, or false.
 * CONTEXT:
 *     Non-IRQ. Non-atomic.
 */
static bool create_bio_entry_list(struct req_entry *reqe, struct block_device *bdev)
{
	struct bio_entry *bioe, *next;
	struct bio *bio;
        
	ASSERT(reqe);
	ASSERT(reqe->req);
	ASSERT(list_empty(&reqe->bio_ent_list));
        
	/* clone all bios. */
	__rq_for_each_bio(bio, reqe->req) {
		/* clone bio */
		bioe = create_bio_entry_by_clone(bio, bdev, GFP_NOIO);
		if (!bioe) {
			LOGd("create_bio_entry() failed.\n"); 
			goto error1;
		}
		list_add_tail(&bioe->list, &reqe->bio_ent_list);
	}
	return true;
error1:
	list_for_each_entry_safe(bioe, next, &reqe->bio_ent_list, list) {
		list_del(&bioe->list);
		destroy_bio_entry(bioe);
	}
	ASSERT(list_empty(&reqe->bio_ent_list));
	return false;
}

/**
 * Submit all bio_entry(s) in a req_entry.
 *
 * @reqe target req_entry.
 *
 * CONTEXT:
 *   non-irq. non-atomic.
 */
static void submit_bio_entry_list(struct req_entry *reqe)
{
	struct bio_entry *bioe;
	list_for_each_entry(bioe, &reqe->bio_ent_list, list) {
		generic_make_request(bioe->bio);
	}
}

/**
 * Wait for completion of all bio_entry(s) related a req_entry
 * and end request if required.
 *
 * @reqe target req_entry.
 * @is_end_request true if end request call is required, or false.

 * CONTEXT:
 *   non-irq. non-atomic.
 */
static void wait_for_bio_entry_list(struct req_entry *reqe, bool is_end_request)
{
	struct bio_entry *bioe, *next;
	int remaining;
	ASSERT(reqe);
        
	remaining = blk_rq_bytes(reqe->req);
	list_for_each_entry_safe(bioe, next, &reqe->bio_ent_list, list) {
		list_del(&bioe->list);
		wait_for_completion(&bioe->done);
		if (is_end_request) {
			blk_end_request(reqe->req, bioe->error, bioe->bi_size);
		}
		remaining -= bioe->bi_size;
		destroy_bio_entry(bioe);
	}
	ASSERT(remaining == 0);
	ASSERT(list_empty(&reqe->bio_ent_list));
}

/**
 * Submit all logpacks related to a call of request_fn.
 * 
 * (1) Complete logpack creation.
 * (2) Submit all logpack-related bio(s).
 * (3) Enqueue logpack_list_wait_task.
 *
 *
 * If an error (memory allocation failure) occurred,
 * logpack_list_wait_task must finalize the logpack resources and
 * must not execute datapack IO.
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
	struct pdata *pdata = pdata_get_from_wdev(wdev);
	struct blk_plug plug;
	struct pack *wpack;
	struct walb_logpack_header *lhead;
	bool ret;

	blk_start_plug(&plug);
	list_for_each_entry(wpack, &plwork->wpack_list, list) {

		ASSERT_SECTOR_DATA(wpack->logpack_header_sector);
		lhead = get_logpack_header(wpack->logpack_header_sector);
		logpack_calc_checksum(lhead, wdev->pbs, &wpack->req_ent_list);

		ret = logpack_submit(lhead, wpack->is_fua,
				&wpack->req_ent_list, &wpack->bio_ent_list,
				wdev->pbs, pdata->ldev, pdata->ring_buffer_off, pdata->ring_buffer_size);
		wpack->is_logpack_failed = !ret;
		if (!ret) { break; }
	}
	blk_finish_plug(&plug);

	/* Enqueue logpack list wait task. */
	INIT_WORK(&plwork->work, logpack_list_wait_task);
	queue_work(wq_logpack_wait_, &plwork->work);
}

/**
 * Wait for completion of all bio(s) related to the logpack.
 *
 * RETURN:
 *   true if all bio(s) succeeded, or false.
 */
static bool logpack_wait(struct pack *wpack)
{
	struct bio_entry *bioe, *next_bioe;
	bool is_failed = false;

	ASSERT(wpack);

	list_for_each_entry_safe(bioe, next_bioe, &wpack->bio_ent_list, list) {
		list_del(&bioe->list);
		wait_for_completion(&bioe->done);
		if (bioe->error) { is_failed = true; }
		destroy_bio_entry(bioe);
	}
	ASSERT(list_empty(&wpack->bio_ent_list));

	return !is_failed && !wpack->is_logpack_failed;
}

/**
 * Enqueue all requests in a writepack for data device IO.
 *
 * @wpack writepack.
 * @wdev wrapper block device.
 *
 * RETURN:
 *   true in success, or false.
 *   When false, partial requests are enqueued, remainings are ended with error.
 * CONTEXT:
 *   this function call is serialized by singlethread workqueue.
 */
static void enqueue_datapack_tasks(struct pack *wpack, struct wrapper_blk_dev *wdev)
{
	struct req_entry *reqe, *next_reqe;
	struct request *req;
	struct req_entry_work *reqe_work;
	bool is_failed = false;
	
	ASSERT(wpack);
	ASSERT(!wpack->is_logpack_failed);
	ASSERT(list_empty(&wpack->bio_ent_list));
	ASSERT(wdev);

	list_for_each_entry_safe(reqe, next_reqe, &wpack->req_ent_list, list) {

		req = reqe->req;
		ASSERT(req);
		if (blk_rq_sectors(req) == 0) {

			ASSERT(req->cmd_flags & REQ_FLUSH);
			/* Already the corresponding logpack is permanent. */
			blk_end_request_all(req, 0);

			list_del(&reqe->list);
			destroy_req_entry(reqe);
		} else {
			/* check and insert to overlapping detection data. */
			/* not yet implemented */

			/* Enqueue as a write req task. */
			if (is_failed) { goto failed; }
			
			reqe_work = create_req_entry_work(reqe, wdev, GFP_NOIO);
			if (!reqe_work) {
				is_failed = true;
				goto failed;
			}
			INIT_WORK(&reqe_work->work, write_req_task);
			queue_work(wq_normal_, &reqe_work->work);
			continue;
		failed:
			blk_end_request_all(req, -EIO);
			list_del(&reqe->list);
			destroy_req_entry(reqe);
		}
	}
}

/**
 * Wait for completion of all logpacks related to a call of request_fn.
 *
 * If submission a logpack is partially failed,
 * this function will end all requests related to the logpack and the followings.
 *
 * All failed (and end_request called) reqe(s) will be destroyed.
 *
 * @work work in a logpack_list_work.
 *
 * CONTEXT:
 *   Workqueue task.
 *   Works are serialized by singlethread workqueue.
 */
static void logpack_list_wait_task(struct work_struct *work)
{
	struct pack_list_work *plwork = container_of(work, struct pack_list_work, work);
	struct wrapper_blk_dev *wdev = plwork->wdev;
	bool is_failed = false;
	struct pack *wpack;
	bool ret;

	list_for_each_entry(wpack, &plwork->wpack_list, list) {

		ret = logpack_wait(wpack);
		if (!ret) { is_failed = true; }
		if (is_failed) { goto failed; }

		/* Enqueue all related requests for data device. */
		enqueue_datapack_tasks(wpack, wdev);
		continue;
	failed:
		logpack_end_err(wdev->queue, wpack);
	}
	
	/* Enqueue logpack list gc task. */
	INIT_WORK(&plwork->work, logpack_list_gc_task);
	queue_work(wq_normal_, &plwork->work);
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
	struct pack_list_work *plwork = container_of(work, struct pack_list_work, work);
	struct pack *wpack, *next_wpack;
	struct req_entry *reqe, *next_reqe;

	list_for_each_entry_safe(wpack, next_wpack, &plwork->wpack_list, list) {
		list_del(&wpack->list);
		list_for_each_entry_safe(reqe, next_reqe, &wpack->req_ent_list, list) {
			list_del(&reqe->list);
			wait_for_completion(&reqe->done);
			destroy_req_entry(reqe);
		}
		ASSERT(list_empty(&wpack->req_ent_list));
		ASSERT(list_empty(&wpack->bio_ent_list));
		destroy_pack(wpack);
	}
	ASSERT(list_empty(&plwork->wpack_list));
	destroy_pack_list_work(plwork);
}

/**
 * Execute a write request.
 *
 * CONTEXT:
 *   Workqueue task.
 *   Works will be executed in parallel.
 *   Queue lock is not held.
 */
static void write_req_task(struct work_struct *work)
{
	struct req_entry_work *reqe_work = container_of(work, struct req_entry_work, work);
	struct req_entry *reqe = reqe_work->reqe;
	struct wrapper_blk_dev *wdev = reqe_work->wdev;
	struct pdata *pdata = pdata_get_from_wdev(wdev);
	struct blk_plug plug;
	bool is_end_request = true;

	ASSERT(list_empty(&reqe->bio_ent_list));
	
	/* Wait for previous overlapping writes. */
	/* not yet implemented */

	/* Create all related bio(s). */
	if (!create_bio_entry_list(reqe, pdata->ddev)) {
		goto alloc_error;
	}
	
	/* Submit all related bio(s). */
	blk_start_plug(&plug);
	submit_bio_entry_list(reqe);
	blk_finish_plug(&plug);

	/* Wait for completion and call end_request. */
	wait_for_bio_entry_list(reqe, is_end_request);

	/* Delete from overlapping detection data. */
	/* not yet implemented */

	/* Notify logpack_list_gc_task(). */
	complete(&reqe->done);

	/* Reqe will be destroyed in logpack_list_gc_task(). */
	reqe_work->reqe = NULL;
	destroy_req_entry_work(reqe_work);
	return;
	
alloc_error:
	ASSERT(list_empty(&reqe->bio_ent_list));
	blk_end_request_all(reqe->req, -EIO);
	complete(&reqe->done);
	reqe_work->reqe = NULL;
	destroy_req_entry_work(reqe_work);
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
	struct req_entry_work *reqe_work = container_of(work, struct req_entry_work, work);
	struct req_entry *reqe = reqe_work->reqe;
	struct wrapper_blk_dev *wdev = reqe_work->wdev;
	struct pdata *pdata = pdata_get_from_wdev(wdev);
	struct blk_plug plug;
	bool is_end_request = true;

	/* Create all related bio(s). */
	if (!create_bio_entry_list(reqe, pdata->ddev)) {
		goto alloc_error;
	}
	
	/* Submit all related bio(s). */
	blk_start_plug(&plug);
	submit_bio_entry_list(reqe);
	blk_finish_plug(&plug);

	/* Wait for completion and call end_request. */
	wait_for_bio_entry_list(reqe, is_end_request);

	destroy_req_entry_work(reqe_work);
	return;
	
alloc_error:
	ASSERT(list_empty(&reqe->bio_ent_list));
	blk_end_request_all(reqe->req, -EIO);
	destroy_req_entry_work(reqe_work);
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
	struct walb_logpack_header *lhead;
	unsigned int pbs;
	struct walb_log_record *lrec;
	unsigned int i;
	struct req_entry *reqe;
	u64 total_pb; /* total io size in physical block. */

	CHECK(pack);
	CHECK(pack->logpack_header_sector);

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

		CHECK(reqe->req->cmd_flags & REQ_WRITE);

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
static bool is_valid_pack_list(struct list_head *pack_list)
{
	struct pack *pack;
	
	list_for_each_entry(pack, pack_list, list) {
		CHECK(is_valid_prepared_pack(pack));
	}
	return true;
error:
	return false;
}

/**
 * Calc checksum of each requests and log header and set it.
 *
 * @lhead log pack header.
 * @physical_bs physical sector size (allocated size as lhead).
 * @reqp_ary requests to add.
 * @n_req number of requests.
 *
 * @return 0 in success, or -1.
 */
static void logpack_calc_checksum(
	struct walb_logpack_header *lhead,
	unsigned int pbs, struct list_head *req_ent_list)
{
        int i;
	struct req_entry *reqe;
        struct request *req;
        struct req_iterator iter;
        struct bio_vec *bvec;
        u64 sum;
        int n_padding;

	ASSERT(lhead);
	ASSERT(lhead->n_records > 0);
	ASSERT(lhead->n_records > lhead->n_padding);
	
        n_padding = 0;
        i = 0;
	list_for_each_entry(reqe, req_ent_list, list) {

                if (lhead->record[i].is_padding) {
                        n_padding ++;
                        i ++;
                        continue;
                }
		
		ASSERT(reqe);
		req = reqe->req;
		ASSERT(req);
		ASSERT(req->cmd_flags & REQ_WRITE);

		if (blk_rq_sectors(req) == 0) {
			ASSERT(req->cmd_flags & REQ_FLUSH);
			continue;
		}

		sum = 0;
                rq_for_each_segment(bvec, req, iter) {
                        sum = checksum_partial
                                (sum,
                                 kmap(bvec->bv_page) + bvec->bv_offset,
                                 bvec->bv_len);
                        kunmap(bvec->bv_page);
                }

                lhead->record[i].checksum = checksum_finish(sum);
                i ++;
	}
	
        ASSERT(n_padding <= 1);
        ASSERT(n_padding == lhead->n_padding);
        ASSERT(i == lhead->n_records);
        ASSERT(lhead->checksum == 0);
        lhead->checksum = checksum((u8 *)lhead, pbs);
        ASSERT(checksum((u8 *)lhead, pbs) == 0);
}

/**
 * Submit bio of header block.
 *
 * RETURN:
 *   bio_entry in success, or NULL.
 */
static struct bio_entry* logpack_submit_lhead(
	struct walb_logpack_header *lhead, bool is_flush, bool is_fua,
	unsigned int pbs, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size)
{
	struct bio *bio;
	struct bio_entry *bioe;
	struct page *page;
	u64 off_pb, off_lb;
	int rw = WRITE;

	if (is_flush) { rw |= WRITE_FLUSH; }
	if (is_fua) { rw |= WRITE_FUA; }
	
	bioe = create_bio_entry(GFP_NOIO);
	if (!bioe) { goto error0; }
	bio = bio_alloc(GFP_NOIO, 1);
	if (!bio) { goto error1; }

	page = virt_to_page(lhead);
	bio->bi_bdev = ldev;
	off_pb = lhead->logpack_lsid % ring_buffer_size + ring_buffer_off;
	
	off_lb = addr_lb(pbs, off_pb);
	bio->bi_sector = off_lb;
	bio->bi_end_io = bio_entry_end_io;
	bio->bi_private = bioe;
	bio_add_page(bio, page, pbs, offset_in_page(lhead));
	
	bioe->bio = bio;
	bioe->bi_size = bio_cur_bytes(bio);
	ASSERT(bioe->bi_size == pbs);

	LOGd("submit logpack header bio: off %llu size %u\n",
		(u64)bio->bi_sector, bio_cur_bytes(bio));
	submit_bio(rw, bio);

	return bioe;

error1:
	destroy_bio_entry(bioe);
error0:
	return NULL;
}

/**
 * Submit all logpack bio(s) for a request.
 *
 * @req original request.
 * @lsid lsid of the request in the logpack.
 * @is_fua true if logpack must be submitted with FUA flag.
 * @bio_ent_list successfully submitted bioe(s) must be added to the tail of this.
 * @pbs physical block size [bytes]
 * @ldev log device.
 * @ring_buffer_off ring buffer offset [physical block].
 * @ring_buffer_size ring buffer size [physical block].
 *
 * RETURN:
 *   true in success, false in partially failed.
 */
static bool logpack_submit_req(
	struct request *req, u64 lsid, bool is_fua,
	struct list_head *bio_ent_list,
	unsigned int pbs, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size)
{
	unsigned int off_lb;
	struct bio_entry *bioe;
	struct bio *bio;
	u64 ldev_off_pb = lsid % ring_buffer_size + ring_buffer_off;
	
	off_lb = 0;
	__rq_for_each_bio(bio, req) {

		bioe = logpack_submit_bio(bio, is_fua, pbs, ldev, ldev_off_pb, off_lb);
		if (!bioe) {
			goto error;
		}
		ASSERT(bioe->bi_size % LOGICAL_BLOCK_SIZE == 0);
		off_lb += bioe->bi_size / LOGICAL_BLOCK_SIZE;
		list_add_tail(&bioe->list, bio_ent_list);
	}
	
	return true;
error:
	return false;
}

/**
 * Create and submit a bio which is a part of logpack.
 *
 * @bio original bio to clone.
 * @is_fua true if logpack must be submitted with FUA flag.
 * @pbs physical block device [bytes].
 * @ldev_offset log device offset for the request [physical block].
 * @bio_offset offset of the bio inside the whole request [logical block].
 *
 * RETURN:
 *   bio_entry in success which bio is submitted, or NULL.
 */
static struct bio_entry* logpack_submit_bio(
	struct bio *bio, bool is_fua, unsigned int pbs,
	struct block_device *ldev,
	u64 ldev_offset, unsigned int bio_offset)
{
	struct bio_entry *bioe;
	struct bio *cbio;

	bioe = create_bio_entry(GFP_NOIO);
	if (!bioe) { goto error0; }

	cbio = bio_clone(bio, GFP_NOIO);
	if (!cbio) { goto error1; }

	cbio->bi_bdev = ldev;
	cbio->bi_end_io = bio_entry_end_io;
	cbio->bi_private = bioe;

	cbio->bi_sector = addr_lb(pbs, ldev_offset) + bio_offset;
	bioe->bio = cbio;
	bioe->bi_size = cbio->bi_size;

	if (is_fua) {
		cbio->bi_rw |= WRITE_FUA;
	}
	submit_bio(cbio->bi_rw, cbio);
	return bioe;

error1:
	destroy_bio_entry(bioe);
error0:
	return NULL;
}

/**
 * Submit a flush request.
 *
 * @bdev block device.
 * @is_fua FUA flag.
 *
 * RETURN:
 *   created bioe containing submitted bio in success, or NULL.
 * CONTEXT:
 *   non-atomic.
 */
static struct bio_entry* logpack_submit_flush(struct block_device *bdev, bool is_fua)
{
	struct bio_entry *bioe;
	struct bio *bio;
	int rw = (is_fua ? WRITE_FLUSH_FUA : WRITE_FLUSH);

	bioe = create_bio_entry(GFP_NOIO);
	if (!bioe) { goto error0; }
	
	bio = bio_alloc(GFP_NOIO, 0);
	if (!bio) { goto error1; }

	bio->bi_end_io = bio_entry_end_io;
	bio->bi_private = bioe;
	bio->bi_bdev = bdev;

	bioe->bio = bio;
	bioe->bi_size = bio->bi_size;
	ASSERT(bioe->bi_size == 0);
	
	submit_bio(rw, bio);

	return bioe;
error1:
	destroy_bio_entry(bioe);
error0:
	return NULL;
}

/**
 * Submit logpack entry.
 *
 * @lhead logpack header.
 * @is_fua FUA flag.
 * @req_ent_list request entry list.
 * @bio_ent_list bio entry list.
 *   all submitted bios will be added to the list.
 * @wdev wrapper block device.
 *
 * RETURN:
 *     true if succeed, or false.
 * CONTEXT:
 *     Non-IRQ. Non-atomic.
 */
static bool logpack_submit(
	struct walb_logpack_header *lhead, bool is_fua,
	struct list_head *req_ent_list, struct list_head *bio_ent_list,
	unsigned int pbs, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size)
{
	struct bio_entry *bioe;
	struct req_entry *reqe;
	struct request *req;
	bool ret;
	bool is_flush;
	u64 req_lsid;
	int i;

	ASSERT(list_empty(bio_ent_list));
	ASSERT(!list_empty(req_ent_list));
	is_flush = is_flush_first_req_entry(req_ent_list);

	/* Submit logpack header block. */
	bioe = logpack_submit_lhead(lhead, is_flush, is_fua, pbs, ldev,
				ring_buffer_off, ring_buffer_size);
	if (!bioe) {
		LOGe("logpack header submit failed.\n");
		goto failed;
	}
	list_add_tail(&bioe->list, bio_ent_list);
	bioe = NULL;

	/* Submit logpack contents for each request. */
	i = 0;
	list_for_each_entry(reqe, req_ent_list, list) {

		req = reqe->req;
		if (blk_rq_sectors(req) == 0) {
			ASSERT(req->cmd_flags & REQ_FLUSH); /* such request must be flush. */
			ASSERT(i == 0); /* such request must be permitted at first only. */
			bioe = logpack_submit_flush(ldev, is_fua);
			if (!bioe) {
				LOGe("memory allocation failed durint logpack submit.\n");
				goto failed;
			}
			list_add_tail(&bioe->list, bio_ent_list);
		} else {
			if (lhead->record[i].is_padding) {
				i ++;
				/* padding record never come last. */
			}
			ASSERT(i < lhead->n_records);
			req_lsid = lhead->record[i].lsid;

			ret = logpack_submit_req(
				req, req_lsid, is_fua, bio_ent_list,
				pbs, ldev, ring_buffer_off, ring_buffer_size);
			if (!ret) {
				LOGe("memory allocation failed during logpack submit.\n");
				goto failed;
			}
		}
		i ++;
	}
	return true;

failed:
	return false;
}

/**
 * Call end_request for all requests related to a logpack.
 *
 * @q queue (for lock).
 * @wpack target write pack.
 *
 * CONTEXT:
 *   non-atomic.
 *   queue lock is not held.
 */
static void logpack_end_err(struct request_queue *q, struct pack *wpack)
{
	unsigned long flags;
	ASSERT(q);
	
	spin_lock_irqsave(q->queue_lock, flags);
	logpack_end_err_wo_lock(wpack);
	spin_unlock_irqrestore(q->queue_lock, flags);
}

/**
 * Call end request for all requests related to a logpck.
 *
 * CONTEXT:
 *   queue lock is held.
 */
static void logpack_end_err_wo_lock(struct pack *wpack)
{
	struct req_entry *reqe, *next_reqe;
	ASSERT(wpack);

	/* end_request for all related requests with error. */
	list_for_each_entry_safe(reqe, next_reqe, &wpack->req_ent_list, list) {
		list_del(&reqe->list);
		__blk_end_request_all(reqe->req, -EIO);
		destroy_req_entry(reqe);
	}
	ASSERT(list_empty(&wpack->req_ent_list));
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
	struct req_entry_work *reqe_work;
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
			reqe_work = create_req_entry_work(reqe, wdev, GFP_ATOMIC);
			if (!reqe_work) { destroy_req_entry(reqe); goto req_error; }
			INIT_WORK(&reqe_work->work, read_req_task);
			queue_work(wq_normal_, &reqe_work->work);
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
	req_entry_work_cache_ = kmem_cache_create(
		KMEM_CACHE_REQ_ENTRY_WORK_NAME,
		sizeof(struct req_entry_work), 0, 0, NULL);
	if (!req_entry_work_cache_) {
		LOGe("failed to create a kmem_cache.\n");
		goto error2;
	}
	bio_entry_cache_ = kmem_cache_create(
		KMEM_CACHE_BIO_ENTRY_NAME,
		sizeof(struct bio_entry), 0, 0, NULL);
	if (!bio_entry_cache_) {
		LOGe("failed to create a kmem_cache.\n");
		goto error3;
	}
	pack_cache_ = kmem_cache_create(
		KMEM_CACHE_PACK_NAME,
		sizeof(struct pack), 0, 0, NULL);
	if (pack_cache_) {
		LOGe("failed to create a kmem_cache.\n");
		goto error4;
	}
	
	/* prepare workqueues. */
	wq_logpack_submit_ = create_singlethread_workqueue(WQ_LOGPACK_SUBMIT);
	if (!wq_logpack_submit_) {
		LOGe("failed to allocate a workqueue.");
		goto error5;
	}
	wq_logpack_wait_ = create_singlethread_workqueue(WQ_LOGPACK_WAIT);
	if (!wq_logpack_wait_) {
		LOGe("failed to allocate a workqueue.");
		goto error6;
	}		
	wq_normal_ = alloc_workqueue(WQ_NORMAL, WQ_MEM_RECLAIM, 0);
	if (!wq_normal_) {
		LOGe("failed to allocate a workqueue.");
		goto error7;
	}

	return true;

#if 0
error8:
	destroy_workqueue(wq_normal_);
#endif
error7:
	destroy_workqueue(wq_logpack_wait_);
error6:
	destroy_workqueue(wq_logpack_submit_);
error5:	
	kmem_cache_destroy(pack_cache_);
error4:
	kmem_cache_destroy(bio_entry_cache_);
error3:	
	kmem_cache_destroy(req_entry_work_cache_);
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
	kmem_cache_destroy(req_entry_work_cache_);
	req_entry_work_cache_ = NULL;
	kmem_cache_destroy(req_entry_cache_);
	req_entry_cache_ = NULL;
	kmem_cache_destroy(pack_list_work_cache_);
	pack_list_work_cache_ = NULL;
}

/* end of file. */
