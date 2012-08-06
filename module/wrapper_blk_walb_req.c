/**
 * wrapper_blk_walb_req.c - WalB block device with request base for test.
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
#include <linux/mutex.h>

#include "wrapper_blk.h"
#include "wrapper_blk_walb.h"
#include "sector_io.h"
#include "logpack.h"
#include "treemap.h"
#include "bio_entry.h"
#include "req_entry.h"
#include "walb/walb.h"
#include "walb/block_size.h"

/*******************************************************************************
 * Static data definition.
 *******************************************************************************/

/**
 * Workqueue for logpack submit/wait/gc task.
 * This should be shared by all walb devices.
 */
#define WQ_LOGPACK "wq_logpack"
struct workqueue_struct *wq_logpack_ = NULL;

/**
 * Workqueue for various task including IO on data devices.
 * This should be shared by all walb devices.
 */
#define WQ_NORMAL "wq_normal"
struct workqueue_struct *wq_normal_ = NULL;

/**
 * Workqueue for read requests.
 * This is because pending data writes prevent
 * read request to be executed.
 * This should be shared by all walb devices.
 */
#define WQ_READ "wq_read"
struct workqueue_struct *wq_read_ = NULL;

/**
 * Writepack work.
 */
struct pack_work
{
	struct work_struct work;
	struct wrapper_blk_dev *wdev;
	struct list_head wpack_list; /* used for gc task only. */
};
/* kmem_cache for logpack_work. */
#define KMEM_CACHE_PACK_WORK_NAME "pack_work_cache"
struct kmem_cache *pack_work_cache_ = NULL;

/**
 * A write pack.
 * There are no overlapping requests in a pack.
 */
struct pack
{
	struct list_head list; /* list entry. */
	struct list_head req_ent_list; /* list head of req_entry. */

	bool is_zero_flush_only; /* true if req_ent_list contains only a zero-size flush. */
	bool is_fua; /* FUA flag. */
	struct sector_data *logpack_header_sector;
	struct list_head bio_ent_list; /* list head for zero_flush bio
					  or logpack header bio. */

	bool is_logpack_failed; /* true if submittion failed. */
};
#define KMEM_CACHE_PACK_NAME "pack_cache"
struct kmem_cache *pack_cache_ = NULL;

/* Completion timeout [msec]. */
static const unsigned long completion_timeo_ms_ = 5000;

/*******************************************************************************
 * Macros definition.
 *******************************************************************************/


/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

/* Print functions for debug. */
UNUSED static void print_req_flags(struct request *req);
UNUSED static void print_pack(const char *level, struct pack *pack);
UNUSED static void print_pack_list(const char *level, struct list_head *wpack_list);

/* pack_work related. */
static struct pack_work* create_pack_work(
	struct wrapper_blk_dev *wdev, gfp_t gfp_mask);
static void destroy_pack_work(struct pack_work *work);

/* bio_entry related. */
static void bio_entry_end_io(struct bio *bio, int error);
static struct bio_entry* create_bio_entry_by_clone(
	struct bio *bio, struct block_device *bdev, gfp_t gfp_mask);
#ifdef WALB_FAST_ALGORITHM
static struct bio_entry* create_bio_entry_by_clone_copy(
	struct bio *bio, struct block_device *bdev, gfp_t gfp_mask);
#endif

/* pack related. */
static struct pack* create_pack(gfp_t gfp_mask);
static struct pack* create_writepack(gfp_t gfp_mask, unsigned int pbs, u64 logpack_lsid);
static void destroy_pack(struct pack *pack);
UNUSED static bool is_overlap_pack_reqe(struct pack *pack, struct req_entry *reqe);
UNUSED static bool is_zero_flush_only(struct pack *pack);
static bool is_pack_size_exceeds(
	struct walb_logpack_header *lhead,
	unsigned int pbs, unsigned int max_logpack_pb,
	struct req_entry *reqe);

/* helper function. */
static bool writepack_add_req(
	struct list_head *wpack_list, struct pack **wpackp, struct request *req,
	u64 ring_buffer_size, unsigned int max_logpack_pb, u64 *latest_lsidp,
	struct wrapper_blk_dev *wdev, gfp_t gfp_mask);
static bool is_flush_first_req_entry(struct list_head *req_ent_list);

/* Workqueue tasks. */
static void logpack_list_submit_task(struct work_struct *work);
static void logpack_list_wait_task(struct work_struct *work);
static void logpack_list_gc_task(struct work_struct *work);
static void write_req_task(struct work_struct *work);
static void read_req_task(struct work_struct *work);

/* Helper functions for tasks. */
static void logpack_list_submit(
	struct wrapper_blk_dev *wdev, struct list_head *wpack_list);
#ifdef WALB_FAST_ALGORITHM
static void read_req_task_fast(struct work_struct *work);
static void write_req_task_fast(struct work_struct *work);
#else
static void read_req_task_easy(struct work_struct *work);
static void write_req_task_easy(struct work_struct *work);
#endif

/* Helper functions for bio_entry list. */
static bool create_bio_entry_list(struct req_entry *reqe, struct block_device *bdev);
#ifdef WALB_FAST_ALGORITHM
static bool create_bio_entry_list_copy(
	struct req_entry *reqe, struct block_device *bdev);
#endif
static void submit_bio_entry_list(struct list_head *bio_ent_list);
static void wait_for_req_entry(
	struct req_entry *reqe, bool is_end_request, bool is_delete);

/* Validator for debug. */
static bool is_valid_prepared_pack(struct pack *pack);
UNUSED static bool is_valid_pack_list(struct list_head *pack_list);

/* Logpack related functions. */
static void logpack_calc_checksum(
	struct walb_logpack_header *lhead,
	unsigned int pbs, struct list_head *req_ent_list);
static bool logpack_submit_lhead(
	struct walb_logpack_header *lhead, bool is_flush, bool is_fua,
	struct list_head *bio_ent_list,
	unsigned int pbs, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size,
	unsigned int chunk_sectors);
static bool logpack_submit_req(
	struct request *req, u64 lsid, bool is_fua,
	struct list_head *bio_ent_list,
	unsigned int pbs, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size,
	unsigned int chunk_sectors);
static struct bio_entry* logpack_create_bio_entry(
	struct bio *bio, bool is_fua, unsigned int pbs,
	struct block_device *ldev,
	u64 ldev_offset, unsigned int bio_offset);
static struct bio_entry* submit_flush(struct block_device *bdev);
static bool logpack_submit_flush(struct block_device *bdev, struct list_head *bio_ent_list);
static bool logpack_submit(
	struct walb_logpack_header *lhead, bool is_fua,
	struct list_head *req_ent_list, struct list_head *bio_ent_list,
	unsigned int pbs, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size,
	unsigned int chunk_sectors);

/* Logpack-datapack related functions. */
static int wait_for_bio_entry_list(struct list_head *bio_ent_list);
static void wait_logpack_and_enqueue_datapack_tasks(
	struct pack *wpack, struct wrapper_blk_dev *wdev);
#ifdef WALB_FAST_ALGORITHM
static void wait_logpack_and_enqueue_datapack_tasks_fast(
	struct pack *wpack, struct wrapper_blk_dev *wdev);
#else
static void wait_logpack_and_enqueue_datapack_tasks_easy(
	struct pack *wpack, struct wrapper_blk_dev *wdev);
#endif

/* Overlapping data functions. */
#ifdef WALB_OVERLAPPING_SERIALIZE
static bool overlapping_check_and_insert(
	struct multimap *overlapping_data, struct req_entry *reqe);
static void overlapping_delete_and_notify(
	struct multimap *overlapping_data, struct req_entry *reqe);
#endif

/* Pending data functions. */
#ifdef WALB_FAST_ALGORITHM
static bool pending_insert(
	struct multimap *pending_data, struct req_entry *reqe);
static void pending_delete(
	struct multimap *pending_data, struct req_entry *reqe);
static bool pending_check_and_copy(
	struct multimap *pending_data, struct req_entry *reqe);
static inline bool should_stop_queue(struct pdata *pdata, struct req_entry *reqe);
static inline bool should_start_queue(struct pdata *pdata, struct req_entry *reqe);
#endif

/* For overlapping data and pending data. */
#if defined(WALB_OVERLAPPING_SERIALIZE) || defined(WALB_FAST_ALGORITHM)
static inline bool is_overlap_req_entry(struct req_entry *reqe0, struct req_entry *reqe1);
#endif

static void flush_all_wq(void);

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
 * Print a pack data for debug.
 */
UNUSED
static void print_pack(const char *level, struct pack *pack)
{
	struct walb_logpack_header *lhead;
	struct req_entry *reqe;
	struct bio_entry *bioe;
	unsigned int i;
	ASSERT(level);
	ASSERT(pack);

	printk("%s""print_pack %p begin\n", level, pack);
	
	i = 0;
	list_for_each_entry(reqe, &pack->req_ent_list, list) {
		i ++;
		print_req_entry(level, reqe);
	}
	printk("%s""number of req_entry in req_ent_list: %u.\n", level, i);

	i = 0;
	list_for_each_entry(bioe, &pack->bio_ent_list, list) {
		i ++;
		print_bio_entry(level, bioe);
	}
	printk("%s""number of bio_entry in bio_ent_list: %u.\n", level, i);

	/* logpack header */
	if (pack->logpack_header_sector) {
		lhead = get_logpack_header(pack->logpack_header_sector);
		walb_logpack_header_print(level, lhead);
	} else {
		printk("%s""logpack_header_sector is NULL.\n", level);
	}

	printk("%s""is_fua: %u\nis_logpack_failed: %u\n",
		level,
		pack->is_fua, pack->is_logpack_failed);

	printk("%s""print_pack %p end\n", level, pack);
}

/**
 * Print a list of pack data for debug.
 */
UNUSED
static void print_pack_list(const char *level, struct list_head *wpack_list)
{
	struct pack *pack;
	unsigned int i = 0;
	ASSERT(level);
	ASSERT(wpack_list);
	
	printk("%s""print_pack_list %p begin.\n", level, wpack_list);
	list_for_each_entry(pack, wpack_list, list) {
		LOGd("%u: ", i);
		print_pack(level, pack);
		i ++;
	}
	printk("%s""print_pack_list %p end.\n", level, wpack_list);
}

/**
 * Create a pack_work.
 *
 * RETURN:
 * NULL if failed.
 * CONTEXT:
 * Any.
 */
static struct pack_work* create_pack_work(
	struct wrapper_blk_dev *wdev, gfp_t gfp_mask)
{
	struct pack_work *pwork;

	ASSERT(wdev);
	ASSERT(pack_work_cache_);

	pwork = kmem_cache_alloc(pack_work_cache_, gfp_mask);
	if (!pwork) {
		goto error0;
	}
	pwork->wdev = wdev;
	INIT_LIST_HEAD(&pwork->wpack_list);
	/* INIT_WORK(&pwork->work, NULL); */
        
	return pwork;
error0:
	return NULL;
}

/**
 * Destory a pack_work.
 */
static void destroy_pack_work(struct pack_work *work)
{
	if (!work) { return; }
	ASSERT(list_empty(&work->wpack_list));
#ifdef WALB_DEBUG
	work->wdev = NULL;
#endif
	kmem_cache_free(pack_work_cache_, work);
}

/**
 * endio callback for bio_entry.
 */
static void bio_entry_end_io(struct bio *bio, int error)
{
	struct bio_entry *bioe = bio->bi_private;
	UNUSED int uptodate = test_bit(BIO_UPTODATE, &bio->bi_flags);
	int bi_cnt;
	ASSERT(bioe);
#ifdef WALB_DEBUG
	if (bioe->bio_orig) {
		ASSERT(bioe->is_splitted);
		ASSERT(bioe->bio_orig == bio);
	} else {
		ASSERT(bioe->bio == bio);
	}
#endif
	if (!uptodate) {
		LOGn("BIO_UPTODATE is false (rw %lu addr %"PRIu64" size %u).\n",
			bioe->bio->bi_rw, (u64)bioe->bio->bi_sector, bioe->bi_size);
	}
        
	/* LOGd("bio_entry_end_io() begin.\n"); */
	bioe->error = error;
	bi_cnt = atomic_read(&bio->bi_cnt);
#ifdef WALB_FAST_ALGORITHM
	if (bio->bi_rw & WRITE) {
		if (bioe->bio_orig) {
			/* 2 for data, 1 for log. */
			ASSERT(bi_cnt == 2 || bi_cnt == 1);
		} else {
			/* 3 for data, 1 for log. */
			ASSERT(bi_cnt == 3 || bi_cnt == 1);
		}
	} else {
		ASSERT(bi_cnt == 1);
	}
#else
	ASSERT(bi_cnt == 1);
#endif
	LOGd_("complete bioe %p addr %"PRIu64" size %u\n",
		bioe, (u64)bio->bi_sector, bioe->bi_size);
	if (bi_cnt == 1) {
		bioe->bio_orig = NULL;
		bioe->bio = NULL;
	}
	bio_put(bio);
	complete(&bioe->done);
	/* LOGd("bio_entry_end_io() end.\n"); */
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

	bioe = alloc_bio_entry(gfp_mask);
	if (!bioe) { goto error0; }
	
	/* clone bio */
	biotmp = bio_clone(bio, gfp_mask);
	if (!biotmp) {
		LOGe("bio_clone() failed.");
		goto error1;
	}
	biotmp->bi_bdev = bdev;
	biotmp->bi_end_io = bio_entry_end_io;
	biotmp->bi_private = bioe;

	init_bio_entry(bioe, biotmp);
        
	/* LOGd("create_bio_entry() end.\n"); */
	return bioe;

error1:
	destroy_bio_entry(bioe);
error0:
	LOGe("create_bio_entry_by_clone() end with error.\n");
	return NULL;
}

/**
 * Create a bio_entry by clone with copy.
 */
#ifdef WALB_FAST_ALGORITHM
static struct bio_entry* create_bio_entry_by_clone_copy(
	struct bio *bio, struct block_device *bdev, gfp_t gfp_mask)
{
	struct bio_entry *bioe;
	struct bio *biotmp;
	
	bioe = alloc_bio_entry(gfp_mask);
	if (!bioe) { goto error0; }

	biotmp = bio_clone_copy(bio, gfp_mask);
	if (!biotmp) {
		LOGe("bio_clone_copy() failed.\n");
		goto error1;
	}
	biotmp->bi_bdev = bdev;
	biotmp->bi_end_io = bio_entry_end_io;
	biotmp->bi_private = bioe;

	init_copied_bio_entry(bioe, biotmp);
	
	return bioe;
error1:
	destroy_bio_entry(bioe);
error0:
	LOGe("create_bio_entry_by_clone_copy() end with error.\n");
	return NULL;
}
#endif

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
	INIT_LIST_HEAD(&pack->bio_ent_list);
	pack->is_zero_flush_only = false;
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
UNUSED
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
 * Check the pack contains zero-size flush only.
 *
 * RETURN:
 *   true if pack contains only one request and it is zero-size flush, or false.
 */
UNUSED
static bool is_zero_flush_only(struct pack *pack)
{
	struct walb_logpack_header *lhead;
	struct req_entry *reqe;
	unsigned int i;
	ASSERT(pack);
	ASSERT(pack->logpack_header_sector);
	
	lhead = get_logpack_header(pack->logpack_header_sector);
	ASSERT(lhead);

	i = 0;
	list_for_each_entry(reqe, &pack->req_ent_list, list) {

		ASSERT(reqe->req);
		if (!((reqe->req->cmd_flags & REQ_FLUSH) && blk_rq_sectors(reqe->req) == 0)) {
			return false;
		}
		i ++;
	}
	return i == 1;
}

/**
 * Check the pack size exceeds max_logpack_pb or not.
 *
 * RETURN:
 *   true if pack is already exceeds or will be exceeds.
 */
static bool is_pack_size_exceeds(
	struct walb_logpack_header *lhead, 
	unsigned int pbs, unsigned int max_logpack_pb,
	struct req_entry *reqe)
{
	/* now editing */
	unsigned int pb;
	ASSERT(lhead);
	ASSERT(pbs);
	ASSERT_PBS(pbs);

	if (max_logpack_pb == 0) {
		return false;
	}

	pb = (unsigned int)capacity_pb(pbs, reqe->req_sectors);
	return lhead->total_io_size + pb > max_logpack_pb;
}

/**
 * Add a request to a writepack.
 *
 * @wpack_list wpack list.
 * @wpackp pointer to a wpack pointer. *wpackp can be NULL.
 * @req request to add.
 * @ring_buffer_size ring buffer size [physical block]
 * @latest_lsidp pointer to the latest_lsid value.
 *   *latest_lsidp must be always (*wpackp)->logpack_lsid.
 * @wdev wrapper block device.
 * @gfp_mask memory allocation mask.
 *
 * RETURN:
 *   true if successfuly added, or false (due to memory allocation failure).
 * CONTEXT:
 *   atomic, serialized.
 */
static bool writepack_add_req(
	struct list_head *wpack_list, struct pack **wpackp, struct request *req,
	u64 ring_buffer_size, unsigned int max_logpack_pb,
	u64 *latest_lsidp, struct wrapper_blk_dev *wdev, gfp_t gfp_mask)
{
	struct req_entry *reqe;
	struct pack *pack;
	bool ret;
	unsigned int pbs;
	struct walb_logpack_header *lhead = NULL;

	LOGd_("begin\n");
	
	ASSERT(wpack_list);
	ASSERT(wpackp);
	ASSERT(req);
	ASSERT(req->cmd_flags & REQ_WRITE);
	ASSERT(wdev);
	pbs = wdev->pbs;
	ASSERT_PBS(pbs);
	
	pack = *wpackp;
	
	reqe = create_req_entry(req, wdev, gfp_mask);
	if (!reqe) { goto error0; }

	if (!pack) {
		goto newpack;
	}

	ASSERT(pack);
	ASSERT(pack->logpack_header_sector);
	ASSERT(pbs == pack->logpack_header_sector->size);
	lhead = get_logpack_header(pack->logpack_header_sector);
	ASSERT(*latest_lsidp == lhead->logpack_lsid);
	
	if (lhead->n_records > 0) {
#if 0
		/* Now we need not overlapping check in a pack
		   because atomicity is kept by unit of request. */
		if ((req->cmd_flags & REQ_FLUSH)
			|| is_pack_size_exceeds(lhead, pbs, max_logpack_pb, reqe)
			|| is_overlap_pack_reqe(pack, reqe)) {
#else
		if (req->cmd_flags & REQ_FLUSH
			|| is_pack_size_exceeds(lhead, pbs, max_logpack_pb, reqe)) {
#endif
			/* Flush request must be the first of the pack. */
			/* overlap found so create a new pack. */
			goto newpack;
		}
	}
	if (!walb_logpack_header_add_req(lhead, req, pbs, ring_buffer_size)) {
		/* logpack header capacity full so create a new pack. */
		goto newpack;
	}

fin:
	if (req->cmd_flags & REQ_FUA) {
		pack->is_fua = true;
	}
	/* The request is just added to the pack. */
	list_add_tail(&reqe->list, &pack->req_ent_list);
	LOGd_("normal end\n");
	return true;

newpack:
	if (lhead) {
		ASSERT(pack);
		if (lhead->n_records == 0) {
			ASSERT(is_zero_flush_only(pack));
			pack->is_zero_flush_only = true;
		}
		ASSERT(is_valid_prepared_pack(pack));
		list_add_tail(&pack->list, wpack_list);
		*latest_lsidp = get_next_lsid_unsafe(lhead);
	}
	pack = create_writepack(gfp_mask, pbs, *latest_lsidp);
	if (!pack) { goto error1; }
	*wpackp = pack;
	lhead = get_logpack_header(pack->logpack_header_sector);
	ret = walb_logpack_header_add_req(lhead, req, pbs, ring_buffer_size);
	ASSERT(ret);
	goto fin;
	
error1:
	destroy_req_entry(reqe);
error0:
	LOGd_("failure end\n");
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
 * This does not copy IO data, bio stubs only.
 *
 * RETURN:
 *     true if succeed, or false.
 * CONTEXT:
 *     Non-IRQ. Non-atomic.
 */
static bool create_bio_entry_list(struct req_entry *reqe, struct block_device *bdev)
{
	struct bio_entry *bioe;
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
	destroy_bio_entry_list(&reqe->bio_ent_list);
	ASSERT(list_empty(&reqe->bio_ent_list));
	return false;
}

/**
 * Create bio_entry list for a request by copying its IO data.
 *
 * RETURN:
 *     true if succeed, or false.
 * CONTEXT:
 *     Non-IRQ, Non-Atomic.
 */
#ifdef WALB_FAST_ALGORITHM
static bool create_bio_entry_list_copy(
	struct req_entry *reqe, struct block_device *bdev)
{
	struct bio_entry *bioe;
	struct bio *bio;
	ASSERT(reqe);
	ASSERT(reqe->req);
	ASSERT(list_empty(&reqe->bio_ent_list));
	ASSERT(reqe->req->cmd_flags & REQ_WRITE);
	
	__rq_for_each_bio(bio, reqe->req) {

		bioe = create_bio_entry_by_clone_copy(bio, bdev, GFP_NOIO);
		if (!bioe) {
			LOGd("create_bio_entry_list_copy() failed.\n");
			goto error0;
		}
		list_add_tail(&bioe->list, &reqe->bio_ent_list);
	}
	return true;
error0:
	destroy_bio_entry_list(&reqe->bio_ent_list);
	ASSERT(list_empty(&reqe->bio_ent_list));
	return false;
}
#endif

/**
 * Submit all bio_entry(s) in a req_entry.
 *
 * @bio_ent_list list head of bio_entry.
 *
 * CONTEXT:
 *   non-irq. non-atomic.
 */
static void submit_bio_entry_list(struct list_head *bio_ent_list)
{
	struct bio_entry *bioe;

	ASSERT(bio_ent_list);
	list_for_each_entry(bioe, bio_ent_list, list) {
#ifdef WALB_FAST_ALGORITHM
#ifdef WALB_DEBUG
		if (!bioe->is_splitted) {
			ASSERT(bioe->bio->bi_end_io == bio_entry_end_io);
		}
#endif /* WALB_DEBUG */
		if (bioe->is_copied) {
			LOGd_("copied: rw %lu bioe %p addr %"PRIu64" size %u\n",
				bioe->bio->bi_rw,
				bioe, (u64)bioe->bio->bi_sector, bioe->bi_size);
			bio_endio(bioe->bio, 0);
		} else {
			LOGd_("submit_d: rw %lu bioe %p addr %"PRIu64" size %u\n",
				bioe->bio->bi_rw,
				bioe, (u64)bioe->bio->bi_sector, bioe->bi_size);
			generic_make_request(bioe->bio);
		}
#else /* WALB_FAST_ALGORITHM */
		LOGd_("submit_d: rw %lu bioe %p addr %"PRIu64" size %u\n",
			bioe->bio->bi_rw,
			bioe, (u64)bioe->bio->bi_sector, bioe->bi_size);
		generic_make_request(bioe->bio);
#endif /* WALB_FAST_ALGORITHM */
	}
}

/**
 * Wait for completion of all bio_entry(s) related a req_entry
 * and end request if required.
 *
 * @reqe target req_entry.
 *   Do not assume reqe->req is available when is_end_request is false.
 * @is_end_request true if end request call is required, or false.
 * @is_delete true if bio_entry deletion is required, or false.
 *
 * CONTEXT:
 *   non-irq. non-atomic.
 */
static void wait_for_req_entry(struct req_entry *reqe, bool is_end_request, bool is_delete)
{
	struct bio_entry *bioe, *next;
	unsigned int remaining;
	const unsigned long timeo = msecs_to_jiffies(completion_timeo_ms_);
	unsigned long rtimeo;
	int c;
	ASSERT(reqe);
        
	remaining = reqe->req_sectors * LOGICAL_BLOCK_SIZE;
	list_for_each_entry(bioe, &reqe->bio_ent_list, list) {
		if (bio_entry_should_wait_completion(bioe)) {
			c = 0;
		retry:
			rtimeo = wait_for_completion_timeout(&bioe->done, timeo);
			if (rtimeo == 0) {
				LOGn("timeout(%d): reqe %p bioe %p bio %p pos %"PRIu64" sectors %u\n",
					c, reqe, bioe, bioe->bio,
					reqe->req_pos, reqe->req_sectors);
				c++;
				goto retry;
			}
		}
		if (is_end_request) {
			blk_end_request(reqe->req, bioe->error, bioe->bi_size);
		}
		remaining -= bioe->bi_size;
	}
	ASSERT(remaining == 0);

	if (is_delete) {
		list_for_each_entry_safe(bioe, next, &reqe->bio_ent_list, list) {
			list_del(&bioe->list);
			destroy_bio_entry(bioe);
		}
		ASSERT(list_empty(&reqe->bio_ent_list));
	}
}

/**
 * Submit all write packs in a list to the log device.
 */
static void logpack_list_submit(
	struct wrapper_blk_dev *wdev, struct list_head *wpack_list)
{
	struct pdata *pdata;
	struct pack *wpack;
	struct blk_plug plug;
	struct walb_logpack_header *lhead;
	bool ret;
	ASSERT(wpack_list);
	ASSERT(wdev);
	pdata = pdata_get_from_wdev(wdev);

	blk_start_plug(&plug);
	list_for_each_entry(wpack, wpack_list, list) {

		ASSERT_SECTOR_DATA(wpack->logpack_header_sector);
		lhead = get_logpack_header(wpack->logpack_header_sector);
		
		if (wpack->is_zero_flush_only) {
			ASSERT(lhead->n_records == 0);
			LOGd("is_zero_flush_only\n"); /* debug */
			ret = logpack_submit_flush(pdata->ldev, &wpack->bio_ent_list);
		} else {
			ASSERT(lhead->n_records > 0);
			logpack_calc_checksum(lhead, wdev->pbs, &wpack->req_ent_list);
			ret = logpack_submit(
				lhead, wpack->is_fua,
				&wpack->req_ent_list, &wpack->bio_ent_list,
				wdev->pbs, pdata->ldev, pdata->ring_buffer_off,
				pdata->ring_buffer_size, pdata->ldev_chunk_sectors);
		}
		wpack->is_logpack_failed = !ret;
		if (!ret) { break; }
	}
	blk_finish_plug(&plug);
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
 * @work work in a pack_work.
 *
 * CONTEXT:
 *   Workqueue task.
 *   Works are serialized by singlethread workqueue.
 */
static void logpack_list_submit_task(struct work_struct *work)
{
	struct pack_work *pwork = container_of(work, struct pack_work, work);
	struct wrapper_blk_dev *wdev = pwork->wdev;
	struct pdata *pdata = pdata_get_from_wdev(wdev);
	struct pack *wpack, *wpack_next;
	struct list_head wpack_list;
	bool is_empty, is_working;

	while (true) {

		/* Dequeue logpack list from the submit queue. */
		INIT_LIST_HEAD(&wpack_list);
		spin_lock(&pdata->logpack_submit_queue_lock);
		is_empty = list_empty(&pdata->logpack_submit_queue);
		list_for_each_entry_safe(wpack, wpack_next,
					&pdata->logpack_submit_queue, list) {
			list_move_tail(&wpack->list, &wpack_list);
		}
		spin_unlock(&pdata->logpack_submit_queue_lock);
		if (is_empty) {
			is_working =
				test_and_clear_bit(PDATA_STATE_SUBMIT_TASK_WORKING,
						&pdata->flags);
			ASSERT(is_working);
			break;
		}

		/* Submit. */
		logpack_list_submit(wdev, &wpack_list);

		/* Enqueue logpack list to the wait queue. */
		spin_lock(&pdata->logpack_wait_queue_lock);
		list_for_each_entry_safe(wpack, wpack_next, &wpack_list, list) {
			list_move_tail(&wpack->list, &pdata->logpack_wait_queue);
		}
		ASSERT(list_empty(&wpack_list));
		spin_unlock(&pdata->logpack_wait_queue_lock);

		if (test_and_set_bit(
				PDATA_STATE_WAIT_TASK_WORKING,
				&pdata->flags) == 0) {
			
			struct pack_work *pwork2;
			pwork2 = create_pack_work(wdev, GFP_NOIO);
			if (!pwork2) {
				/* You must do error handling. */
				BUG();
			}
			INIT_WORK(&pwork2->work, logpack_list_wait_task);
			queue_work(wq_logpack_, &pwork2->work);
		}
	}
	LOGd_("destroy_pack_work begin %p\n", pwork);
	destroy_pack_work(pwork);
	LOGd_("destroy_pack_work end %p\n", pwork);
}

/**
 * Wait for all bio(s) completion in a bio_entry list.
 * Each bio_entry will be deleted.
 *
 * @bio_ent_list list head of bio_entry.
 *
 * RETURN:
 *   error of the last failed bio (0 means success).
 */
static int wait_for_bio_entry_list(struct list_head *bio_ent_list)
{
	struct bio_entry *bioe, *next_bioe;
	int bio_error = 0;
	const unsigned long timeo = msecs_to_jiffies(completion_timeo_ms_);
	unsigned long rtimeo;
	int c;
	ASSERT(bio_ent_list);
	
	/* wait for completion. */
	list_for_each_entry(bioe, bio_ent_list, list) {

		if (bio_entry_should_wait_completion(bioe)) {
			c = 0;
		retry:
			rtimeo = wait_for_completion_timeout(&bioe->done, timeo);
			if (rtimeo == 0) {
				LOGn("timeout(%d): bioe %p bio %p size %u\n",
					c, bioe, bioe->bio, bioe->bi_size);
				c++;
				goto retry;
			}
		}
		if (bioe->error) { bio_error = bioe->error; }
	}
	/* destroy. */
	list_for_each_entry_safe(bioe, next_bioe, bio_ent_list, list) {
		list_del(&bioe->list);
		destroy_bio_entry(bioe);
	}
	ASSERT(list_empty(bio_ent_list));
	return bio_error;
}

/**
 * Wait for completion of all bio(s) and enqueue datapack tasks.
 *
 * Request success -> enqueue datapack.
 * Request failure-> all subsequent requests must fail.
 *
 * If any write failed, wdev will be read-only mode.
 */
static void wait_logpack_and_enqueue_datapack_tasks(
	struct pack *wpack, struct wrapper_blk_dev *wdev)
{
#ifdef WALB_FAST_ALGORITHM
	wait_logpack_and_enqueue_datapack_tasks_fast(
		wpack, wdev);
#else
	wait_logpack_and_enqueue_datapack_tasks_easy(
		wpack, wdev);
#endif
}

#ifdef WALB_FAST_ALGORITHM
static void wait_logpack_and_enqueue_datapack_tasks_fast(
	struct pack *wpack, struct wrapper_blk_dev *wdev)
{
	int bio_error;
	struct req_entry *reqe, *next_reqe;
	struct request *req;
	bool is_failed = false;
	struct pdata *pdata;
#ifdef WALB_OVERLAPPING_SERIALIZE
	bool is_overlapping_insert_succeeded;
#endif
	bool is_pending_insert_succeeded;
	bool is_stop_queue = false;
	unsigned long flags;

	ASSERT(wpack);
	ASSERT(wdev);

	/* Check read only mode. */
	pdata = pdata_get_from_wdev(wdev);
	if (is_read_only_mode(pdata)) { is_failed = true; }
	
	/* Wait for logpack header bio or zero_flush pack bio. */
	bio_error = wait_for_bio_entry_list(&wpack->bio_ent_list);
	if (bio_error) { is_failed = true; }
	
	list_for_each_entry_safe(reqe, next_reqe, &wpack->req_ent_list, list) {

		req = reqe->req;
		ASSERT(req);
		
		bio_error = wait_for_bio_entry_list(&reqe->bio_ent_list);
		if (is_failed || bio_error) { goto failed0; }
		
		if (blk_rq_sectors(req) == 0) {

			ASSERT(req->cmd_flags & REQ_FLUSH);
			/* Already the corresponding logpack is permanent. */
			list_del(&reqe->list);
			blk_end_request_all(req, 0);
			destroy_req_entry(reqe);
		} else {
			/* Create all related bio(s) by copying IO data. */
			if (!create_bio_entry_list_copy(reqe, pdata->ddev)) {
				goto failed0;
			}
			/* Split if required due to chunk limitations. */
			if (!split_bio_entry_list_for_chunk(
					&reqe->bio_ent_list,
					pdata->ddev_chunk_sectors)) {
				goto failed1;
			}

			/* Get related bio(s) */
			get_bio_entry_list(&reqe->bio_ent_list);

			/* Try to insert pending data. */
			mutex_lock(&pdata->pending_data_mutex);
			LOGd_("pending_sectors %u\n", pdata->pending_sectors);
			is_stop_queue = should_stop_queue(pdata, reqe);
			pdata->pending_sectors += reqe->req_sectors;
			is_pending_insert_succeeded =
				pending_insert(pdata->pending_data, reqe);
			mutex_unlock(&pdata->pending_data_mutex);
			if (!is_pending_insert_succeeded) { goto failed2; }

			/* Check pending data size and stop the queue if needed. */
			if (is_stop_queue) {
				LOGd("stop queue.\n");
				spin_lock_irqsave(&wdev->lock, flags);
				blk_stop_queue(wdev->queue);
				spin_unlock_irqrestore(&wdev->lock, flags);
			}

			/* call end_request where with fast algorithm
			   while easy algorithm call it after data device IO. */
			blk_end_request_all(req, 0);
#ifdef WALB_OVERLAPPING_SERIALIZE
			/* check and insert to overlapping detection data. */
			mutex_lock(&pdata->overlapping_data_mutex);
			is_overlapping_insert_succeeded =
				overlapping_check_and_insert(pdata->overlapping_data, reqe);
			mutex_unlock(&pdata->overlapping_data_mutex);
			if (!is_overlapping_insert_succeeded) {
				mutex_lock(&pdata->pending_data_mutex);
				pending_delete(pdata->pending_data, reqe);
				pdata->pending_sectors -= reqe->req_sectors;
				mutex_unlock(&pdata->pending_data_mutex);
				if (is_stop_queue) {
					spin_lock_irqsave(&wdev->lock, flags);
					blk_start_queue(wdev->queue);
					spin_unlock_irqrestore(&wdev->lock, flags);
				}
				goto failed2;
			}
#endif
			/* Enqueue as a write req task. */
			INIT_WORK(&reqe->work, write_req_task);
			queue_work(wq_normal_, &reqe->work);
		}
		continue;
	failed2:
		put_bio_entry_list(&reqe->bio_ent_list);
	failed1:
		destroy_bio_entry_list(&reqe->bio_ent_list);
	failed0:
		is_failed = true;
		set_read_only_mode(pdata);
		LOGe("WalB changes device minor:%u to read-only mode.\n", wdev->minor);
		blk_end_request_all(req, -EIO);
		list_del(&reqe->list);
		destroy_req_entry(reqe);
	}
}
#else /* WALB_FAST_ALGORITHM */
static void wait_logpack_and_enqueue_datapack_tasks_easy(
	struct pack *wpack, struct wrapper_blk_dev *wdev)
{
	int bio_error;
	struct req_entry *reqe, *next_reqe;
	struct request *req;
	bool is_failed = false;
	struct pdata *pdata;
#ifdef WALB_OVERLAPPING_SERIALIZE
	bool is_overlapping_insert_succeeded;
#endif

	ASSERT(wpack);
	ASSERT(wdev);

	/* Check read only mode. */
	pdata = pdata_get_from_wdev(wdev);
	if (is_read_only_mode(pdata)) { is_failed = true; }
	
	/* Wait for logpack header bio or zero_flush pack bio. */
	bio_error = wait_for_bio_entry_list(&wpack->bio_ent_list);
	if (bio_error) { is_failed = true; }
	
	list_for_each_entry_safe(reqe, next_reqe, &wpack->req_ent_list, list) {

		req = reqe->req;
		ASSERT(req);
		
		bio_error = wait_for_bio_entry_list(&reqe->bio_ent_list);
		if (is_failed || bio_error) { goto failed0; }
		
		if (blk_rq_sectors(req) == 0) {

			ASSERT(req->cmd_flags & REQ_FLUSH);
			/* Already the corresponding logpack is permanent. */
			list_del(&reqe->list);
			blk_end_request_all(req, 0);
			destroy_req_entry(reqe);
		} else {
			/* Create all related bio(s). */
			if (!create_bio_entry_list(reqe, pdata->ddev)) { goto failed0; }

			/* Split if required due to chunk limitations. */
			if (!split_bio_entry_list_for_chunk(
					&reqe->bio_ent_list,
					pdata->ddev_chunk_sectors)) {
				goto failed1;
			}
			
#ifdef WALB_OVERLAPPING_SERIALIZE
			/* check and insert to overlapping detection data. */
			mutex_lock(&pdata->overlapping_data_mutex);
			is_overlapping_insert_succeeded =
				overlapping_check_and_insert(pdata->overlapping_data, reqe);
			mutex_unlock(&pdata->overlapping_data_mutex);
			if (!is_overlapping_insert_succeeded) {
				goto failed1;
			}
#endif
			/* Enqueue as a write req task. */
			INIT_WORK(&reqe->work, write_req_task);
			queue_work(wq_normal_, &reqe->work);
		}
		continue;
	failed1:
		destroy_bio_entry_list(&reqe->bio_ent_list);
	failed0:
		is_failed = true;
		set_read_only_mode(pdata);
		blk_end_request_all(req, -EIO);
		list_del(&reqe->list);
		destroy_req_entry(reqe);
	}
}
#endif /* WALB_FAST_ALGORITHM */

/**
 * Wait for completion of all logpacks related to a call of request_fn.
 *
 * If submission a logpack is partially failed,
 * this function will end all requests related to the logpack and the followings.
 *
 * All failed (and end_request called) reqe(s) will be destroyed.
 *
 * @work work in a logpack_work.
 *
 * CONTEXT:
 *   Workqueue task.
 *   Works are serialized by singlethread workqueue.
 */
static void logpack_list_wait_task(struct work_struct *work)
{
	struct pack_work *pwork = container_of(work, struct pack_work, work);
	struct wrapper_blk_dev *wdev = pwork->wdev;
	struct pdata *pdata = pdata_get_from_wdev(wdev);
	struct pack *wpack, *wpack_next;
	bool is_empty, is_working;
	struct list_head wpack_list;
	struct pack_work *pwork2;

	while (true) {
	
		/* Dequeue logpack list from the submit queue. */
		INIT_LIST_HEAD(&wpack_list);
		spin_lock(&pdata->logpack_wait_queue_lock);
		is_empty = list_empty(&pdata->logpack_wait_queue);
		list_for_each_entry_safe(wpack, wpack_next,
					&pdata->logpack_wait_queue, list) {
			list_move_tail(&wpack->list, &wpack_list);
		}
		spin_unlock(&pdata->logpack_wait_queue_lock);
		if (is_empty) {
			is_working = test_and_clear_bit(
				PDATA_STATE_WAIT_TASK_WORKING,
				&pdata->flags);
			ASSERT(is_working);
			break;
		}
		
		/* Allocate work struct for gc task. */
		pwork2 = create_pack_work(wdev, GFP_NOIO);
		if (!pwork2) {
			/* We must do error handling. */
			BUG();
		}
		
		/* Wait logpack completion and submit datapacks. */
		list_for_each_entry_safe(wpack, wpack_next, &wpack_list, list) {
			wait_logpack_and_enqueue_datapack_tasks(wpack, wdev);
			list_move_tail(&wpack->list, &pwork2->wpack_list);
		}
		/* Enqueue logpack list gc task. */
		INIT_WORK(&pwork2->work, logpack_list_gc_task);
		queue_work(wq_normal_, &pwork2->work);
	}
	LOGd_("destroy_pack_work begin\n");
	destroy_pack_work(pwork);
	LOGd_("destroy_pack_work end\n");
}

/**
 * Wait all related write requests done and
 * free all related resources.
 *
 * @work work in a pack_work.
 *
 * CONTEXT:
 *   Workqueue task.
 *   Works will be executed in parallel.
 */
static void logpack_list_gc_task(struct work_struct *work)
{
	struct pack_work *pwork = container_of(work, struct pack_work, work);
	struct pack *wpack, *next_wpack;
	struct req_entry *reqe, *next_reqe;
	const unsigned long timeo = msecs_to_jiffies(completion_timeo_ms_);
	unsigned long rtimeo;
	int c;

	list_for_each_entry_safe(wpack, next_wpack, &pwork->wpack_list, list) {
		list_del(&wpack->list);
		list_for_each_entry_safe(reqe, next_reqe, &wpack->req_ent_list, list) {
			list_del(&reqe->list);
			c = 0;
		retry:
			rtimeo = wait_for_completion_timeout(&reqe->done, timeo);
			if (rtimeo == 0) {
				LOGn("timeout(%d): reqe %p pos %"PRIu64" sectors %u\n",
					c, reqe, reqe->req_pos, reqe->req_sectors);
				c++;
				goto retry;
			}
			destroy_req_entry(reqe);
		}
		ASSERT(list_empty(&wpack->req_ent_list));
		ASSERT(list_empty(&wpack->bio_ent_list));
		destroy_pack(wpack);
	}
	ASSERT(list_empty(&pwork->wpack_list));
	LOGd_("destroy_pack_work begin\n");
	destroy_pack_work(pwork);
	LOGd_("destroy_pack_work end\n");
}

/**
 * Execute a write request.
 *
 * (1) create (already done)
 * (2) wait for overlapping write requests done
 *     (only when WALB_OVERLAPPING_SERIALIZE)
 * (3) submit
 * (4) wait for completion
 * (5) notify waiting overlapping write requests
 *     (only when WALB_OVERLAPPING_SERIALIZE)
 * (6) notify gc_task.
 *
 * CONTEXT:
 *   Workqueue task.
 *   Works will be executed in parallel.
 *   Queue lock is not held.
 */
static void write_req_task(struct work_struct *work)
{
	might_sleep();
	
#ifdef WALB_FAST_ALGORITHM
	write_req_task_fast(work);
#else
	write_req_task_easy(work);
#endif
}

/**
 * Execute a write request (Fast algortihm version).
 */
#ifdef WALB_FAST_ALGORITHM
static void write_req_task_fast(struct work_struct *work)
{
	struct req_entry *reqe = container_of(work, struct req_entry, work);
	struct wrapper_blk_dev *wdev = reqe->wdev;
	struct pdata *pdata = pdata_get_from_wdev(wdev);
	struct blk_plug plug;
	const bool is_end_request = false;
	const bool is_delete = false;
	bool is_start_queue = false;
	unsigned long flags;
#ifdef WALB_OVERLAPPING_SERIALIZE
	const unsigned long timeo = msecs_to_jiffies(completion_timeo_ms_);
	unsigned long rtimeo;
	int c;
#endif

#ifdef WALB_OVERLAPPING_SERIALIZE
	/* Wait for previous overlapping writes. */
	if (reqe->n_overlapping > 0) {
		c = 0;
	retry:
		rtimeo = wait_for_completion_timeout(
			&reqe->overlapping_done, timeo);
		if (rtimeo == 0) {
			LOGw("timeout(%d): reqe %p pos %"PRIu64" sectors %u\n",
				c, reqe, reqe->req_pos, reqe->req_sectors);
			c++;
			goto retry;
		}
	}
#endif
	/* Submit all related bio(s). */
	blk_start_plug(&plug);
	submit_bio_entry_list(&reqe->bio_ent_list);
	blk_finish_plug(&plug);

	/* Wait for completion and call end_request. */
	wait_for_req_entry(reqe, is_end_request, is_delete);

	/* Delete from overlapping detection data. */
#ifdef WALB_OVERLAPPING_SERIALIZE
	mutex_lock(&pdata->overlapping_data_mutex);
	overlapping_delete_and_notify(pdata->overlapping_data, reqe);
	mutex_unlock(&pdata->overlapping_data_mutex);
#endif

	/* Delete from pending data. */
	mutex_lock(&pdata->pending_data_mutex);
	is_start_queue = should_start_queue(pdata, reqe);
	pdata->pending_sectors -= reqe->req_sectors;
	pending_delete(pdata->pending_data, reqe);
	mutex_unlock(&pdata->pending_data_mutex);

	/* Check queue restart is required. */
	if (is_start_queue) {
		LOGd("restart queue.\n");
		spin_lock_irqsave(&wdev->lock, flags);
		blk_start_queue(wdev->queue);
		spin_unlock_irqrestore(&wdev->lock, flags);
	}

	/* put related bio(s). */
	put_bio_entry_list(&reqe->bio_ent_list);
	
	/* Free resources. */
	destroy_bio_entry_list(&reqe->bio_ent_list);
	
	ASSERT(list_empty(&reqe->bio_ent_list));
	
	/* Notify logpack_list_gc_task().
	   Reqe will be destroyed in logpack_list_gc_task(). */
	complete(&reqe->done);
}
#else /* WALB_FAST_ALGORITHM */
/**
 * Execute a write request (Easy algortihm version).
 */
static void write_req_task_easy(struct work_struct *work)
{
	struct req_entry *reqe = container_of(work, struct req_entry, work);
	struct wrapper_blk_dev *wdev = reqe->wdev;
	UNUSED struct pdata *pdata = pdata_get_from_wdev(wdev);
	struct blk_plug plug;
	const bool is_end_request = true;
	const bool is_delete = true;
#ifdef WALB_OVERLAPPING_SERIALIZE
	const unsigned long timeo = msecs_to_jiffies(completion_timeo_ms_);
	unsigned long rtimeo;
	int c;
#endif

#ifdef WALB_OVERLAPPING_SERIALIZE
	/* Wait for previous overlapping writes. */
	if (reqe->n_overlapping > 0) {
		c = 0;
	retry:
		rtimeo = wait_for_completion_timeout(
			&reqe->overlapping_done, timeo);
		if (rtimeo == 0) {
			LOGw("timeout(%d): reqe %p pos %"PRIu64" sectors %u\n",
				c, reqe, reqe->req_pos, reqe->req_sectors);
			c++;
			goto retry;
		}
	}
#endif
	/* Submit all related bio(s). */
	blk_start_plug(&plug);
	submit_bio_entry_list(&reqe->bio_ent_list);
	blk_finish_plug(&plug);

	/* Wait for completion and call end_request. */
	wait_for_req_entry(reqe, is_end_request, is_delete);

	/* Delete from overlapping detection data. */
#ifdef WALB_OVERLAPPING_SERIALIZE
	mutex_lock(&pdata->overlapping_data_mutex);
	overlapping_delete_and_notify(pdata->overlapping_data, reqe);
	mutex_unlock(&pdata->overlapping_data_mutex);
#endif

	ASSERT(list_empty(&reqe->bio_ent_list));
	
	/* Notify logpack_list_gc_task().
	   Reqe will be destroyed in logpack_list_gc_task(). */
	complete(&reqe->done);
}
#endif /* WALB_FAST_ALGORITHM */

/**
 * Execute a read request.
 *
 * (1) create
 * (2) submit
 * (3) wait for completion
 * (4) end request
 * (5) free the related resources
 *
 * CONTEXT:
 *   Workqueue task.
 *   Works will be executed in parallel.
 */
static void read_req_task(struct work_struct *work)
{
	might_sleep();
	
#ifdef WALB_FAST_ALGORITHM
	read_req_task_fast(work);
#else
	read_req_task_easy(work);
#endif
}

/**
 * Execute a read request (Fast algortihm version).
 */
#ifdef WALB_FAST_ALGORITHM
static void read_req_task_fast(struct work_struct *work)
{
	struct req_entry *reqe = container_of(work, struct req_entry, work);
	struct wrapper_blk_dev *wdev = reqe->wdev;
	struct pdata *pdata = pdata_get_from_wdev(wdev);
	struct blk_plug plug;
	const bool is_end_request = true;
	const bool is_delete = true;
	bool ret;

	/* Create all related bio(s). */
	if (!create_bio_entry_list(reqe, pdata->ddev)) {
		goto error0;
	}

	/* Split if required due to chunk limitations. */
	if (!split_bio_entry_list_for_chunk(
			&reqe->bio_ent_list, pdata->ddev_chunk_sectors)) {
		goto error1;
	}

	/* Check pending data and copy data from executing write requests. */
	mutex_lock(&pdata->pending_data_mutex);
	ret = pending_check_and_copy(pdata->pending_data, reqe);
	mutex_unlock(&pdata->pending_data_mutex);
	if (!ret) {
		goto error1;
	}
		
	/* Submit all related bio(s). */
	blk_start_plug(&plug);
	submit_bio_entry_list(&reqe->bio_ent_list);
	blk_finish_plug(&plug);

	/* Wait for completion and call end_request. */
	wait_for_req_entry(reqe, is_end_request, is_delete);
	goto fin;

error1:
	destroy_bio_entry_list(&reqe->bio_ent_list);
error0:
	blk_end_request_all(reqe->req, -EIO);
fin:
	ASSERT(list_empty(&reqe->bio_ent_list));
	destroy_req_entry(reqe);
}
#else /* WALB_FAST_ALGORITHM */
/**
 * Execute a read request (Easy algortihm version).
 */
static void read_req_task_easy(struct work_struct *work)
{
	struct req_entry *reqe = container_of(work, struct req_entry, work);
	struct wrapper_blk_dev *wdev = reqe->wdev;
	struct pdata *pdata = pdata_get_from_wdev(wdev);
	struct blk_plug plug;
	const bool is_end_request = true;
	const bool is_delete = true;

	/* Create all related bio(s). */
	if (!create_bio_entry_list(reqe, pdata->ddev)) {
		goto error0;
	}

	/* Split if required due to chunk limitations. */
	if (!split_bio_entry_list_for_chunk(
			&reqe->bio_ent_list, pdata->ddev_chunk_sectors)) {
		goto error1;
	}
		
	/* Submit all related bio(s). */
	blk_start_plug(&plug);
	submit_bio_entry_list(&reqe->bio_ent_list);
	blk_finish_plug(&plug);

	/* Wait for completion and call end_request. */
	wait_for_req_entry(reqe, is_end_request, is_delete);
	goto fin;

error1:
	destroy_bio_entry_list(&reqe->bio_ent_list);
error0:
	blk_end_request_all(reqe->req, -EIO);
fin:
	ASSERT(list_empty(&reqe->bio_ent_list));
	destroy_req_entry(reqe);
}
#endif /* WALB_FAST_ALGORITHM */

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
	unsigned int n_padding = 0;

	LOGd_("is_valid_prepared_pack begin.\n");
	
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

		CHECK(reqe->req);
		if (blk_rq_sectors(reqe->req) == 0) {
			CHECK(reqe->req->cmd_flags & REQ_FLUSH);
			continue;
		}

		CHECK(i < lhead->n_records);
		lrec = &lhead->record[i];
		CHECK(lrec);
		CHECK(lrec->is_exist);
		
		if (lrec->is_padding) {
			LOGd_("padding found.\n"); /* debug */
			total_pb += capacity_pb(pbs, lrec->io_size);
			n_padding ++;
			i ++;

			/* The padding record is not the last. */
			CHECK(i < lhead->n_records);
			lrec = &lhead->record[i];
			CHECK(lrec);
			CHECK(lrec->is_exist);
		}

		/* Normal record. */
		CHECK(reqe->req);
		CHECK(reqe->req->cmd_flags & REQ_WRITE);

		CHECK(blk_rq_pos(reqe->req) == (sector_t)lrec->offset);
		CHECK(lhead->logpack_lsid == lrec->lsid - lrec->lsid_local);
		CHECK(blk_rq_sectors(reqe->req) == lrec->io_size);
		total_pb += capacity_pb(pbs, lrec->io_size);
		
		i ++;
	}
	CHECK(i == lhead->n_records);
	CHECK(total_pb == lhead->total_io_size);
	CHECK(n_padding == lhead->n_padding);
	if (lhead->n_records == 0) {
		CHECK(pack->is_zero_flush_only);
	}
	LOGd_("is_valid_prepared_pack succeeded.\n");
	return true;
error:
	LOGd_("is_valid_prepared_pack failed.\n");
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
	u8 *buf;

	ASSERT(lhead);
	ASSERT(lhead->n_records > 0);
	ASSERT(lhead->n_records > lhead->n_padding);
	
        n_padding = 0;
        i = 0;
	list_for_each_entry(reqe, req_ent_list, list) {

                if (lhead->record[i].is_padding) {
                        n_padding ++;
                        i ++;
			/* A padding record is not the last in the logpack header. */
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
			buf = (u8 *)kmap_atomic(bvec->bv_page) + bvec->bv_offset;
                        sum = checksum_partial(sum, buf, bvec->bv_len);
                        kunmap_atomic(buf);
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
 * @lhead logpack header data.
 * @is_flush flush is required.
 * @is_fua fua is required.
 * @bio_ent_list must be empty.
 *     submitted lhead bio(s) will be added to this.
 * @pbs physical block size [bytes].
 * @ldev log device.
 * @ring_buffer_off ring buffer offset [physical blocks].
 * @ring_buffer_size ring buffer size [physical blocks].
 *
 * RETURN:
 *   true in success, or false.
 */
static bool logpack_submit_lhead(
	struct walb_logpack_header *lhead, bool is_flush, bool is_fua,
	struct list_head *bio_ent_list,
	unsigned int pbs, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size,
	unsigned int chunk_sectors)
{
	struct bio *bio;
	struct bio_entry *bioe;
	struct page *page;
	u64 off_pb, off_lb;
	int rw = WRITE;
	int len;
#ifdef WALB_DEBUG
	struct page *page2;
#endif
	if (is_flush) { rw |= WRITE_FLUSH; }
	if (is_fua) { rw |= WRITE_FUA; }
	
	bioe = alloc_bio_entry(GFP_NOIO);
	if (!bioe) { goto error0; }
	bio = bio_alloc(GFP_NOIO, 1);
	if (!bio) { goto error1; }

	page = virt_to_page(lhead);
#ifdef WALB_DEBUG
	page2 = virt_to_page((unsigned long)lhead + pbs - 1);
	ASSERT(page == page2);
#endif
	bio->bi_bdev = ldev;
	off_pb = lhead->logpack_lsid % ring_buffer_size + ring_buffer_off;
	off_lb = addr_lb(pbs, off_pb);
	bio->bi_sector = off_lb;
	bio->bi_rw = rw;
	bio->bi_end_io = bio_entry_end_io;
	bio->bi_private = bioe;
	len = bio_add_page(bio, page, pbs, offset_in_page(lhead));
	if (len != pbs) { goto error2; }

	init_bio_entry(bioe, bio);
	ASSERT(bioe->bi_size == pbs);

	ASSERT(bio_ent_list);
	list_add_tail(&bioe->list, bio_ent_list);

#ifdef WALB_DEBUG
	if (should_split_bio_entry_list_for_chunk(bio_ent_list, chunk_sectors)) {
		LOGw("logpack header bio should be splitted.\n");
	}
#endif
	submit_bio_entry_list(bio_ent_list);
	return true;
error2:
	bio_put(bio);
	bioe->bio = NULL;
error1:
	destroy_bio_entry(bioe);
error0:
	return false;
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
	u64 ring_buffer_off, u64 ring_buffer_size,
	unsigned int chunk_sectors)
{
	unsigned int off_lb;
	struct bio_entry *bioe, *bioe_next;
	struct bio *bio;
	u64 ldev_off_pb = lsid % ring_buffer_size + ring_buffer_off;
	struct list_head tmp_list;

	ASSERT(list_empty(bio_ent_list));
	INIT_LIST_HEAD(&tmp_list);
	off_lb = 0;
	__rq_for_each_bio(bio, req) {
		bioe = logpack_create_bio_entry(
			bio, is_fua, pbs, ldev, ldev_off_pb, off_lb);
		if (!bioe) {
			goto error0;
		}
		ASSERT(bioe->bi_size % LOGICAL_BLOCK_SIZE == 0);
		off_lb += bioe->bi_size / LOGICAL_BLOCK_SIZE;
		list_add_tail(&bioe->list, &tmp_list);
	}
	/* split if required. */
	if (!split_bio_entry_list_for_chunk(
			&tmp_list, chunk_sectors)) {
		goto error0;
	}
	/* move all bioe to the bio_ent_list. */
#if 0
	*bio_ent_list = tmp_list;
	INIT_LIST_HEAD(&tmp_list);
#else
	list_for_each_entry_safe(bioe, bioe_next, &tmp_list, list) {
		list_move_tail(&bioe->list, bio_ent_list);
	}
	ASSERT(list_empty(&tmp_list));
#endif
	/* really submit. */
	list_for_each_entry_safe(bioe, bioe_next, bio_ent_list, list) {
		LOGd_("submit_lr: bioe %p addr %"PRIu64" size %u\n",
			bioe, (u64)bioe->bio->bi_sector, bioe->bi_size);
		generic_make_request(bioe->bio);
	}
	return true;
error0:
	list_for_each_entry_safe(bioe, bioe_next, &tmp_list, list) {
		list_del(&bioe->list);
		destroy_bio_entry(bioe);
	}
	ASSERT(list_empty(&tmp_list));
	return false;
}

/**
 * Create a bio_entry which is a part of logpack.
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
static struct bio_entry* logpack_create_bio_entry(
	struct bio *bio, bool is_fua, unsigned int pbs,
	struct block_device *ldev,
	u64 ldev_offset, unsigned int bio_offset)
{
	struct bio_entry *bioe;
	struct bio *cbio;

	bioe = alloc_bio_entry(GFP_NOIO);
	if (!bioe) { goto error0; }

	cbio = bio_clone(bio, GFP_NOIO);
	if (!cbio) { goto error1; }

	cbio->bi_bdev = ldev;
	cbio->bi_end_io = bio_entry_end_io;
	cbio->bi_private = bioe;
	cbio->bi_sector = addr_lb(pbs, ldev_offset) + bio_offset;

	init_bio_entry(bioe, cbio);

	if (is_fua) {
		cbio->bi_rw |= WRITE_FUA;
	}
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
 *
 * RETURN:
 *   created bioe containing submitted bio in success, or NULL.
 * CONTEXT:
 *   non-atomic.
 */
static struct bio_entry* submit_flush(struct block_device *bdev)
{
	struct bio_entry *bioe;
	struct bio *bio;

	bioe = alloc_bio_entry(GFP_NOIO);
	if (!bioe) { goto error0; }
	
	bio = bio_alloc(GFP_NOIO, 0);
	if (!bio) { goto error1; }

	bio->bi_end_io = bio_entry_end_io;
	bio->bi_private = bioe;
	bio->bi_bdev = bdev;
	bio->bi_rw = WRITE_FLUSH;

	init_bio_entry(bioe, bio);
	ASSERT(bioe->bi_size == 0);
	
	generic_make_request(bio);

	return bioe;
error1:
	destroy_bio_entry(bioe);
error0:
	return NULL;
}

/**
 * Submit flush for logpack.
 */
static bool logpack_submit_flush(struct block_device *bdev, struct list_head *bio_ent_list)
{
	struct bio_entry *bioe;
	ASSERT(bdev);
	ASSERT(bio_ent_list);

	bioe = submit_flush(bdev);
	if (!bioe) { goto error0; }
	list_add_tail(&bioe->list, bio_ent_list);
	return true;
	
error0:
	return false;
}


/**
 * Submit logpack entry.
 *
 * @lhead logpack header.
 * @is_fua FUA flag.
 * @req_ent_list request entry list.
 * @bio_ent_list bio entry list.
 *   submitted bios for logpack header will be added to the list.
 * @pbs physical block size.
 * @ldev log block device.
 * @ring_buffer_off ring buffer offset.
 * @ring_buffer_size ring buffer size.
 * @chunk_sectors chunk_sectors for bio alignment.
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
	u64 ring_buffer_off, u64 ring_buffer_size,
	unsigned int chunk_sectors)
{
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
	ret = logpack_submit_lhead(lhead, is_flush, is_fua,
				bio_ent_list, pbs, ldev,
				ring_buffer_off, ring_buffer_size,
				chunk_sectors);
	if (!ret) {
		LOGe("logpack header submit failed.\n");
		goto failed;
	}
	ASSERT(!list_empty(bio_ent_list));
	
	/* Submit logpack contents for each request. */
	i = 0;
	list_for_each_entry(reqe, req_ent_list, list) {

		req = reqe->req;
		if (blk_rq_sectors(req) == 0) {
			ASSERT(req->cmd_flags & REQ_FLUSH); /* such request must be flush. */
			ASSERT(i == 0); /* such request must be permitted at first only. */
			ASSERT(is_flush); /* logpack header bio must have REQ_FLUSH. */
			/* You do not need to submit it
			   because logpack header bio already has REQ_FLUSH. */
		} else {
			if (lhead->record[i].is_padding) {
				i ++;
				/* padding record never come last. */
			}
			ASSERT(i < lhead->n_records);
			req_lsid = lhead->record[i].lsid;

			/* submit bio(s) for a request. */
			ret = logpack_submit_req(
				req, req_lsid, is_fua, &reqe->bio_ent_list,
				pbs, ldev, ring_buffer_off, ring_buffer_size,
				chunk_sectors);
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
 * Overlapping check and insert.
 *
 * CONTEXT:
 *   overlapping_data lock must be held.
 * RETURN:
 *   true in success, or false (memory allocation failure).
 */
#ifdef WALB_OVERLAPPING_SERIALIZE
static bool overlapping_check_and_insert(
	struct multimap *overlapping_data, struct req_entry *reqe)
{
	struct multimap_cursor cur;
	u64 max_io_size, start_pos;
	int ret;
	struct req_entry *reqe_tmp;

	ASSERT(overlapping_data);
	ASSERT(reqe);
	ASSERT(reqe->req_sectors > 0);

	/* Decide search start position. */
	max_io_size = queue_max_sectors(reqe->wdev->queue);
	if (reqe->req_pos > max_io_size) {
		start_pos = reqe->req_pos - max_io_size;
	} else {
		start_pos = 0;
	}

	multimap_cursor_init(overlapping_data, &cur);
	reqe->n_overlapping = 0;
	
	/* Search the smallest candidate. */
	if (!multimap_cursor_search(&cur, start_pos, MAP_SEARCH_GE, 0)) {
		goto fin;
	}

	/* Count overlapping requests previously. */
	while (multimap_cursor_key(&cur) < reqe->req_pos + reqe->req_sectors) {

		ASSERT(multimap_cursor_is_valid(&cur));
		
		reqe_tmp = (struct req_entry *)multimap_cursor_val(&cur);
		ASSERT(reqe_tmp);
		if (is_overlap_req_entry(reqe, reqe_tmp)) {
			reqe->n_overlapping ++;
		}
		if (!multimap_cursor_next(&cur)) {
			break;
		}
	}
#if 0
	/* debug */
	if (reqe->n_overlapping > 0) {
		LOGn("n_overlapping %u\n", reqe->n_overlapping);
	}
#endif

fin:
	ret = multimap_add(overlapping_data, reqe->req_pos, (unsigned long)reqe, GFP_NOIO);
	ASSERT(ret != EEXIST);
	ASSERT(ret != EINVAL);
	if (ret) {
		ASSERT(ret == ENOMEM);
		LOGe("overlapping_check_and_insert failed.\n");
		return false;
	}
	if (reqe->n_overlapping == 0) {
		complete(&reqe->overlapping_done);
	}
	return true;
}
#endif

/**
 * Delete a req_entry from the overlapping data,
 * and notify waiting overlapping requests.
 *
 * CONTEXT:
 *   overlapping_data lock must be held.
 */
#ifdef WALB_OVERLAPPING_SERIALIZE
static void overlapping_delete_and_notify(
	struct multimap *overlapping_data, struct req_entry *reqe)
{
	struct multimap_cursor cur;
	u64 max_io_size, start_pos;
	struct req_entry *reqe_tmp;

	ASSERT(overlapping_data);
	ASSERT(reqe);
	ASSERT(reqe->n_overlapping == 0);
	
	max_io_size = queue_max_sectors(reqe->wdev->queue);
	if (reqe->req_pos > max_io_size) {
		start_pos = reqe->req_pos - max_io_size;
	} else {
		start_pos = 0;
	}

	/* Delete from the overlapping data. */
	reqe_tmp = (struct req_entry *)multimap_del(
		overlapping_data, reqe->req_pos, (unsigned long)reqe);
	LOGd_("reqe_tmp %p reqe %p\n", reqe_tmp, reqe); /* debug */
	ASSERT(reqe_tmp == reqe);
	
	/* Search the smallest candidate. */
	multimap_cursor_init(overlapping_data, &cur);
	if (!multimap_cursor_search(&cur, start_pos, MAP_SEARCH_GE, 0)) {
		return;
	}
	/* Decrement count of overlapping requests afterward and notify if need. */
	while (multimap_cursor_key(&cur) < reqe->req_pos + reqe->req_sectors) {

		ASSERT(multimap_cursor_is_valid(&cur));
		
		reqe_tmp = (struct req_entry *)multimap_cursor_val(&cur);
		ASSERT(reqe_tmp);
		if (is_overlap_req_entry(reqe, reqe_tmp)) {
			ASSERT(reqe_tmp->n_overlapping > 0);
			reqe_tmp->n_overlapping --;
			if (reqe_tmp->n_overlapping == 0) {
				/* There is no overlapping request before it. */
				complete(&reqe_tmp->overlapping_done);
			}
		}
		if (!multimap_cursor_next(&cur)) {
			break;
		}
	}
}
#endif

/**
 * Insert a req_entry from a pending data.
 *
 * CONTEXT:
 *   pending_data lock must be held.
 */
#ifdef WALB_FAST_ALGORITHM
static bool pending_insert(
	struct multimap *pending_data, struct req_entry *reqe)
{
	int ret;

	ASSERT(pending_data);
	ASSERT(reqe);
	ASSERT(reqe->req);
	ASSERT(reqe->req->cmd_flags & REQ_WRITE);
	ASSERT(reqe->req_sectors > 0);

	/* Insert the entry. */
	ret = multimap_add(pending_data, blk_rq_pos(reqe->req),
			(unsigned long)reqe, GFP_NOIO);
	ASSERT(ret != EEXIST);
	ASSERT(ret != EINVAL);
	if (ret) {
		ASSERT(ret == ENOMEM);
		LOGe("pending_insert failed.\n");
		return false;
	}
	return true;
}
#endif

/**
 * Delete a req_entry from a pending data.
 *
 * CONTEXT:
 *   pending_data lock must be held.
 */
#ifdef WALB_FAST_ALGORITHM
static void pending_delete(
	struct multimap *pending_data, struct req_entry *reqe)
{
	struct req_entry *reqe_tmp;

	ASSERT(pending_data);
	ASSERT(reqe);
	
	/* Delete the entry. */
	reqe_tmp = (struct req_entry *)multimap_del(
		pending_data, reqe->req_pos, (unsigned long)reqe);
	LOGd_("reqe_tmp %p reqe %p\n", reqe_tmp, reqe);
	ASSERT(reqe_tmp == reqe);
}
#endif

/**
 * Check overlapping writes and copy from them.
 *
 * RETURN:
 *   true in success, or false due to data copy failed.
 *
 * CONTEXT:
 *   pending_data lock must be held.
 */
#ifdef WALB_FAST_ALGORITHM
UNUSED static bool pending_check_and_copy(
	struct multimap *pending_data, struct req_entry *reqe)
{
	struct multimap_cursor cur;
	u64 max_io_size, start_pos;
	struct req_entry *reqe_tmp;

	ASSERT(pending_data);
	ASSERT(reqe);

	/* Decide search start position. */
	max_io_size = queue_max_sectors(reqe->wdev->queue);
	if (reqe->req_pos > max_io_size) {
		start_pos = reqe->req_pos - max_io_size;
	} else {
		start_pos = 0;
	}
	
	/* Search the smallest candidate. */
	multimap_cursor_init(pending_data, &cur);
	if (!multimap_cursor_search(&cur, start_pos, MAP_SEARCH_GE, 0)) {
		/* No overlapping requests. */
		return true;
	}
	/* Copy data from pending and overlapping write requests. */
	while (multimap_cursor_key(&cur) < reqe->req_pos + reqe->req_sectors) {

		ASSERT(multimap_cursor_is_valid(&cur));
		
		reqe_tmp = (struct req_entry *)multimap_cursor_val(&cur);
		ASSERT(reqe_tmp);
		if (is_overlap_req_entry(reqe, reqe_tmp)) {
			if (!data_copy_req_entry(reqe, reqe_tmp)) {
				return false;
			}
		}
		if (!multimap_cursor_next(&cur)) {
			break;
		}
	}
	return true;
}
#endif

/**
 * Check whether walb should stop the queue
 * due to too much pending data.
 *
 * CONTEXT:
 *   pending_data_mutex must be held.
 */
#ifdef WALB_FAST_ALGORITHM
static inline bool should_stop_queue(struct pdata *pdata, struct req_entry *reqe)
{
	bool should_stop;
	ASSERT(pdata);
	ASSERT(reqe);

	if (pdata->is_queue_stopped) {
		return false;
	}

	should_stop = pdata->pending_sectors + reqe->req_sectors
		> pdata->max_pending_sectors;

	if (should_stop) {
		pdata->queue_restart_jiffies = jiffies +
			msecs_to_jiffies(pdata->queue_stop_timeout_ms);
		pdata->is_queue_stopped = true;
		return true;
	} else {
		return false;
	}
}
#endif

/**
 * Check whether walb should restart the queue
 * because pending data is not too much now.
 *
 * CONTEXT:
 *   pending_data_mutex must be held.
 */
#ifdef WALB_FAST_ALGORITHM
static inline bool should_start_queue(struct pdata *pdata, struct req_entry *reqe)
{
	bool is_size;
	bool is_timeout;
	ASSERT(pdata);
	ASSERT(reqe);
	ASSERT(pdata->pending_sectors >= reqe->req_sectors);

	if (!pdata->is_queue_stopped) {
		return false;
	}
	
	is_size = pdata->pending_sectors - reqe->req_sectors
		< pdata->min_pending_sectors;
	is_timeout = time_is_before_jiffies(pdata->queue_restart_jiffies);

	if (is_size || is_timeout) {
		pdata->is_queue_stopped = false;
		return true;
	} else {
		return false;
	}
}
#endif

/**
 * Check two request entrys is overlapping.
 */
#if defined(WALB_OVERLAPPING_SERIALIZE) || defined(WALB_FAST_ALGORITHM)
static inline bool is_overlap_req_entry(struct req_entry *reqe0, struct req_entry *reqe1)
{
	ASSERT(reqe0);
	ASSERT(reqe1);
	ASSERT(reqe0 != reqe1);

	return (reqe0->req_pos + reqe0->req_sectors > reqe1->req_pos &&
		reqe1->req_pos + reqe1->req_sectors > reqe0->req_pos);
}
#endif

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
	struct pack_work *pwork;
	struct pack *wpack = NULL, *wpack_next;
	struct walb_logpack_header *lhead;
	bool ret;
	u64 latest_lsid, latest_lsid_old;
	struct list_head wpack_list;

	LOGd_("wrapper_blk_req_request_fn: begin.\n");

	if (!test_bit(0, &wdev->is_started)) {
		goto error0;
	}

	INIT_LIST_HEAD(&wpack_list);
	
	/* Load latest_lsid */
	spin_lock(&pdata->lsid_lock);
	latest_lsid = pdata->latest_lsid;
	spin_unlock(&pdata->lsid_lock);
	latest_lsid_old = latest_lsid;

	/* Initialize pack_work. */
	pwork = create_pack_work(wdev, GFP_ATOMIC);
	if (!pwork) { goto error0; }
			
	/* Fetch requests and create pack list. */
	while ((req = blk_fetch_request(q)) != NULL) {

		/* print_req_flags(req); */
		if (req->cmd_flags & REQ_WRITE) {

			if (is_read_only_mode(pdata)) { goto req_error; }
			
			/* REQ_FLUSH must be here. */
			if (req->cmd_flags & REQ_FLUSH) {
				LOGd("REQ_FLUSH request with size %u.\n", blk_rq_bytes(req));
			}
			LOGd_("call writepack_add_req\n"); /* debug */
			ret = writepack_add_req(&wpack_list, &wpack, req,
						pdata->ring_buffer_size,
						pdata->max_logpack_pb,
						&latest_lsid, wdev, GFP_ATOMIC);
			if (!ret) { goto req_error; }
		} else {
			/* Read request */
			reqe = create_req_entry(req, wdev, GFP_ATOMIC);
			if (!reqe) { goto req_error; }
			INIT_WORK(&reqe->work, read_req_task);
			queue_work(wq_read_, &reqe->work);
		}
		continue;
	req_error:
		__blk_end_request_all(req, -EIO);
	}
	LOGd_("latest_lsid: %"PRIu64"\n", latest_lsid);
	if (wpack) {
		lhead = get_logpack_header(wpack->logpack_header_sector);
		ASSERT(lhead);
		/* Check whether zero-flush-only or not. */
		if (lhead->n_records == 0) {
			ASSERT(is_zero_flush_only(wpack));
			wpack->is_zero_flush_only = true;
		}
		ASSERT(is_valid_prepared_pack(wpack));
		/* Update the latest lsid. */
		latest_lsid = get_next_lsid_unsafe(lhead);
		LOGd_("calculated latest_lsid: %"PRIu64"\n", latest_lsid);

		/* Add the last writepack to the list. */
		ASSERT(!list_empty(&wpack->req_ent_list));
		list_add_tail(&wpack->list, &wpack_list);
	}
	
	if (list_empty(&wpack_list)) {
		/* No write request. */
		destroy_pack_work(pwork);
		pwork = NULL;
	} else {
		/* Currently all requests are packed and lsid of all writepacks is defined. */
		ASSERT(is_valid_pack_list(&wpack_list));

		/* Enqueue all writepacks. */
		spin_lock(&pdata->logpack_submit_queue_lock);
		list_for_each_entry_safe(wpack, wpack_next, &wpack_list, list) {
			list_move_tail(&wpack->list, &pdata->logpack_submit_queue);
		}
		spin_unlock(&pdata->logpack_submit_queue_lock);
		
		if (test_and_set_bit(
				PDATA_STATE_SUBMIT_TASK_WORKING,
				&pdata->flags) == 0) {
			INIT_WORK(&pwork->work, logpack_list_submit_task);
			queue_work(wq_logpack_, &pwork->work);
		} else {
			destroy_pack_work(pwork);
			pwork = NULL;
		}

		/* Store latest_lsid */
		ASSERT(latest_lsid >= latest_lsid_old);
		spin_lock(&pdata->lsid_lock);
		ASSERT(pdata->latest_lsid == latest_lsid_old);
		pdata->latest_lsid = latest_lsid;
		spin_unlock(&pdata->lsid_lock);
	}
	ASSERT(list_empty(&wpack_list));
	
	LOGd_("wrapper_blk_req_request_fn: end.\n");
	return;
#if 0
error1:
	destroy_pack_work(pwork);
#endif
error0:
	while ((req = blk_fetch_request(q)) != NULL) {
		__blk_end_request_all(req, -EIO);
	}
	LOGd_("wrapper_blk_req_request_fn: error.\n");
}

/* Called before register. */
bool pre_register(void)
{
	LOGd("pre_register called.");

	/* Prepare kmem_cache data. */
	pack_work_cache_ = kmem_cache_create(
		KMEM_CACHE_PACK_WORK_NAME,
		sizeof(struct pack_work), 0, 0, NULL);
	if (!pack_work_cache_) {
		LOGe("failed to create a kmem_cache (pack_work).\n");
		goto error0;
	}
	if (!req_entry_init()) {
		goto error1;
	}
	pack_cache_ = kmem_cache_create(
		KMEM_CACHE_PACK_NAME,
		sizeof(struct pack), 0, 0, NULL);
	if (!pack_cache_) {
		LOGe("failed to create a kmem_cache (pack).\n");
		goto error2;
	}
	if (!bio_entry_init()) {
		goto error3;
	}
	
	/* prepare workqueues. */
	wq_logpack_ = alloc_workqueue(WQ_LOGPACK, WQ_MEM_RECLAIM, 0);
	if (!wq_logpack_) {
		LOGe("failed to allocate a workqueue (wq_logpack_).");
		goto error4;
	}
	wq_normal_ = alloc_workqueue(WQ_NORMAL, WQ_MEM_RECLAIM, 0);
	if (!wq_normal_) {
		LOGe("failed to allocate a workqueue (wq_normal_).");
		goto error5;
	}
	wq_read_ = alloc_workqueue(WQ_READ, WQ_MEM_RECLAIM, 0);
	if (!wq_read_) {
		LOGe("failed to allocate a workqueue (wq_read_).");
		goto error6;
	}

	if (!treemap_init()) {
		goto error7;
	}

#ifdef WALB_OVERLAPPING_SERIALIZE
	LOGn("WalB Overlapping Detection supported.\n");
#else
	LOGn("WalB Overlapping Detection not supported.\n");
#endif
#ifdef WALB_FAST_ALGORITHM
	LOGn("WalB Fast Algorithm.\n");
#else
	LOGn("WalB Easy Algorithm.\n");
#endif
	
	return true;

#if 0
error8:
	treemap_exit();
#endif
error7:
	destroy_workqueue(wq_read_);
error6:
	destroy_workqueue(wq_normal_);
error5:
	destroy_workqueue(wq_logpack_);
error4:
	bio_entry_exit();
error3:
	kmem_cache_destroy(pack_cache_);
error2:
	req_entry_exit();
error1:
	kmem_cache_destroy(pack_work_cache_);
error0:
	return false;
}

static void flush_all_wq(void)
{
	flush_workqueue(wq_logpack_); /* complete submit task. */
	flush_workqueue(wq_logpack_); /* complete wait task. */
	flush_workqueue(wq_normal_); /* complete write for data device */
	flush_workqueue(wq_normal_); /* complete all gc tasks. */
	flush_workqueue(wq_read_); /* complete all read tasks. */
}

/* Called before unregister. */
void pre_unregister(void)
{
	LOGn("begin\n");
	flush_all_wq();
	LOGn("end\n");
}

/* Called before destroy_private_data. */
void pre_destroy_private_data(void)
{
	LOGn("begin\n");
	flush_all_wq();
	LOGn("end\n");
}

/* Called after unregister. */
void post_unregister(void)
{
	LOGd_("begin\n");

	treemap_exit();
	
	/* finalize workqueue data. */
	destroy_workqueue(wq_read_);
	wq_read_ = NULL;
	destroy_workqueue(wq_normal_);
	wq_normal_ = NULL;
	destroy_workqueue(wq_logpack_);
	wq_logpack_ = NULL;

	/* Destory kmem_cache data. */
	bio_entry_exit();
	kmem_cache_destroy(pack_cache_);
	pack_cache_ = NULL;
	req_entry_exit();
	kmem_cache_destroy(pack_work_cache_);
	pack_work_cache_ = NULL;

	LOGd_("end\n");
}

/* end of file. */
