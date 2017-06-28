/**
 * io.c - IO processing core of WalB.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/delay.h>
#include <linux/ratelimit.h>
#include <linux/printk.h>
#include <linux/time.h>
#include <linux/kmod.h>
#include "linux/walb/logger.h"
#include "kern.h"
#include "io.h"
#include "bio_wrapper.h"
#include "bio_entry.h"
#include "treemap.h"
#include "worker.h"
#include "bio_util.h"
#include "pack_work.h"
#include "logpack.h"
#include "super.h"
#include "sysfs.h"
#include "pending_io.h"
#include "overlapped_io.h"

/*******************************************************************************
 * Static data definition.
 *******************************************************************************/

/**
 * A write pack.
 */
struct pack
{
	struct list_head list; /* list entry. */
	struct list_head biow_list; /* list head of bio_wrapper. */

	struct sector_data *logpack_header_sector;
	struct list_head bioe_list; /* list head for zero_flush bio
				       or logpack header bio. */
	u64 new_permanent_lsid; /* not invalid if the pack contains flush. */
	bool is_zero_flush_only; /* true if req_ent_list contains only a zero-size flush. */
	bool is_flush_header; /* true if the header IO must flush request. */
	bool is_fua_contained; /* true if the logpack contains FUA request. */
	bool is_logpack_failed; /* true if submittion failed. */
};

static atomic_t n_users_of_pack_cache_ = ATOMIC_INIT(0);
#define KMEM_CACHE_PACK_NAME "pack_cache"
struct kmem_cache *pack_cache_ = NULL;

/* All treemap(s) in this module will share a treemap memory manager. */
static atomic_t n_users_of_memory_manager_ = ATOMIC_INIT(0);
static struct treemap_memory_manager mmgr_;
#define TREE_NODE_CACHE_NAME "walb_iocore_bio_node_cache"
#define TREE_CELL_HEAD_CACHE_NAME "walb_iocore_bio_cell_head_cache"
#define TREE_CELL_CACHE_NAME "walb_iocore_bio_cell_cache"
#define N_ITEMS_IN_MEMPOOL (128 * 2) /* for pending data and overlapped data. */

/*******************************************************************************
 * Macros definition.
 *******************************************************************************/

#define WORKER_NAME_GC "walb_gc"

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/* bio_entry related. */
static void bio_entry_end_io(struct bio *bio, int error);
static struct bio_entry* create_bio_entry_by_clone(
	struct bio *bio, struct block_device *bdev, gfp_t gfp_mask);

/* Helper functions for bio_entry list. */
static void submit_bio_entry_list(struct list_head *bioe_list);
static int wait_for_bio_entry_list(struct list_head *bioe_list, struct walb_dev *wdev);
static void clear_flush_bit_of_bio_entry_list(struct list_head *bioe_list);

/* pack related. */
static struct pack* create_pack(gfp_t gfp_mask);
static struct pack* create_writepack(gfp_t gfp_mask, unsigned int pbs, u64 logpack_lsid);
static void destroy_pack(struct pack *pack);
static bool is_zero_flush_only(const struct pack *pack);
static bool is_pack_size_too_large(
	struct walb_logpack_header *lhead,
	unsigned int pbs, unsigned int max_logpack_pb,
	struct bio_wrapper *biow);
UNUSED static void print_pack(
	const char *level, struct pack *pack);
UNUSED static void print_pack_list(
	const char *level, struct list_head *wpack_list);
static bool pack_header_should_flush(const struct pack *pack);
static void get_wdev_and_iocored_from_work(
	struct walb_dev **pwdev, struct iocore_data **piocored,
	struct work_struct *work);

/* Workqueue tasks. */
static void task_submit_logpack_list(struct work_struct *work);
static void task_wait_for_logpack_list(struct work_struct *work);
static void task_wait_and_gc_read_bio_wrapper(struct work_struct *work);
static void task_submit_bio_wrapper_list(struct work_struct *work);
static void task_wait_for_bio_wrapper_list(struct work_struct *work);

/* Logpack GC */
static void run_gc_logpack_list(void *data);

/* Logpack related functions. */
static bool create_logpack_list(
	struct walb_dev *wdev, struct list_head *biow_list,
	struct list_head *pack_list);
static void submit_logpack_list(
	struct walb_dev *wdev, struct list_head *wpack_list);
static void logpack_calc_checksum(
	struct walb_logpack_header *lhead,
	unsigned int pbs, u32 salt, struct list_head *biow_list);
static void submit_logpack(
	struct walb_logpack_header *logh,
	struct list_head *biow_list, struct list_head *bioe_list,
	unsigned int pbs, bool is_flush, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size,
	unsigned int chunk_sectors);
static void logpack_submit_header(
	struct walb_logpack_header *logh,
	struct list_head *bioe_list,
	unsigned int pbs, bool is_flush, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size,
	unsigned int chunk_sectors);
UNUSED static void logpack_submit_bio_wrapper_zero(
	struct bio_wrapper *biow, struct list_head *bioe_list,
	unsigned int pbs, struct block_device *ldev); /* DEPRECATED */
static void logpack_submit_bio_wrapper(
	struct bio_wrapper *biow, u64 lsid,
	struct list_head *bioe_list,
	unsigned int pbs, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size,
	unsigned int chunk_sectors);
static struct bio_entry* logpack_create_bio_entry(
	struct bio *bio, unsigned int pbs,
	struct block_device *ldev,
	u64 ldev_offset, unsigned int bio_offset);
static void logpack_submit_flush(
	struct block_device *ldev, struct list_head *bioe_list);
static void gc_logpack_list(struct walb_dev *wdev, struct list_head *wpack_list);
static void dequeue_and_gc_logpack_list(struct walb_dev *wdev);

/* Validator for debug. */
static bool is_prepared_pack_valid(struct pack *pack);
UNUSED static bool is_pack_list_valid(struct list_head *pack_list);

/* IOcore data related. */
static struct iocore_data* create_iocore_data(gfp_t gfp_mask);
static void destroy_iocore_data(struct iocore_data *iocored);

/* Other helper functions. */
static bool push_into_lpack_submit_queue(struct bio_wrapper *biow);
static bool writepack_add_bio_wrapper(
	struct list_head *wpack_list, struct pack **wpackp,
	struct bio_wrapper *biow,
	u64 ring_buffer_size, unsigned int max_logpack_pb,
	u64 *latest_lsidp, struct walb_dev *wdev, gfp_t gfp_mask, bool *is_flushp);
static void insert_to_sorted_bio_wrapper_list_by_pos(
	struct bio_wrapper *biow, struct list_head *biow_list);
static void writepack_check_and_set_zeroflush(struct pack *wpack, bool *is_flushp);
static void wait_for_logpack_and_submit_datapack(
	struct walb_dev *wdev, struct pack *wpack);
static void wait_for_write_bio_wrapper(
	struct walb_dev *wdev, struct bio_wrapper *biow);
static void wait_for_bio_wrapper(
	struct bio_wrapper *biow, bool is_endio, bool is_delete);
static void submit_write_bio_wrapper(
	struct bio_wrapper *biow, bool is_plugging);
static void cancel_write_bio_wrapper(
	struct walb_dev *wdev, struct bio_wrapper *biow);
static void submit_read_bio_wrapper(
	struct walb_dev *wdev, struct bio_wrapper *biow);
static struct bio_entry* submit_flush(struct block_device *bdev);
static void dispatch_submit_log_task(struct walb_dev *wdev);
static void dispatch_wait_log_task(struct walb_dev *wdev);
static void dispatch_submit_data_task(struct walb_dev *wdev);
static void dispatch_wait_data_task(struct walb_dev *wdev);
static void start_write_bio_wrapper(
	struct walb_dev *wdev, struct bio_wrapper *biow);
static void wait_for_logpack_submit_queue_empty(struct walb_dev *wdev);
static void wait_for_all_started_write_io_done(struct walb_dev *wdev);
static void wait_for_all_pending_gc_done(struct walb_dev *wdev);
static void force_flush_ldev(struct walb_dev *wdev);
static bool wait_for_log_permanent(struct walb_dev *wdev, u64 lsid);
static void flush_all_wq(void);
static void clear_working_flag(int working_bit, unsigned long *flag_p);
static void invoke_userland_exec(struct walb_dev *wdev, const char *event);
static void fail_and_destroy_bio_wrapper_list(
	struct walb_dev *wdev, struct list_head *biow_list);
static void update_flush_lsid_if_necessary(struct walb_dev *wdev, u64 flush_lsid);
static bool delete_bio_wrapper_from_pending_data(
	struct walb_dev *wdev, struct bio_wrapper *biow);

/* Stop/start queue for fast algorithm. */
static bool should_stop_queue(
	struct walb_dev *wdev, struct bio_wrapper *biow);
static bool should_start_queue(
	struct walb_dev *wdev, struct bio_wrapper *biow);

/* For treemap memory manager. */
static bool treemap_memory_manager_get(void);
static void treemap_memory_manager_put(void);

/* For pack_cache. */
static bool pack_cache_get(void);
static void pack_cache_put(void);

/* For diskstats. */
static void io_acct_start(struct bio_wrapper *biow);
static void io_acct_end(struct bio_wrapper *biow);

/* For freeze/melt. */
static bool is_frozen(struct iocore_data *iocored);
static void set_frozen(struct iocore_data *iocored, bool is_usr, bool value);
static void freeze_detail(struct iocore_data *iocored, bool is_usr);
static bool melt_detail(struct iocore_data *iocored, bool is_usr);

/*******************************************************************************
 * Static functions implementation.
 *******************************************************************************/

/**
 * endio callback for bio_entry.
 */
static void bio_entry_end_io(struct bio *bio, int error)
{
	struct bio_entry *bioe = bio->bi_private;
	int uptodate = test_bit(BIO_UPTODATE, &bio->bi_flags);
	int bi_cnt;
	ASSERT(bioe);
	ASSERT(bio->bi_bdev);
#ifdef WALB_DEBUG
	if (bioe->bio_orig) {
		ASSERT(bio_entry_state_is_splitted(bioe));
		ASSERT(bioe->bio_orig == bio);
	} else {
		ASSERT(bioe->bio == bio);
	}
#endif
	if (!uptodate) {
		UNUSED unsigned int devt = bio->bi_bdev->bd_dev;
		LOG_("BIO_UPTODATE is false (dev %u:%u rw %lu pos %" PRIu64 " len %u).\n",
			MAJOR(devt), MINOR(devt),
			bio->bi_rw, (u64)bioe->pos, bioe->len);
	}

	bioe->error = error;
	bi_cnt = atomic_read(&bio->bi_cnt);
	if (bio->bi_rw & REQ_WRITE) {
		/* 1 for ldev, 2 for ddev. */
		ASSERT(bi_cnt == 1 || bi_cnt == 2);
	} else {
		ASSERT(bi_cnt == 1);
	}
	LOG_("complete bioe %p pos %" PRIu64 " len %u\n",
		bioe, (u64)bioe->pos, bioe->len);
	if (bi_cnt == 1) {
		bioe->bio_orig = NULL;
		bioe->bio = NULL;
	}
	bio_put(bio);
	complete(&bioe->done);
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
 * Submit all bio_entry(s) in a req_entry.
 *
 * @bioe_list list head of bio_entry.
 *
 * CONTEXT:
 *   non-irq. non-atomic.
 */
static void submit_bio_entry_list(struct list_head *bioe_list)
{
	struct bio_entry *bioe;

	ASSERT(bioe_list);
	list_for_each_entry(bioe, bioe_list, list) {
#ifdef WALB_DEBUG
		if (!bio_entry_state_is_splitted(bioe)) {
			ASSERT(bioe->bio->bi_end_io == bio_entry_end_io);
		}
#endif /* WALB_DEBUG */
		if (bio_entry_state_is_copied(bioe)) {
			LOG_("copied: rw %lu bioe %p pos %"PRIu64" len %u\n",
				bioe->bio->bi_rw,
				bioe, (u64)bioe->pos, bioe->len);
			set_bit(BIO_UPTODATE, &bioe->bio->bi_flags);
			bio_endio(bioe->bio, 0);
		} else {
			LOG_("submit_d: rw %lu bioe %p pos %"PRIu64" len %u\n",
				bioe->bio->bi_rw,
				bioe, (u64)bioe->pos, bioe->len);
			generic_make_request(bioe->bio);
		}
	}
}

/**
 * Wait for all bio(s) completion in a bio_entry list.
 * Each bio_entry will be deleted.
 *
 * @bioe_list list head of bio_entry.
 *
 * RETURN:
 *   error of the last failed bio (0 means success).
 */
static int wait_for_bio_entry_list(struct list_head *bioe_list, struct walb_dev *wdev)
{
	struct bio_entry *bioe, *next_bioe;
	int bio_error = 0;
	const unsigned long timeo = msecs_to_jiffies(completion_timeo_ms_);
	unsigned long rtimeo;
	ASSERT(bioe_list);

	/* wait for completion. */
	list_for_each_entry(bioe, bioe_list, list) {

		if (bio_entry_should_wait_completion(bioe)) {
			int c = 0;
		retry:
			rtimeo = wait_for_completion_io_timeout(&bioe->done, timeo);
			if (rtimeo == 0) {
				LOGn("%u: timeout(%d): bioe %p bio %p len %u\n",
					wdev_minor(wdev), c, bioe, bioe->bio, bioe->len);
				c++;
				goto retry;
			}
		}
		if (bioe->error) { bio_error = bioe->error; }
	}
	/* destroy. */
	list_for_each_entry_safe(bioe, next_bioe, bioe_list, list) {
		list_del(&bioe->list);
		destroy_bio_entry(bioe);
	}
	ASSERT(list_empty(bioe_list));
	return bio_error;
}

/**
 * Clear REQ_FLUSH and REQ_FUA bit of all bios inside bio entry list.
 */
static void clear_flush_bit_of_bio_entry_list(struct list_head *bioe_list)
{
	struct bio_entry *bioe;
	const unsigned long mask = REQ_FLUSH | REQ_FUA;

	list_for_each_entry(bioe, bioe_list, list) {
		ASSERT(bioe->bio);
		ASSERT(bioe->bio->bi_rw & REQ_WRITE);
		bioe->bio->bi_rw &= ~mask;
	}
}

/**
 * Create a pack.
 */
static struct pack* create_pack(gfp_t gfp_mask)
{
	struct pack *pack;

	pack = kmem_cache_alloc(pack_cache_, gfp_mask);
	if (!pack) {
		LOGd("kmem_cache_alloc() failed.");
		goto error0;
	}
	INIT_LIST_HEAD(&pack->list);
	INIT_LIST_HEAD(&pack->biow_list);
	INIT_LIST_HEAD(&pack->bioe_list);
	pack->is_zero_flush_only = false;
	pack->is_flush_header = false;
	pack->is_fua_contained = false;
	pack->is_logpack_failed = false;
	pack->new_permanent_lsid = INVALID_LSID;

	return pack;
#if 0
error1:
	destory_pack(pack);
#endif
error0:
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
	struct bio_wrapper *biow, *biow_next;

	if (!pack) { return; }

	list_for_each_entry_safe(biow, biow_next, &pack->biow_list, list) {
		list_del(&biow->list);
		destroy_bio_wrapper_dec((struct walb_dev *)biow->private_data, biow);
	}
	if (pack->logpack_header_sector) {
		sector_free(pack->logpack_header_sector);
		pack->logpack_header_sector = NULL;
	}
#ifdef WALB_DEBUG
	INIT_LIST_HEAD(&pack->biow_list);
#endif
	kmem_cache_free(pack_cache_, pack);
}

/**
 * Check the pack contains zero-size flush only.
 *
 * RETURN:
 *   true if pack contains only one request and it is zero-size flush, or false.
 */
static bool is_zero_flush_only(const struct pack *pack)
{
	struct walb_logpack_header *logh;
	bool ret;

	ASSERT(pack);
	ASSERT(pack->logpack_header_sector);
	logh = get_logpack_header(pack->logpack_header_sector);
	ASSERT(logh);

	ret = logh->n_records == 0 && !list_empty(&pack->biow_list);
#ifdef WALB_DEBUG
	if (ret) {
		struct bio_wrapper *biow;
		int i = 0;
		list_for_each_entry(biow, &pack->biow_list, list) {
			ASSERT(biow->bio);
			ASSERT(biow->bio->bi_rw & REQ_FLUSH);
			ASSERT(biow->len == 0);
			i++;
		}
		ASSERT(i == 1);
	}
#endif
	return ret;
}

/**
 * Check the pack size exceeds max_logpack_pb or not.
 *
 * RETURN:
 *   true if pack is already exceeds or will be exceeds.
 */
static bool is_pack_size_too_large(
	struct walb_logpack_header *lhead,
	unsigned int pbs, unsigned int max_logpack_pb,
	struct bio_wrapper *biow)
{
	unsigned int pb;
	ASSERT(lhead);
	ASSERT(pbs);
	ASSERT_PBS(pbs);
	ASSERT(biow);

	if (max_logpack_pb == 0) {
		return false;
	}

	if (bio_wrapper_state_is_discard(biow))
		return false;

	pb = (unsigned int)capacity_pb(pbs, biow->len);
	return pb + lhead->total_io_size > max_logpack_pb;
}

/**
 * Print a pack data for debug.
 */
static void print_pack(const char *level, struct pack *pack)
{
	struct walb_logpack_header *lhead;
	struct bio_wrapper *biow;
	struct bio_entry *bioe;
	unsigned int i;
	ASSERT(level);
	ASSERT(pack);

	printk("%s""print_pack %p begin\n", level, pack);

	i = 0;
	list_for_each_entry(biow, &pack->biow_list, list) {
		i++;
		print_bio_wrapper(level, biow);
	}
	printk("%s""number of bio_wrapper in biow_list: %u.\n", level, i);

	i = 0;
	list_for_each_entry(bioe, &pack->bioe_list, list) {
		i++;
		print_bio_entry(level, bioe);
	}
	printk("%s""number of bio_entry in bioe_list: %u.\n", level, i);

	/* logpack header */
	if (pack->logpack_header_sector) {
		lhead = get_logpack_header(pack->logpack_header_sector);
		walb_logpack_header_print(level, lhead);
	} else {
		printk("%s""logpack_header_sector is NULL.\n", level);
	}

	printk("%s"
		"is_logpack_failed: %u\n"
		"new_permanent_lsid: %" PRIu64 "\n"
		"is_zero_flush_only: %u\n"
		"is_flush_header: %u\n"
		"is_fua_contained: %u\n"
		, level
		, pack->is_logpack_failed
		, pack->new_permanent_lsid
		, pack->is_zero_flush_only
		, pack->is_flush_header
		, pack->is_fua_contained);

	printk("%s""print_pack %p end\n", level, pack);
}

/**
 * Print a list of pack data for debug.
 */
static void print_pack_list(const char *level, struct list_head *wpack_list)
{
	struct pack *pack;
	unsigned int i = 0;
	ASSERT(level);
	ASSERT(wpack_list);

	printk("%s""print_pack_list %p begin.\n", level, wpack_list);
	list_for_each_entry(pack, wpack_list, list) {
		printk("%s""%u: ", level, i);
		print_pack(level, pack);
		i++;
	}
	printk("%s""print_pack_list %p end.\n", level, wpack_list);
}

/**
 * RETURN:
 *   true if pack header must have flush request for log device.
 */
static bool pack_header_should_flush(const struct pack *pack)
{
	return pack->is_flush_header && !pack->is_fua_contained;

}

/**
 * Get pointer of wdev and iocored from the work struct in a pwork.
 * The pwork will be destroyed.
 */
static void get_wdev_and_iocored_from_work(
	struct walb_dev **pwdev, struct iocore_data **piocored,
	struct work_struct *work)
{
	struct pack_work *pwork = container_of(work, struct pack_work, work);
	*pwdev = pwork->data;
	*piocored = get_iocored_from_wdev(*pwdev);
	destroy_pack_work(pwork);
}

/**
 * Submit all logpacks generated from bio_wrapper list.
 *
 * (1) Create logpack list.
 * (2) Submit all logpack-related bio(s).
 * (3) Enqueue task_wait_for_logpack_list.
 *
 * If an error (memory allocation failure) occurred inside this,
 * allocator will retry allocation after calling scheule() infinitely.
 *
 * @work work in a pack_work.
 *
 * CONTEXT:
 *   Workqueue task.
 *   The same task is not executed concurrently.
 */
static void task_submit_logpack_list(struct work_struct *work)
{
	struct walb_dev *wdev;
	struct iocore_data *iocored;
	struct list_head wpack_list;
	struct list_head biow_list;

	get_wdev_and_iocored_from_work(&wdev, &iocored, work);
	LOG_("begin\n");

	INIT_LIST_HEAD(&biow_list);
	INIT_LIST_HEAD(&wpack_list);
	while (true) {
		struct pack *wpack, *wpack_next;
		struct bio_wrapper *biow, *biow_next;
		bool is_empty;
		unsigned int n_io = 0;

		ASSERT(list_empty(&biow_list));
		ASSERT(list_empty(&wpack_list));

		/* Dequeue all bio wrappers from the submit queue. */
		spin_lock(&iocored->logpack_submit_queue_lock);
		is_empty = list_empty(&iocored->logpack_submit_queue);
		if (is_empty) {
			clear_working_flag(
				IOCORE_STATE_SUBMIT_LOG_TASK_WORKING,
				&iocored->flags);
		}
		list_for_each_entry_safe(biow, biow_next,
					&iocored->logpack_submit_queue, list) {
			list_move_tail(&biow->list, &biow_list);
			start_write_bio_wrapper(wdev, biow);
			n_io++;
			if (n_io >= wdev->n_io_bulk) { break; }
		}
		spin_unlock(&iocored->logpack_submit_queue_lock);
		if (is_empty) { break; }

		/* Failure mode. */
		if (test_bit(WALB_STATE_READ_ONLY, &wdev->flags)) {
			fail_and_destroy_bio_wrapper_list(wdev, &biow_list);
			continue;
		}

		/* Create and submit. */
		if (!create_logpack_list(wdev, &biow_list, &wpack_list)) {
			continue;
		}
		submit_logpack_list(wdev, &wpack_list);

		/* Enqueue logpack list to the wait queue. */
		spin_lock(&iocored->logpack_wait_queue_lock);
		list_for_each_entry_safe(wpack, wpack_next, &wpack_list, list) {
			list_move_tail(&wpack->list, &iocored->logpack_wait_queue);
		}
		spin_unlock(&iocored->logpack_wait_queue_lock);

		/* Enqueue wait task. */
		dispatch_wait_log_task(wdev);
	}

	LOG_("end\n");
}

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
static void task_wait_for_logpack_list(struct work_struct *work)
{
	struct walb_dev *wdev;
	struct iocore_data *iocored;
	struct list_head wpack_list;

	get_wdev_and_iocored_from_work(&wdev, &iocored, work);
	LOG_("begin\n");

	INIT_LIST_HEAD(&wpack_list);
	while (true) {
		struct pack *wpack, *wpack_next;
		bool is_empty;
		unsigned int n_pack = 0;
		ASSERT(list_empty(&wpack_list));

		/* Dequeue logpack list from the submit queue. */
		spin_lock(&iocored->logpack_wait_queue_lock);
		is_empty = list_empty(&iocored->logpack_wait_queue);
		if (is_empty) {
			clear_working_flag(
				IOCORE_STATE_WAIT_LOG_TASK_WORKING,
				&iocored->flags);
		}
		list_for_each_entry_safe(wpack, wpack_next,
					&iocored->logpack_wait_queue, list) {
			list_move_tail(&wpack->list, &wpack_list);
			n_pack++;
			if (n_pack >= wdev->n_pack_bulk) { break; }
		}
		spin_unlock(&iocored->logpack_wait_queue_lock);
		if (is_empty) { break; }

		/* Wait logpack completion and submit datapacks. */
		list_for_each_entry_safe(wpack, wpack_next, &wpack_list, list) {
			wait_for_logpack_and_submit_datapack(wdev, wpack);
		}
		dispatch_submit_data_task(wdev);

		/* Put packs into the gc queue. */
		atomic_add(n_pack, &iocored->n_pending_gc);
		spin_lock(&iocored->logpack_gc_queue_lock);
		list_for_each_entry_safe(wpack, wpack_next, &wpack_list, list) {
			list_move_tail(&wpack->list, &iocored->logpack_gc_queue);
		}
		spin_unlock(&iocored->logpack_gc_queue_lock);

		/* Wakeup the gc task. */
		wakeup_worker(&iocored->gc_worker_data);
	}

	LOG_("end\n");
}

/**
 * Wait for all related bio(s) for a bio_wrapper and gc it.
 */
static void task_wait_and_gc_read_bio_wrapper(struct work_struct *work)
{
	struct bio_wrapper *biow = container_of(work, struct bio_wrapper, work);
	struct walb_dev *wdev;
	const bool is_endio = true;
	const bool is_delete = true;

	ASSERT(biow);
	wdev = (struct walb_dev *)biow->private_data;
	ASSERT(wdev);

	wait_for_bio_wrapper(biow, is_endio, is_delete);
	destroy_bio_wrapper_dec(wdev, biow);
}

/**
 * Submit bio wrapper list for data device.
 */
static void task_submit_bio_wrapper_list(struct work_struct *work)
{
	struct walb_dev *wdev;
	struct iocore_data *iocored;
	struct list_head biow_list, biow_list_sorted;

	get_wdev_and_iocored_from_work(&wdev, &iocored, work);
	LOG_("begin\n");

	INIT_LIST_HEAD(&biow_list);
	INIT_LIST_HEAD(&biow_list_sorted);
	while (true) {
		struct bio_wrapper *biow, *biow_next;
		bool is_empty;
		u64 lsid = 0;
		u32 pb = 0;
		unsigned int n_io = 0;
		struct blk_plug plug;
#ifdef WALB_OVERLAPPED_SERIALIZE
		bool ret;
#endif

		ASSERT(list_empty(&biow_list));
		ASSERT(list_empty(&biow_list_sorted));

		/* Dequeue all bio wrappers from the submit queue. */
		spin_lock(&iocored->datapack_submit_queue_lock);
		is_empty = list_empty(&iocored->datapack_submit_queue);
		if (is_empty) {
			clear_working_flag(
				IOCORE_STATE_SUBMIT_DATA_TASK_WORKING,
				&iocored->flags);
		}
		list_for_each_entry_safe(biow, biow_next,
					&iocored->datapack_submit_queue, list2) {
			list_move_tail(&biow->list2, &biow_list);
			n_io++;
			lsid = biow->lsid;
			ASSERT(biow->len > 0);
			if (bio_wrapper_state_is_discard(biow))
				pb = 0;
			else
				pb = capacity_pb(wdev->physical_bs, biow->len);
#ifdef WALB_DEBUG
			atomic_inc(&biow->state);
#endif
			if (n_io >= wdev->n_io_bulk) { break; }
		}
		spin_unlock(&iocored->datapack_submit_queue_lock);
		if (is_empty) { break; }

		/* Wait for all previous log must be permanent
		   before submitting data IO. */
		if (!wait_for_log_permanent(wdev, lsid + pb)) {
			/* The device became read-only mode
			   so all the write data IOs must fail. */
			size_t nr = 0;
			list_for_each_entry_safe(biow, biow_next, &biow_list, list2) {
				list_del(&biow->list2);
				cancel_write_bio_wrapper(wdev, biow);
				nr++;
			}
			WLOGi(wdev, "write data IOs were canceled due to READ_ONLY mode: %zu\n", nr);
			continue;
		}

#ifdef WALB_OVERLAPPED_SERIALIZE
		/* Check and insert to overlapped detection data. */
		list_for_each_entry(biow, &biow_list, list2) {
		retry_insert_ol:
			spin_lock(&iocored->overlapped_data_lock);
			ret = overlapped_check_and_insert(
				iocored->overlapped_data,
				&iocored->max_sectors_in_overlapped,
				biow, GFP_ATOMIC
#ifdef WALB_DEBUG
				, &iocored->overlapped_in_id
#endif
				);
			spin_unlock(&iocored->overlapped_data_lock);
			if (!ret) {
				schedule();
				goto retry_insert_ol;
			}
		}
#endif /* WALB_OVERLAPPED_SERIALIZE */

		/* Sort IOs. */
		list_for_each_entry_safe(biow, biow_next, &biow_list, list2) {
			/* Clear flush bit. */
			clear_flush_bit_of_bio_entry_list(&biow->bioe_list);

#ifdef WALB_OVERLAPPED_SERIALIZE
			if (!bio_wrapper_state_is_delayed(biow)) {
				ASSERT(biow->n_overlapped == 0);
				if (sort_data_io_) {
					/* Sort. */
					insert_to_sorted_bio_wrapper_list_by_pos(
						biow, &biow_list_sorted);
				} else {
					list_add_tail(&biow->list4, &biow_list_sorted);
				}
			} else {
				/* Delayed. */
			}
#else /* WALB_OVERLAPPED_SERIALIZE */
			if (sort_data_io_) {
				/* Sort. */
				insert_to_sorted_bio_wrapper_list_by_pos(
					biow, &biow_list_sorted);
			} else {
				list_add_tail(&biow->list4, &biow_list_sorted);
			}
#endif /* WALB_OVERLAPPED_SERIALIZE */

		}

		/* Submit. */
		blk_start_plug(&plug);
		list_for_each_entry_safe(biow, biow_next, &biow_list_sorted, list4) {
			const bool is_plugging = false;
			/* Submit bio wrapper. */
			list_del(&biow->list4);
#ifdef WALB_DEBUG
			atomic_inc(&biow->state);
#endif
			submit_write_bio_wrapper(biow, is_plugging);
		}
		blk_finish_plug(&plug);

		/* Enqueue wait task. */
		spin_lock(&iocored->datapack_wait_queue_lock);
		list_for_each_entry_safe(biow, biow_next, &biow_list, list2) {
#ifdef WALB_DEBUG
			atomic_inc(&biow->state);
#endif
			list_move_tail(&biow->list2, &iocored->datapack_wait_queue);
		}
		spin_unlock(&iocored->datapack_wait_queue_lock);
		dispatch_wait_data_task(wdev);
	}

	LOG_("end.\n");
}

/**
 * Wait for bio wrapper list for data device.
 */
static void task_wait_for_bio_wrapper_list(struct work_struct *work)
{
	struct walb_dev *wdev;
	struct iocore_data *iocored;
	struct list_head biow_list;

	get_wdev_and_iocored_from_work(&wdev, &iocored, work);
	LOG_("begin.\n");

	INIT_LIST_HEAD(&biow_list);
	while (true) {
		struct bio_wrapper *biow, *biow_next;
		bool is_empty;
		unsigned int n_io = 0;

		ASSERT(list_empty(&biow_list));

		/* Dequeue all bio wrappers from the submit queue. */
		spin_lock(&iocored->datapack_wait_queue_lock);
		is_empty = list_empty(&iocored->datapack_wait_queue);
		if (is_empty) {
			clear_working_flag(
				IOCORE_STATE_WAIT_DATA_TASK_WORKING,
				&iocored->flags);
		}
		list_for_each_entry_safe(biow, biow_next,
					&iocored->datapack_wait_queue, list2) {
			list_move_tail(&biow->list2, &biow_list);
			n_io++;
#ifdef WALB_DEBUG
			atomic_inc(&biow->state);
#endif
			if (n_io >= wdev->n_io_bulk) { break; }
		}
		spin_unlock(&iocored->datapack_wait_queue_lock);
		if (is_empty) { break; }
		ASSERT(n_io <= wdev->n_io_bulk);

		/* Wait for write bio wrapper and notify to gc task. */
		list_for_each_entry_safe(biow, biow_next, &biow_list, list2) {
			list_del(&biow->list2);
			wait_for_write_bio_wrapper(wdev, biow);
#ifdef WALB_PERFORMANCE_ANALYSIS
			getnstimeofday(&biow->ts[WALB_TIME_DATA_COMPLETED]);
#endif
			complete(&biow->done);
		}
	}

	LOG_("end.\n");
}

/**
 * Run gc logpack list.
 */
static void run_gc_logpack_list(void *data)
{
	struct walb_dev *wdev = (struct walb_dev *)data;
	ASSERT(wdev);

	dequeue_and_gc_logpack_list(wdev);
}

/**
 * Create logpack list using bio_wrapper(s) in biow_list,
 * and add to wpack_list.
 *
 * @wdev walb device.
 * @biow_list list of bio_wrapper.
 *   When all bio wrappers are successfuly processed,
 *   biow_list will be empty.
 *   When memory allocation errors occur,
 *   biow_list will not be empty.
 * @wpack_list list of pack (must be empty).
 *   Finally all biow(s) in the biow_list will be
 *   moved to pack(s) in the wpack_list.
 */
static bool create_logpack_list(
	struct walb_dev *wdev, struct list_head *biow_list,
	struct list_head *wpack_list)
{
	struct iocore_data *iocored;
	struct bio_wrapper *biow, *biow_next;
	struct pack *wpack = NULL;
	u64 latest_lsid, latest_lsid_old,
		completed_lsid, flush_lsid, written_lsid, prev_written_lsid, oldest_lsid;
	unsigned long log_flush_jiffies;
	bool ret, is_flush = false;

	ASSERT(wdev);
	iocored = get_iocored_from_wdev(wdev);
	ASSERT(iocored);
	ASSERT(list_empty(wpack_list));
	ASSERT(!list_empty(biow_list));
	might_sleep();

	/* Load latest_lsid */
	spin_lock(&wdev->lsid_lock);
	latest_lsid = wdev->lsids.latest;
	oldest_lsid = wdev->lsids.oldest;
	completed_lsid = wdev->lsids.completed;
	flush_lsid = wdev->lsids.flush;
	prev_written_lsid = wdev->lsids.prev_written;
	written_lsid = wdev->lsids.written;
	log_flush_jiffies = iocored->log_flush_jiffies;
	spin_unlock(&wdev->lsid_lock);
	latest_lsid_old = latest_lsid;

	/* Create logpack(s). */
	list_for_each_entry_safe(biow, biow_next, biow_list, list) {
		list_del(&biow->list);
	retry:
		ret = writepack_add_bio_wrapper(
			wpack_list, &wpack, biow,
			wdev->ring_buffer_size, wdev->max_logpack_pb,
			&latest_lsid, wdev, GFP_NOIO, &is_flush);
		if (!ret) {
			WLOGw(wdev, "writepack_add_bio_wrapper failed.\n");
			schedule();
			goto retry;
		}
	}
	if (wpack) {
		struct walb_logpack_header *logh
			= get_logpack_header(wpack->logpack_header_sector);
		writepack_check_and_set_zeroflush(wpack, &is_flush);
		ASSERT(is_prepared_pack_valid(wpack));
		list_add_tail(&wpack->list, wpack_list);
		latest_lsid = get_next_lsid_unsafe(logh);
	}

	/* Currently all requests are packed and lsid of all writepacks is defined. */
	ASSERT(is_pack_list_valid(wpack_list));
	ASSERT(!list_empty(wpack_list));
	ASSERT(list_empty(biow_list));

	if (!is_flush && supports_flush_request_bdev(wdev->ldev)) {
		/* Decide to flush the log device or not. */
		bool is_flush_size = wdev->log_flush_interval_pb > 0 &&
			completed_lsid - flush_lsid > wdev->log_flush_interval_pb;
		bool is_flush_period = wdev->log_flush_interval_jiffies > 0 &&
			log_flush_jiffies < jiffies;
		if (is_flush_size || is_flush_period)
			is_flush = true;
	}
	if (is_flush) {
		/* Flush flag should be set on only the first logpack header. */
		wpack = list_first_entry(wpack_list, struct pack, list);
		wpack->is_flush_header = true;
#ifdef WALB_DEBUG
		atomic_inc(&iocored->n_flush_logpack);
#endif
	}

	/* Check whether we must avoid ring buffer overflow. */
	if (error_before_overflow_ && wdev->ring_buffer_size < latest_lsid - oldest_lsid)
		goto error;

	/* Store lsids. */
	ASSERT(latest_lsid >= latest_lsid_old);
	spin_lock(&wdev->lsid_lock);
	ASSERT(wdev->lsids.latest == latest_lsid_old);
	wdev->lsids.latest = latest_lsid;
	if (is_flush) {
		wpack->new_permanent_lsid = wdev->lsids.completed;
		update_flush_lsid_if_necessary(wdev, wpack->new_permanent_lsid);
	}
	spin_unlock(&wdev->lsid_lock);

	/* Check ring buffer overflow. */
	ASSERT(latest_lsid >= oldest_lsid);
	if (latest_lsid - oldest_lsid > wdev->ring_buffer_size) {
		if (!test_and_set_bit(WALB_STATE_OVERFLOW, &wdev->flags)) {
			WLOGe(wdev, "Ring buffer for log has been overflowed."
				" reset_wal is required.\n");
			invoke_userland_exec(wdev, "overflow");
		}
	}

	/* Check consistency. */
	ASSERT(latest_lsid >= written_lsid);
	ASSERT(written_lsid >= prev_written_lsid);
	while (latest_lsid - prev_written_lsid > wdev->ring_buffer_size) {
		if (latest_lsid - written_lsid > wdev->ring_buffer_size) {
			if (test_bit(WALB_STATE_READ_ONLY, &wdev->flags))
				goto error;
			WLOGw(wdev, "Ring buffer size is too small: sleep 100ms: "
				"latest %" PRIu64 " written %" PRIu64 " prev_written %" PRIu64 "\n"
				, latest_lsid, written_lsid, prev_written_lsid);

			/* In order to avoid live lock of IOs waiting their logs to be permanent */
			force_flush_ldev(wdev);

			msleep(100);
		} else {
			WLOGw(wdev, "Ring buffer size is too small: try to take checkpoint: "
				"latest %" PRIu64 " written %" PRIu64 " prev_written %" PRIu64 "\n"
				, latest_lsid, written_lsid, prev_written_lsid);
			stop_checkpointing(&wdev->cpd);
			if (!take_checkpoint(&wdev->cpd))
				goto error;
			start_checkpointing(&wdev->cpd);
		}
		spin_lock(&wdev->lsid_lock);
		prev_written_lsid = wdev->lsids.prev_written;
		written_lsid = wdev->lsids.written;
		spin_unlock(&wdev->lsid_lock);
	}

	/* Now the logpack can be submitted. */
	return true;

error:
	{
		struct pack *wpack_next;
		/* We must fail the IOs to avoid ring buffer overflow. */
		list_for_each_entry_safe(wpack, wpack_next, wpack_list, list) {
			list_del(&wpack->list);
			fail_and_destroy_bio_wrapper_list(wdev, &wpack->biow_list);
#ifdef DEBUG
			if (wpack->is_flush_header)
				atomic_dec(&iocored->n_flush_logpack);
#endif
			ASSERT(list_empty(&wpack->bioe_list));
			destroy_pack(wpack);
		}
		ASSERT(list_empty(wpack_list));
	}
	return false;
}

/**
 * Submit all write packs in a list to the log device.
 */
static void submit_logpack_list(
	struct walb_dev *wdev, struct list_head *wpack_list)
{
	struct iocore_data *iocored;
	struct pack *wpack;
	struct blk_plug plug;
	ASSERT(wpack_list);
	ASSERT(wdev);
	iocored = get_iocored_from_wdev(wdev);
	ASSERT(iocored);

	blk_start_plug(&plug);
	list_for_each_entry(wpack, wpack_list, list) {
		struct walb_logpack_header *logh;
		const bool is_flush = pack_header_should_flush(wpack);

		ASSERT_SECTOR_DATA(wpack->logpack_header_sector);
		logh = get_logpack_header(wpack->logpack_header_sector);

		if (wpack->is_zero_flush_only) {
			ASSERT(logh->n_records == 0);
			if (!is_flush) {
				/* do nothing because only the first wpack should submit flush request. */
				continue;
			}
#if 0
			WLOGi(wdev, "zero_flush lsid %" PRIu64 " %" PRIu64 "\n", wpack->new_permanent_lsid, logh->logpack_lsid);
#endif
			logpack_submit_flush(wdev->ldev, &wpack->bioe_list);
		} else {
			ASSERT(logh->n_records > 0);
			logpack_calc_checksum(logh, wdev->physical_bs,
					wdev->log_checksum_salt, &wpack->biow_list);
#if 0
			if (is_flush)
				WLOGi(wdev, "header_flush lsid %" PRIu64 " %" PRIu64 "\n", wpack->new_permanent_lsid, logh->logpack_lsid);
#endif
			submit_logpack(
				logh, &wpack->biow_list, &wpack->bioe_list,
				wdev->physical_bs, is_flush,
				wdev->ldev, wdev->ring_buffer_off,
				wdev->ring_buffer_size, wdev->ldev_chunk_sectors);
		}

	}
	blk_finish_plug(&plug);
}

/**
 * Set checksum of each bio and calc/set log header checksum.
 *
 * @logh log pack header.
 * @pbs physical sector size (allocated size as logh).
 * @biow_list list of biow.
 *   checksum of each bio has already been calculated as biow->csum.
 */
static void logpack_calc_checksum(
	struct walb_logpack_header *logh,
	unsigned int pbs, u32 salt, struct list_head *biow_list)
{
	int i;
	struct bio_wrapper *biow;
	int n_padding;

	ASSERT(logh);
	ASSERT(logh->n_records > 0);
	ASSERT(logh->n_records > logh->n_padding);

	n_padding = 0;
	i = 0;
	list_for_each_entry(biow, biow_list, list) {
		if (test_bit_u32(LOG_RECORD_PADDING, &logh->record[i].flags)) {
			n_padding++;
			i++;
			/* The corresponding record of the biow must be the next. */
			ASSERT(i < logh->n_records);
		}

		ASSERT(biow);
		ASSERT(biow->copied_bio);
		ASSERT(biow->copied_bio->bi_rw & REQ_WRITE);

		if (biow->len == 0) {
			ASSERT(biow->copied_bio->bi_rw & REQ_FLUSH);
			continue;
		}

		biow->csum = bio_calc_checksum(
			biow->copied_bio,
			((struct walb_dev *)biow->private_data)->log_checksum_salt);
		logh->record[i].checksum = biow->csum;
		i++;
	}
	if (i < logh->n_records) {
		/* This is the last padding record. */
		ASSERT(test_bit_u32(LOG_RECORD_PADDING, &logh->record[i].flags));
		n_padding++;
		i++;
	}
	ASSERT(n_padding <= 1);
	ASSERT(n_padding == logh->n_padding);
	ASSERT(i == logh->n_records);
	ASSERT(logh->checksum == 0);
	logh->checksum = checksum((u8 *)logh, pbs, salt);
	ASSERT(checksum((u8 *)logh, pbs, salt) == 0);
}

/**
 * Submit logpack entry.
 *
 * @logh logpack header.
 * @biow_list bio wrapper list. must not be empty.
 * @bioe_list bio entry list. must be empty.
 *   submitted bios for logpack header will be added to the list.
 * @pbs physical block size.
 * @is_flush true if the logpack header's REQ_FLUSH flag must be on.
 * @ldev log block device.
 * @ring_buffer_off ring buffer offset.
 * @ring_buffer_size ring buffer size.
 * @chunk_sectors chunk_sectors for bio alignment.
 *
 * CONTEXT:
 *     Non-IRQ. Non-atomic.
 */
static void submit_logpack(
	struct walb_logpack_header *logh,
	struct list_head *biow_list, struct list_head *bioe_list,
	unsigned int pbs, bool is_flush, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size,
	unsigned int chunk_sectors)
{
	struct bio_wrapper *biow;
	int i;

	ASSERT(list_empty(bioe_list));
	ASSERT(!list_empty(biow_list));

	/* Submit logpack header block. */
	logpack_submit_header(
		logh, bioe_list, pbs, is_flush, ldev,
		ring_buffer_off, ring_buffer_size,
		chunk_sectors);
	ASSERT(!list_empty(bioe_list));

	/* Submit logpack contents for each request. */
	i = 0;
	list_for_each_entry(biow, biow_list, list) {
		struct walb_log_record *rec = &logh->record[i];
		if (test_bit_u32(LOG_RECORD_PADDING, &rec->flags)) {
			i++;
			rec = &logh->record[i];
			/* The biow must be for the next record. */
		}
#ifdef WALB_PERFORMANCE_ANALYSIS
		getnstimeofday(&biow->ts[WALB_TIME_LOG_SUBMITTED]);
#endif
		if (test_bit_u32(LOG_RECORD_DISCARD, &rec->flags)) {
			/* No need to execute IO to the log device. */
			ASSERT(bio_wrapper_state_is_discard(biow));
			ASSERT(biow->copied_bio->bi_rw & REQ_DISCARD);
			ASSERT(biow->len > 0);
		} else if (biow->len == 0) {
			/* Zero-sized IO will not be stored in logpack header.
			   We just submit it and will wait for it. */

			/* such bio must be flush. */
			ASSERT(biow->copied_bio->bi_rw & REQ_FLUSH);
			/* such bio must be permitted at first only. */
			ASSERT(i == 0);

#if 0 /* No need to submit. */
			logpack_submit_bio_wrapper_zero(
				biow, &biow->bioe_list, pbs, ldev);
#endif
		} else {
			/* Normal IO. */
			ASSERT(i < logh->n_records);

			/* submit bio(s) for the biow. */
			logpack_submit_bio_wrapper(
				biow, rec->lsid, &biow->bioe_list,
				pbs, ldev, ring_buffer_off, ring_buffer_size,
				chunk_sectors);
		}
		i++;
	}
}

/**
 * Submit bio of header block.
 *
 * @lhead logpack header data.
 * @bioe_list must be empty.
 *     submitted lhead bio(s) will be added to this.
 * @pbs physical block size [bytes].
 * @is_flush if true, REQ_FLUSH must be added.
 * @ldev log device.
 * @ring_buffer_off ring buffer offset [physical blocks].
 * @ring_buffer_size ring buffer size [physical blocks].
 */
static void logpack_submit_header(
	struct walb_logpack_header *lhead,
	struct list_head *bioe_list,
	unsigned int pbs, bool is_flush, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size,
	unsigned int chunk_sectors)
{
	struct bio *bio;
	struct bio_entry *bioe;
	struct page *page;
	u64 off_pb, off_lb;
	int len;
#ifdef WALB_DEBUG
	struct page *page2;
#endif

retry_bio_entry:
	bioe = alloc_bio_entry(GFP_NOIO);
	if (!bioe) { schedule(); goto retry_bio_entry; }
retry_bio:
	bio = bio_alloc(GFP_NOIO, 1);
	if (!bio) { schedule(); goto retry_bio; }

	page = virt_to_page(lhead);
#ifdef WALB_DEBUG
	page2 = virt_to_page((unsigned long)lhead + pbs - 1);
	ASSERT(page == page2);
#endif
	bio->bi_bdev = ldev;
	off_pb = lhead->logpack_lsid % ring_buffer_size + ring_buffer_off;
	off_lb = addr_lb(pbs, off_pb);
	bio->bi_sector = off_lb;
	bio->bi_rw = (is_flush ? WRITE_FLUSH : WRITE);
	bio->bi_end_io = bio_entry_end_io;
	bio->bi_private = bioe;
	len = bio_add_page(bio, page, pbs, offset_in_page(lhead));
	ASSERT(len == pbs);

	init_bio_entry(bioe, bio);
	ASSERT(bioe->len * LOGICAL_BLOCK_SIZE == pbs);

	ASSERT(bioe_list);
	ASSERT(list_empty(bioe_list));
	list_add_tail(&bioe->list, bioe_list);

#ifdef WALB_DEBUG
	if (should_split_bio_entry_list_for_chunk(bioe_list, chunk_sectors)) {
		LOGw("logpack header bio should be splitted.\n");
	}
#endif
	submit_bio_entry_list(bioe_list);

	return;
#if 0
error2:
	bio_put(bio);
	bioe->bio = NULL;
error1:
	destroy_bio_entry(bioe);
error0:
	return;
#endif
}

/**
 * Submit a logpack bio for a flush request.
 *
 * @biow bio wrapper(which contains original bio).
 *       The bio->size must be 0.
 * @bioe_list list of bio_entry. must be empty.
 *   successfully submitted bioe(s) must be added to the tail of this.
 * @pbs physical block size [bytes]
 * @ldev log device.
 */
static void logpack_submit_bio_wrapper_zero(
	struct bio_wrapper *biow, struct list_head *bioe_list,
	unsigned int pbs, struct block_device *ldev)
{
	struct bio_entry *bioe, *bioe_next;

	ASSERT(biow->len == 0);
	ASSERT(biow->copied_bio);
	ASSERT(biow->copied_bio->bi_size == 0);
	ASSERT(list_empty(bioe_list));

retry_bio_entry:
	bioe = logpack_create_bio_entry(biow->copied_bio, pbs, ldev, 0, 0);
	if (!bioe) {
		schedule();
		goto retry_bio_entry;
	}
	list_add_tail(&bioe->list, bioe_list);

	/* really submit. */
	list_for_each_entry_safe(bioe, bioe_next, bioe_list, list) {
		LOG_("submit_lr: bioe %p pos %"PRIu64" len %u\n",
			bioe, (u64)bioe->pos, bioe->len);
		generic_make_request(bioe->bio);
	}
}

/**
 * Submit all logpack bio(s) for a request.
 *
 * @biow bio wrapper(which contains original bio).
 * @lsid lsid of the bio in the logpack.
 * @bioe_list list of bio_entry. must be empty.
 *   successfully submitted bioe(s) must be added to the tail of this.
 * @pbs physical block size [bytes]
 * @ldev log device.
 * @ring_buffer_off ring buffer offset [physical block].
 * @ring_buffer_size ring buffer size [physical block].
 */
static void logpack_submit_bio_wrapper(
	struct bio_wrapper *biow, u64 lsid,
	struct list_head *bioe_list,
	unsigned int pbs, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size,
	unsigned int chunk_sectors)
{
	unsigned int off_lb;
	struct bio_entry *bioe, *bioe_next;
	u64 ldev_off_pb = lsid % ring_buffer_size + ring_buffer_off;
	struct list_head tmp_list;

	ASSERT(list_empty(bioe_list));
	INIT_LIST_HEAD(&tmp_list);
	off_lb = 0;
	ASSERT(biow);
	ASSERT(biow->copied_bio);
	ASSERT(!bio_wrapper_state_is_discard(biow));
	ASSERT((biow->copied_bio->bi_rw & REQ_DISCARD) == 0);

retry_bio_entry:
	bioe = logpack_create_bio_entry(
		biow->copied_bio, pbs, ldev, ldev_off_pb, off_lb);
	if (!bioe) {
		schedule();
		goto retry_bio_entry;
	}
	off_lb += bioe->len;
	list_add_tail(&bioe->list, &tmp_list);

	/* split if required. */
retry_bio_split:
	if (!split_bio_entry_list_for_chunk(
			&tmp_list, chunk_sectors, GFP_NOIO)) {
		schedule();
		goto retry_bio_split;
	}

	/* move all bioe to the bioe_list. */
#if 0
	*bioe_list = tmp_list;
	INIT_LIST_HEAD(&tmp_list);
#else
	list_for_each_entry_safe(bioe, bioe_next, &tmp_list, list) {
		list_move_tail(&bioe->list, bioe_list);
	}
	ASSERT(list_empty(&tmp_list));
#endif

	/* really submit. */
	list_for_each_entry_safe(bioe, bioe_next, bioe_list, list) {
		LOG_("submit_lr: bioe %p pos %"PRIu64" len %u\n",
			bioe, (u64)bioe->pos, bioe->len);
		generic_make_request(bioe->bio);
	}
}

/**
 * Create a bio_entry which is a part of logpack.
 *
 * @bio original bio to clone.
 * @pbs physical block device [bytes].
 * @ldev_offset log device offset for the request [physical block].
 * @bio_offset offset of the bio inside the whole request [logical block].
 *
 * RETURN:
 *   bio_entry in success which bio is submitted, or NULL.
 */
static struct bio_entry* logpack_create_bio_entry(
	struct bio *bio, unsigned int pbs,
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

	/* REQ_FLUSH and REQ_FUA must be off while they are processed in other ways. */
	{
		const unsigned long mask = REQ_FLUSH | REQ_FUA;
		cbio->bi_rw &= ~mask;
	}
	return bioe;

error1:
	destroy_bio_entry(bioe);
error0:
	return NULL;
}

/**
 * Submit flush for logpack.
 */
static void logpack_submit_flush(struct block_device *ldev, struct list_head *bioe_list)
{
	struct bio_entry *bioe;
	ASSERT(ldev);
	ASSERT(bioe_list);

	if (!supports_flush_request_bdev(ldev))
		return;

retry:
	bioe = submit_flush(ldev);
	if (!bioe) {
		schedule();
		goto retry;
	}
	list_add_tail(&bioe->list, bioe_list);
}

/**
 * Gc logpack list.
 */
static void gc_logpack_list(struct walb_dev *wdev, struct list_head *wpack_list)
{
	struct pack *wpack, *wpack_next;
	struct bio_wrapper *biow, *biow_next;
	u64 written_lsid = INVALID_LSID;

	ASSERT(!list_empty(wpack_list));

	list_for_each_entry_safe(wpack, wpack_next, wpack_list, list) {
		list_del(&wpack->list);
		list_for_each_entry_safe(biow, biow_next, &wpack->biow_list, list) {
			const unsigned long timeo = msecs_to_jiffies(completion_timeo_ms_);
			unsigned long rtimeo;
			int c = 0;

			list_del(&biow->list);
			ASSERT(bio_wrapper_state_is_prepared(biow));
		retry:
			rtimeo = wait_for_completion_timeout(&biow->done, timeo);
			if (rtimeo == 0) {
				char buf[32];
				snprintf(buf, sizeof(buf), "%u: timeout(%d): ", wdev_minor(wdev), c);
				print_bio_wrapper_short(KERN_NOTICE, biow, buf);
				c++;
				goto retry;
			}
			if (!test_bit(WALB_STATE_READ_ONLY, &wdev->flags)) {
				ASSERT(bio_wrapper_state_is_submitted(biow));
				ASSERT(bio_wrapper_state_is_completed(biow));
			}
			if (biow->error &&
				!test_and_set_bit(WALB_STATE_READ_ONLY, &wdev->flags))
				WLOGe(wdev, "data IO error. to be read-only mode.\n");
#ifdef WALB_PERFORMANCE_ANALYSIS
			getnstimeofday(&biow->ts[WALB_TIME_END]);
#if 0
			print_bio_wrapper_performance(KERN_NOTICE, biow);
#endif
#endif
			destroy_bio_wrapper_dec(wdev, biow);
		}
		ASSERT(list_empty(&wpack->biow_list));
		ASSERT(list_empty(&wpack->bioe_list));

		written_lsid = get_next_lsid_unsafe(
			get_logpack_header(wpack->logpack_header_sector));

		destroy_pack(wpack);
	}
	ASSERT(list_empty(wpack_list));

	/* Update written_lsid. */
	ASSERT(written_lsid != INVALID_LSID);
	spin_lock(&wdev->lsid_lock);
	wdev->lsids.written = written_lsid;
	spin_unlock(&wdev->lsid_lock);
}

/**
 * Get logpack(s) from the gc queue and execute gc for them.
 */
static void dequeue_and_gc_logpack_list(struct walb_dev *wdev)
{
	struct pack *wpack, *wpack_next;
	struct list_head wpack_list;
	struct iocore_data *iocored;

	ASSERT(wdev);
	iocored = get_iocored_from_wdev(wdev);
	ASSERT(iocored);

	INIT_LIST_HEAD(&wpack_list);
	while (true) {
		bool is_empty;
		int n_pack = 0;
		/* Dequeue logpack list */
		spin_lock(&iocored->logpack_gc_queue_lock);
		is_empty = list_empty(&iocored->logpack_gc_queue);
		list_for_each_entry_safe(wpack, wpack_next,
					&iocored->logpack_gc_queue, list) {
			list_move_tail(&wpack->list, &wpack_list);
			n_pack++;
			if (n_pack >= wdev->n_pack_bulk) { break; }
		}
		spin_unlock(&iocored->logpack_gc_queue_lock);
		if (is_empty) { break; }

		/* Gc */
		gc_logpack_list(wdev, &wpack_list);
		ASSERT(list_empty(&wpack_list));
		atomic_sub(n_pack, &iocored->n_pending_gc);
	}
}

/**
 * Check whether pack is valid.
 *   Assume just created and filled. checksum is not calculated at all.
 *
 * RETURN:
 *   true if valid, or false.
 */
static bool is_prepared_pack_valid(struct pack *pack)
{
	struct walb_logpack_header *lhead;
	unsigned int pbs;
	unsigned int i;
	struct bio_wrapper *biow;
	u64 total_pb; /* total io size in physical block. */
	unsigned int n_padding = 0;
	struct walb_log_record *lrec;

	LOG_("is_prepared_pack_valid begin.\n");

	CHECKd(pack);
	CHECKd(pack->logpack_header_sector);

	lhead = get_logpack_header(pack->logpack_header_sector);
	pbs = pack->logpack_header_sector->size;
	ASSERT_PBS(pbs);
	CHECKd(lhead);
	CHECKd(is_valid_logpack_header(lhead));

	CHECKd(!list_empty(&pack->biow_list));

	i = 0;
	total_pb = 0;
	list_for_each_entry(biow, &pack->biow_list, list) {
		CHECKd(biow->bio);
		if (biow->len == 0) {
			CHECKd(biow->bio->bi_rw & REQ_FLUSH);
			CHECKd(i == 0);
			CHECKd(lhead->n_records == 0);
			CHECKd(lhead->total_io_size == 0);
			/* The loop must be done. */
			continue;
		}

		CHECKd(i < lhead->n_records);
		lrec = &lhead->record[i];
		CHECKd(test_bit_u32(LOG_RECORD_EXIST, &lrec->flags));

		if (test_bit_u32(LOG_RECORD_PADDING, &lrec->flags)) {
			LOG_("padding found.\n");
			total_pb += capacity_pb(pbs, lrec->io_size);
			n_padding++;
			i++;
			/* The corresponding record of the biow must be the next. */
			CHECKd(i < lhead->n_records);
			lrec = &lhead->record[i];
			CHECKd(test_bit_u32(LOG_RECORD_EXIST, &lrec->flags));
		}

		/* Normal record. */
		CHECKd(biow->bio);
		CHECKd(biow->bio->bi_rw & REQ_WRITE);
		CHECKd(biow->pos == (sector_t)lrec->offset);
		CHECKd(lhead->logpack_lsid == lrec->lsid - lrec->lsid_local);
		CHECKd(biow->len == lrec->io_size);
		if (test_bit_u32(LOG_RECORD_DISCARD, &lrec->flags)) {
			CHECKd(bio_wrapper_state_is_discard(biow));
		} else {
			CHECKd(!bio_wrapper_state_is_discard(biow));
			total_pb += capacity_pb(pbs, lrec->io_size);
		}
		i++;
	}
	if (i < lhead->n_records) {
		/* It must be the last padding record. */
		lrec = &lhead->record[i];
		CHECKd(test_bit_u32(LOG_RECORD_EXIST, &lrec->flags));
		CHECKd(test_bit_u32(LOG_RECORD_PADDING, &lrec->flags));
		total_pb += capacity_pb(pbs, lrec->io_size);
		n_padding++;
		i++;
	}
	CHECKd(i == lhead->n_records);
	CHECKd(total_pb == lhead->total_io_size);
	CHECKd(n_padding <= 1);
	CHECKd(n_padding == lhead->n_padding);
	if (lhead->n_records == 0) {
		CHECKd(pack->is_zero_flush_only);
	}
	LOG_("valid.\n");
	return true;
error:
	LOG_("not valid.\n");
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
static bool is_pack_list_valid(struct list_head *pack_list)
{
	struct pack *pack;

	list_for_each_entry(pack, pack_list, list) {
		CHECKd(is_prepared_pack_valid(pack));
	}
	return true;
error:
	return false;
}

/**
 * Create iocore data.
 * GC worker will not be started inside this function.
 */
static struct iocore_data* create_iocore_data(gfp_t gfp_mask)
{
	struct iocore_data *iocored;

	iocored = kmalloc(sizeof(struct iocore_data), gfp_mask);
	if (!iocored) {
		LOGe("memory allocation failure.\n");
		goto error0;
	}

	/* Flags. */
	iocored->flags = 0;

	/* Queues and their locks. */
	spin_lock_init(&iocored->logpack_submit_queue_lock);
	iocored->is_frozen_sys = false;
	iocored->is_frozen_usr = false;
	INIT_LIST_HEAD(&iocored->frozen_queue);
	INIT_LIST_HEAD(&iocored->logpack_submit_queue);
	spin_lock_init(&iocored->logpack_wait_queue_lock);
	INIT_LIST_HEAD(&iocored->logpack_wait_queue);
	spin_lock_init(&iocored->datapack_submit_queue_lock);
	INIT_LIST_HEAD(&iocored->datapack_submit_queue);
	spin_lock_init(&iocored->datapack_wait_queue_lock);
	INIT_LIST_HEAD(&iocored->datapack_wait_queue);
	spin_lock_init(&iocored->logpack_gc_queue_lock);
	INIT_LIST_HEAD(&iocored->logpack_gc_queue);

	/* To wait all IO for underlying devices done. */
	atomic_set(&iocored->n_started_write_bio, 0);
	atomic_set(&iocored->n_pending_bio, 0);
	atomic_set(&iocored->n_pending_gc, 0);

	/* Log flush time. */
	iocored->log_flush_jiffies = jiffies;

#ifdef WALB_OVERLAPPED_SERIALIZE
	spin_lock_init(&iocored->overlapped_data_lock);
	iocored->overlapped_data = multimap_create(gfp_mask, &mmgr_);
	if (!iocored->overlapped_data) {
		LOGe("overlapped_data allocation failure.\n");
		goto error1;
	}
	iocored->max_sectors_in_overlapped = 0;
#ifdef WALB_DEBUG
	iocored->overlapped_in_id = 0;
	iocored->overlapped_out_id = 0;
#endif
#endif

	spin_lock_init(&iocored->pending_data_lock);
	iocored->pending_data = multimap_create(gfp_mask, &mmgr_);
	if (!iocored->pending_data) {
		LOGe("pending_data allocation failure.\n");
		goto error2;
	}
	iocored->pending_sectors = 0;
	iocored->queue_restart_jiffies = jiffies;
	iocored->max_sectors_in_pending = 0;

#ifdef WALB_DEBUG
	atomic_set(&iocored->n_flush_io, 0);
	atomic_set(&iocored->n_flush_logpack, 0);
	atomic_set(&iocored->n_flush_force, 0);

	atomic_set(&iocored->n_io_acct, 0);
#endif

	return iocored;

error2:
	multimap_destroy(iocored->pending_data);

#ifdef WALB_OVERLAPPED_SERIALIZE
error1:
	multimap_destroy(iocored->overlapped_data);
#endif
	kfree(iocored);
error0:
	return NULL;
}

/**
 * Destroy iocore data.
 */
static void destroy_iocore_data(struct iocore_data *iocored)
{
	ASSERT(iocored);

	multimap_destroy(iocored->pending_data);
#ifdef WALB_OVERLAPPED_SERIALIZE
	multimap_destroy(iocored->overlapped_data);
#endif
	kfree(iocored);
}

/**
 * The lock iocored->logpack_submit_queue_lock must be held.
 */
static void make_frozen_queue_empty(struct iocore_data *iocored)
{
	struct list_head *fq = &iocored->frozen_queue;
	struct list_head *sq = &iocored->logpack_submit_queue;
	struct bio_wrapper *biow, *next;

	if (list_empty(fq))
		return;

	list_for_each_entry_safe(biow, next, fq, list) {
		list_move_tail(&biow->list, sq);
	}
	ASSERT(list_empty(fq));
}

/**
 * Push a bio wrapper into the logpack submit queue if not stopped,
 * else use frozen queue.
 *
 * RETURN:
 *   true if the logpack submit queue is not empty.
 */
static bool push_into_lpack_submit_queue(struct bio_wrapper *biow)
{
	struct walb_dev *wdev = biow->private_data;
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);
	bool ret;

	spin_lock(&iocored->logpack_submit_queue_lock);
	if (is_frozen(iocored)) {
		list_add_tail(&biow->list, &iocored->frozen_queue);
		ret = !list_empty(&iocored->logpack_submit_queue);
	} else {
		make_frozen_queue_empty(iocored);
		list_add_tail(&biow->list, &iocored->logpack_submit_queue);
		ret = true;
	}
	spin_unlock(&iocored->logpack_submit_queue_lock);

	return ret;
}

static void update_biow_lsid(struct walb_logpack_header *logh, struct bio_wrapper *biow)
{
	struct walb_log_record *rec;

	if (logh->n_records == 0)
		return;

	rec = &logh->record[logh->n_records - 1];
	ASSERT(rec->offset == biow->pos);
	ASSERT(rec->io_size == biow->len);
	biow->lsid = rec->lsid;
}

/**
 * Add a bio_wrapper to a writepack.
 *
 * @wpack_list wpack list.
 * @wpackp pointer to a wpack pointer. *wpackp can be NULL.
 * @biow bio_wrapper to add.
 * @ring_buffer_size ring buffer size [physical block]
 * @latest_lsidp pointer to the latest_lsid value.
 *   *latest_lsidp must be always (*wpackp)->logpack_lsid.
 * @wdev wrapper block device.
 * @gfp_mask memory allocation mask.
 *
 * RETURN:
 *   true if successfuly added, or false (due to memory allocation failure).
 * CONTEXT:
 *   serialized.
 */
static bool writepack_add_bio_wrapper(
	struct list_head *wpack_list, struct pack **wpackp,
	struct bio_wrapper *biow,
	u64 ring_buffer_size, unsigned int max_logpack_pb,
	u64 *latest_lsidp, struct walb_dev *wdev, gfp_t gfp_mask, bool *is_flushp)
{
	struct pack *pack;
	bool ret;
	unsigned int pbs;
	struct walb_logpack_header *lhead = NULL;
	struct bio *bio;

	LOG_("begin\n");

	ASSERT(wpack_list);
	ASSERT(wpackp);
	ASSERT(biow);
	ASSERT(biow->copied_bio);
	ASSERT(biow->copied_bio->bi_rw & REQ_WRITE);
	ASSERT(wdev);
	pbs = wdev->physical_bs;
	ASSERT_PBS(pbs);

	bio = biow->copied_bio;
	pack = *wpackp;

	if (!pack) {
		goto newpack;
	}

	ASSERT(pack);
	ASSERT(pack->logpack_header_sector);
	ASSERT(pbs == pack->logpack_header_sector->size);
	lhead = get_logpack_header(pack->logpack_header_sector);
	ASSERT(*latest_lsidp == lhead->logpack_lsid);

	if (is_zero_flush_only(pack)) {
		goto newpack;
	}
	if (lhead->n_records > 0 &&
		((bio->bi_rw & REQ_FLUSH)
			|| is_pack_size_too_large(lhead, pbs, max_logpack_pb, biow))) {
		/* Flush request must be the first of the pack. */
		goto newpack;
	}
	if (!walb_logpack_header_add_bio(lhead, bio, pbs, ring_buffer_size)) {
		/* logpack header capacity full so create a new pack. */
		goto newpack;
	}
	update_biow_lsid(lhead, biow);
	goto fin;

newpack:
	if (lhead) {
		writepack_check_and_set_zeroflush(pack, is_flushp);
		ASSERT(is_prepared_pack_valid(pack));
		list_add_tail(&pack->list, wpack_list);
		*latest_lsidp = get_next_lsid_unsafe(lhead);
	}
	pack = create_writepack(gfp_mask, pbs, *latest_lsidp);
	if (!pack) { goto error0; }
	*wpackp = pack;
	lhead = get_logpack_header(pack->logpack_header_sector);
	ret = walb_logpack_header_add_bio(lhead, bio, pbs, ring_buffer_size);
	ASSERT(ret);
	update_biow_lsid(lhead, biow);

fin:
	/* The request is just added to the pack. */
	list_add_tail(&biow->list, &pack->biow_list);
	if (bio->bi_rw & REQ_FLUSH && !(bio->bi_rw & REQ_FUA)) {
#if 0
		if (biow->len == 0)
			WLOGi(wdev, "REQ_FLUSH_ZERO %" PRIu64 "\n", *latest_lsidp);
		else
			WLOGi(wdev, "REQ_FLUSH %" PRIu64 " %" PRIu64 "\n", *latest_lsidp, biow->lsid);
#endif
		*is_flushp = true;

		/* debug */
		if (bio_wrapper_state_is_discard(biow)) {
			WLOGw(wdev, "The bio has both REQ_FLUSH and REQ_DISCARD.\n");
		}
	}
	if (bio->bi_rw & REQ_FUA) {
		WARN_ON(biow->len == 0);
		pack->is_fua_contained = true;
#if 0
		WLOGi(wdev, "REQ_FUA(%d) %" PRIu64 " %" PRIu64 " (sector %" PRIu64 " len %u)\n"
			, (bio->bi_rw & REQ_FLUSH) != 0, *latest_lsidp, biow->lsid
			, (u64)biow->pos, biow->len);
#endif
	}
	LOG_("normal end\n");
	return true;
error0:
	LOG_("failure end\n");
	return false;
}

/**
 * Insert a bio wrapper to a sorted bio wrapper list.
 * using insertion sort.
 *
 * They are sorted by biow->pos.
 * Use biow->list4 for list operations.
 *
 * Sort cost is O(n^2) in a worst case,
 * while the cost is O(1) in sequential write.
 *
 * @biow (struct bio_wrapper *)
 * @biow_list (struct list_head *)
 */
static void insert_to_sorted_bio_wrapper_list_by_pos(
	struct bio_wrapper *biow, struct list_head *biow_list)
{
	struct bio_wrapper *biow_tmp, *biow_next;
	bool moved;
#ifdef WALB_DEBUG
	sector_t pos;
#endif

	ASSERT(biow);
	ASSERT(biow_list);

	if (!list_empty(biow_list)) {
		/* last entry. */
		biow_tmp = list_entry(biow_list->prev, struct bio_wrapper, list4);
		ASSERT(biow_tmp);
		if (biow->pos > biow_tmp->pos) {
			list_add_tail(&biow->list4, biow_list);
			return;
		}
	}
	moved = false;
	list_for_each_entry_safe_reverse(biow_tmp, biow_next, biow_list, list4) {
		if (biow->pos > biow_tmp->pos) {
			list_add(&biow->list4, &biow_tmp->list4);
			moved = true;
			break;
		}
	}
	if (!moved) {
		list_add(&biow->list4, biow_list);
	}

#ifdef WALB_DEBUG
	pos = 0;
	list_for_each_entry_safe(biow_tmp, biow_next, biow_list, list4) {
		LOG_("%" PRIu64 "\n", (u64)biow_tmp->pos);
		ASSERT(pos <= biow_tmp->pos);
		pos = biow_tmp->pos;
	}
#endif
}

/**
 * Check whether wpack is zero-flush-only and set the flag.
 */
static void writepack_check_and_set_zeroflush(struct pack *wpack, bool *is_flushp)
{
	struct walb_logpack_header *logh =
		get_logpack_header(wpack->logpack_header_sector);

	/* Check whether zero-flush-only or not. */
	if (logh->n_records == 0) {
		ASSERT(is_zero_flush_only(wpack));
		wpack->is_zero_flush_only = true;
		*is_flushp = true;
	}
}

/**
 * Wait for completion of all bio(s) and enqueue datapack tasks.
 *
 * Request success -> enqueue datapack.
 * Request failure -> all subsequent requests must fail.
 *
 * If any write failed, wdev will be read-only mode.
 */
static void wait_for_logpack_and_submit_datapack(
	struct walb_dev *wdev, struct pack *wpack)
{
	int bio_error;
	struct bio_wrapper *biow, *biow_next;
	bool is_failed = false;
	struct iocore_data *iocored;
	int reti;
	bool is_pending_insert_succeeded;
	bool is_stop_queue = false;

	ASSERT(wpack);
	ASSERT(wdev);

	/* Check read only mode. */
	iocored = get_iocored_from_wdev(wdev);
	if (test_bit(WALB_STATE_READ_ONLY, &wdev->flags))
		is_failed = true;

	/* Wait for logpack header bio or zero_flush pack bio. */
	bio_error = wait_for_bio_entry_list(&wpack->bioe_list, wdev);
	if (bio_error)
		is_failed = true;

	/* Update permanent_lsid if necessary. */
	if (!is_failed && pack_header_should_flush(wpack)) {
		bool should_notice = false;
		bool updated = false;
		ASSERT(wpack->new_permanent_lsid != INVALID_LSID);
		spin_lock(&wdev->lsid_lock);
		if (wdev->lsids.permanent < wpack->new_permanent_lsid) {
			should_notice = is_permanent_log_empty(&wdev->lsids);
			wdev->lsids.permanent = wpack->new_permanent_lsid;
			updated = true;
			LOG_("log_flush_completed_header\n");
		}
		spin_unlock(&wdev->lsid_lock);
#if 0
		if (updated)
			WLOGi(wdev, "permanent_lsid_header %" PRIu64 "\n", wpack->new_permanent_lsid);
#endif
		if (should_notice)
			walb_sysfs_notify(wdev, "lsids");
	}

	list_for_each_entry_safe(biow, biow_next, &wpack->biow_list, list) {
		ASSERT(biow->bio);
		ASSERT(biow->copied_bio);
		bio_error = wait_for_bio_entry_list(&biow->bioe_list, wdev);
		if (is_failed || bio_error)
			goto error_io;

#ifdef WALB_PERFORMANCE_ANALYSIS
		getnstimeofday(&biow->ts[WALB_TIME_LOG_COMPLETED]);
#endif
		if (biow->len == 0) {
			/* Zero-flush. */
			ASSERT(wpack->is_zero_flush_only);
			ASSERT(biow->copied_bio->bi_rw & REQ_FLUSH);
			list_del(&biow->list);
			io_acct_end(biow);
			set_bit(BIO_UPTODATE, &biow->bio->bi_flags);
			bio_endio(biow->bio, 0);
			destroy_bio_wrapper_dec(wdev, biow);
		} else {
			if (!bio_wrapper_state_is_discard(biow) ||
				blk_queue_discard(bdev_get_queue(wdev->ddev))) {
				/* Create all related bio(s) by copying IO data. */
				struct bio_entry *bioe;
			retry_create:
				ASSERT(list_empty(&biow->bioe_list));
				bioe = create_bio_entry_by_clone(
					biow->copied_bio, wdev->ddev, GFP_NOIO);
				if (!bioe) {
					schedule();
					goto retry_create;
				}
				list_add_tail(&bioe->list, &biow->bioe_list);
			}

			if (!bio_wrapper_state_is_discard(biow)) {
				/* Split if required due to chunk limitations. */
			retry_split:
				if (!split_bio_entry_list_for_chunk(
						&biow->bioe_list,
						wdev->ddev_chunk_sectors,
						GFP_NOIO)) {
					schedule();
					goto retry_split;
				}
			}

			/* get related bio(s) to keep them
			   until deleting from pending data. */
			get_bio_entry_list(&biow->bioe_list);

			/* Try to insert pending data. */
		retry_insert_pending:
			spin_lock(&iocored->pending_data_lock);
			LOG_("pending_sectors %u\n", iocored->pending_sectors);
			is_stop_queue = should_stop_queue(wdev, biow);
			if (bio_wrapper_state_is_discard(biow)) {
				/* Discard IO does not have buffer of biow->len bytes.
				   We consider its metadata only. */
				iocored->pending_sectors++;
				is_pending_insert_succeeded = true;
			} else {
				iocored->pending_sectors += biow->len;
				is_pending_insert_succeeded =
					pending_insert_and_delete_fully_overwritten(
						iocored->pending_data,
						&iocored->max_sectors_in_pending,
						biow, GFP_ATOMIC);
			}
			spin_unlock(&iocored->pending_data_lock);
			if (!is_pending_insert_succeeded) {
				spin_lock(&iocored->pending_data_lock);
				if (bio_wrapper_state_is_discard(biow)) {
					iocored->pending_sectors--;
				} else {
					iocored->pending_sectors -= biow->len;
				}
				spin_unlock(&iocored->pending_data_lock);
				schedule();
				goto retry_insert_pending;
			}

			/* Check pending data size and stop the queue if needed. */
			if (is_stop_queue && !test_and_set_bit(IOCORE_STATE_IS_QUEUE_STOPPED, &iocored->flags))
				freeze_detail(iocored, false);

			/* We must flush here for REQ_FUA request before calling bio_endio().
			   because WalB must flush all the previous logpacks and
			   the logpack header and all the previous IOs and itself in the same logpack
			   in order to make the IO be permanent in the log device. */
			if (biow->copied_bio->bi_rw & REQ_FUA) {
				u32 pb;
#if 0
				WLOGi(wdev, "force_flush_fua %" PRIu64 "\n", biow->lsid);
#endif
				if (bio_wrapper_state_is_discard(biow))
					pb = 0;
				else
					pb = capacity_pb(wdev->physical_bs, biow->len);
				spin_lock(&wdev->lsid_lock);
				wdev->lsids.completed = biow->lsid + pb;
				spin_unlock(&wdev->lsid_lock);
				force_flush_ldev(wdev);
			}


			/* call endio here in fast algorithm,
			   while easy algorithm call it after data device IO. */
			io_acct_end(biow);
			set_bit(BIO_UPTODATE, &biow->bio->bi_flags);
			bio_endio(biow->bio, 0);
			biow->bio = NULL;

			reti = test_and_set_bit(BIO_WRAPPER_PREPARED, &biow->flags);
			ASSERT(reti == 0);
#ifdef WALB_DEBUG
			atomic_inc(&biow->state);
#endif

			/* Enqueue submit datapack task. */
			spin_lock(&iocored->datapack_submit_queue_lock);
			list_add_tail(&biow->list2, &iocored->datapack_submit_queue);
			spin_unlock(&iocored->datapack_submit_queue_lock);
		}
		continue;
	error_io:
		is_failed = true;
		if (!test_and_set_bit(WALB_STATE_READ_ONLY, &wdev->flags))
			WLOGe(wdev, "changed to read-only mode.\n");
		io_acct_end(biow);
		bio_endio(biow->bio, -EIO);
		list_del(&biow->list);
		destroy_bio_wrapper_dec(wdev, biow);
	}

	/* Update completed_lsid. */
	if (!is_failed) {
		struct walb_logpack_header *logh =
			get_logpack_header(wpack->logpack_header_sector);
		spin_lock(&wdev->lsid_lock);
		wdev->lsids.completed = get_next_lsid(logh);
		spin_unlock(&wdev->lsid_lock);
	}
}

/**
 * Wait for completion of datapack IO.
 */
static void wait_for_write_bio_wrapper(
	struct walb_dev *wdev, struct bio_wrapper *biow)
{
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);
	const bool is_endio = false;
	const bool is_delete = false;
	bool starts_queue;
#ifdef WALB_OVERLAPPED_SERIALIZE
	struct bio_wrapper *biow_tmp, *biow_tmp_next;
	unsigned int n_should_submit;
	struct list_head should_submit_list;
	unsigned int c = 0;
	struct blk_plug plug;
#endif
	int reti;

	ASSERT(bio_wrapper_state_is_prepared(biow));
	ASSERT(bio_wrapper_state_is_submitted(biow));
#ifdef WALB_OVERLAPPED_SERIALIZE
	ASSERT(biow->n_overlapped == 0);
#endif

	/* Wait for completion and call end_request. */
	wait_for_bio_wrapper(biow, is_endio, is_delete);
	reti = test_and_set_bit(BIO_WRAPPER_COMPLETED, &biow->flags);
	ASSERT(reti == 0);

#ifdef WALB_OVERLAPPED_SERIALIZE
	/* Delete from overlapped detection data. */
	INIT_LIST_HEAD(&should_submit_list);
	spin_lock(&iocored->overlapped_data_lock);
	n_should_submit = overlapped_delete_and_notify(
		iocored->overlapped_data,
		&iocored->max_sectors_in_overlapped,
		&should_submit_list, biow
#ifdef WALB_DEBUG
		, &iocored->overlapped_out_id
#endif
		);
	spin_unlock(&iocored->overlapped_data_lock);

	/* Submit bio wrapper(s) which n_overlapped became 0. */
	if (n_should_submit > 0) {
		blk_start_plug(&plug);
		list_for_each_entry_safe(biow_tmp, biow_tmp_next,
					&should_submit_list, list4) {
			const bool is_plug = false;
			ASSERT(biow_tmp->n_overlapped == 0);
			ASSERT(bio_wrapper_state_is_delayed(biow_tmp));
			ASSERT(biow_tmp != biow);
			list_del(&biow_tmp->list4);
			LOG_("submit overlapped biow %p pos %" PRIu64 " len %u\n",
				biow_tmp, (u64)biow_tmp->pos, biow_tmp->len);
			c++;
			submit_write_bio_wrapper(biow_tmp, is_plug);
		}
		blk_finish_plug(&plug);
	}
	ASSERT(c == n_should_submit);
	ASSERT(list_empty(&should_submit_list));
#endif

	/* Delete from pending data. */
	starts_queue = delete_bio_wrapper_from_pending_data(wdev, biow);
	if (starts_queue && test_bit(IOCORE_STATE_IS_QUEUE_STOPPED, &iocored->flags)) {
		if (melt_detail(iocored, false))
			dispatch_submit_log_task(wdev);
		clear_bit(IOCORE_STATE_IS_QUEUE_STOPPED, &iocored->flags);
	}

	/* Put related bio(s). */
	put_bio_entry_list(&biow->bioe_list);

	/* Free resources. */
	destroy_bio_entry_list(&biow->bioe_list);

	ASSERT(list_empty(&biow->bioe_list));
}

/**
 * Wait for completion of all bio_entry(s) related to a bio_wrapper.
 * and call bio_endio() if required.
 *
 * @biow target bio_wrapper.
 *   Do not assume biow->bio is available when is_endio is false.
 * @is_endio true if bio_endio() call is required, or false.
 * @is_delete true if bio_entry deletion is required, or false.
 *
 * CONTEXT:
 *   non-irq. non-atomic.
 */
static void wait_for_bio_wrapper(
	struct bio_wrapper *biow, bool is_endio, bool is_delete)
{
	struct bio_entry *bioe;
	unsigned int remaining;
	const unsigned long timeo = msecs_to_jiffies(completion_timeo_ms_);
	unsigned long rtimeo;
	unsigned i = 0;
	const struct walb_dev *wdev = biow->private_data;

	ASSERT(biow);
	ASSERT(biow->error == 0);

	remaining = biow->len;
	list_for_each_entry(bioe, &biow->bioe_list, list) {
		if (bio_entry_should_wait_completion(bioe)) {
			int c = 0;
		retry:
			rtimeo = wait_for_completion_io_timeout(&bioe->done, timeo);
			if (rtimeo == 0) {
				char buf[32];
				snprintf(buf, sizeof(buf), "%u: timeout(%d): ", wdev_minor(wdev), c);
				print_bio_wrapper_short(KERN_NOTICE, biow, buf);
				c++;
				goto retry;
			}
		}
		if (bioe->error) {
			biow->error = bioe->error;
		}
		remaining -= bioe->len;
		i++;
	}
#ifdef WALB_DEBUG
	{
		if (bio_wrapper_state_is_discard(biow) &&
			!blk_queue_discard(bdev_get_queue(wdev->ddev))) {
			ASSERT(remaining == biow->len);
			ASSERT(list_empty(&biow->bioe_list));
		} else {
			ASSERT(remaining == 0);
		}
	}
#endif

	if (is_endio) {
		ASSERT(biow->bio);
		if (!biow->error) {
			set_bit(BIO_UPTODATE, &biow->bio->bi_flags);
		}
		io_acct_end(biow);
		bio_endio(biow->bio, biow->error);
		biow->bio = NULL;
	}

	if (is_delete) {
		destroy_bio_entry_list(&biow->bioe_list);
		ASSERT(list_empty(&biow->bioe_list));
	}
}

/**
 * Submit data io.
 */
static void submit_write_bio_wrapper(struct bio_wrapper *biow, bool is_plugging)
{
#ifdef WALB_DEBUG
	struct walb_dev *wdev = biow->private_data;
#endif
	struct blk_plug plug;

	ASSERT(biow);
#ifdef WALB_OVERLAPPED_SERIALIZE
	ASSERT(biow->n_overlapped == 0);
#endif

	ASSERT(bio_wrapper_state_is_prepared(biow));
	if (test_and_set_bit(BIO_WRAPPER_SUBMITTED, &biow->flags) != 0) {
		/* Already submitted. */
		WARN_ON(1);
	}

#ifdef WALB_DEBUG
	if (bio_wrapper_state_is_discard(biow) &&
		!blk_queue_discard(bdev_get_queue(wdev->ddev))) {
		/* Data device does not support REQ_DISCARD. */
		ASSERT(list_empty(&biow->bioe_list));
	} else {
		ASSERT(!list_empty(&biow->bioe_list));
	}
#endif
	/* Submit all related bio(s). */
	if (is_plugging) { blk_start_plug(&plug); }
	submit_bio_entry_list(&biow->bioe_list);
	if (is_plugging) { blk_finish_plug(&plug); }

#ifdef WALB_PERFORMANCE_ANALYSIS
	getnstimeofday(&biow->ts[WALB_TIME_DATA_SUBMITTED]);
#endif
}

static void cancel_write_bio_wrapper(
	struct walb_dev *wdev, struct bio_wrapper *biow)
{
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);
	bool starts_queue;
#ifdef WALB_DEBUG
	ASSERT(bio_wrapper_state_is_prepared(biow));
	ASSERT(!bio_wrapper_state_is_submitted(biow));
#endif

	starts_queue = delete_bio_wrapper_from_pending_data(wdev, biow);
	if (starts_queue && melt_detail(iocored, false))
		dispatch_submit_log_task(wdev);

	/* Put related bio(s) and free resources */
	put_bio_entry_list(&biow->bioe_list); // 1st.
	put_bio_entry_list(&biow->bioe_list); // 2nd.
	destroy_bio_entry_list(&biow->bioe_list);
	ASSERT(list_empty(&biow->bioe_list));

	biow->error = -EIO;
	complete(&biow->done);
}

/**
 * Submit bio wrapper for read.
 *
 * @wdev walb device.
 * @biow bio wrapper (read).
 */
static void submit_read_bio_wrapper(
	struct walb_dev *wdev, struct bio_wrapper *biow)
{
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);
	bool ret;
	struct bio_entry *bioe;

	ASSERT(biow);
	ASSERT(biow->bio);

	/* Create cloned bio. */
	ASSERT(list_empty(&biow->bioe_list));
	bioe = create_bio_entry_by_clone(biow->bio, wdev->ddev, GFP_NOIO);
	if (!bioe)
		goto error0;

	list_add_tail(&bioe->list, &biow->bioe_list);

	/* Split if required due to chunk limitations. */
	if (!split_bio_entry_list_for_chunk(
			&biow->bioe_list, wdev->ddev_chunk_sectors, GFP_NOIO)) {
		goto error1;
	}

	/* Check pending data and copy data from executing write requests. */
	spin_lock(&iocored->pending_data_lock);
	ret = pending_check_and_copy(
		iocored->pending_data,
		iocored->max_sectors_in_pending, biow, GFP_ATOMIC);
	spin_unlock(&iocored->pending_data_lock);
	if (!ret) {
		goto error1;
	}

	/* Submit all related bio(s). */
	submit_bio_entry_list(&biow->bioe_list);

	/* Enqueue wait/gc task. */
	INIT_WORK(&biow->work, task_wait_and_gc_read_bio_wrapper);
	queue_work(wq_unbound_, &biow->work);

	return;
error1:
	destroy_bio_entry_list(&biow->bioe_list);
error0:
	io_acct_end(biow);
	bio_endio(biow->bio, -ENOMEM);
	ASSERT(list_empty(&biow->bioe_list));
	destroy_bio_wrapper_dec(wdev, biow);
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
	ASSERT(bioe->len == 0);

	generic_make_request(bio);

	return bioe;
error1:
	destroy_bio_entry(bioe);
error0:
	return NULL;
}

/**
 * Dispatch logpack submit task if necessary.
 */
static void dispatch_submit_log_task(struct walb_dev *wdev)
{
	dispatch_task_if_necessary(
		wdev,
		IOCORE_STATE_SUBMIT_LOG_TASK_WORKING,
		&get_iocored_from_wdev(wdev)->flags,
		wq_unbound_,
		task_submit_logpack_list);
}

/**
 * Dispatch logpack wait task if necessary.
 */
static void dispatch_wait_log_task(struct walb_dev *wdev)
{
	dispatch_task_if_necessary(
		wdev,
		IOCORE_STATE_WAIT_LOG_TASK_WORKING,
		&get_iocored_from_wdev(wdev)->flags,
		wq_unbound_,
		task_wait_for_logpack_list);
}

/**
 * Dispatch datapack submit task if necessary.
 */
static void dispatch_submit_data_task(struct walb_dev *wdev)
{
	dispatch_task_if_necessary(
		wdev,
		IOCORE_STATE_SUBMIT_DATA_TASK_WORKING,
		&get_iocored_from_wdev(wdev)->flags,
		wq_unbound_,
		task_submit_bio_wrapper_list);
}

/**
 * Dispatch datapack wait task if necessary.
 */
static void dispatch_wait_data_task(struct walb_dev *wdev)
{
	dispatch_task_if_necessary(
		wdev,
		IOCORE_STATE_WAIT_DATA_TASK_WORKING,
		&get_iocored_from_wdev(wdev)->flags,
		wq_unbound_,
		task_wait_for_bio_wrapper_list);
}

/**
 * Start to processing write bio_wrapper.
 */
static void start_write_bio_wrapper(
	struct walb_dev *wdev, struct bio_wrapper *biow)
{
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);
	ASSERT(biow);

	ASSERT(!biow->is_started);
	biow->is_started = true;
	atomic_inc(&iocored->n_started_write_bio);
}

/**
 * Wait for all data write IO(s) done.
 */
static void wait_for_all_started_write_io_done(struct walb_dev *wdev)
{
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);
	int nr = atomic_read(&iocored->n_started_write_bio);

	while (nr > 0) {
		WLOGi(wdev, "n_started_write_bio %d\n", nr);
		msleep(100);
		nr = atomic_read(&iocored->n_started_write_bio);
	}
	WLOGi(wdev, "n_started_write_bio %d\n", nr);
}

static void wait_for_logpack_submit_queue_empty(struct walb_dev *wdev)
{
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);

	for (;;) {
		bool is_empty;
		spin_lock(&iocored->logpack_submit_queue_lock);
		is_empty = list_empty(&iocored->logpack_submit_queue);
		spin_unlock(&iocored->logpack_submit_queue_lock);

		if (is_empty)
			return;
		WLOGi(wdev, "wait_for_logpack_submit_queue_empty\n");
		msleep(100);
	}
}

/**
 * Wait for all gc task done.
 */
static void wait_for_all_pending_gc_done(struct walb_dev *wdev)
{
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);
	int nr = atomic_read(&iocored->n_pending_gc);

	while (nr > 0) {
		WLOGi(wdev, "n_pending_gc %d\n", nr);
		msleep(100);
		nr = atomic_read(&iocored->n_pending_gc);
	}
	WLOGi(wdev, "n_pending_gc %d\n", nr);
}

/**
 * lsid: if lsid is INVALID_LSID, use completed lsid.
 */
static void force_flush_ldev(struct walb_dev *wdev)
{
	int err;
	u64 new_permanent_lsid;
	bool should_notice = false;
	bool updated = false;

	/* Get completed_lsid and update flush_lsid. */
	spin_lock(&wdev->lsid_lock);
	new_permanent_lsid = wdev->lsids.completed;
	update_flush_lsid_if_necessary(wdev, new_permanent_lsid);
	spin_unlock(&wdev->lsid_lock);

#if 0
	WLOGi(wdev, "force_flush lsid %" PRIu64 "\n", new_permanent_lsid);
#endif

	/* Execute a flush request. */
	if (supports_flush_request_bdev(wdev->ldev)) {
		err = blkdev_issue_flush(wdev->ldev, GFP_NOIO, NULL);
		if (err) {
			WLOGe(wdev, "log device flush failed. try to be read-only mode\n");
			set_bit(WALB_STATE_READ_ONLY, &wdev->flags);
		}
	}

#ifdef WALB_DEBUG
	atomic_inc(&get_iocored_from_wdev(wdev)->n_flush_force);
#endif

	/* Update permanent_lsid. */
	spin_lock(&wdev->lsid_lock);
	if (wdev->lsids.permanent < new_permanent_lsid) {
		should_notice = is_permanent_log_empty(&wdev->lsids);
		ASSERT(new_permanent_lsid <= wdev->lsids.flush);
		wdev->lsids.permanent = new_permanent_lsid;
		updated = true;
		LOG_("log_flush_completed_data\n");
	}
	ASSERT(lsid_set_is_valid(&wdev->lsids));
	spin_unlock(&wdev->lsid_lock);
#if 0
	if (updated)
		WLOGi(wdev, "permanent_lsid_force %" PRIu64 "\n", new_permanent_lsid);
#endif
	if (should_notice)
		walb_sysfs_notify(wdev, "lsids");
}

/**
 * Wait for all logs permanent which lsid <= specified 'lsid'.
 *
 * We must confirm the corresponding log has been permanent
 * before submitting data IOs.
 *
 * Do nothing if wdev->log_flush_interval_jiffies is 0,
 * In such case, WalB device concistency is not be kept.
 * Set log_flush_interval_jiffies to 0 for test only.
 *
 * @wdev walb device.
 * @lsid threshold lsid.
 * RETURN:
 *   true if the log has been permanent.
 *   false if wdev became read-only mode.
 */
static bool wait_for_log_permanent(struct walb_dev *wdev, u64 lsid)
{
	struct lsid_set lsids;
	unsigned long timeout_jiffies;

	/* We will wait for log flush at most the given interval period. */
	timeout_jiffies = jiffies + wdev->log_flush_interval_jiffies;
retry:
	if (test_bit(WALB_STATE_READ_ONLY, &wdev->flags))
		return false;
	spin_lock(&wdev->lsid_lock);
	lsids = wdev->lsids;
	spin_unlock(&wdev->lsid_lock);
	if (lsid <= lsids.permanent) {
		/* No need to wait. */
		return true;
	}
	if (lsid > lsids.completed) {
		/* The ldev IO is still not completed. */
		msleep(1);
		goto retry;
	}
	if (lsid <= lsids.flush) {
		/* Flush request to make lsid permanent will be completed soon. */
		msleep(1);
		goto retry;
	}
	if (time_is_after_jiffies(timeout_jiffies) &&
		lsid < lsids.flush + wdev->log_flush_interval_pb) {
		/* Too early to force flush log device.
		   Wait for a while. */
		msleep(1);
		goto retry;
	}

	force_flush_ldev(wdev);
	return !test_bit(WALB_STATE_READ_ONLY, &wdev->flags);
}

/**
 * Flush all workqueues for IO.
 */
static void flush_all_wq(void)
{
	flush_workqueue(wq_normal_);
	flush_workqueue(wq_nrt_);
	flush_workqueue(wq_unbound_);
}

/**
 * Clear working bit.
 */
static void clear_working_flag(int working_bit, unsigned long *flag_p)
{
	int ret;
	ret = test_and_clear_bit(working_bit, flag_p);
	ASSERT(ret);
}

/**
 * Invoke the userland executable binary.
 * @event_str event string. It must be zero-terminatd.
 */
static void invoke_userland_exec(struct walb_dev *wdev, const char *event_str)
{
	size_t len;
	int ret;
	/* uint max is "4294967295\0" so size 11 is enough. */
	const int UINT_STR_LEN = 11;
	char major_str[UINT_STR_LEN];
	char minor_str[UINT_STR_LEN];
	char *argv[] = { exec_path_on_error_, NULL, NULL, NULL, NULL };
	char *envp[] = { "HOME=/", "TERM=linux", "PATH=/bin:/usr/bin:/sbin:/usr/sbin", NULL };

	len = strnlen(exec_path_on_error_, EXEC_PATH_ON_ERROR_LEN);
	if (len == 0 || len == EXEC_PATH_ON_ERROR_LEN) { return; }

	ASSERT(wdev);
	ret = snprintf(major_str, UINT_STR_LEN, "%u", MAJOR(wdev->devt));
	ASSERT(ret < UINT_STR_LEN);
	ret = snprintf(minor_str, UINT_STR_LEN, "%u", MINOR(wdev->devt));
	ASSERT(ret < UINT_STR_LEN);

	argv[1] = major_str;
	argv[2] = minor_str;
	argv[3] = (char *)event_str;

	ret = call_usermodehelper(exec_path_on_error_, argv, envp, UMH_WAIT_EXEC);
	if (ret) {
		WLOGe(wdev, "Execute userland command failed: %s %s %s %s\n"
			, exec_path_on_error_
			, major_str
			, minor_str
			, event_str);
	}
}

/**
 * Fail all bios in specified bio wrapper list and destroy them.
 */
static void fail_and_destroy_bio_wrapper_list(
	struct walb_dev *wdev, struct list_head *biow_list)
{
	struct bio_wrapper *biow, *biow_next;
	list_for_each_entry_safe(biow, biow_next, biow_list, list) {
		list_del(&biow->list);
		io_acct_end(biow);
		bio_endio(biow->bio, -EIO);
		destroy_bio_wrapper_dec(wdev, biow);
	}
	ASSERT(list_empty(biow_list));
}

/**
 * Update flush lsid.
 * The lsids spinlock must be held.
 */
static void update_flush_lsid_if_necessary(struct walb_dev *wdev, u64 flush_lsid)
{
	if (wdev->lsids.flush < flush_lsid) {
		wdev->lsids.flush = flush_lsid;
		get_iocored_from_wdev(wdev)->log_flush_jiffies =
			jiffies + wdev->log_flush_interval_jiffies;
	}
}

/**
 * RETURN:
 *   sould_start_queue() return value.
 */
static bool delete_bio_wrapper_from_pending_data(
	struct walb_dev *wdev, struct bio_wrapper *biow)
{
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);
	bool starts_queue;

	spin_lock(&iocored->pending_data_lock);
	starts_queue = should_start_queue(wdev, biow);
	if (bio_wrapper_state_is_discard(biow)) {
		iocored->pending_sectors--;
	} else {
		iocored->pending_sectors -= biow->len;
		if (!bio_wrapper_state_is_overwritten(biow)) {
			pending_delete(iocored->pending_data,
				&iocored->max_sectors_in_pending, biow);
		}
	}
	spin_unlock(&iocored->pending_data_lock);

	return starts_queue;
}

/**
 * Check whether walb should stop the queue
 * due to too much pending data.
 *
 * CONTEXT:
 *   pending_data_lock must be held.
 */
static bool should_stop_queue(
	struct walb_dev *wdev, struct bio_wrapper *biow)
{
	bool should_stop;
	struct iocore_data *iocored;

	ASSERT(wdev);
	ASSERT(biow);
	iocored = get_iocored_from_wdev(wdev);
	ASSERT(iocored);

	if (test_bit(IOCORE_STATE_IS_QUEUE_STOPPED, &iocored->flags))
		return false;

	should_stop = iocored->pending_sectors + biow->len
		> wdev->max_pending_sectors;

	if (should_stop) {
		iocored->queue_restart_jiffies =
			jiffies + wdev->queue_stop_timeout_jiffies;
		return true;
	} else {
		return false;
	}
}

/**
 * Check whether walb should restart the queue
 * because pending data is not too much now.
 *
 * CONTEXT:
 *   pending_data_lock must be held.
 */
static bool should_start_queue(
	struct walb_dev *wdev, struct bio_wrapper *biow)
{
	bool is_size;
	bool is_timeout;
	struct iocore_data *iocored;

	ASSERT(wdev);
	ASSERT(biow);
	iocored = get_iocored_from_wdev(wdev);
	ASSERT(iocored);

	if (!test_bit(IOCORE_STATE_IS_QUEUE_STOPPED, &iocored->flags))
		return false;

	if (iocored->pending_sectors >= biow->len)
		is_size = iocored->pending_sectors - biow->len
			< wdev->min_pending_sectors;
	else
		is_size = true;

	is_timeout = time_is_before_jiffies(iocored->queue_restart_jiffies);

	return is_size || is_timeout;
}

/**
 * Increment n_users of treemap memory manager and
 * iniitialize mmgr_ if necessary.
 */
static bool treemap_memory_manager_get(void)
{
	bool ret;

	if (atomic_inc_return(&n_users_of_memory_manager_) == 1) {
		ret = initialize_treemap_memory_manager(
			&mmgr_, N_ITEMS_IN_MEMPOOL,
			TREE_NODE_CACHE_NAME,
			TREE_CELL_HEAD_CACHE_NAME,
			TREE_CELL_CACHE_NAME);
		if (!ret) { goto error; }
	}
	return true;
error:
	atomic_dec(&n_users_of_memory_manager_);
	return false;
}

/**
 * Decrement n_users of treemap memory manager and
 * finalize mmgr_ if necessary.
 */
static void treemap_memory_manager_put(void)
{
	if (atomic_dec_return(&n_users_of_memory_manager_) == 0) {
		finalize_treemap_memory_manager(&mmgr_);
	}
}

static bool pack_cache_get(void)
{
	if (atomic_inc_return(&n_users_of_pack_cache_) == 1) {
		pack_cache_ = kmem_cache_create(
			KMEM_CACHE_PACK_NAME,
			sizeof(struct pack), 0, 0, NULL);
		if (!pack_cache_) {
			goto error;
		}
	}
	return true;
error:
	atomic_dec(&n_users_of_pack_cache_);
	return false;
}

static void pack_cache_put(void)
{
	if (atomic_dec_return(&n_users_of_pack_cache_) == 0) {
		kmem_cache_destroy(pack_cache_);
		pack_cache_ = NULL;
	}
}

static void io_acct_start(struct bio_wrapper *biow)
{
	int cpu;
	int rw = bio_data_dir(biow->bio);
	struct walb_dev *wdev = biow->private_data;
	struct hd_struct *part0 = &wdev->gd->part0;

	biow->start_time = jiffies;

	cpu = part_stat_lock();
	part_round_stats(cpu, part0);
	part_stat_inc(cpu, part0, ios[rw]);
	part_stat_add(cpu, part0, sectors[rw], biow->len);
	part_inc_in_flight(part0, rw);
	part_stat_unlock();

#ifdef WALB_DEBUG
	atomic_inc(&get_iocored_from_wdev(wdev)->n_io_acct);
#endif
}

static void io_acct_end(struct bio_wrapper *biow)
{
	int cpu;
	int rw = bio_data_dir(biow->bio);
	struct walb_dev *wdev = biow->private_data;
	struct hd_struct *part0 = &wdev->gd->part0;
	unsigned long duration = jiffies - biow->start_time;
	unsigned long duration_ms = jiffies_to_msecs(duration);

	cpu = part_stat_lock();
	part_stat_add(cpu, part0, ticks[rw], duration);
	part_round_stats(cpu, part0);
	part_dec_in_flight(part0, rw);
	part_stat_unlock();

	if (io_latency_threshold_ms_ > 0 && duration_ms > io_latency_threshold_ms_) {
		char buf[64];
		snprintf(buf, sizeof(buf), "%u: IO latency exceeds threshold: %lu %c "
			 , wdev_minor(wdev), duration_ms, rw == WRITE ? 'W' : 'R');
		print_bio_wrapper_short(KERN_WARNING, biow, buf);
#ifdef WALB_PERFORMANCE_ANALYSIS
		print_bio_wrapper_performance(KERN_WARNING, biow);
#endif
	}

#ifdef WALB_DEBUG
	atomic_dec(&get_iocored_from_wdev(wdev)->n_io_acct);
#endif
}

/**
 * iocored->logpack_submit_queue_lock must be held.
 */
static bool is_frozen(struct iocore_data *iocored)
{
	return iocored->is_frozen_sys || iocored->is_frozen_usr;
}

/**
 * iocored->logpack_submit_queue_lock must be held.
 */
static void set_frozen(struct iocore_data *iocored, bool is_usr, bool value)
{
	bool *p;

	if (is_usr)
		p = &iocored->is_frozen_usr;
	else
		p = &iocored->is_frozen_sys;

	if (*p != value)
		*p = value;
}

static void freeze_detail(struct iocore_data *iocored, bool is_usr)
{
	spin_lock(&iocored->logpack_submit_queue_lock);
	set_frozen(iocored, is_usr, true);
	spin_unlock(&iocored->logpack_submit_queue_lock);
}

static bool melt_detail(struct iocore_data *iocored, bool is_usr)
{
	bool melted;

	spin_lock(&iocored->logpack_submit_queue_lock);
	set_frozen(iocored, is_usr, false);
	melted = !is_frozen(iocored);
	if (melted)
		make_frozen_queue_empty(iocored);

	spin_unlock(&iocored->logpack_submit_queue_lock);

	return melted;
}

/*******************************************************************************
 * Global functions implementation.
 *******************************************************************************/

/**
 * Initialize iocore data for a wdev.
 */
bool iocore_initialize(struct walb_dev *wdev)
{
	int ret;
	struct iocore_data *iocored;

	if (!treemap_memory_manager_get()) {
		LOGe("Treemap memory manager inc failed.\n");
		goto error0;
	}

	if (!pack_cache_get()) {
		LOGe("Failed to create a kmem_cache for pack.\n");
		goto error1;
	}

	if (!bio_entry_init()) {
		LOGe("Failed to init bio_entry.\n");
		goto error2;
	}

	if (!bio_wrapper_init()) {
		LOGe("Failed to init bio_wrapper.\n");
		goto error3;
	}

	if (!pack_work_init()) {
		LOGe("Failed to init pack_work.\n");
		goto error4;
	}

	iocored = create_iocore_data(GFP_KERNEL);
	if (!iocored) {
		LOGe("Memory allocation failed.\n");
		goto error5;
	}
	wdev->private_data = iocored;

	/* Decide gc worker name and start it. */
	ret = snprintf(iocored->gc_worker_data.name, WORKER_NAME_MAX_LEN,
		"%s/%u", WORKER_NAME_GC, MINOR(wdev->devt) / 2);
	if (ret >= WORKER_NAME_MAX_LEN) {
		LOGe("Thread name size too long.\n");
		goto error6;
	}
	initialize_worker(&iocored->gc_worker_data,
			run_gc_logpack_list, (void *)wdev);

	return true;

#if 0
error7:
	finalize_worker(&iocored->gc_worker_data);
#endif
error6:
	destroy_iocore_data(iocored);
	wdev->private_data = NULL;
error5:
	pack_work_exit();
error4:
	bio_wrapper_exit();
error3:
	bio_entry_exit();
error2:
	pack_cache_put();
error1:
	treemap_memory_manager_put();
error0:
	return false;
}

/**
 * Finalize iocore data for a wdev.
 */
void iocore_finalize(struct walb_dev *wdev)
{
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);

#ifdef WALB_DEBUG
	int n_flush_io, n_flush_logpack, n_flush_force;
	n_flush_io = atomic_read(&iocored->n_flush_io);
	n_flush_logpack = atomic_read(&iocored->n_flush_logpack);
	n_flush_force = atomic_read(&iocored->n_flush_force);
#endif

	finalize_worker(&iocored->gc_worker_data);
	destroy_iocore_data(iocored);
	wdev->private_data = NULL;

	pack_work_exit();
	bio_wrapper_exit();
	bio_entry_exit();
	pack_cache_put();
	treemap_memory_manager_put();

#ifdef WALB_DEBUG
	LOGn("n_allocated_pages: %u\n"
		"n_flush_io: %d\n"
		"n_flush_logpack: %d\n"
		"n_flush_force: %d\n",
		bio_entry_get_n_allocated_pages(),
		n_flush_io, n_flush_logpack, n_flush_force);
#endif
}

/**
 * Stop (write) IO processing.
 *
 * After freezed, there is no IO pending underlying
 * data/log devices.
 * Upper layer can submit IOs but the walb driver
 * just queues them and does not start processing during frozen.
 */
void iocore_freeze(struct walb_dev *wdev)
{
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);
	struct lsid_set lsids;

	might_sleep();

	freeze_detail(iocored, true);

	/* We must wait for this at first. */
	wait_for_logpack_submit_queue_empty(wdev);

	/* After logpack_submit_queue became empty,
	   the number of started write IOs will monotonically decrease. */
	wait_for_all_started_write_io_done(wdev);

	/* Wait for all pending gc task done
	   which update wdev->written_lsid. */
	wait_for_all_pending_gc_done(wdev);

	spin_lock(&wdev->lsid_lock);
	lsids = wdev->lsids;
	spin_unlock(&wdev->lsid_lock);
	WLOGi(wdev, "iocore frozen."
		" latest %" PRIu64 ""
		" written %" PRIu64 "\n"
		, lsids.latest, lsids.written);
	ASSERT(lsids.latest == lsids.written);
}

/**
 * (Re)start (write) IO processing.
 */
void iocore_melt(struct walb_dev *wdev)
{
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);

	might_sleep();

	if (melt_detail(iocored, true)) {
		dispatch_submit_log_task(wdev);
		WLOGi(wdev, "iocore melted.\n");
	}
}

/**
 * Make request.
 */
void iocore_make_request(struct walb_dev *wdev, struct bio *bio)
{
	struct bio_wrapper *biow;
	struct iocore_data *iocored;
	const bool is_write = (bio->bi_rw & REQ_WRITE) != 0;

	/* Check whether the device is dying. */
	if (is_wdev_dying(wdev)) {
		bio_endio(bio, -ENODEV);
		return;
	}
	iocored = get_iocored_from_wdev(wdev);

	/* Check whether read-only mode. */
	if (is_write && test_bit(WALB_STATE_READ_ONLY, &wdev->flags)) {
		bio_endio(bio, -EIO);
		return;
	}

	/* Create bio wrapper. */
	biow = alloc_bio_wrapper_inc(wdev, GFP_NOIO);
	if (!biow) {
		bio_endio(bio, -ENOMEM);
		return;
	}
	init_bio_wrapper(biow, bio);
	biow->private_data = wdev;

	/* IO accounting for diskstats. */
	io_acct_start(biow);

	if (is_write) {
#ifdef WALB_PERFORMANCE_ANALYSIS
		getnstimeofday(&biow->ts[WALB_TIME_BEGIN]);
#endif

		/* Copy bio. Do not use original bio's data ever after. */
		biow->copied_bio = bio_clone_copy(bio, GFP_NOIO);
		if (!biow->copied_bio)
			goto error0;

		/* Push into queue and invoke submit task. */
		if (push_into_lpack_submit_queue(biow))
			dispatch_submit_log_task(wdev);
	} else {
		submit_read_bio_wrapper(wdev, biow);

		/* TODO: support IOCORE_STATE_QUEUE_STOPPED for read also. */
	}
	return;
error0:
	io_acct_end(biow);
	destroy_bio_wrapper_dec(wdev, biow);
	bio_endio(bio, -EIO);
}


/* #define WLDEV_ANALYSIS */
#ifdef WLDEV_ANALYSIS

struct wldev_analysis
{
	struct bio *bio;
	struct walb_dev *wdev;
};

static void wldev_analysis_end_io(struct bio *bio, int error)
{
	struct wldev_analysis *ana = bio->bi_private;
	struct walb_dev *wdev = ana->wdev;
	struct bio *bio_orig = ana->bio;
	struct lsid_set lsids;
	const u64 rb_size = wdev->ring_buffer_size;
	spin_lock(&wdev->lsid_lock);
	lsids = wdev->lsids;
	spin_unlock(&wdev->lsid_lock);

	LOGi("read_end %" PRIu64 " %u\n", (u64)bio_orig->bi_sector, (uint)bio_sectors(bio_orig));
	while (bio_sectors(bio_orig) > 0) {
		u32 bio_off = bio_offset(bio_orig);
		u32 bytes = bio_iovec(bio_orig)->bv_len;
		u8 *bio_buf = bio_buf = kmap_atomic(bio_page(bio_orig));
		u64 lsid = INVALID_LSID;
		struct walb_logpack_header *logh = (struct walb_logpack_header *)(bio_buf + bio_off);
		ASSERT(bytes >= LOGICAL_BLOCK_SIZE);
		if (is_valid_logpack_header(logh)) {
			lsid = logh->logpack_lsid;
		}
		kunmap_atomic(bio_buf);

		if (lsid != INVALID_LSID) {
			if (lsids.permanent < rb_size || lsids.permanent - (rb_size / 2) < lsid) {
				LOGi("read_logpack_header %" PRIu64 "\n", lsid);
			}
		}
		bio_advance(bio_orig, LOGICAL_BLOCK_SIZE);
	}

	set_bit(BIO_UPTODATE, &bio_orig->bi_flags);
	bio_endio(bio_orig, error);
	bio_put(bio);
	kfree(ana);
}
#endif /* WLDEV_ANALYSIS */

/**
 * Make request for wrapper log device.
 */
void iocore_log_make_request(struct walb_dev *wdev, struct bio *bio)
{
	if (is_wdev_dying(wdev)) {
		bio_endio(bio, -ENODEV);
		return;
	}
	if (bio->bi_rw & WRITE) {
		bio_endio(bio, -EIO);
		return;
	}
#ifndef WLDEV_ANALYSIS
	bio->bi_bdev = wdev->ldev;
	generic_make_request(bio);
#else
	LOGi("read_bgn %" PRIu64 " %u\n", (u64)bio->bi_sector, (uint)bio_sectors(bio));
	{
		struct bio *biotmp;
		struct wldev_analysis *ana;
	retry:
		biotmp = bio_clone(bio, GFP_NOIO);
		if (!biotmp) {
			schedule();
			goto retry;
		}
	retry2:
		ana = kmalloc(sizeof(struct wldev_analysis), GFP_NOIO);
		if (!ana) {
			schedule();
			goto retry2;
		}
		ana->wdev = wdev;
		ana->bio = bio;
		biotmp->bi_bdev = wdev->ldev;
		biotmp->bi_end_io = wldev_analysis_end_io;
		biotmp->bi_private = ana;
		generic_make_request(biotmp);
	}
#endif
}

/**
 * Wait for all pending IO(s) for underlying data/log devices.
 */
void iocore_flush(struct walb_dev *wdev)
{
	wait_for_all_pending_io_done(wdev);
	flush_all_wq();
}

/**
 * Wait for all pending IO(s) done.
 */
void wait_for_all_pending_io_done(struct walb_dev *wdev)
{
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);
	int nr = atomic_read(&iocored->n_pending_bio);

	while (nr > 0) {
		WLOGi(wdev, "n_pending_bio %d\n", nr);
		msleep(100);
		nr = atomic_read(&iocored->n_pending_bio);
	}
	WLOGi(wdev, "n_pending_bio %d\n", nr);

#ifdef WALB_DEBUG
	{
		int n_io_acct = atomic_read(&iocored->n_io_acct);
		if (n_io_acct > 0) {
			WLOGw(wdev, "n_io_acct %d\n", n_io_acct);
		}
	}
#endif
}

/**
 * Allocate a bio wrapper and increment
 * n_pending_read_bio or n_pending_write_bio.
 */
struct bio_wrapper* alloc_bio_wrapper_inc(
	struct walb_dev *wdev, gfp_t gfp_mask)
{
	struct bio_wrapper *biow;
	struct iocore_data *iocored;

	ASSERT(wdev);
	iocored = get_iocored_from_wdev(wdev);
	ASSERT(iocored);

	biow = alloc_bio_wrapper(gfp_mask);
	if (!biow) { return NULL; }

	atomic_inc(&iocored->n_pending_bio);
	biow->is_started = false;
	return biow;
}

/**
 * Destroy a bio wrapper and decrement n_pending_bio.
 */
void destroy_bio_wrapper_dec(
	struct walb_dev *wdev, struct bio_wrapper *biow)
{
	struct iocore_data *iocored;
	bool started;

	ASSERT(wdev);
	iocored = get_iocored_from_wdev(wdev);
	ASSERT(iocored);
	ASSERT(biow);

	started = biow->is_started;
	destroy_bio_wrapper(biow);

	atomic_dec(&iocored->n_pending_bio);
	if (started) {
		atomic_dec(&iocored->n_started_write_bio);
	}
}

/**
 * Make request.
 */
void walb_make_request(struct request_queue *q, struct bio *bio)
{
	struct walb_dev *wdev = get_wdev_from_queue(q);
	iocore_make_request(wdev, bio);
}

/**
 * Walblog device make request.
 *
 * 1. Completion with error if write.
 * 2. Just forward to underlying log device if read.
 *
 * @q request queue.
 * @bio bio.
 */
void walblog_make_request(struct request_queue *q, struct bio *bio)
{
	struct walb_dev *wdev = get_wdev_from_queue(q);
	iocore_log_make_request(wdev, bio);
}

MODULE_LICENSE("Dual BSD/GPL");
