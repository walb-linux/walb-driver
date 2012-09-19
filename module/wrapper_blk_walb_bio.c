/**
 * wrapper_blk_walb_bio.c - WalB block device with bio-interface for test.
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
#include <linux/spinlock.h>
#include <linux/types.h>

#include "walb/walb.h"
#include "walb/block_size.h"
#include "walb/sector.h"
#include "wrapper_blk.h"
#include "sector_io.h"
#include "logpack.h"
#include "treemap.h"
#include "bio_util.h"
#include "bio_entry.h"
#include "bio_wrapper.h"

/*******************************************************************************
 * Module variables definition.
 *******************************************************************************/

/* Device size list string. The unit of each size is bytes. */
char *log_device_str_ = "/dev/simple_blk/0";
char *data_device_str_ = "/dev/simple_blk/1";
/* Minor id start. */
int start_minor_ = 0;

/* Physical block size [bytes]. */
int physical_block_size_ = 512;

/* Pending data limit size [MB]. */
int max_pending_mb_ = 64;
int min_pending_mb_ = 64 * 7 / 8;

/* Queue stop timeout [msec]. */
int queue_stop_timeout_ms_ = 100;

/* Maximum logpack size [KB].
   A logpack containing a requests can exceeds the limitation.
   This must be the integral multiple of physical block size.
   0 means there is no limitation of logpack size
   (practically limited by physical block size for logpack header). */
int max_logpack_size_kb_ = 256;

/*******************************************************************************
 * Module parameters definition.
 *******************************************************************************/

module_param_named(log_device_str, log_device_str_, charp, S_IRUGO);
module_param_named(data_device_str, data_device_str_, charp, S_IRUGO);
module_param_named(start_minor, start_minor_, int, S_IRUGO);
module_param_named(pbs, physical_block_size_, int, S_IRUGO);
module_param_named(max_pending_mb, max_pending_mb_, int, S_IRUGO);
module_param_named(min_pending_mb, min_pending_mb_, int, S_IRUGO);
module_param_named(queue_stop_timeout_ms, queue_stop_timeout_ms_, int, S_IRUGO);
module_param_named(max_logpack_size_kb, max_logpack_size_kb_, int, S_IRUGO);

/*******************************************************************************
 * Static data definition.
 *******************************************************************************/

/**
 * Workqueue for logpack submit/wait/gc task.
 * This should be shared by all walb devices.
 */
#define WQ_IO "wq_io"
struct workqueue_struct *wq_io_ = NULL;

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
	struct list_head biow_list; /* list head of bio_wrapper. */

	bool is_zero_flush_only; /* true if req_ent_list contains only a zero-size flush. */
	bool is_fua; /* FUA flag. */
	struct sector_data *logpack_header_sector;
	struct list_head bioe_list; /* list head for zero_flush bio
				       or logpack header bio. */

	bool is_logpack_failed; /* true if submittion failed. */
};
#define KMEM_CACHE_PACK_NAME "pack_cache"
struct kmem_cache *pack_cache_ = NULL;

/* Completion timeout [msec]. */
static const unsigned long completion_timeo_ms_ = 5000;

/**
 * Private data as wrapper_blk_dev.private_data.
 */
struct pdata
{
	struct block_device *ldev; /* underlying log device. */
	struct block_device *ddev; /* underlying data device. */

	spinlock_t lsid_lock;
	u64 latest_lsid; /* latest lsid.
			    This is lsid of next created logpack.
			    lsid_lock must be held. */
	u64 oldest_lsid; /* oldest lsid.
			    All previous logpacks of the logpack with
			    the oldest lsid can be overwritten.
			    lsid_lock must be held. */
	u64 written_lsid; /* written lsid.
			     All previous logpacks of the logpack with
			     the written_lsid have been stored.
			     lsid_lock must be held. */
	
	spinlock_t lsuper0_lock; /* Use spin_lock() and spin_unlock(). */
	struct sector_data *lsuper0; /* lsuper0_lock must be held
					to access the sector image. */

	/* To avoid lock lsuper0 during request processing. */
	u64 ring_buffer_off; 
	u64 ring_buffer_size;
	
	/* bit 0: all write must failed.
	   bit 1: logpack submit task working.
	   bit 2: logpack wait task working. */
	unsigned long flags;

	/* chunk sectors.
	   if chunk_sectors > 0:
	     (1) bio size must not exceed the size.
	     (2) bio must not cross over multiple chunks.
	   else:
	     no limitation. */
	unsigned int ldev_chunk_sectors;
	unsigned int ddev_chunk_sectors;

	spinlock_t logpack_submit_queue_lock;
	struct list_head logpack_submit_queue; /* writepack list.
						  logpack_submit_queue_lock
						  must be held. */
	
	spinlock_t logpack_wait_queue_lock;
	struct list_head logpack_wait_queue; /* writepack list.
						logpack_wait_queue_lock
						must be held. */

	unsigned int max_logpack_pb; /* Maximum logpack size [physical block].
					This will be used for logpack size
					not to be too long
					This will avoid decrease of
					sequential write performance. */

#ifdef WALB_OVERLAPPING_SERIALIZE
	/**
	 * All req_entry data may not keep reqe->bioe_list.
	 * You must keep address and size information in another way.
	 */
	spinlock_t overlapping_data_lock; /* Use spin_lock()/spin_unlock(). */
	struct multimap *overlapping_data; /* key: blk_rq_pos(req),
					      val: pointer to req_entry. */
	unsigned int max_sectors_in_overlapping; /* Maximum request size [logical block]. */
#endif
	
#ifdef WALB_FAST_ALGORITHM
	/**
	 * All bio_wrapper data must keep
	 * biow->bioe_list while they are stored in the pending_data.
	 */
	spinlock_t pending_data_lock; /* Use spin_lock()/spin_unlock(). */
	struct multimap *pending_data; /* key: biow->pos,
					  val: pointer to bio_wrapper. */
	unsigned int max_sectors_in_pending; /* Maximum request size [logical block]. */
	
	unsigned int pending_sectors; /* Number of sectors pending
					 [logical block]. */
	unsigned int max_pending_sectors; /* max_pending_sectors < pending_sectors
					     we must stop the queue. */
	unsigned int min_pending_sectors; /* min_pending_sectors > pending_sectors
					     we can restart the queue. */
	unsigned int queue_stop_timeout_ms; /* queue stopped period must not exceed
					       queue_stop_time_ms. */
	unsigned long queue_restart_jiffies; /* For queue stopped timeout check. */
	bool is_queue_stopped; /* true if queue is stopped. */
#endif
};

/*******************************************************************************
 * Macros definition.
 *******************************************************************************/

/* pdata->flags bit. */
#define PDATA_STATE_READ_ONLY            0
#define PDATA_STATE_SUBMIT_TASK_WORKING 1
#define PDATA_STATE_WAIT_TASK_WORKING   2

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

/* Make request callback. */
static void wrapper_blk_make_request_fn(struct request_queue *q, struct bio *bio);

/* Module helper functions. */

/* Called before register. */
static bool pre_register(void);
/* Called before unregister */
static void pre_unregister(void);
static void flush_all_wq(void);
/* Called just before destroy_private_data. */
static void pre_destroy_private_data(void);
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
UNUSED static bool is_zero_flush_only(struct pack *pack);
static bool is_pack_size_exceeds(
	struct walb_logpack_header *lhead,
	unsigned int pbs, unsigned int max_logpack_pb,
	struct bio_wrapper *biow);

/* helper function. */
static bool writepack_add_bio_wrapper(
	struct list_head *wpack_list, struct pack **wpackp,
	struct bio_wrapper *biow,
	u64 ring_buffer_size, unsigned int max_logpack_pb, u64 *latest_lsidp,
	struct wrapper_blk_dev *wdev, gfp_t gfp_mask);
static bool is_flush_first_bio_wrapper(struct list_head *biow_list);
static void writepack_check_and_set_flush(struct pack *wpack);

/* Workqueue tasks. */
static void logpack_list_submit_task(struct work_struct *work);
static void logpack_list_wait_task(struct work_struct *work);
static void logpack_list_gc_task(struct work_struct *work);
#ifdef WALB_OVERLAPPING_SERIALIZE
static void datapack_submit_task(struct work_struct *work);
#endif
static void bio_wrapper_read_wait_and_gc_task(struct work_struct *work);

/* Helper functions for tasks. */
static void logpack_list_submit(
	struct wrapper_blk_dev *wdev, struct list_head *wpack_list);

/* Helper functions for bio_entry list. */
static bool create_bio_entry_list(
	struct bio_wrapper *biow, struct block_device *bdev);
#ifdef WALB_FAST_ALGORITHM
static bool create_bio_entry_list_copy(
	struct bio_wrapper *biow, struct block_device *bdev);
#endif
static void submit_bio_entry_list(struct list_head *bioe_list);
static void wait_for_bio_wrapper(
	struct bio_wrapper *biow, bool is_endio, bool is_delete);

/* Validator for debug. */
static bool is_valid_prepared_pack(struct pack *pack);
UNUSED static bool is_valid_pack_list(struct list_head *pack_list);

/* Logpack related functions. */
static void logpack_list_create(
	struct wrapper_blk_dev *wdev, struct list_head *biow_list,
	struct list_head *pack_list);
static void logpack_calc_checksum(
	struct walb_logpack_header *lhead,
	unsigned int pbs, struct list_head *biow_list);
static void logpack_submit_header(
	struct walb_logpack_header *logh, bool is_flush, bool is_fua,
	struct list_head *bioe_list,
	unsigned int pbs, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size,
	unsigned int chunk_sectors);
static void logpack_submit_bio(
	struct bio_wrapper *biow, u64 lsid, bool is_fua,
	struct list_head *bioe_list,
	unsigned int pbs, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size,
	unsigned int chunk_sectors);
static struct bio_entry* logpack_create_bio_entry(
	struct bio *bio, bool is_fua, unsigned int pbs,
	struct block_device *ldev,
	u64 ldev_offset, unsigned int bio_offset);
static struct bio_entry* submit_flush(struct block_device *bdev);
static void logpack_submit_flush(struct block_device *bdev, struct list_head *bioe_list);
static void logpack_submit(
	struct walb_logpack_header *logh, bool is_fua,
	struct list_head *biow_list, struct list_head *bioe_list,
	unsigned int pbs, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size,
	unsigned int chunk_sectors);

/* Logpack-datapack related functions. */
static int wait_for_bio_entry_list(struct list_head *bioe_list);
static void wait_logpack_and_submit_datapack(
	struct wrapper_blk_dev *wdev, struct pack *wpack);

/* Helper functions for write. */
static void datapack_wait(struct wrapper_blk_dev *wdev, struct bio_wrapper *biow);

/* Helper functions for read. */
static void submit_bio_wrapper_read(
	struct wrapper_blk_dev *wdev, struct bio_wrapper *biow);

/* Overlapping data functions. */
#ifdef WALB_OVERLAPPING_SERIALIZE
static bool overlapping_check_and_insert(
	struct multimap *overlapping_data, unsigned int *max_sectors_p,
	struct bio_wrapper *biow, gfp_t gfp_mask);
static void overlapping_delete_and_notify(
	struct multimap *overlapping_data, unsigned int *max_sectors_p,
	struct bio_wrapper *biow);
#endif

/* Pending data functions. */
#ifdef WALB_FAST_ALGORITHM
static bool pending_insert(
	struct multimap *pending_data, unsigned int *max_sectors_p,
	struct bio_wrapper *biow, gfp_t gfp_mask);
static void pending_delete(
	struct multimap *pending_data, unsigned int *max_sectors_p,
	struct bio_wrapper *biow);
static bool pending_check_and_copy(
	struct multimap *pending_data, unsigned int max_sectors,
	struct bio_wrapper *biow, gfp_t gfp_mask);
static inline bool should_stop_queue(struct pdata *pdata, struct bio_wrapper *biow);
static inline bool should_start_queue(struct pdata *pdata, struct bio_wrapper *biow);
#endif

/*******************************************************************************
 * Utility functions.
 *******************************************************************************/

static inline struct pdata* pdata_get_from_wdev(struct wrapper_blk_dev *wdev)
{
	return (struct pdata *)wdev->private_data;
}

/**
 * Check two requests are overlapping.
 */
static inline bool is_overlap_req(struct request *req0, struct request *req1)
{
	ASSERT(req0);
	ASSERT(req1);
	ASSERT(req0 != req1);

	return (blk_rq_pos(req0) + blk_rq_sectors(req0) > blk_rq_pos(req1) &&
		blk_rq_pos(req1) + blk_rq_sectors(req1) > blk_rq_pos(req0));
}

/**
 * Check read-only mode.
 */
static inline int is_read_only_mode(struct pdata *pdata)
{
	return test_bit(PDATA_STATE_READ_ONLY, &pdata->flags);
}

/**
 * Set read-only mode.
 */
static inline void set_read_only_mode(struct pdata *pdata)
{
	set_bit(PDATA_STATE_READ_ONLY, &pdata->flags);
}

/**
 * Clear read-only mode.
 */
static inline void clear_read_only_mode(struct pdata *pdata)
{
	clear_bit(PDATA_STATE_READ_ONLY, &pdata->flags);
}


/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/* Create private data for wdev. */
static bool create_private_data(struct wrapper_blk_dev *wdev)
{
	struct pdata *pdata;
        struct block_device *ldev, *ddev;
        unsigned int lbs, pbs;
	struct walb_super_sector *ssect;
	struct request_queue *lq, *dq;
        
        LOGd("create_private_data called");

	/* Allocate pdata. */
	pdata = kmalloc(sizeof(struct pdata), GFP_KERNEL);
	if (!pdata) {
		LOGe("kmalloc failed.\n");
		goto error0;
	}
	pdata->ldev = NULL;
	pdata->ddev = NULL;
	spin_lock_init(&pdata->lsid_lock);
	spin_lock_init(&pdata->lsuper0_lock);

#ifdef WALB_OVERLAPPING_SERIALIZE
	spin_lock_init(&pdata->overlapping_data_lock);
	pdata->overlapping_data = multimap_create(GFP_KERNEL);
	if (!pdata->overlapping_data) {
		LOGe("multimap creation failed.\n");
		goto error01;
	}
	pdata->max_sectors_in_overlapping = 0;
#endif
#ifdef WALB_FAST_ALGORITHM
	spin_lock_init(&pdata->pending_data_lock);
	pdata->pending_data = multimap_create(GFP_KERNEL);
	if (!pdata->pending_data) {
		LOGe("multimap creation failed.\n");
		goto error02;
	}
	pdata->max_sectors_in_pending = 0;
	
	pdata->pending_sectors = 0;
	pdata->max_pending_sectors = max_pending_mb_
		* (1024 * 1024 / LOGICAL_BLOCK_SIZE);
	pdata->min_pending_sectors = min_pending_mb_
		* (1024 * 1024 / LOGICAL_BLOCK_SIZE);
	LOGn("max pending sectors: %u\n", pdata->max_pending_sectors);

	pdata->queue_stop_timeout_ms = queue_stop_timeout_ms_;
	pdata->queue_restart_jiffies = jiffies;
	LOGn("queue stop timeout: %u ms\n", queue_stop_timeout_ms_);
	
	pdata->is_queue_stopped = false;
#endif
        /* open underlying log device. */
        ldev = blkdev_get_by_path(
                log_device_str_, FMODE_READ|FMODE_WRITE|FMODE_EXCL,
                create_private_data);
        if (IS_ERR(ldev)) {
                LOGe("open %s failed.", log_device_str_);
                goto error1;
        }
	LOGn("ldev (%d,%d) %d\n", MAJOR(ldev->bd_dev), MINOR(ldev->bd_dev),
		ldev->bd_contains == ldev);

        /* open underlying data device. */
	ddev = blkdev_get_by_path(
		data_device_str_, FMODE_READ|FMODE_WRITE|FMODE_EXCL,
                create_private_data);
	if (IS_ERR(ddev)) {
		LOGe("open %s failed.", data_device_str_);
		goto error2;
	}
	LOGn("ddev (%d,%d) %d\n", MAJOR(ddev->bd_dev), MINOR(ddev->bd_dev),
		ddev->bd_contains == ddev);

        /* Block size */
        lbs = bdev_logical_block_size(ddev);
        pbs = bdev_physical_block_size(ddev);
	LOGn("pbs: %u lbs: %u\n", pbs, lbs);
        
        if (lbs != LOGICAL_BLOCK_SIZE) {
		LOGe("logical block size must be %u but %u.\n",
			LOGICAL_BLOCK_SIZE, lbs);
                goto error3;
        }
	ASSERT(bdev_logical_block_size(ldev) == lbs);
	if (bdev_physical_block_size(ldev) != pbs) {
		LOGe("physical block size is different (ldev: %u, ddev: %u).\n",
			bdev_physical_block_size(ldev), pbs);
		goto error3;
	}
	wdev->pbs = pbs;
	blk_set_default_limits(&wdev->queue->limits);
        blk_queue_logical_block_size(wdev->queue, lbs);
        blk_queue_physical_block_size(wdev->queue, pbs);
	/* blk_queue_io_min(wdev->queue, pbs); */
	/* blk_queue_io_opt(wdev->queue, pbs); */

	/* Set max_logpack_pb. */
	ASSERT(max_logpack_size_kb_ >= 0);
	ASSERT((max_logpack_size_kb_ * 1024) % pbs == 0);
	pdata->max_logpack_pb = (max_logpack_size_kb_ * 1024) / pbs;
	LOGn("max_logpack_size_kb: %u max_logpack_pb: %u\n",
		max_logpack_size_kb_, pdata->max_logpack_pb);
	
	/* Set underlying devices. */
	pdata->ldev = ldev;
	pdata->ddev = ddev;
        wdev->private_data = pdata;

	/* Load super block. */
	pdata->lsuper0 = sector_alloc(pbs, GFP_KERNEL);
	if (!pdata->lsuper0) {
		goto error3;
	}
	if (!walb_read_super_sector(pdata->ldev, pdata->lsuper0)) {
		LOGe("read super sector 0 failed.\n");
		goto error4;
	}
	ssect = get_super_sector(pdata->lsuper0);
	pdata->written_lsid = ssect->written_lsid;
	pdata->oldest_lsid = ssect->oldest_lsid;
	pdata->latest_lsid = pdata->written_lsid; /* redo must be done. */
	pdata->ring_buffer_size = ssect->ring_buffer_size;
	pdata->ring_buffer_off = get_ring_buffer_offset_2(ssect);
	pdata->flags = 0;
	
        /* capacity */
        wdev->capacity = ddev->bd_part->nr_sects;
        set_capacity(wdev->gd, wdev->capacity);
	LOGn("capacity %"PRIu64"\n", wdev->capacity);

	/* Set limit. */
	lq = bdev_get_queue(ldev);
	dq = bdev_get_queue(ddev);
        blk_queue_stack_limits(wdev->queue, lq);
        blk_queue_stack_limits(wdev->queue, dq);
	LOGn("ldev limits: lbs %u pbs %u io_min %u io_opt %u max_hw_sec %u max_sectors %u align %u\n",
		lq->limits.logical_block_size,
		lq->limits.physical_block_size,
		lq->limits.io_min,
		lq->limits.io_opt,
		lq->limits.max_hw_sectors,
		lq->limits.max_sectors,
		lq->limits.alignment_offset);
	LOGn("ddev limits: lbs %u pbs %u io_min %u io_opt %u max_hw_sec %u max_sectors %u align %u\n",
		dq->limits.logical_block_size,
		dq->limits.physical_block_size,
		dq->limits.io_min,
		dq->limits.io_opt,
		dq->limits.max_hw_sectors,
		dq->limits.max_sectors,
		dq->limits.alignment_offset);
	LOGn("wdev limits: lbs %u pbs %u io_min %u io_opt %u max_hw_sec %u max_sectors %u align %u\n",
		wdev->queue->limits.logical_block_size,
		wdev->queue->limits.physical_block_size,
		wdev->queue->limits.io_min,
		wdev->queue->limits.io_opt,
		wdev->queue->limits.max_hw_sectors,
		wdev->queue->limits.max_sectors,
		wdev->queue->limits.alignment_offset);

	/* Chunk size. */
	if (queue_io_min(lq) > wdev->pbs) {
		pdata->ldev_chunk_sectors = queue_io_min(lq) / LOGICAL_BLOCK_SIZE;
	} else {
		pdata->ldev_chunk_sectors = 0;
	}
	if (queue_io_min(dq) > wdev->pbs) {
		pdata->ddev_chunk_sectors = queue_io_min(dq) / LOGICAL_BLOCK_SIZE;
	} else {
		pdata->ddev_chunk_sectors = 0;
	}
	LOGn("chunk_sectors ldev %u ddev %u.\n",
		pdata->ldev_chunk_sectors, pdata->ddev_chunk_sectors);
	
	/* Prepare logpack submit/wait queue. */
	spin_lock_init(&pdata->logpack_submit_queue_lock);
	spin_lock_init(&pdata->logpack_wait_queue_lock);
	INIT_LIST_HEAD(&pdata->logpack_submit_queue);
	INIT_LIST_HEAD(&pdata->logpack_wait_queue);
	
        return true;

error4:
	sector_free(pdata->lsuper0);
error3:
        blkdev_put(ddev, FMODE_READ|FMODE_WRITE|FMODE_EXCL);
error2:
        blkdev_put(ldev, FMODE_READ|FMODE_WRITE|FMODE_EXCL);
error1:
#ifdef WALB_FAST_ALGORITHM
error02:
	multimap_destroy(pdata->pending_data);
#endif
#ifdef WALB_OVERLAPPING_SERIALIZE
error01:
	multimap_destroy(pdata->overlapping_data);
#endif
	kfree(pdata);
	wdev->private_data = NULL;
error0:
        return false;
}

/* Destroy private data for ssev. */
static void destroy_private_data(struct wrapper_blk_dev *wdev)
{
	struct pdata *pdata;
	struct walb_super_sector *ssect;

        LOGd("destoroy_private_data called.");
	
	pdata = wdev->private_data;
	if (!pdata) { return; }
	ASSERT(pdata);

	/* sync super block.
	   The locks are not required because
	   block device is now offline. */
	ssect = get_super_sector(pdata->lsuper0);
	ssect->written_lsid = pdata->written_lsid;
	ssect->oldest_lsid = pdata->oldest_lsid;
	if (!walb_write_super_sector(pdata->ldev, pdata->lsuper0)) {
		LOGe("super block write failed.\n");
	}
	
        /* close underlying devices. */
        blkdev_put(pdata->ddev, FMODE_READ|FMODE_WRITE|FMODE_EXCL);
        blkdev_put(pdata->ldev, FMODE_READ|FMODE_WRITE|FMODE_EXCL);

	sector_free(pdata->lsuper0);
#ifdef WALB_FAST_ALGORITHM
	multimap_destroy(pdata->pending_data);
#endif
#ifdef WALB_OVERLAPPING_SERIALIZE
	multimap_destroy(pdata->overlapping_data);
#endif
	kfree(pdata);
	wdev->private_data = NULL;
}

/* Customize wdev after register before start. */
static void customize_wdev(struct wrapper_blk_dev *wdev)
{
        struct request_queue *q, *lq, *dq;
	struct pdata *pdata;
        ASSERT(wdev);
        q = wdev->queue;
	pdata = wdev->private_data;
	ASSERT(pdata);

	lq = bdev_get_queue(pdata->ldev);
        dq = bdev_get_queue(pdata->ddev);
        /* Accept REQ_FLUSH and REQ_FUA. */
        if (lq->flush_flags & REQ_FLUSH && dq->flush_flags & REQ_FLUSH) {
                if (lq->flush_flags & REQ_FUA && dq->flush_flags & REQ_FUA) {
                        LOGn("Supports REQ_FLUSH | REQ_FUA.");
                        blk_queue_flush(q, REQ_FLUSH | REQ_FUA);
                } else {
                        LOGn("Supports REQ_FLUSH.");
                        blk_queue_flush(q, REQ_FLUSH);
                }
		blk_queue_flush_queueable(q, true);
        } else {
                LOGn("Supports neither REQ_FLUSH nor REQ_FUA.");
        }

#if 0
        if (blk_queue_discard(dq)) {
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
#endif 
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

        LOGn("begin\n");
        
        /* capacity must be set lator. */
	ret = wdev_register_with_bio(get_minor(i), capacity,
				physical_block_size_,
				wrapper_blk_make_request_fn);
                
        if (!ret) {
                goto error;
        }
        wdev = wdev_get(get_minor(i));
        if (!create_private_data(wdev)) {
                goto error;
        }
        customize_wdev(wdev);

        LOGn("end\n");

        return true;
error:
        unregister_dev();
        return false;
}

static void unregister_dev(void)
{
        unsigned int i = 0;
        struct wrapper_blk_dev *wdev;

	LOGn("begin\n");
	
        wdev = wdev_get(get_minor(i));
        wdev_unregister(get_minor(i));
        if (wdev) {
		pre_destroy_private_data();
                destroy_private_data(wdev);
		FREE(wdev);
        }
	
	LOGn("end\n");
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
	struct bio_wrapper *biow;
	struct bio_entry *bioe;
	unsigned int i;
	ASSERT(level);
	ASSERT(pack);

	printk("%s""print_pack %p begin\n", level, pack);
	
	i = 0;
	list_for_each_entry(biow, &pack->biow_list, list) {
		i ++;
		print_bio_wrapper(level, biow);
	}
	printk("%s""number of bio_wrapper in biow_list: %u.\n", level, i);

	i = 0;
	list_for_each_entry(bioe, &pack->bioe_list, list) {
		i ++;
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
		LOGn("BIO_UPTODATE is false (rw %lu pos %"PRIu64" len %u).\n",
			bioe->bio->bi_rw, (u64)bioe->pos, bioe->len);
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
	LOGd_("complete bioe %p pos %"PRIu64" len %u\n",
		bioe, (u64)bioe->pos, bioe->len);
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

	pack = kmem_cache_alloc(pack_cache_, gfp_mask);
	if (!pack) {
		LOGd("kmem_cache_alloc() failed.");
		goto error0;
	}
	INIT_LIST_HEAD(&pack->list);
	INIT_LIST_HEAD(&pack->biow_list);
	INIT_LIST_HEAD(&pack->bioe_list);
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
	struct bio_wrapper *biow, *biow_next;
	
	if (!pack) { return; }
	
	list_for_each_entry_safe(biow, biow_next, &pack->biow_list, list) {
		list_del(&biow->list);
		destroy_bio_wrapper(biow);
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
UNUSED
static bool is_zero_flush_only(struct pack *pack)
{
	struct walb_logpack_header *logh;
	struct bio_wrapper *biow;
	unsigned int i;
	ASSERT(pack);
	ASSERT(pack->logpack_header_sector);
	
	logh = get_logpack_header(pack->logpack_header_sector);
	ASSERT(logh);

	i = 0;
	list_for_each_entry(biow, &pack->biow_list, list) {

		ASSERT(biow->bio);
		if (!((biow->bio->bi_rw & REQ_FLUSH) && biow->len == 0)) {
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

	pb = (unsigned int)capacity_pb(pbs, biow->len);
	return pb + (unsigned int)lhead->total_io_size > max_logpack_pb;
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
	u64 ring_buffer_size, unsigned int max_logpack_pb, u64 *latest_lsidp,
	struct wrapper_blk_dev *wdev, gfp_t gfp_mask)
{
	struct pack *pack;
	bool ret;
	unsigned int pbs;
	struct walb_logpack_header *lhead = NULL;

	LOGd_("begin\n");
	
	ASSERT(wpack_list);
	ASSERT(wpackp);
	ASSERT(biow);
	ASSERT(biow->bio);
	ASSERT(biow->bio->bi_rw & REQ_WRITE);
	ASSERT(wdev);
	pbs = wdev->pbs;
	ASSERT_PBS(pbs);
	
	pack = *wpackp;
	
	if (!pack) {
		goto newpack;
	}

	ASSERT(pack);
	ASSERT(pack->logpack_header_sector);
	ASSERT(pbs == pack->logpack_header_sector->size);
	lhead = get_logpack_header(pack->logpack_header_sector);
	ASSERT(*latest_lsidp == lhead->logpack_lsid);
	
	if (lhead->n_records > 0 &&
		(biow->bio->bi_rw & REQ_FLUSH
			|| is_pack_size_exceeds(lhead, pbs, max_logpack_pb, biow))) {
		/* Flush request must be the first of the pack. */
		goto newpack;
	}
	if (!walb_logpack_header_add_bio(lhead, biow->bio, pbs, ring_buffer_size)) {
		/* logpack header capacity full so create a new pack. */
		goto newpack;
	}
	goto fin;

newpack:
	if (lhead) {
		writepack_check_and_set_flush(pack);
		ASSERT(is_valid_prepared_pack(pack));
		list_add_tail(&pack->list, wpack_list);
		*latest_lsidp = get_next_lsid_unsafe(lhead);
	}
	pack = create_writepack(gfp_mask, pbs, *latest_lsidp);
	if (!pack) { goto error0; }
	*wpackp = pack;
	lhead = get_logpack_header(pack->logpack_header_sector);
	ret = walb_logpack_header_add_bio(lhead, biow->bio, pbs, ring_buffer_size);
	ASSERT(ret);
fin:
	if (biow->bio->bi_rw & REQ_FUA) {
		pack->is_fua = true;
	}
	/* The request is just added to the pack. */
	list_add_tail(&biow->list, &pack->biow_list);
	LOGd_("normal end\n");
	return true;
error0:
	LOGd_("failure end\n");
	return false;
}

/**
 * Check whether first bio_wrapper is flush in the list.
 *
 * @biow_list bio_wrapper list. Never empty.
 *
 * RETURN:
 *   true if the first req_entry is flush request, or false.
 */
static bool is_flush_first_bio_wrapper(struct list_head *biow_list)
{
	struct bio_wrapper *biow;
	ASSERT(!list_empty(biow_list));
	
	biow = list_first_entry(biow_list, struct bio_wrapper, list);
	ASSERT(biow);
	ASSERT(biow->bio);
	return biow->bio->bi_rw == REQ_FLUSH;
}

/**
 * Check whether wpack is zero-flush-only and set the flag.
 */
static void writepack_check_and_set_flush(struct pack *wpack)
{
	struct walb_logpack_header *logh = NULL;
	
	ASSERT(wpack);
	
	logh = get_logpack_header(wpack->logpack_header_sector);
	ASSERT(logh);

	/* Check whether zero-flush-only or not. */
	if (logh->n_records == 0) {
		ASSERT(is_zero_flush_only(wpack));
		wpack->is_zero_flush_only = true;
	}
}

/**
 * Create bio_entry list for a bio_wrapper.
 * This does not copy IO data, bio stubs only.
 *
 * RETURN:
 *     true if succeed, or false.
 * CONTEXT:
 *     Non-IRQ. Non-atomic.
 */
static bool create_bio_entry_list(struct bio_wrapper *biow, struct block_device *bdev)
{
	struct bio_entry *bioe;
        
	ASSERT(biow);
	ASSERT(biow->bio);;
	ASSERT(list_empty(&biow->bioe_list));
        
	/* clone bio. */
	bioe = create_bio_entry_by_clone(biow->bio, bdev, GFP_NOIO);
	if (!bioe) {
		LOGe("create_bio_entry() failed.\n");
		goto error0;
	}
	list_add_tail(&bioe->list, &biow->bioe_list);
	
	return true;
error0:
	destroy_bio_entry_list(&biow->bioe_list);
	ASSERT(list_empty(&biow->bioe_list));
	return false;
}

/**
 * Create bio_entry list for a bio_wrapper by copying its IO data.
 *
 * RETURN:
 *     true if succeed, or false.
 * CONTEXT:
 *     Non-IRQ, Non-Atomic.
 */
#ifdef WALB_FAST_ALGORITHM
static bool create_bio_entry_list_copy(
	struct bio_wrapper *biow, struct block_device *bdev)
{
	struct bio_entry *bioe;
	
	ASSERT(biow);
	ASSERT(biow->bio);
	ASSERT(list_empty(&biow->bioe_list));
	ASSERT(biow->bio->bi_rw & REQ_WRITE);
	
	bioe = create_bio_entry_by_clone_copy(biow->bio, bdev, GFP_NOIO);
	if (!bioe) {
		LOGd("create_bio_entry_list_copy() failed.\n");
		goto error0;
	}
	list_add_tail(&bioe->list, &biow->bioe_list);
	return true;
error0:
	destroy_bio_entry_list(&biow->bioe_list);
	ASSERT(list_empty(&biow->bioe_list));
	return false;
}
#endif

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
#ifdef WALB_FAST_ALGORITHM
#ifdef WALB_DEBUG
		if (!bioe->is_splitted) {
			ASSERT(bioe->bio->bi_end_io == bio_entry_end_io);
		}
#endif /* WALB_DEBUG */
		if (bioe->is_copied) {
			LOGd_("copied: rw %lu bioe %p pos %"PRIu64" len %u\n",
				bioe->bio->bi_rw,
				bioe, (u64)bioe->pos, bioe->len);
			set_bit(BIO_UPTODATE, &bioe->bio->bi_flags);
			bio_endio(bioe->bio, 0);
		} else {
			LOGd_("submit_d: rw %lu bioe %p pos %"PRIu64" len %u\n",
				bioe->bio->bi_rw,
				bioe, (u64)bioe->pos, bioe->len);
			generic_make_request(bioe->bio);
		}
#else /* WALB_FAST_ALGORITHM */
		LOGd_("submit_d: rw %lu bioe %p pos %"PRIu64" len %u\n",
			bioe->bio->bi_rw,
			bioe, (u64)bioe->pos, bioe->len);
		generic_make_request(bioe->bio);
#endif /* WALB_FAST_ALGORITHM */
	}
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
	struct bio_entry *bioe, *next;
	unsigned int remaining;
	const unsigned long timeo = msecs_to_jiffies(completion_timeo_ms_);
	unsigned long rtimeo;
	int c;
	
	ASSERT(biow);
        
	remaining = biow->len;
	list_for_each_entry(bioe, &biow->bioe_list, list) {
		if (bio_entry_should_wait_completion(bioe)) {
			c = 0;
		retry:
			rtimeo = wait_for_completion_timeout(&bioe->done, timeo);
			if (rtimeo == 0) {
				LOGn("timeout(%d): biow %p bioe %p bio %p pos %"PRIu64" len %u\n",
					c, biow, bioe, bioe->bio,
					(u64)bioe->pos, bioe->len);
				c++;
				goto retry;
			}
		}
		if (bioe->error) {
			biow->error = bioe->error;
		}
		remaining -= bioe->len;
	}
	ASSERT(remaining == 0);

	if (is_endio) {
		ASSERT(biow->bio);
		bio_endio(biow->bio, biow->error);
	}
	
	if (is_delete) {
		list_for_each_entry_safe(bioe, next, &biow->bioe_list, list) {
			list_del(&bioe->list);
			destroy_bio_entry(bioe);
		}
		ASSERT(list_empty(&biow->bioe_list));
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
	struct walb_logpack_header *logh;
	ASSERT(wpack_list);
	ASSERT(wdev);
	pdata = pdata_get_from_wdev(wdev);

	blk_start_plug(&plug);
	list_for_each_entry(wpack, wpack_list, list) {

		ASSERT_SECTOR_DATA(wpack->logpack_header_sector);
		logh = get_logpack_header(wpack->logpack_header_sector);
		
		if (wpack->is_zero_flush_only) {
			ASSERT(logh->n_records == 0);
			LOGd("is_zero_flush_only\n"); /* debug */
			logpack_submit_flush(pdata->ldev, &wpack->bioe_list);
		} else {
			ASSERT(logh->n_records > 0);
			logpack_calc_checksum(logh, wdev->pbs, &wpack->biow_list);

			logpack_submit(
				logh, wpack->is_fua,
				&wpack->biow_list, &wpack->bioe_list,
				wdev->pbs, pdata->ldev, pdata->ring_buffer_off,
				pdata->ring_buffer_size, pdata->ldev_chunk_sectors);
		}
	}
	blk_finish_plug(&plug);
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
static int wait_for_bio_entry_list(struct list_head *bioe_list)
{
	struct bio_entry *bioe, *next_bioe;
	int bio_error = 0;
	const unsigned long timeo = msecs_to_jiffies(completion_timeo_ms_);
	unsigned long rtimeo;
	int c;
	ASSERT(bioe_list);
	
	/* wait for completion. */
	list_for_each_entry(bioe, bioe_list, list) {

		if (bio_entry_should_wait_completion(bioe)) {
			c = 0;
		retry:
			rtimeo = wait_for_completion_timeout(&bioe->done, timeo);
			if (rtimeo == 0) {
				LOGn("timeout(%d): bioe %p bio %p len %u\n",
					c, bioe, bioe->bio, bioe->len);
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
 * Wait for completion of all bio(s) and enqueue datapack tasks.
 *
 * Request success -> enqueue datapack.
 * Request failure-> all subsequent requests must fail.
 *
 * If any write failed, wdev will be read-only mode.
 */
static void wait_logpack_and_submit_datapack(
	struct wrapper_blk_dev *wdev, struct pack *wpack)
{
	int bio_error;
	struct bio_wrapper *biow, *biow_next;
	bool is_failed = false;
	struct pdata *pdata;
#ifdef WALB_OVERLAPPING_SERIALIZE
	bool is_overlapping_insert_succeeded;
#endif
	bool is_pending_insert_succeeded;
	bool is_stop_queue = false;

	ASSERT(wpack);
	ASSERT(wdev);

	/* Check read only mode. */
	pdata = pdata_get_from_wdev(wdev);
	if (is_read_only_mode(pdata)) { is_failed = true; }
	
	/* Wait for logpack header bio or zero_flush pack bio. */
	bio_error = wait_for_bio_entry_list(&wpack->bioe_list);
	if (bio_error) { is_failed = true; }
	
	list_for_each_entry_safe(biow, biow_next, &wpack->biow_list, list) {

		ASSERT(biow->bio);
		bio_error = wait_for_bio_entry_list(&biow->bioe_list);
		if (is_failed || bio_error) { goto error_io; }
		
		if (biow->len == 0) {
			ASSERT(biow->bio->bi_rw & REQ_FLUSH);
			/* Already the corresponding logpack is permanent. */
			list_del(&biow->list);
			bio_endio(biow->bio, 0);
			destroy_bio_wrapper(biow);
		} else {
			/* Create all related bio(s) by copying IO data. */
		retry_create:
			if (!create_bio_entry_list_copy(biow, pdata->ddev)) {
				schedule();
				goto retry_create;
			}
			/* Split if required due to chunk limitations. */
		retry_split:
			if (!split_bio_entry_list_for_chunk(
					&biow->bioe_list,
					pdata->ddev_chunk_sectors,
					GFP_NOIO)) {
				schedule();
				goto retry_split;
			}

			/* Call bio_get() for all bio(s) */
			get_bio_entry_list(&biow->bioe_list);

			/* Try to insert pending data. */
		retry_insert_pending:
			spin_lock(&pdata->pending_data_lock);
			LOGd_("pending_sectors %u\n", pdata->pending_sectors);
			is_stop_queue = should_stop_queue(pdata, biow);
			pdata->pending_sectors += biow->len;
			is_pending_insert_succeeded =
				pending_insert(pdata->pending_data,
					&pdata->max_sectors_in_pending,
					biow, GFP_ATOMIC);
			spin_unlock(&pdata->pending_data_lock);
			if (!is_pending_insert_succeeded) {
				spin_lock(&pdata->pending_data_lock);
				pdata->pending_sectors -= biow->len;
				spin_unlock(&pdata->pending_data_lock);
				schedule();
				goto retry_insert_pending;
			}

			/* Check pending data size and stop the queue if needed. */
			if (is_stop_queue) {
				LOGd("stop queue.\n");
#if 0
				spin_lock_irqsave(&wdev->lock, flags);
				blk_stop_queue(wdev->queue);
				spin_unlock_irqrestore(&wdev->lock, flags);
#endif
				/* now editing */
				/* start/stop queue must be controlled by ourself. */
			}

			/* call endio here in fast algorithm,
			   while easy algorithm call it after data device IO. */
			bio_endio(biow->bio, 0);
			
#ifdef WALB_OVERLAPPING_SERIALIZE
			/* check and insert to overlapping detection data. */
		retry_insert_ol:
			spin_lock(&pdata->overlapping_data_lock);
			is_overlapping_insert_succeeded =
				overlapping_check_and_insert(pdata->overlapping_data,
							&pdata->max_sectors_in_overlapping,
							biow, GFP_ATOMIC);
			spin_unlock(&pdata->overlapping_data_lock);
			if (!is_overlapping_insert_succeeded) {
				schedule();
				goto retry_insert_ol;
			}
			/* Submit bio(s) or enqueue submit task. */
			if (biow->n_overlapping == 0) {
				submit_bio_entry_list(&biow->bioe_list);
			} else {
				INIT_WORK(&biow->work, datapack_submit_task);
				queue_work(wq_io_, &biow->work);
			}
#else /* WALB_OVERLAPPING_SERIALIZE */
			/* Submit bio(s). */
			submit_bio_entry_list(&biow->bioe_list);
#endif /* WALB_OVERLAPPING_SERIALIZE */

			/* datapack_wait() will be called in the logpack gc task. */
		}
		continue;
	error_io:
		is_failed = true;
		set_read_only_mode(pdata);
		LOGe("WalB changes device minor:%u to read-only mode.\n", wdev->minor);
		bio_endio(biow->bio, -EIO);
		list_del(&biow->list);
		destroy_bio_wrapper(biow);
	}
}

/**
 * Submit all logpacks generated from bio_wrapper list.
 * 
 * (1) Create logpack list.
 * (2) Submit all logpack-related bio(s).
 * (3) Enqueue logpack_list_wait_task.
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
static void logpack_list_submit_task(struct work_struct *work)
{
	struct pack_work *pwork = container_of(work, struct pack_work, work);
	struct wrapper_blk_dev *wdev = pwork->wdev;
	struct pdata *pdata = pdata_get_from_wdev(wdev);
	struct pack *wpack, *wpack_next;
	struct list_head wpack_list;
	bool is_empty, is_working;
	struct list_head biow_list;
	struct bio_wrapper *biow, *biow_next;

	LOGd_("begin.\n");
	
	destroy_pack_work(pwork);
	pwork = NULL;
	
	INIT_LIST_HEAD(&wpack_list);
	while (true) {
		/* Dequeue all bio wrappers from the submit queue. */
		INIT_LIST_HEAD(&biow_list);
		spin_lock(&pdata->logpack_submit_queue_lock);
		is_empty = list_empty(&pdata->logpack_submit_queue);
		list_for_each_entry_safe(biow, biow_next,
					&pdata->logpack_submit_queue, list) {
			list_move_tail(&pdata->logpack_submit_queue, &biow_list);
		}
		spin_unlock(&pdata->logpack_submit_queue_lock);
		if (is_empty) {
			is_working = test_and_clear_bit(
				PDATA_STATE_SUBMIT_TASK_WORKING,
				&pdata->flags);
			ASSERT(is_working);
			break;
		}

		/* Create and submit. */
		logpack_list_create(wdev, &biow_list, &wpack_list);
		ASSERT(list_empty(&biow_list));
		ASSERT(!list_empty(&wpack_list));
		logpack_list_submit(wdev, &wpack_list);

		/* Enqueue logpack list to the wait queue. */
		spin_lock(&pdata->logpack_wait_queue_lock);
		list_for_each_entry_safe(wpack, wpack_next, &wpack_list, list) {
			list_move_tail(&wpack->list, &pdata->logpack_wait_queue);
		}
		spin_unlock(&pdata->logpack_wait_queue_lock);

		if (test_and_set_bit(
				PDATA_STATE_WAIT_TASK_WORKING,
				&pdata->flags) == 0) {
			
		retry_pack_work:
			pwork = create_pack_work(wdev, GFP_NOIO);
			if (!pwork) {
				LOGn("memory allocation failed.\n");
				schedule();
				goto retry_pack_work;
			}
			INIT_WORK(&pwork->work, logpack_list_wait_task);
			queue_work(wq_io_, &pwork->work);
			pwork = NULL;
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
	struct blk_plug plug;

	destroy_pack_work(pwork);
	pwork = NULL;
	
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
	retry_pack_work:
		pwork = create_pack_work(wdev, GFP_NOIO);
		if (!pwork) {
			LOGn("memory allocation failed.\n");
			schedule();
			goto retry_pack_work;
		}
		
		/* Wait logpack completion and submit datapacks. */
		blk_start_plug(&plug);
		list_for_each_entry_safe(wpack, wpack_next, &wpack_list, list) {
			wait_logpack_and_submit_datapack(wdev, wpack);
			list_move_tail(&wpack->list, &pwork->wpack_list);
		}
		blk_finish_plug(&plug);

		/* Enqueue logpack list gc task. */
		INIT_WORK(&pwork->work, logpack_list_gc_task);
		queue_work(wq_io_, &pwork->work);
		pwork = NULL;
	}
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
	struct wrapper_blk_dev *wdev = pwork->wdev;
	struct pack *wpack, *wpack_next;
	struct bio_wrapper *biow, *biow_next;

	list_for_each_entry_safe(wpack, wpack_next, &pwork->wpack_list, list) {
		list_del(&wpack->list);
		list_for_each_entry_safe(biow, biow_next, &wpack->biow_list, list) {
			list_del(&biow->list);
			datapack_wait(wdev, biow);
			destroy_bio_wrapper(biow);
		}
		ASSERT(list_empty(&wpack->biow_list));
		ASSERT(list_empty(&wpack->bioe_list));
		destroy_pack(wpack);
	}
	ASSERT(list_empty(&pwork->wpack_list));
	destroy_pack_work(pwork);
}

/**
 * Datapack submit task.
 */
#ifdef WALB_OVERLAPPING_SERIALIZE
static void datapack_submit_task(struct work_struct *work)
{
	struct bio_wrapper *biow = container_of(work, struct bio_wrapper, work);
	struct blk_plug plug;
	const unsigned long timeo = msecs_to_jiffies(completion_timeo_ms_);
	unsigned long rtimeo;
	int c;
	
	/* Wait for previous overlapping writes. */
	if (biow->n_overlapping > 0) {
		c = 0;
	retry:
		rtimeo = wait_for_completion_timeout(
			&biow->overlapping_done, timeo);
		if (rtimeo == 0) {
			LOGw("timeout(%d): biow %p pos %"PRIu64" len %u\n",
				c, biow, (u64)biow->pos, biow->len);
			c++;
			goto retry;
		}
	}

	ASSERT(!list_empty(&biow->bioe_list));
	
	/* Submit all related bio(s). */
	blk_start_plug(&plug);
	submit_bio_entry_list(&biow->bioe_list);
	blk_finish_plug(&plug);
}
#endif

/**
 * Wait for completion of datapack IO.
 */
static void datapack_wait(struct wrapper_blk_dev *wdev, struct bio_wrapper *biow)
{
	struct pdata *pdata = pdata_get_from_wdev(wdev);
	const bool is_endio = false;
	const bool is_delete = false;
	bool is_start_queue = false;
#if 0
	unsigned long flags;
#endif
	
	/* Wait for completion and call end_request. */
	wait_for_bio_wrapper(biow, is_endio, is_delete);

	/* Delete from overlapping detection data. */
#ifdef WALB_OVERLAPPING_SERIALIZE
	spin_lock(&pdata->overlapping_data_lock);
	overlapping_delete_and_notify(pdata->overlapping_data,
				&pdata->max_sectors_in_overlapping,
				biow);
	spin_unlock(&pdata->overlapping_data_lock);
#endif

	/* Delete from pending data. */
	spin_lock(&pdata->pending_data_lock);
	is_start_queue = should_start_queue(pdata, biow);
	pdata->pending_sectors -= biow->len;
	pending_delete(pdata->pending_data, &pdata->max_sectors_in_pending, biow);
	spin_unlock(&pdata->pending_data_lock);

	/* Check queue restart is required. */
	if (is_start_queue) {
		LOGd("restart queue.\n");
#if 0
		spin_lock_irqsave(&wdev->lock, flags);
		blk_start_queue(wdev->queue);
		spin_unlock_irqrestore(&wdev->lock, flags);
#endif
		/* now editing */
	}

	/* put related bio(s). */
	put_bio_entry_list(&biow->bioe_list);
	
	/* Free resources. */
	destroy_bio_entry_list(&biow->bioe_list);
	ASSERT(list_empty(&biow->bioe_list));
}

/**
 * Wait for all related bio(s) for a bio_wrapper and gc it.
 */
static void bio_wrapper_read_wait_and_gc_task(struct work_struct *work)
{
	struct bio_wrapper *biow = container_of(work, struct bio_wrapper, work);
	const bool is_endio = true;
	const bool is_delete = true;

	ASSERT(biow);

	wait_for_bio_wrapper(biow, is_endio, is_delete);
	destroy_bio_wrapper(biow);
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
	struct bio_wrapper *biow;
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

	CHECK(!list_empty(&pack->biow_list));
	
	i = 0;
	total_pb = 0;
	list_for_each_entry(biow, &pack->biow_list, list) {

		CHECK(biow->bio);
		if (biow->len == 0) {
			CHECK(biow->bio->bi_rw & REQ_FLUSH);
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
		CHECK(biow->bio);
		CHECK(biow->bio->bi_rw & REQ_WRITE);

		CHECK(biow->pos == (sector_t)lrec->offset);
		CHECK(lhead->logpack_lsid == lrec->lsid - lrec->lsid_local);
		CHECK(biow->len == lrec->io_size);
		total_pb += capacity_pb(pbs, lrec->io_size);
		
		i ++;
	}
	CHECK(i == lhead->n_records);
	CHECK(total_pb == lhead->total_io_size);
	CHECK(n_padding == lhead->n_padding);
	if (lhead->n_records == 0) {
		CHECK(pack->is_zero_flush_only);
	}
	LOGd_("valid.\n");
	return true;
error:
	LOGd_("not valid.\n");
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
 * Set checksum of each bio and calc/set log header checksum.
 *
 * @logh log pack header.
 * @pbs physical sector size (allocated size as logh).
 * @biow_list list of biow.
 *   checksum of each bio has already been calculated as biow->csum.
 */
static void logpack_calc_checksum(
	struct walb_logpack_header *logh,
	unsigned int pbs, struct list_head *biow_list)
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

                if (logh->record[i].is_padding) {
                        n_padding ++;
                        i ++;
			/* A padding record is not the last in the logpack header. */
                }
		
		ASSERT(biow);
		ASSERT(biow->bio);
		ASSERT(biow->bio->bi_rw & REQ_WRITE);

		if (biow->len == 0) {
			ASSERT(biow->bio->bi_rw & REQ_FLUSH);
			continue;
		}

                logh->record[i].checksum = biow->csum;
                i ++;
	}
	
        ASSERT(n_padding <= 1);
        ASSERT(n_padding == logh->n_padding);
        ASSERT(i == logh->n_records);
        ASSERT(logh->checksum == 0);
        logh->checksum = checksum((u8 *)logh, pbs);
        ASSERT(checksum((u8 *)logh, pbs) == 0);
}

/**
 * Submit bio of header block.
 *
 * @lhead logpack header data.
 * @is_flush flush is required.
 * @is_fua fua is required.
 * @bioe_list must be empty.
 *     submitted lhead bio(s) will be added to this.
 * @pbs physical block size [bytes].
 * @ldev log device.
 * @ring_buffer_off ring buffer offset [physical blocks].
 * @ring_buffer_size ring buffer size [physical blocks].
 */
static void logpack_submit_header(
	struct walb_logpack_header *lhead, bool is_flush, bool is_fua,
	struct list_head *bioe_list,
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
	bio->bi_rw = rw;
	bio->bi_end_io = bio_entry_end_io;
	bio->bi_private = bioe;
	len = bio_add_page(bio, page, pbs, offset_in_page(lhead));
	ASSERT(len == pbs);

	init_bio_entry(bioe, bio);
	ASSERT(bioe->len << 9 == pbs);

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
 * Submit all logpack bio(s) for a request.
 *
 * @biow bio wrapper(which contains original bio).
 * @lsid lsid of the bio in the logpack.
 * @is_fua true if logpack must be submitted with FUA flag.
 * @bioe_list list of bio_entry. must be empty.
 *   successfully submitted bioe(s) must be added to the tail of this.
 * @pbs physical block size [bytes]
 * @ldev log device.
 * @ring_buffer_off ring buffer offset [physical block].
 * @ring_buffer_size ring buffer size [physical block].
 *
 * RETURN:
 *   true in success, false in partially failed.
 */
static void logpack_submit_bio(
	struct bio_wrapper *biow, u64 lsid, bool is_fua,
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
	ASSERT(biow->bio);

retry_bio_entry:
	bioe = logpack_create_bio_entry(
		biow->bio, is_fua, pbs, ldev, ldev_off_pb, off_lb);
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
		LOGd_("submit_lr: bioe %p pos %"PRIu64" len %u\n",
			bioe, (u64)bioe->pos, bioe->len);
		generic_make_request(bioe->bio);
	}
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
	ASSERT(bioe->len == 0);
	
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
static void logpack_submit_flush(struct block_device *bdev, struct list_head *bioe_list)
{
	struct bio_entry *bioe;
	ASSERT(bdev);
	ASSERT(bioe_list);

retry:
	bioe = submit_flush(bdev);
	if (!bioe) {
		schedule();
		goto retry;
	}
	list_add_tail(&bioe->list, bioe_list);
}


/**
 * Submit logpack entry.
 *
 * @logh logpack header.
 * @is_fua FUA flag.
 * @biow_list bio wrapper list. must not be empty.
 * @bioe_list bio entry list. must be empty.
 *   submitted bios for logpack header will be added to the list.
 * @pbs physical block size.
 * @ldev log block device.
 * @ring_buffer_off ring buffer offset.
 * @ring_buffer_size ring buffer size.
 * @chunk_sectors chunk_sectors for bio alignment.
 *
 * CONTEXT:
 *     Non-IRQ. Non-atomic.
 */
static void logpack_submit(
	struct walb_logpack_header *logh, bool is_fua,
	struct list_head *biow_list, struct list_head *bioe_list,
	unsigned int pbs, struct block_device *ldev,
	u64 ring_buffer_off, u64 ring_buffer_size,
	unsigned int chunk_sectors)
{
	struct bio_wrapper *biow;
	bool is_flush;
	u64 lsid;
	int i;

	ASSERT(list_empty(bioe_list));
	ASSERT(!list_empty(biow_list));
	is_flush = is_flush_first_bio_wrapper(biow_list);

	/* Submit logpack header block. */
	logpack_submit_header(logh, is_flush, is_fua,
			bioe_list, pbs, ldev,
			ring_buffer_off, ring_buffer_size,
			chunk_sectors);
	ASSERT(!list_empty(bioe_list));
	
	/* Submit logpack contents for each request. */
	i = 0;
	list_for_each_entry(biow, biow_list, list) {

		if (biow->len == 0) {
			ASSERT(biow->bio->bi_rw & REQ_FLUSH); /* such bio must be flush. */
			ASSERT(i == 0); /* such bio must be permitted at first only. */
			ASSERT(is_flush); /* logpack header bio must have REQ_FLUSH. */
			/* You do not need to submit it
			   because logpack header bio already has REQ_FLUSH. */
		} else {
			if (logh->record[i].is_padding) {
				i ++;
				/* padding record never come last. */
			}
			ASSERT(i < logh->n_records);
			lsid = logh->record[i].lsid;

			/* submit bio(s) for the biow. */
			logpack_submit_bio(
				biow, lsid, is_fua, &biow->bioe_list,
				pbs, ldev, ring_buffer_off, ring_buffer_size,
				chunk_sectors);
		}
		i ++;
	}
}

/**
 * Create logpack list using bio_wrapper(s) in biow_list,
 * and add to wpack_list.
 *
 * @wdev wrapper block device.
 * @biow_list list of bio_wrapper.
 *   When all bio wrappers are uccessfuly processed,
 *   biow_list will be empty.
 *   When memory allocation errors occur,
 *   biow_list will not be empty.
 * @wpack_list list of pack (must be empty).
 *   Finally all biow(s) in the biow_list will be
 *   moved to pack(s) in the wpack_list.
 */
static void logpack_list_create(
	struct wrapper_blk_dev *wdev, struct list_head *biow_list,
	struct list_head *wpack_list)
{
	struct pdata *pdata;
	struct bio_wrapper *biow, *biow_next;
	struct pack *wpack = NULL;
	u64 latest_lsid, latest_lsid_old;
	bool ret;

	ASSERT(wdev);
	pdata = pdata_get_from_wdev(wdev);
	ASSERT(pdata);
	ASSERT(list_empty(wpack_list));
	ASSERT(!list_empty(biow_list));

	/* Load latest_lsid */
	spin_lock(&pdata->lsid_lock);
	latest_lsid = pdata->latest_lsid;
	spin_unlock(&pdata->lsid_lock);
	latest_lsid_old = latest_lsid;

	/* Create logpack(s). */
	list_for_each_entry_safe(biow, biow_next, biow_list, list) {
		list_del(&biow->list);
	retry:
		ret = writepack_add_bio_wrapper(
			wpack_list, &wpack, biow,
			pdata->ring_buffer_size, pdata->max_logpack_pb,
			&latest_lsid, wdev, GFP_NOIO);
		if (!ret) {
			schedule();
			goto retry;
		}
	}
	if (wpack) {
		writepack_check_and_set_flush(wpack);
		list_add_tail(&wpack->list, wpack_list);
		latest_lsid = get_next_lsid_unsafe(
			get_logpack_header(wpack->logpack_header_sector));
	}

	/* Currently all requests are packed and lsid of all writepacks is defined. */
	ASSERT(is_valid_pack_list(wpack_list));
	ASSERT(!list_empty(wpack_list));

	/* Store latest_lsid */
	ASSERT(latest_lsid >= latest_lsid_old);
	spin_lock(&pdata->lsid_lock);
	ASSERT(pdata->latest_lsid == latest_lsid_old);
	pdata->latest_lsid = latest_lsid;
	spin_unlock(&pdata->lsid_lock);
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
	struct multimap *overlapping_data,
	unsigned int *max_sectors_p,
	struct bio_wrapper *biow, gfp_t gfp_mask)
{
	struct multimap_cursor cur;
	u64 max_io_size, start_pos;
	int ret;
	struct bio_wrapper *biow_tmp;

	ASSERT(overlapping_data);
	ASSERT(max_sectors_p);
	ASSERT(biow);
	ASSERT(biow->len > 0);

	/* Decide search start position. */
	max_io_size = *max_sectors_p;
	if (biow->pos > max_io_size) {
		start_pos = biow->pos - max_io_size;
	} else {
		start_pos = 0;
	}

	multimap_cursor_init(overlapping_data, &cur);
	biow->n_overlapping = 0;
	
	/* Search the smallest candidate. */
	if (!multimap_cursor_search(&cur, start_pos, MAP_SEARCH_GE, 0)) {
		goto fin;
	}

	/* Count overlapping requests previously. */
	while (multimap_cursor_key(&cur) < biow->pos + biow->len) {

		ASSERT(multimap_cursor_is_valid(&cur));
		
		biow_tmp = (struct bio_wrapper *)multimap_cursor_val(&cur);
		ASSERT(biow_tmp);
		if (bio_wrapper_is_overlap(biow, biow_tmp)) {
			biow->n_overlapping ++;
		}
		if (!multimap_cursor_next(&cur)) {
			break;
		}
	}
#if 0
	/* debug */
	if (biow->n_overlapping > 0) {
		LOGn("n_overlapping %u\n", biow->n_overlapping);
	}
#endif

fin:
	ret = multimap_add(overlapping_data, biow->pos, (unsigned long)biow, gfp_mask);
	ASSERT(ret != -EEXIST);
	ASSERT(ret != -EINVAL);
	if (ret) {
		ASSERT(ret == -ENOMEM);
		LOGe("overlapping_check_and_insert failed.\n");
		return false;
	}
	*max_sectors_p = max(*max_sectors_p, biow->len);
	if (biow->n_overlapping == 0) {
		complete(&biow->overlapping_done);
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
	struct multimap *overlapping_data,
	unsigned int *max_sectors_p,
	struct bio_wrapper *biow)
{
	struct multimap_cursor cur;
	u64 max_io_size, start_pos;
	struct bio_wrapper *biow_tmp;

	ASSERT(overlapping_data);
	ASSERT(max_sectors_p);
	ASSERT(biow);
	ASSERT(biow->n_overlapping == 0);
	
	max_io_size = *max_sectors_p;
	if (biow->pos > max_io_size) {
		start_pos = biow->pos - max_io_size;
	} else {
		start_pos = 0;
	}

	/* Delete from the overlapping data. */
	biow_tmp = (struct bio_wrapper *)multimap_del(
		overlapping_data, biow->pos, (unsigned long)biow);
	LOGd_("biow_tmp %p biow %p\n", biow_tmp, biow); /* debug */
	ASSERT(biow_tmp == biow);

	/* Initialize max_sectors. */
	if (multimap_is_empty(overlapping_data)) {
		*max_sectors_p = 0;
	}
	
	/* Search the smallest candidate. */
	multimap_cursor_init(overlapping_data, &cur);
	if (!multimap_cursor_search(&cur, start_pos, MAP_SEARCH_GE, 0)) {
		return;
	}
	/* Decrement count of overlapping requests afterward and notify if need. */
	while (multimap_cursor_key(&cur) < biow->pos + biow->len) {

		ASSERT(multimap_cursor_is_valid(&cur));
		
		biow_tmp = (struct bio_wrapper *)multimap_cursor_val(&cur);
		ASSERT(biow_tmp);
		if (bio_wrapper_is_overlap(biow, biow_tmp)) {
			ASSERT(biow_tmp->n_overlapping > 0);
			biow_tmp->n_overlapping --;
			if (biow_tmp->n_overlapping == 0) {
				/* There is no overlapping request before it. */
				complete(&biow_tmp->overlapping_done);
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
	struct multimap *pending_data,
	unsigned int *max_sectors_p,
	struct bio_wrapper *biow, gfp_t gfp_mask)
{
	int ret;

	ASSERT(pending_data);
	ASSERT(max_sectors_p);
	ASSERT(biow);
	ASSERT(biow->bio);
	ASSERT(biow->bio->bi_rw & REQ_WRITE);
	ASSERT(biow->len > 0);
	
	/* Insert the entry. */
	ret = multimap_add(pending_data, biow->pos,
			(unsigned long)biow, gfp_mask);
	ASSERT(ret != EEXIST);
	ASSERT(ret != EINVAL);
	if (ret) {
		ASSERT(ret == ENOMEM);
		LOGe("pending_insert failed.\n");
		return false;
	}
	*max_sectors_p = max(*max_sectors_p, biow->len);
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
	struct multimap *pending_data,
	unsigned int *max_sectors_p,
	struct bio_wrapper *biow)
{
	struct bio_wrapper *biow_tmp;

	ASSERT(pending_data);
	ASSERT(max_sectors_p);
	ASSERT(biow);
	
	/* Delete the entry. */
	biow_tmp = (struct bio_wrapper *)multimap_del(
		pending_data, biow->pos, (unsigned long)biow);
	LOGd_("biow_tmp %p biow %p\n", biow_tmp, biow);
	ASSERT(biow_tmp == biow);
	if (multimap_is_empty(pending_data)) {
		*max_sectors_p = 0;
	}
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
static bool pending_check_and_copy(
	struct multimap *pending_data, unsigned int max_sectors,
	struct bio_wrapper *biow, gfp_t gfp_mask)
{
	struct multimap_cursor cur;
	u64 max_io_size, start_pos;
	struct bio_wrapper *biow_tmp;

	ASSERT(pending_data);
	ASSERT(biow);

	/* Decide search start position. */
	max_io_size = max_sectors;
	if (biow->pos > max_io_size) {
		start_pos = biow->pos - max_io_size;
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
	while (multimap_cursor_key(&cur) < biow->pos + biow->len) {

		ASSERT(multimap_cursor_is_valid(&cur));
		
		biow_tmp = (struct bio_wrapper *)multimap_cursor_val(&cur);
		ASSERT(biow_tmp);
		if (bio_wrapper_is_overlap(biow, biow_tmp)) {
			if (!data_copy_bio_wrapper(biow, biow_tmp, gfp_mask)) {
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
 *   pending_data_lock must be held.
 */
#ifdef WALB_FAST_ALGORITHM
static inline bool should_stop_queue(struct pdata *pdata, struct bio_wrapper *biow)
{
	bool should_stop;
	ASSERT(pdata);
	ASSERT(biow);

	if (pdata->is_queue_stopped) {
		return false;
	}

	should_stop = pdata->pending_sectors + biow->len
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
 *   pending_data_lock must be held.
 */
#ifdef WALB_FAST_ALGORITHM
static inline bool should_start_queue(struct pdata *pdata, struct bio_wrapper *biow)
{
	bool is_size;
	bool is_timeout;
	ASSERT(pdata);
	ASSERT(biow);
	ASSERT(pdata->pending_sectors >= biow->len);

	if (!pdata->is_queue_stopped) {
		return false;
	}
	
	is_size = pdata->pending_sectors - biow->len
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
 * Submit bio wrapper for read.
 *
 * @wdev wrapper block device.
 * @biow bio wrapper (read).
 */
static void submit_bio_wrapper_read(
	struct wrapper_blk_dev *wdev, struct bio_wrapper *biow)
{
	struct pdata *pdata = pdata_get_from_wdev(wdev);
	bool ret;

	ASSERT(biow);
	ASSERT(biow->bio);
	
	/* Create cloned bio. */
	if (!create_bio_entry_list(biow, pdata->ddev)) {
		goto error0;
	}

	/* Split if required due to chunk limitations. */
	if (!split_bio_entry_list_for_chunk(
			&biow->bioe_list, pdata->ddev_chunk_sectors, GFP_NOIO)) {
		goto error1;
	}

	/* Check pending data and copy data from executing write requests. */
	spin_lock(&pdata->pending_data_lock);
	ret = pending_check_and_copy(pdata->pending_data,
				pdata->max_sectors_in_pending,
				biow, GFP_ATOMIC);
	spin_unlock(&pdata->pending_data_lock);
	if (!ret) {
		goto error1;
	}
	
	/* Submit all related bio(s). */
	submit_bio_entry_list(&biow->bioe_list);

	/* Enqueue wait/gc task. */
	INIT_WORK(&biow->work, bio_wrapper_read_wait_and_gc_task);
	queue_work(wq_io_, &biow->work);
	
	return;
error1:
	destroy_bio_entry_list(&biow->bioe_list);
error0:
	bio_endio(biow->bio, -ENOMEM);
	ASSERT(list_empty(&biow->bioe_list));
	destroy_bio_wrapper(biow);
}

/**
 * Make request callback.
 */
static void wrapper_blk_make_request_fn(struct request_queue *q, struct bio *bio)
{
	struct wrapper_blk_dev *wdev;
	struct pdata *pdata;
	struct bio_wrapper *biow;
	struct pack_work *pwork;
	int error = -EIO;
	
	ASSERT(q);
	ASSERT(bio);
	wdev = wdev_get_from_queue(q);
	ASSERT(wdev);
	pdata = pdata_get_from_wdev(wdev);
	ASSERT(pdata);

	/* Create bio wrapper. */
	biow = alloc_bio_wrapper(GFP_NOIO);
	if (!biow) {
		error = -ENOMEM;
		goto error0;
	}
	init_bio_wrapper(biow, bio);

	if (biow->bio->bi_rw & REQ_WRITE) {
		/* Calculate checksum. */
		biow->csum = bio_calc_checksum(biow->bio);
		
		/* Push into queue and (re)start logpack_submit task */
		spin_lock(&pdata->logpack_submit_queue_lock);
		list_add_tail(&biow->list, &pdata->logpack_submit_queue);
		spin_unlock(&pdata->logpack_submit_queue_lock);

		if (test_and_set_bit(
				PDATA_STATE_SUBMIT_TASK_WORKING,
				&pdata->flags) == 0) {
			pwork = create_pack_work(wdev, GFP_NOIO);
			if (!pwork) {
				error = -ENOMEM;
				goto error1;
			}
			INIT_WORK(&pwork->work, logpack_list_submit_task);
			queue_work(wq_io_, &pwork->work);
		}
	} else { /* read */
		submit_bio_wrapper_read(wdev, biow);
	}
	return;
error1:
	destroy_bio_wrapper(biow);
error0:
	bio_endio(bio, error);
}

/* Called before register. */
static bool pre_register(void)
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
	if (!bio_wrapper_init()) {
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
	wq_io_ = alloc_workqueue(WQ_IO, WQ_MEM_RECLAIM, 0);
	if (!wq_io_) {
		LOGe("failed to allocate a workqueue (wq_io_).");
		goto error4;
	}

	if (!treemap_init()) {
		goto error5;
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
error6:
	treemap_exit();
#endif
error5:
	destroy_workqueue(wq_io_);
error4:
	bio_entry_exit();
error3:
	kmem_cache_destroy(pack_cache_);
error2:
	bio_wrapper_exit();
error1:
	kmem_cache_destroy(pack_work_cache_);
error0:
	return false;
}

static void flush_all_wq(void)
{
	flush_workqueue(wq_io_); /* complete logpack submit task. */
	flush_workqueue(wq_io_); /* complete logpack wait task. */
	flush_workqueue(wq_io_); /* complete data io task. */
	flush_workqueue(wq_io_); /* complete logpack gc task. */
}

/* Called before unregister. */
static void pre_unregister(void)
{
	LOGn("begin\n");
	flush_all_wq();
	LOGn("end\n");
}

/* Called before destroy_private_data. */
static void pre_destroy_private_data(void)
{
	LOGn("begin\n");
	flush_all_wq();
	LOGn("end\n");
}

/* Called after unregister. */
static void post_unregister(void)
{
	LOGd_("begin\n");

	treemap_exit();
	
	/* finalize workqueue data. */
	destroy_workqueue(wq_io_);
	wq_io_ = NULL;

	/* Destory kmem_cache data. */
	bio_entry_exit();
	kmem_cache_destroy(pack_cache_);
	pack_cache_ = NULL;
	bio_wrapper_exit();
	kmem_cache_destroy(pack_work_cache_);
	pack_work_cache_ = NULL;

	LOGd_("end\n");
}

/*******************************************************************************
 * Init/exit definition.
 *******************************************************************************/

static int __init wrapper_blk_init(void)
{
	if (!is_valid_pbs(physical_block_size_)) {
		LOGe("pbs is invalid.\n");
		goto error0;
	}
	if (queue_stop_timeout_ms_ < 1) {
		LOGe("queue_stop_timeout_ms must > 0.\n");
		goto error0;
	}
	if (max_logpack_size_kb_ < 0 ||
		(max_logpack_size_kb_ * 1024) % physical_block_size_ != 0) {
		LOGe("max_logpack_size_kb must >= 0 and"
			" the integral multiple of physical block size if positive.\n");
		goto error0;
	}

        if (!pre_register()) {
		LOGe("pre_register failed.\n");
		goto error0;
	}
        
        if (!register_dev()) {
                goto error1;
        }
        if (!start_dev()) {
                goto error2;
        }

        return 0;
#if 0
error3:
        stop_dev();
#endif
error2:
	pre_unregister();
        unregister_dev();
error1:
	post_unregister();
error0:
        return -1;
}

static void wrapper_blk_exit(void)
{
        stop_dev();
	pre_unregister();
        unregister_dev();
        post_unregister();
}

module_init(wrapper_blk_init);
module_exit(wrapper_blk_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Walb block bio device for Test");
MODULE_ALIAS("wrapper_blk_walb_bio");

/* end of file. */
