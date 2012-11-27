/**
 * walb_proto_req.c - WalB block device with request base for test.
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
#include "bio_entry.h"
#include "req_entry.h"
#include "worker.h"
#include "pack_work.h"

/*******************************************************************************
 * Module variables definition.
 *******************************************************************************/

/* Device size list string. The unit of each size is bytes. */
char *log_device_str_ = "/dev/simple_blk/0";
char *data_device_str_ = "/dev/simple_blk/1";
/* Minor id start. */
int start_minor_ = 0;

/* Physical block size [bytes]. */
int physical_block_size_ = 4096;

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
 * GC worker name.
 */
#define WORKER_NAME_GC "walb_gc"

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
	atomic_t n_logpack_submit_queue;
	spinlock_t logpack_wait_queue_lock;
	struct list_head logpack_wait_queue; /* writepack list.
						logpack_wait_queue_lock
						must be held. */
	atomic_t n_logpack_wait_queue;
	spinlock_t logpack_gc_queue_lock;
	struct list_head logpack_gc_queue; /* writepack list.
					      logpack_gc_lock
					      must be held. */
	atomic_t n_logpack_gc_queue;
	struct worker_data gc_worker_data; /* for gc worker. */

	unsigned int max_logpack_pb; /* Maximum logpack size [physical block].
					This will be used for logpack size
					not to be too long
					This will avoid decrease of
					sequential write performance. */

	atomic_t n_pending_req; /* Number of pending request(s).
				   This will be used for exit. */

#ifdef WALB_OVERLAPPING_SERIALIZE
	/**
	 * All req_entry data may not keep reqe->bio_ent_list.
	 * You must keep address and size information in another way.
	 */
	spinlock_t overlapping_data_lock; /* Use spin_lock()/spin_unlock(). */
	struct multimap *overlapping_data; /* key: blk_rq_pos(req),
					      val: pointer to req_entry. */
	unsigned int max_req_sectors_in_overlapping; /* Maximum request size [logical block]. */
#endif

#ifdef WALB_FAST_ALGORITHM
	/**
	 * All req_entry data must keep
	 * reqe->bio_ent_list while they are stored in the pending_data.
	 */
	spinlock_t pending_data_lock; /* Use spin_lock()/spin_unlock(). */
	struct multimap *pending_data; /* key: blk_rq_pos(req),
					  val: pointer to req_entry. */
	unsigned int max_req_sectors_in_pending; /* Maximum request size [logical block]. */

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

/* All treemap(s) in this module will share a treemap memory manager. */
static atomic_t n_users_of_memory_manager_ = ATOMIC_INIT(0);
static struct treemap_memory_manager mmgr_;
#define TREE_NODE_CACHE_NAME "walb_proto_req_node_cache"
#define TREE_CELL_HEAD_CACHE_NAME "walb_proto_req_cell_head_cache"
#define TREE_CELL_CACHE_NAME "walb_proto_req_cell_cache"
#define N_ITEMS_IN_MEMPOOL (128 * 2) /* for pending data and overlapping data. */

/*******************************************************************************
 * Macros definition.
 *******************************************************************************/

/* pdata->flags bit. */
#define PDATA_STATE_READ_ONLY		 0
#define PDATA_STATE_SUBMIT_TASK_WORKING 1
#define PDATA_STATE_WAIT_TASK_WORKING	2
#define PDATA_STATE_FAILURE		 3

#define get_pdata_from_wrdev(wrdev) ((struct pdata *)wrdev->private_data)

#define N_PACK_BULK 32

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

/* Make requrest for wrapper_blk_walb_* modules. */
static void wrapper_blk_req_request_fn(struct request_queue *q);

/* Called before register. */
static bool pre_register(void);

/* Called before unregister */
static void pre_unregister(void);

/* Called just before destroy_private_data. */
static void pre_destroy_private_data(void);

/* Called after unregister. */
static void post_unregister(void);

/* Create private data for wrdev. */
static bool create_private_data(struct wrapper_blk_dev *wrdev);
/* Destroy private data for ssev. */
static void destroy_private_data(struct wrapper_blk_dev *wrdev);
/* Customize wrdev after register before start. */
static void customize_wrdev(struct wrapper_blk_dev *wrdev);

static unsigned int get_minor(unsigned int id);
static bool register_dev(void);
static void unregister_dev(void);
static bool start_dev(void);
static void stop_dev(void);

/* Print functions for debug. */
UNUSED static void print_req_flags(struct request *req);
UNUSED static void print_pack(const char *level, struct pack *pack);
UNUSED static void print_pack_list(const char *level, struct list_head *wpack_list);

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
	struct wrapper_blk_dev *wrdev, gfp_t gfp_mask);
static bool is_flush_first_req_entry(struct list_head *req_ent_list);
static struct req_entry* create_req_entry_inc(
	struct request *req, struct wrapper_blk_dev *wrdev, gfp_t gfp_mask);
static void destroy_req_entry_dec(struct req_entry *reqe);

/* Workqueue tasks. */
static void logpack_list_submit_task(struct work_struct *work);
static void logpack_list_wait_task(struct work_struct *work);
static void gc_logpack_list(struct pdata *pdata, struct list_head *wpack_list);
static void write_req_task(struct work_struct *work);
static void read_req_task(struct work_struct *work);

/* Thread worer. */
static void run_gc_logpack_list(void *data);
static void dequeue_and_gc_logpack_list(struct pdata *pdata);

/* Helper functions for tasks. */
static void logpack_list_submit(
	struct wrapper_blk_dev *wrdev, struct list_head *wpack_list);
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
	struct pack *wpack, struct wrapper_blk_dev *wrdev);
#ifdef WALB_FAST_ALGORITHM
static void wait_logpack_and_enqueue_datapack_tasks_fast(
	struct pack *wpack, struct wrapper_blk_dev *wrdev);
#else
static void wait_logpack_and_enqueue_datapack_tasks_easy(
	struct pack *wpack, struct wrapper_blk_dev *wrdev);
#endif

/* Overlapping data functions. */
#ifdef WALB_OVERLAPPING_SERIALIZE
static bool overlapping_check_and_insert(
	struct multimap *overlapping_data, unsigned int *max_req_sectors_p,
	struct req_entry *reqe, gfp_t gfp_mask);
static void overlapping_delete_and_notify(
	struct multimap *overlapping_data, unsigned int *max_req_sectors_p,
	struct req_entry *reqe);
#endif

/* Pending data functions. */
#ifdef WALB_FAST_ALGORITHM
static bool pending_insert(
	struct multimap *pending_data, unsigned int *max_req_sectors_p,
	struct req_entry *reqe, gfp_t gfp_mask);
static void pending_delete(
	struct multimap *pending_data, unsigned int *max_req_sectors_p,
	struct req_entry *reqe);
static bool pending_check_and_copy(
	struct multimap *pending_data, unsigned int max_req_sectors,
	struct req_entry *reqe, gfp_t gfp_mask);
static inline bool should_stop_queue(struct pdata *pdata, struct req_entry *reqe);
static inline bool should_start_queue(struct pdata *pdata, struct req_entry *reqe);
#endif

/* For overlapping data and pending data. */
#if defined(WALB_OVERLAPPING_SERIALIZE) || defined(WALB_FAST_ALGORITHM)
static inline bool is_overlap_req_entry(struct req_entry *reqe0, struct req_entry *reqe1);
#endif

static void flush_all_wq(void);

/* For treemap memory manager. */
static bool treemap_memory_manager_inc(void);
static void treemap_memory_manager_dec(void);

/*******************************************************************************
 * For debug.
 *******************************************************************************/

#ifdef PERIODIC_DEBUG
#error
#else
/* #define PERIODIC_DEBUG */
#endif

#ifdef PERIODIC_DEBUG
#define PERIODIC_PRINT_INTERVAL_MS 1000
static struct delayed_work shared_dwork_;
static struct pdata *pdata_;

static void task_periodic_print(struct work_struct *work)
{
	struct delayed_work *dwork = container_of(work, struct delayed_work, work);

	if (!pdata_) {
		LOGn("pdata_ is not assigned.\n");
		goto fin;
	}
	LOGn("n_pending_req %d\n"
		"queue length: submit %d wait %d gc %d\n",
		atomic_read(&pdata_->n_pending_req),
		atomic_read(&pdata_->n_logpack_submit_queue),
		atomic_read(&pdata_->n_logpack_wait_queue),
		atomic_read(&pdata_->n_logpack_gc_queue));

fin:
	INIT_DELAYED_WORK(dwork, task_periodic_print);
	queue_delayed_work(system_wq, dwork,
			msecs_to_jiffies(PERIODIC_PRINT_INTERVAL_MS));
}

static void start_periodic_print_for_debug(struct pdata *pdata)
{
	ASSERT(pdata);
	pdata_ = pdata;

	INIT_DELAYED_WORK(&shared_dwork_, task_periodic_print);
	queue_delayed_work(system_wq, &shared_dwork_,
			msecs_to_jiffies(PERIODIC_PRINT_INTERVAL_MS));
}

static void stop_periodic_print_for_debug(void)
{
	cancel_delayed_work_sync(&shared_dwork_);
	pdata_ = NULL;
}
#endif /* PERIODIC_DEBUG */

/*******************************************************************************
 * Utility functions.
 *******************************************************************************/

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

/* Create private data for wrdev. */
static bool create_private_data(struct wrapper_blk_dev *wrdev)
{
	struct pdata *pdata;
	struct block_device *ldev, *ddev;
	unsigned int lbs, pbs;
	struct walb_super_sector *ssect;
	struct request_queue *lq, *dq;
	int ret;

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
	pdata->overlapping_data = multimap_create(GFP_KERNEL, &mmgr_);
	if (!pdata->overlapping_data) {
		LOGe("multimap creation failed.\n");
		goto error11;
	}
	pdata->max_req_sectors_in_overlapping = 0;
#endif
#ifdef WALB_FAST_ALGORITHM
	spin_lock_init(&pdata->pending_data_lock);
	pdata->pending_data = multimap_create(GFP_KERNEL, &mmgr_);
	if (!pdata->pending_data) {
		LOGe("multimap creation failed.\n");
		goto error12;
	}
	pdata->max_req_sectors_in_pending = 0;

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
		goto error2;
	}
	LOGn("ldev (%d,%d) %d\n", MAJOR(ldev->bd_dev), MINOR(ldev->bd_dev),
		ldev->bd_contains == ldev);

	/* open underlying data device. */
	ddev = blkdev_get_by_path(
		data_device_str_, FMODE_READ|FMODE_WRITE|FMODE_EXCL,
		create_private_data);
	if (IS_ERR(ddev)) {
		LOGe("open %s failed.", data_device_str_);
		goto error3;
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
		goto error4;
	}
	ASSERT(bdev_logical_block_size(ldev) == lbs);
	if (bdev_physical_block_size(ldev) != pbs) {
		LOGe("physical block size is different (ldev: %u, ddev: %u).\n",
			bdev_physical_block_size(ldev), pbs);
		goto error4;
	}
	wrdev->pbs = pbs;
	blk_set_default_limits(&wrdev->queue->limits);
	blk_queue_logical_block_size(wrdev->queue, lbs);
	blk_queue_physical_block_size(wrdev->queue, pbs);
	/* blk_queue_io_min(wrdev->queue, pbs); */
	/* blk_queue_io_opt(wrdev->queue, pbs); */

	/* Set max_logpack_pb. */
	ASSERT(max_logpack_size_kb_ >= 0);
	ASSERT((max_logpack_size_kb_ * 1024) % pbs == 0);
	pdata->max_logpack_pb = (max_logpack_size_kb_ * 1024) / pbs;
	LOGn("max_logpack_size_kb: %u max_logpack_pb: %u\n",
		max_logpack_size_kb_, pdata->max_logpack_pb);

	/* Set underlying devices. */
	pdata->ldev = ldev;
	pdata->ddev = ddev;
	wrdev->private_data = pdata;

	/* Load super block. */
	pdata->lsuper0 = sector_alloc(pbs, GFP_KERNEL);
	if (!pdata->lsuper0) {
		goto error4;
	}
	if (!walb_read_super_sector(pdata->ldev, pdata->lsuper0)) {
		LOGe("read super sector 0 failed.\n");
		goto error5;
	}
	ssect = get_super_sector(pdata->lsuper0);
	pdata->written_lsid = ssect->written_lsid;
	pdata->oldest_lsid = ssect->oldest_lsid;
	pdata->latest_lsid = pdata->written_lsid; /* redo must be done. */
	pdata->ring_buffer_size = ssect->ring_buffer_size;
	pdata->ring_buffer_off = get_ring_buffer_offset_2(ssect);
	pdata->flags = 0;

	/* capacity */
	wrdev->capacity = ddev->bd_part->nr_sects;
	set_capacity(wrdev->gd, wrdev->capacity);
	LOGn("capacity %"PRIu64"\n", wrdev->capacity);

	/* Set limit. */
	lq = bdev_get_queue(ldev);
	dq = bdev_get_queue(ddev);
	blk_queue_stack_limits(wrdev->queue, lq);
	blk_queue_stack_limits(wrdev->queue, dq);
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
	LOGn("wrdev limits: lbs %u pbs %u io_min %u io_opt %u max_hw_sec %u max_sectors %u align %u\n",
		wrdev->queue->limits.logical_block_size,
		wrdev->queue->limits.physical_block_size,
		wrdev->queue->limits.io_min,
		wrdev->queue->limits.io_opt,
		wrdev->queue->limits.max_hw_sectors,
		wrdev->queue->limits.max_sectors,
		wrdev->queue->limits.alignment_offset);

	/* Chunk size. */
	if (queue_io_min(lq) > wrdev->pbs) {
		pdata->ldev_chunk_sectors = queue_io_min(lq) / LOGICAL_BLOCK_SIZE;
	} else {
		pdata->ldev_chunk_sectors = 0;
	}
	if (queue_io_min(dq) > wrdev->pbs) {
		pdata->ddev_chunk_sectors = queue_io_min(dq) / LOGICAL_BLOCK_SIZE;
	} else {
		pdata->ddev_chunk_sectors = 0;
	}
	LOGn("chunk_sectors ldev %u ddev %u.\n",
		pdata->ldev_chunk_sectors, pdata->ddev_chunk_sectors);

	/* Prepare logpack submit/wait queue. */
	spin_lock_init(&pdata->logpack_submit_queue_lock);
	spin_lock_init(&pdata->logpack_wait_queue_lock);
	spin_lock_init(&pdata->logpack_gc_queue_lock);
	INIT_LIST_HEAD(&pdata->logpack_submit_queue);
	INIT_LIST_HEAD(&pdata->logpack_wait_queue);
	INIT_LIST_HEAD(&pdata->logpack_gc_queue);
#ifdef PERIODIC_DEBUG
	atomic_set(&pdata->n_logpack_submit_queue, 0);
	atomic_set(&pdata->n_logpack_wait_queue, 0);
	atomic_set(&pdata->n_logpack_gc_queue, 0);
#endif

	/* Initialize n_pending_req. */
	atomic_set(&pdata->n_pending_req, 0);

	/* Prepare GC worker. */
	ret = snprintf(pdata->gc_worker_data.name, WORKER_NAME_MAX_LEN,
		"%s/%u", WORKER_NAME_GC, wrdev->minor);
	if (ret >= WORKER_NAME_MAX_LEN) {
		LOGe("Thread name size too long.\n");
		goto error5;
	}
	initialize_worker(&pdata->gc_worker_data,
			run_gc_logpack_list, (void *)wrdev);

	return true;

error5:
	sector_free(pdata->lsuper0);
error4:
	blkdev_put(ddev, FMODE_READ|FMODE_WRITE|FMODE_EXCL);
error3:
	blkdev_put(ldev, FMODE_READ|FMODE_WRITE|FMODE_EXCL);
error2:
#ifdef WALB_FAST_ALGORITHM
error12:
	multimap_destroy(pdata->pending_data);
#endif
#ifdef WALB_OVERLAPPING_SERIALIZE
error11:
	multimap_destroy(pdata->overlapping_data);
#endif
	kfree(pdata);
	wrdev->private_data = NULL;
error0:
	return false;
}

/* Destroy private data for ssev. */
static void destroy_private_data(struct wrapper_blk_dev *wrdev)
{
	struct pdata *pdata;
	struct walb_super_sector *ssect;

	LOGd("destoroy_private_data called.");

	pdata = wrdev->private_data;
	if (!pdata) { return; }
	ASSERT(pdata);

	/* Finalize worker. */
	finalize_worker(&pdata->gc_worker_data);

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
	wrdev->private_data = NULL;
}

/* Customize wrdev after register before start. */
static void customize_wrdev(struct wrapper_blk_dev *wrdev)
{
	struct request_queue *q, *lq, *dq;
	struct pdata *pdata;
	ASSERT(wrdev);
	q = wrdev->queue;
	pdata = wrdev->private_data;

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
	struct wrapper_blk_dev *wrdev;

	LOGn("begin\n");

	/* capacity must be set lator. */
	ret = wrdev_register_with_req(get_minor(i), capacity,
				physical_block_size_,
				wrapper_blk_req_request_fn);

	if (!ret) {
		goto error;
	}
	wrdev = wrdev_get(get_minor(i));
	if (!create_private_data(wrdev)) {
		goto error;
	}
	customize_wrdev(wrdev);

	LOGn("end\n");

	return true;
error:
	unregister_dev();
	return false;
}

static void unregister_dev(void)
{
	unsigned int i = 0;
	struct wrapper_blk_dev *wrdev;

	LOGn("begin\n");

	wrdev = wrdev_get(get_minor(i));
	wrdev_unregister(get_minor(i));
	if (wrdev) {
		pre_destroy_private_data();
		destroy_private_data(wrdev);
		FREE(wrdev);
	}

	LOGn("end\n");
}

static bool start_dev(void)
{
	unsigned int i = 0;
	unsigned int minor;
	struct wrapper_blk_dev *wrdev;

	minor = get_minor(i);
	wrdev = wrdev_get(minor);
	ASSERT(wrdev);

#ifdef PERIODIC_DEBUG
	start_periodic_print_for_debug(get_pdata_from_wrdev(wrdev));
#endif
	if (!wrdev_start(minor)) {
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
	unsigned int minor;
	struct wrapper_blk_dev *wrdev;
	struct pdata *pdata;

#ifdef PERIODIC_DEBUG
	stop_periodic_print_for_debug();
#endif

	minor = get_minor(i);
	wrdev_stop(minor);
	wrdev = wrdev_get(minor);
	ASSERT(wrdev);
	pdata = get_pdata_from_wrdev(wrdev);
	ASSERT(pdata);

	/* Flush all pending IOs. */
	set_bit(PDATA_STATE_FAILURE, &pdata->flags);
	LOGn("n_pending_req %d\n", atomic_read(&pdata->n_pending_req));
	while (atomic_read(&pdata->n_pending_req) > 0) {
		LOGn("n_pending_req %d\n", atomic_read(&pdata->n_pending_req));
		msleep(100);
	}
	flush_all_wq();
	LOGn("n_pending_req %d\n", atomic_read(&pdata->n_pending_req));
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
		((req->cmd_flags & REQ_WRITE) ?		     "REQ_WRITE" : ""),
		((req->cmd_flags & REQ_FAILFAST_DEV) ?	     " REQ_FAILFAST_DEV" : ""),
		((req->cmd_flags & REQ_FAILFAST_TRANSPORT) ? " REQ_FAILFAST_TRANSPORT" : ""),
		((req->cmd_flags & REQ_FAILFAST_DRIVER) ?    " REQ_FAILFAST_DRIVER" : ""),
		((req->cmd_flags & REQ_SYNC) ?		     " REQ_SYNC" : ""),
		((req->cmd_flags & REQ_META) ?		     " REQ_META" : ""),
		((req->cmd_flags & REQ_PRIO) ?		     " REQ_PRIO" : ""),
		((req->cmd_flags & REQ_DISCARD) ?	     " REQ_DISCARD" : ""),
		((req->cmd_flags & REQ_NOIDLE) ?	     " REQ_NOIDLE" : ""),
		((req->cmd_flags & REQ_RAHEAD) ?	     " REQ_RAHEAD" : ""),
		((req->cmd_flags & REQ_THROTTLED) ?	     " REQ_THROTTLED" : ""),
		((req->cmd_flags & REQ_SORTED) ?	     " REQ_SORTED" : ""),
		((req->cmd_flags & REQ_SOFTBARRIER) ?	     " REQ_SOFTBARRIER" : ""),
		((req->cmd_flags & REQ_FUA) ?		     " REQ_FUA" : ""),
		((req->cmd_flags & REQ_NOMERGE) ?	     " REQ_NOMERGE" : ""),
		((req->cmd_flags & REQ_STARTED) ?	     " REQ_STARTED" : ""),
		((req->cmd_flags & REQ_DONTPREP) ?	     " REQ_DONTPREP" : ""),
		((req->cmd_flags & REQ_QUEUED) ?	     " REQ_QUEUED" : ""),
		((req->cmd_flags & REQ_ELVPRIV) ?	     " REQ_ELVPRIV" : ""),
		((req->cmd_flags & REQ_FAILED) ?	     " REQ_FAILED" : ""),
		((req->cmd_flags & REQ_QUIET) ?		     " REQ_QUIET" : ""),
		((req->cmd_flags & REQ_PREEMPT) ?	     " REQ_PREEMPT" : ""),
		((req->cmd_flags & REQ_ALLOCED) ?	     " REQ_ALLOCED" : ""),
		((req->cmd_flags & REQ_COPY_USER) ?	     " REQ_COPY_USER" : ""),
		((req->cmd_flags & REQ_FLUSH) ?		     " REQ_FLUSH" : ""),
		((req->cmd_flags & REQ_FLUSH_SEQ) ?	     " REQ_FLUSH_SEQ" : ""),
		((req->cmd_flags & REQ_IO_STAT) ?	     " REQ_IO_STAT" : ""),
		((req->cmd_flags & REQ_MIXED_MERGE) ?	     " REQ_MIXED_MERGE" : ""),
		((req->cmd_flags & REQ_SECURE) ?	     " REQ_SECURE" : ""));
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
		i++;
		print_req_entry(level, reqe);
	}
	printk("%s""number of req_entry in req_ent_list: %u.\n", level, i);

	i = 0;
	list_for_each_entry(bioe, &pack->bio_ent_list, list) {
		i++;
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
		i++;
	}
	printk("%s""print_pack_list %p end.\n", level, wpack_list);
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
		destroy_req_entry_dec(reqe);
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
		i++;
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
	unsigned int pb;
	ASSERT(lhead);
	ASSERT(pbs);
	ASSERT_PBS(pbs);

	if (max_logpack_pb == 0) {
		return false;
	}

	pb = (unsigned int)capacity_pb(pbs, reqe->req_sectors);
	return pb + (unsigned int)lhead->total_io_size > max_logpack_pb;
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
 * @wrdev wrapper block device.
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
	u64 *latest_lsidp, struct wrapper_blk_dev *wrdev, gfp_t gfp_mask)
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
	ASSERT(wrdev);
	pbs = wrdev->pbs;
	ASSERT_PBS(pbs);

	pack = *wpackp;
	reqe = create_req_entry_inc(req, wrdev, gfp_mask);
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
		if ((req->cmd_flags & REQ_FLUSH)
			|| is_pack_size_exceeds(lhead, pbs, max_logpack_pb, reqe)
			|| is_overlap_pack_reqe(pack, reqe)) {
			/* overlap found so create a new pack. */
			goto newpack;
		}
#else
		/* Now we need not overlapping check in a pack
		   because atomicity is kept by unit of request. */
		if (req->cmd_flags & REQ_FLUSH
			|| is_pack_size_exceeds(lhead, pbs, max_logpack_pb, reqe)) {
			/* Flush request must be the first of the pack. */
			goto newpack;
		}
#endif
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
	destroy_req_entry_dec(reqe);
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
 * Create a request entry and increment n_pending_req.
 */
static struct req_entry* create_req_entry_inc(
	struct request *req, struct wrapper_blk_dev *wrdev, gfp_t gfp_mask)
{
	struct req_entry *reqe;

	reqe = create_req_entry(req, wrdev, gfp_mask);
	if (!reqe) {
		goto error0;
	}
	atomic_inc(&get_pdata_from_wrdev(wrdev)->n_pending_req);
	return reqe;

error0:
	return NULL;
}

/**
 * Destroy a request entry and decrement n_pending_req.
 */
static void destroy_req_entry_dec(struct req_entry *reqe)
{
	struct wrapper_blk_dev *wrdev = reqe->data;
	struct pdata *pdata = get_pdata_from_wrdev(wrdev);

	ASSERT(pdata);
	destroy_req_entry(reqe);
	atomic_dec(&pdata->n_pending_req);
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
			set_bit(BIO_UPTODATE, &bioe->bio->bi_flags);
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

	remaining = reqe->req_sectors;
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
			blk_end_request(reqe->req, bioe->error, bioe->len << 9);
		}
		remaining -= bioe->len;
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
	struct wrapper_blk_dev *wrdev, struct list_head *wpack_list)
{
	struct pdata *pdata;
	struct pack *wpack;
	struct blk_plug plug;
	struct walb_logpack_header *lhead;
	bool ret;
	ASSERT(wpack_list);
	ASSERT(wrdev);
	pdata = get_pdata_from_wrdev(wrdev);

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
			logpack_calc_checksum(lhead, wrdev->pbs, &wpack->req_ent_list);
			ret = logpack_submit(
				lhead, wpack->is_fua,
				&wpack->req_ent_list, &wpack->bio_ent_list,
				wrdev->pbs, pdata->ldev, pdata->ring_buffer_off,
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
	struct wrapper_blk_dev *wrdev = pwork->data;
	struct pdata *pdata = get_pdata_from_wrdev(wrdev);
	struct pack *wpack, *wpack_next;
	struct list_head wpack_list;
	bool is_empty, is_working;

	destroy_pack_work(pwork);
	pwork = NULL;

	while (true) {
		/* Dequeue logpack list from the submit queue. */
		INIT_LIST_HEAD(&wpack_list);
		spin_lock(&pdata->logpack_submit_queue_lock);
		is_empty = list_empty(&pdata->logpack_submit_queue);
		if (is_empty) {
			is_working =
				test_and_clear_bit(PDATA_STATE_SUBMIT_TASK_WORKING,
						&pdata->flags);
			ASSERT(is_working);
		}
		list_for_each_entry_safe(wpack, wpack_next,
					&pdata->logpack_submit_queue, list) {
			list_move_tail(&wpack->list, &wpack_list);
			atomic_dec(&pdata->n_logpack_submit_queue); /* debug */
		}
		spin_unlock(&pdata->logpack_submit_queue_lock);
		if (is_empty) { break; }

		/* Submit. */
		logpack_list_submit(wrdev, &wpack_list);

		/* Enqueue logpack list to the wait queue. */
		spin_lock(&pdata->logpack_wait_queue_lock);
		list_for_each_entry_safe(wpack, wpack_next, &wpack_list, list) {
			list_move_tail(&wpack->list, &pdata->logpack_wait_queue);
			atomic_inc(&pdata->n_logpack_wait_queue); /* debug */
		}
		ASSERT(list_empty(&wpack_list));
		spin_unlock(&pdata->logpack_wait_queue_lock);

		/* Run task. */
		enqueue_task_if_necessary(
			wrdev,
			PDATA_STATE_WAIT_TASK_WORKING,
			&pdata->flags,
			wq_logpack_,
			logpack_list_wait_task);
	}
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
				LOGn("timeout(%d): bioe %p bio %p len %u\n",
					c, bioe, bioe->bio, bioe->len);
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
 * If any write failed, wrdev will be read-only mode.
 */
static void wait_logpack_and_enqueue_datapack_tasks(
	struct pack *wpack, struct wrapper_blk_dev *wrdev)
{
#ifdef WALB_FAST_ALGORITHM
	wait_logpack_and_enqueue_datapack_tasks_fast(
		wpack, wrdev);
#else
	wait_logpack_and_enqueue_datapack_tasks_easy(
		wpack, wrdev);
#endif
}

#ifdef WALB_FAST_ALGORITHM
static void wait_logpack_and_enqueue_datapack_tasks_fast(
	struct pack *wpack, struct wrapper_blk_dev *wrdev)
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
	ASSERT(wrdev);

	/* Check read only mode. */
	pdata = get_pdata_from_wrdev(wrdev);
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
			destroy_req_entry_dec(reqe);
		} else {
			/* Create all related bio(s) by copying IO data. */
			if (!create_bio_entry_list_copy(reqe, pdata->ddev)) {
				goto failed0;
			}
			/* Split if required due to chunk limitations. */
			if (!split_bio_entry_list_for_chunk(
					&reqe->bio_ent_list,
					pdata->ddev_chunk_sectors,
					GFP_NOIO)) {
				goto failed1;
			}

			/* Get related bio(s) */
			get_bio_entry_list(&reqe->bio_ent_list);

			/* Try to insert pending data. */
			spin_lock(&pdata->pending_data_lock);
			LOGd_("pending_sectors %u\n", pdata->pending_sectors);
			is_stop_queue = should_stop_queue(pdata, reqe);
			pdata->pending_sectors += reqe->req_sectors;
			is_pending_insert_succeeded =
				pending_insert(pdata->pending_data,
					&pdata->max_req_sectors_in_pending,
					reqe, GFP_ATOMIC);
			spin_unlock(&pdata->pending_data_lock);
			if (!is_pending_insert_succeeded) { goto failed2; }

			/* Check pending data size and stop the queue if needed. */
			if (is_stop_queue) {
				LOGd("stop queue.\n");
				spin_lock_irqsave(&wrdev->lock, flags);
				blk_stop_queue(wrdev->queue);
				spin_unlock_irqrestore(&wrdev->lock, flags);
			}

			/* call end_request where with fast algorithm
			   while easy algorithm call it after data device IO. */
			blk_end_request_all(req, 0);
#ifdef WALB_OVERLAPPING_SERIALIZE
			/* check and insert to overlapping detection data. */
			spin_lock(&pdata->overlapping_data_lock);
			is_overlapping_insert_succeeded =
				overlapping_check_and_insert(pdata->overlapping_data,
							&pdata->max_req_sectors_in_overlapping,
							reqe, GFP_ATOMIC);
			spin_unlock(&pdata->overlapping_data_lock);
			if (!is_overlapping_insert_succeeded) {
				spin_lock(&pdata->pending_data_lock);
				pending_delete(pdata->pending_data,
					&pdata->max_req_sectors_in_pending, reqe);
				pdata->pending_sectors -= reqe->req_sectors;
				spin_unlock(&pdata->pending_data_lock);
				if (is_stop_queue) {
					spin_lock_irqsave(&wrdev->lock, flags);
					blk_start_queue(wrdev->queue);
					spin_unlock_irqrestore(&wrdev->lock, flags);
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
		LOGe("WalB changes device minor:%u to read-only mode.\n", wrdev->minor);
		blk_end_request_all(req, -EIO);
		list_del(&reqe->list);
		destroy_req_entry_dec(reqe);
	}
}
#else /* WALB_FAST_ALGORITHM */
static void wait_logpack_and_enqueue_datapack_tasks_easy(
	struct pack *wpack, struct wrapper_blk_dev *wrdev)
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
	ASSERT(wrdev);

	/* Check read only mode. */
	pdata = get_pdata_from_wrdev(wrdev);
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
			destroy_req_entry_dec(reqe);
		} else {
			/* Create all related bio(s). */
			if (!create_bio_entry_list(reqe, pdata->ddev)) { goto failed0; }

			/* Split if required due to chunk limitations. */
			if (!split_bio_entry_list_for_chunk(
					&reqe->bio_ent_list,
					pdata->ddev_chunk_sectors, GFP_NOIO)) {
				goto failed1;
			}

#ifdef WALB_OVERLAPPING_SERIALIZE
			/* check and insert to overlapping detection data. */
			spin_lock(&pdata->overlapping_data_lock);
			is_overlapping_insert_succeeded =
				overlapping_check_and_insert(pdata->overlapping_data,
							&pdata->max_req_sectors_in_overlapping,
							reqe, GFP_ATOMIC);
			spin_unlock(&pdata->overlapping_data_lock);
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
		destroy_req_entry_dec(reqe);
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
	struct wrapper_blk_dev *wrdev = pwork->data;
	struct pdata *pdata = get_pdata_from_wrdev(wrdev);
	struct pack *wpack, *wpack_next;
	bool is_empty, is_working;
	struct list_head wpack_list;

	destroy_pack_work(pwork);
	pwork = NULL;

	while (true) {
		/* Dequeue logpack list from the submit queue. */
		INIT_LIST_HEAD(&wpack_list);
		spin_lock(&pdata->logpack_wait_queue_lock);
		is_empty = list_empty(&pdata->logpack_wait_queue);
		if (is_empty) {
			is_working = test_and_clear_bit(
				PDATA_STATE_WAIT_TASK_WORKING,
				&pdata->flags);
			ASSERT(is_working);
		}
		list_for_each_entry_safe(wpack, wpack_next,
					&pdata->logpack_wait_queue, list) {
			list_move_tail(&wpack->list, &wpack_list);
			atomic_dec(&pdata->n_logpack_wait_queue); /* debug */
		}
		spin_unlock(&pdata->logpack_wait_queue_lock);
		if (is_empty) { break; }

		/* Wait logpack completion and submit datapacks. */
		list_for_each_entry_safe(wpack, wpack_next, &wpack_list, list) {
			wait_logpack_and_enqueue_datapack_tasks(wpack, wrdev);
		}

		/* Put packs into the gc queue. */
		spin_lock(&pdata->logpack_gc_queue_lock);
		list_for_each_entry_safe(wpack, wpack_next, &wpack_list, list) {
			list_move_tail(&wpack->list, &pdata->logpack_gc_queue);
			atomic_inc(&pdata->n_logpack_gc_queue); /* debug */
		}
		spin_unlock(&pdata->logpack_gc_queue_lock);

		/* Wakeup the gc task. */
		wakeup_worker(&pdata->gc_worker_data);
	}
}

/**
 * Wait all related write requests done and
 * free all related resources.
 *
 * @pdata pdata.
 * @wpack_list pack list to gc.
 *
 * CONTEXT:
 *   Called from a thread worker.
 */
static void gc_logpack_list(struct pdata *pdata, struct list_head *wpack_list)
{
	struct pack *wpack, *next_wpack;
	struct req_entry *reqe, *next_reqe;
	const unsigned long timeo = msecs_to_jiffies(completion_timeo_ms_);
	unsigned long rtimeo;
	int c;

	list_for_each_entry_safe(wpack, next_wpack, wpack_list, list) {
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
			destroy_req_entry_dec(reqe);
		}
		ASSERT(list_empty(&wpack->req_ent_list));
		ASSERT(list_empty(&wpack->bio_ent_list));
		destroy_pack(wpack);
	}
	ASSERT(list_empty(wpack_list));
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
	struct wrapper_blk_dev *wrdev = reqe->data;
	struct pdata *pdata = get_pdata_from_wrdev(wrdev);
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
	spin_lock(&pdata->overlapping_data_lock);
	overlapping_delete_and_notify(pdata->overlapping_data,
				&pdata->max_req_sectors_in_overlapping,
				reqe);
	spin_unlock(&pdata->overlapping_data_lock);
#endif

	/* Delete from pending data. */
	spin_lock(&pdata->pending_data_lock);
	is_start_queue = should_start_queue(pdata, reqe);
	pdata->pending_sectors -= reqe->req_sectors;
	pending_delete(pdata->pending_data, &pdata->max_req_sectors_in_pending, reqe);
	spin_unlock(&pdata->pending_data_lock);

	/* Check queue restart is required. */
	if (is_start_queue) {
		LOGd("restart queue.\n");
		spin_lock_irqsave(&wrdev->lock, flags);
		blk_start_queue(wrdev->queue);
		spin_unlock_irqrestore(&wrdev->lock, flags);
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
	struct wrapper_blk_dev *wrdev = reqe->data;
	UNUSED struct pdata *pdata = get_pdata_from_wrdev(wrdev);
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
	spin_lock(&pdata->overlapping_data_lock);
	overlapping_delete_and_notify(pdata->overlapping_data,
				&pdata->max_req_sectors_in_overlapping,
				reqe);
	spin_unlock(&pdata->overlapping_data_lock);
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
	struct wrapper_blk_dev *wrdev = reqe->data;
	struct pdata *pdata = get_pdata_from_wrdev(wrdev);
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
			&reqe->bio_ent_list, pdata->ddev_chunk_sectors, GFP_NOIO)) {
		goto error1;
	}

	/* Check pending data and copy data from executing write requests. */
	spin_lock(&pdata->pending_data_lock);
	ret = pending_check_and_copy(pdata->pending_data,
				pdata->max_req_sectors_in_pending,
				reqe, GFP_ATOMIC);
	spin_unlock(&pdata->pending_data_lock);
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
	destroy_req_entry_dec(reqe);
}
#else /* WALB_FAST_ALGORITHM */
/**
 * Execute a read request (Easy algortihm version).
 */
static void read_req_task_easy(struct work_struct *work)
{
	struct req_entry *reqe = container_of(work, struct req_entry, work);
	struct wrapper_blk_dev *wrdev = reqe->data;
	struct pdata *pdata = get_pdata_from_wrdev(wrdev);
	struct blk_plug plug;
	const bool is_end_request = true;
	const bool is_delete = true;

	/* Create all related bio(s). */
	if (!create_bio_entry_list(reqe, pdata->ddev)) {
		goto error0;
	}

	/* Split if required due to chunk limitations. */
	if (!split_bio_entry_list_for_chunk(
			&reqe->bio_ent_list, pdata->ddev_chunk_sectors, GFP_NOIO)) {
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
	destroy_req_entry_dec(reqe);
}
#endif /* WALB_FAST_ALGORITHM */

/**
 * Run gc logpack list.
 */
static void run_gc_logpack_list(void *data)
{
	struct wrapper_blk_dev *wrdev = (struct wrapper_blk_dev *)data;
	ASSERT(wrdev);

	dequeue_and_gc_logpack_list(get_pdata_from_wrdev(wrdev));
}

/**
 * Get logpack(s) from the gc queue and execute gc for them.
 *
 * @pdata pdata.
 */
static void dequeue_and_gc_logpack_list(struct pdata *pdata)
{
	struct pack *wpack, *wpack_next;
	bool is_empty;
	struct list_head wpack_list;
	int n_pack;

	ASSERT(pdata);

	INIT_LIST_HEAD(&wpack_list);
	while (true) {
		/* Dequeue logpack list */
		spin_lock(&pdata->logpack_gc_queue_lock);
		is_empty = list_empty(&pdata->logpack_gc_queue);
		n_pack = 0;
		list_for_each_entry_safe(wpack, wpack_next,
					&pdata->logpack_gc_queue, list) {
			list_move_tail(&wpack->list, &wpack_list);
			n_pack++;
			atomic_dec(&pdata->n_logpack_gc_queue); /* debug */
			if (n_pack >= N_PACK_BULK) { break; }
		}
		spin_unlock(&pdata->logpack_gc_queue_lock);
		if (is_empty) { break; }

		/* Gc */
		gc_logpack_list(pdata, &wpack_list);
		ASSERT(list_empty(&wpack_list));
	}
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
			n_padding++;
			i++;

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

		i++;
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
	u32 sum;
	int n_padding;
	u8 *buf;

	ASSERT(lhead);
	ASSERT(lhead->n_records > 0);
	ASSERT(lhead->n_records > lhead->n_padding);

	n_padding = 0;
	i = 0;
	list_for_each_entry(reqe, req_ent_list, list) {

		if (lhead->record[i].is_padding) {
			n_padding++;
			i++;
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
		i++;
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
	ASSERT(bioe->len << 9 == pbs);

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
		off_lb += bioe->len;
		list_add_tail(&bioe->list, &tmp_list);
	}
	/* split if required. */
	if (!split_bio_entry_list_for_chunk(
			&tmp_list, chunk_sectors, GFP_NOIO)) {
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
				i++;
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
		i++;
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
	struct multimap *overlapping_data,
	unsigned int *max_req_sectors_p,
	struct req_entry *reqe, gfp_t gfp_mask)
{
	struct multimap_cursor cur;
	u64 max_io_size, start_pos;
	int ret;
	struct req_entry *reqe_tmp;

	ASSERT(overlapping_data);
	ASSERT(max_req_sectors_p);
	ASSERT(reqe);
	ASSERT(reqe->req_sectors > 0);

	/* Decide search start position. */
	max_io_size = *max_req_sectors_p;
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
			reqe->n_overlapping++;
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
	ret = multimap_add(overlapping_data, reqe->req_pos, (unsigned long)reqe, gfp_mask);
	ASSERT(ret != -EEXIST);
	ASSERT(ret != -EINVAL);
	if (ret) {
		ASSERT(ret == -ENOMEM);
		LOGe("overlapping_check_and_insert failed.\n");
		return false;
	}
	*max_req_sectors_p = max(*max_req_sectors_p, reqe->req_sectors);
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
	struct multimap *overlapping_data,
	unsigned int *max_req_sectors_p,
	struct req_entry *reqe)
{
	struct multimap_cursor cur;
	u64 max_io_size, start_pos;
	struct req_entry *reqe_tmp;

	ASSERT(overlapping_data);
	ASSERT(max_req_sectors_p);
	ASSERT(reqe);
	ASSERT(reqe->n_overlapping == 0);

	max_io_size = *max_req_sectors_p;
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

	/* Initialize max_req_sectors. */
	if (multimap_is_empty(overlapping_data)) {
		*max_req_sectors_p = 0;
	}

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
			reqe_tmp->n_overlapping--;
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
	struct multimap *pending_data,
	unsigned int *max_req_sectors_p,
	struct req_entry *reqe, gfp_t gfp_mask)
{
	int ret;

	ASSERT(pending_data);
	ASSERT(max_req_sectors_p);
	ASSERT(reqe);
	ASSERT(reqe->req);
	ASSERT(reqe->req->cmd_flags & REQ_WRITE);
	ASSERT(reqe->req_sectors > 0);

	/* Insert the entry. */
	ret = multimap_add(pending_data, reqe->req_pos,
			(unsigned long)reqe, gfp_mask);
	ASSERT(ret != EEXIST);
	ASSERT(ret != EINVAL);
	if (ret) {
		ASSERT(ret == ENOMEM);
		LOGe("pending_insert failed.\n");
		return false;
	}
	*max_req_sectors_p = max(*max_req_sectors_p, reqe->req_sectors);
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
	unsigned int *max_req_sectors_p,
	struct req_entry *reqe)
{
	struct req_entry *reqe_tmp;

	ASSERT(pending_data);
	ASSERT(max_req_sectors_p);
	ASSERT(reqe);

	/* Delete the entry. */
	reqe_tmp = (struct req_entry *)multimap_del(
		pending_data, reqe->req_pos, (unsigned long)reqe);
	LOGd_("reqe_tmp %p reqe %p\n", reqe_tmp, reqe);
	ASSERT(reqe_tmp == reqe);
	if (multimap_is_empty(pending_data)) {
		*max_req_sectors_p = 0;
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
UNUSED static bool pending_check_and_copy(
	struct multimap *pending_data, unsigned int max_req_sectors,
	struct req_entry *reqe, gfp_t gfp_mask)
{
	struct multimap_cursor cur;
	u64 max_io_size, start_pos;
	struct req_entry *reqe_tmp;

	ASSERT(pending_data);
	ASSERT(reqe);

	/* Decide search start position. */
	max_io_size = max_req_sectors;
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
			if (!data_copy_req_entry(reqe, reqe_tmp, gfp_mask)) {
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
 *   pending_data_lock must be held.
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
static void wrapper_blk_req_request_fn(struct request_queue *q)
{
	struct wrapper_blk_dev *wrdev = get_wrdev_from_queue(q);
	struct pdata *pdata = get_pdata_from_wrdev(wrdev);
	struct request *req;
	struct req_entry *reqe;
	struct pack *wpack = NULL, *wpack_next;
	struct walb_logpack_header *lhead;
	bool ret;
	u64 latest_lsid, latest_lsid_old;
	struct list_head wpack_list;

	LOGd_("wrapper_blk_req_request_fn: begin.\n");

	if (!test_bit(0, &wrdev->is_started)) {
		goto error0;
	}
	if (test_bit(PDATA_STATE_FAILURE, &pdata->flags)) {
		goto error0;
	}

	INIT_LIST_HEAD(&wpack_list);

	/* Load latest_lsid */
	spin_lock(&pdata->lsid_lock);
	latest_lsid = pdata->latest_lsid;
	spin_unlock(&pdata->lsid_lock);
	latest_lsid_old = latest_lsid;

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
						&latest_lsid, wrdev, GFP_ATOMIC);
			if (!ret) { goto req_error; }
		} else {
			/* Read request */
			reqe = create_req_entry_inc(req, wrdev, GFP_ATOMIC);
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
	if (!list_empty(&wpack_list)) {
		/* Currently all requests are packed and lsid of all writepacks is defined. */
		ASSERT(is_valid_pack_list(&wpack_list));

		/* Enqueue all writepacks. */
		spin_lock(&pdata->logpack_submit_queue_lock);
		list_for_each_entry_safe(wpack, wpack_next, &wpack_list, list) {
			list_move_tail(&wpack->list, &pdata->logpack_submit_queue);
			atomic_inc(&pdata->n_logpack_submit_queue); /* debug */
		}
		spin_unlock(&pdata->logpack_submit_queue_lock);

		/* Run task. */
		enqueue_task_if_necessary(
			wrdev,
			PDATA_STATE_SUBMIT_TASK_WORKING,
			&pdata->flags,
			wq_logpack_,
			logpack_list_submit_task);

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

error0:
	while ((req = blk_fetch_request(q)) != NULL) {
		__blk_end_request_all(req, -EIO);
	}
	LOGd_("wrapper_blk_req_request_fn: error.\n");
}

/* Called before register. */
static bool pre_register(void)
{
	LOGd("pre_register called.");

	/* Prepare kmem_cache data. */
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

	if (!treemap_memory_manager_inc()) {
		LOGe("memory manager inc failed.\n");
		goto error7;
	}

	if (!pack_work_init()) {
		LOGe("pack_work init failed.\n");
		goto error8;
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
error9:
	pack_work_exit();
#endif
error8:
	treemap_memory_manager_dec();
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

	pack_work_exit();
	treemap_memory_manager_dec();

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

	LOGd_("end\n");
}

/**
 * Increment n_users of treemap memory manager and
 * iniitialize mmgr_ if necessary.
 */
static bool treemap_memory_manager_inc(void)
{
	bool ret;

	if (atomic_inc_return(&n_users_of_memory_manager_) == 1) {
		ret = initialize_treemap_memory_manager(
			&mmgr_, N_ITEMS_IN_MEMPOOL,
			TREE_NODE_CACHE_NAME,
			TREE_CELL_HEAD_CACHE_NAME,
			TREE_CELL_CACHE_NAME);
		if (!ret) {
			atomic_dec(&n_users_of_memory_manager_);
			goto error;
		}
	}
	return true;
error:
	return false;
}

/**
 * Decrement n_users of treemap memory manager and
 * finalize mmgr_ if necessary.
 */
static void treemap_memory_manager_dec(void)
{
	if (atomic_dec_return(&n_users_of_memory_manager_) == 0) {
		finalize_treemap_memory_manager(&mmgr_);
	}
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
MODULE_DESCRIPTION("Walb block req device for Test");
MODULE_ALIAS("walb_proto_req");
