/**
 * walb_proto_bio.c - WalB block device with bio-interface for test.
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
#include <linux/kthread.h>

#include "walb/walb.h"
#include "walb/block_size.h"
#include "walb/sector.h"
#include "wrapper_blk.h"
#include "sector_io.h"
#include "treemap.h"
#include "bio_util.h"
#include "bio_entry.h"
#include "bio_wrapper.h"
#include "kern.h"
#include "io.h"

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
 * Workqueues.
 */
/* Normal wq for IO. */
#define WQ_NORMAL_NAME "wq_normal"
struct workqueue_struct *wq_normal_ = NULL;

/* Unbound wq for IO. */
#define WQ_UNBOUND_NAME "wq_unbound"
struct workqueue_struct *wq_unbound_ = NULL;

/* Misc */
#define WQ_MISC_NAME "wq_misc"
struct workqueue_struct *wq_misc_ = NULL;

/*******************************************************************************
 * Macros definition.
 *******************************************************************************/


/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

/* Module helper functions. */

/* Called before/after register. */
static bool pre_register(void);
static void post_unregister(void);

/* For wrdev->private_data as walb_dev.. */
static bool create_private_data(struct wrapper_blk_dev *wrdev);
static void destroy_private_data(struct wrapper_blk_dev *wrdev);

/* Customize wrdev after register before start. */
static void customize_wrdev(struct wrapper_blk_dev *wrdev);
static unsigned int get_minor(unsigned int id);
static bool register_dev(void);
static void unregister_dev(void);
static bool start_dev(void);
static void stop_dev(void);

/* Make request callback. */
static void wrapper_blk_make_request_fn(
	struct request_queue *q, struct bio *bio);

/*******************************************************************************
 * For debug.
 *******************************************************************************/

#if 0
struct delayed_work shared_dwork;
static atomic_t wbiow_n_pending = ATOMIC_INIT(0);

static void task_periodic_print(struct work_struct *work)
{
	struct delayed_work *dwork = container_of(work, struct delayed_work, work);

	LOGn("wbiow_n_pending %d\n",
		atomic_read(&wbiow_n_pending));

	/* self-recursive call. */
	INIT_DELAYED_WORK(dwork, task_periodic_print);
	queue_delayed_work(system_wq, dwork, msecs_to_jiffies(1000));
}
#endif

/*******************************************************************************
 * Utility functions.
 *******************************************************************************/

static inline struct walb_dev* get_wdev_from_wrdev(
	struct wrapper_blk_dev *wrdev)
{
	return (struct walb_dev *)wrdev->private_data;
}

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/* Create private data for wrdev. */
static bool create_private_data(struct wrapper_blk_dev *wrdev)
{
	struct walb_dev *wdev;
	struct block_device *ldev, *ddev;
	unsigned int lbs, pbs;
	struct walb_super_sector *ssect;
	struct request_queue *lq, *dq;
	unsigned int major;

	LOGd("create_private_data called");

	/* Allocate wdev. */
	wdev = kmalloc(sizeof(struct walb_dev), GFP_KERNEL | __GFP_ZERO);
	if (!wdev) {
		LOGe("kmalloc failed.\n");
		goto error0;
	}

	/* Initialize fields. */
	wdev->ldev = NULL;
	wdev->ddev = NULL;
	spin_lock_init(&wdev->lsid_lock);
	spin_lock_init(&wdev->lsuper0_lock);
	atomic_set(&wdev->is_read_only, 0);
	INIT_LIST_HEAD(&wdev->list);

	/* Device number. */
	major = wrdev_get_major();
	ASSERT(major > 0);
	wdev->devt = MKDEV(major, wrdev->minor);

	/* Queue and disk are shared with wrdev. */
	wdev->queue = wrdev->queue;
	wdev->gd = wrdev->gd;

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
	wrdev->pbs = pbs;
	wdev->physical_bs = wrdev->pbs;
	blk_set_default_limits(&wrdev->queue->limits);
	blk_queue_logical_block_size(wrdev->queue, lbs);
	blk_queue_physical_block_size(wrdev->queue, pbs);
	/* blk_queue_io_min(wrdev->queue, pbs); */
	/* blk_queue_io_opt(wrdev->queue, pbs); */

	/* Set pagameters. */
	ASSERT(max_logpack_size_kb_ >= 0);
	ASSERT((max_logpack_size_kb_ * 1024) % pbs == 0);
	wdev->max_logpack_pb = (max_logpack_size_kb_ * 1024) / pbs;
	LOGn("max_logpack_size_kb: %u max_logpack_pb: %u\n",
		max_logpack_size_kb_, wdev->max_logpack_pb);
#ifdef WALB_FAST_ALGORITHM
	wdev->max_pending_sectors = max_pending_mb_
		* (1024 * 1024 / LOGICAL_BLOCK_SIZE);
	wdev->min_pending_sectors = min_pending_mb_
		* (1024 * 1024 / LOGICAL_BLOCK_SIZE);
	LOGn("max pending sectors: %u\n", wdev->max_pending_sectors);
	wdev->queue_stop_timeout_jiffies =
		msecs_to_jiffies(queue_stop_timeout_ms_);
	LOGn("qeue_stop_timeout_ms: %u\n", queue_stop_timeout_ms_);
#endif

	/* Set underlying devices. */
	wdev->ldev = ldev;
	wdev->ddev = ddev;
	wrdev->private_data = wdev;

	/* Load super block. */
	wdev->lsuper0 = sector_alloc(pbs, GFP_KERNEL);
	if (!wdev->lsuper0) {
		goto error3;
	}
	if (!walb_read_super_sector(wdev->ldev, wdev->lsuper0)) {
		LOGe("read super sector 0 failed.\n");
		goto error4;
	}
	ssect = get_super_sector(wdev->lsuper0);
	init_checkpointing(&wdev->cpd);
	wdev->oldest_lsid = ssect->oldest_lsid;
	wdev->written_lsid = ssect->written_lsid;
	wdev->latest_lsid = wdev->written_lsid; /* redo must be done. */
#ifdef WALB_FAST_ALGORITHM
	wdev->completed_lsid = wdev->written_lsid; /* redo must be done. */
#endif
	wdev->ring_buffer_size = ssect->ring_buffer_size;
	wdev->ring_buffer_off = get_ring_buffer_offset_2(ssect);
	wdev->log_checksum_salt = ssect->log_checksum_salt;

	/* capacity */
	wdev->ddev_size = ddev->bd_part->nr_sects;
	wdev->ldev_size = ldev->bd_part->nr_sects;
	wrdev->capacity = wdev->ddev_size;
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
		wdev->ldev_chunk_sectors = queue_io_min(lq) / LOGICAL_BLOCK_SIZE;
	} else {
		wdev->ldev_chunk_sectors = 0;
	}
	if (queue_io_min(dq) > wrdev->pbs) {
		wdev->ddev_chunk_sectors = queue_io_min(dq) / LOGICAL_BLOCK_SIZE;
	} else {
		wdev->ddev_chunk_sectors = 0;
	}
	LOGn("chunk_sectors ldev %u ddev %u.\n",
		wdev->ldev_chunk_sectors, wdev->ddev_chunk_sectors);

#if 0
	/* unused by the prototype. */
	wdev->n_snapshots;
	wdev->n_users;
	wdev->log_queue;
	wdev->log_gd;
	wdev->log_n_users;
	wdev->cpd;
	wdev->snapd;
#endif

	/* Initialize iocore data. */
	if (!iocore_initialize(wdev)) {
		LOGe("initialize iocore failed.\n");
		goto error4;
	}

	return true;
#if 0
error5:
	iocore_finalize(wdev);
#endif
error4:
	sector_free(wdev->lsuper0);
error3:
	blkdev_put(ddev, FMODE_READ|FMODE_WRITE|FMODE_EXCL);
error2:
	blkdev_put(ldev, FMODE_READ|FMODE_WRITE|FMODE_EXCL);
error1:
	kfree(wdev);
	wrdev->private_data = NULL;
error0:
	return false;
}

/* Destroy private data for ssev. */
static void destroy_private_data(struct wrapper_blk_dev *wrdev)
{
	struct walb_dev *wdev;
	struct walb_super_sector *ssect;

	LOGd("destoroy_private_data called.");

	wdev = wrdev->private_data;
	if (!wdev) { return; }
	ASSERT(wdev);

	/* Finalize iocore. */
	iocore_finalize(wdev);

	/* sync super block.
	   The locks are not required because
	   block device is now offline. */
	ssect = get_super_sector(wdev->lsuper0);
	ssect->written_lsid = wdev->written_lsid;
	ssect->oldest_lsid = wdev->oldest_lsid;
	if (!walb_write_super_sector(wdev->ldev, wdev->lsuper0)) {
		LOGe("super block write failed.\n");
	}

	/* close underlying devices. */
	blkdev_put(wdev->ddev, FMODE_READ|FMODE_WRITE|FMODE_EXCL);
	blkdev_put(wdev->ldev, FMODE_READ|FMODE_WRITE|FMODE_EXCL);

	sector_free(wdev->lsuper0);
	kfree(wdev);
	wrdev->private_data = NULL;
}

/* Customize wrdev after register before start. */
static void customize_wrdev(struct wrapper_blk_dev *wrdev)
{
	struct request_queue *q, *lq, *dq;
	struct walb_dev *wdev;
	ASSERT(wrdev);
	q = wrdev->queue;
	wdev = get_wdev_from_wrdev(wrdev);
	ASSERT(wdev);

	lq = bdev_get_queue(wdev->ldev);
	dq = bdev_get_queue(wdev->ddev);
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
	ret = wrdev_register_with_bio(get_minor(i), capacity,
				physical_block_size_,
				wrapper_blk_make_request_fn);

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
		destroy_private_data(wrdev);
		FREE(wrdev);
	}

	LOGn("end\n");
}

static bool start_dev(void)
{
	unsigned int i = 0;

	if (!wrdev_start(get_minor(i))) {
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
	struct walb_dev *wdev;

	minor = get_minor(i);
	wrdev = wrdev_get(minor);
	ASSERT(wrdev);

	/* set_capacity(wrdev->gd, 0); */
	wrdev_stop(minor);

	wdev = get_wdev_from_wrdev(wrdev);
	ASSERT(wdev);

	/* Flush all remaining IOs for underlying devices. */
	iocore_set_failure(wdev);
	iocore_flush(wdev);
}

/* Called before register. */
static bool pre_register(void)
{
	LOGd("pre_register called.");

	if (!bio_wrapper_init()) {
		goto error1;
	}

	if (!bio_entry_init()) {
		goto error2;
	}

	/* prepare workqueues. */
	wq_normal_ = alloc_workqueue(WQ_NORMAL_NAME, WQ_MEM_RECLAIM, 0);
	if (!wq_normal_) {
		LOGe("failed to allocate a workqueue (wq_normal_).");
		goto error3;
	}
	wq_unbound_ = alloc_workqueue(WQ_UNBOUND_NAME, WQ_MEM_RECLAIM | WQ_UNBOUND, 0);
	if (!wq_unbound_) {
		LOGe("failed to allocate a workqueue (wq_unbound_).");
		goto error4;
	}
	wq_misc_ = alloc_workqueue(WQ_MISC_NAME, WQ_MEM_RECLAIM, 0);
	if (!wq_misc_) {
		LOGe("failed to allocate a workqueue (wq_misc_).");
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
	destroy_workqueue(wq_misc_);
#endif
error5:
	destroy_workqueue(wq_unbound_);
error4:
	destroy_workqueue(wq_normal_);
error3:
	bio_entry_exit();
error2:
	bio_wrapper_exit();
error1:
	return false;
}

/* Called after unregister. */
static void post_unregister(void)
{
	LOGd_("begin\n");

	/* finalize workqueue data. */
	destroy_workqueue(wq_misc_);
	wq_misc_ = NULL;
	destroy_workqueue(wq_unbound_);
	wq_unbound_ = NULL;
	destroy_workqueue(wq_normal_);
	wq_normal_ = NULL;

	/* Destory kmem_cache data. */
	bio_entry_exit();
	bio_wrapper_exit();

	LOGd_("end\n");
}

/**
 * Make request callback.
 */
static void wrapper_blk_make_request_fn(struct request_queue *q, struct bio *bio)
{
	struct wrapper_blk_dev *wrdev = get_wrdev_from_queue(q);
	struct walb_dev *wdev = get_wdev_from_wrdev(wrdev);

	iocore_make_request(wdev, bio);
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

#if 0
	/* debug */
	INIT_DELAYED_WORK(&shared_dwork, task_periodic_print);
	queue_delayed_work(system_wq, &shared_dwork, msecs_to_jiffies(1000));
#endif

	return 0;
#if 0
error3:
	stop_dev();
#endif
error2:
	unregister_dev();
error1:
	post_unregister();
error0:
	return -1;
}

static void wrapper_blk_exit(void)
{
#if 0
	/* debug */
	cancel_delayed_work_sync(&shared_dwork);
#endif

	stop_dev();
	unregister_dev();
	post_unregister();
}

module_init(wrapper_blk_init);
module_exit(wrapper_blk_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Walb block bio device for Test");
MODULE_ALIAS("walb_proto_bio");
