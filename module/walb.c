/**
 * walb.c - Block-level WAL module.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include <linux/sched.h>
#include <linux/slab.h>
#include <linux/fs.h>
#include <linux/errno.h>
#include <linux/types.h>
#include <linux/fcntl.h>
#include <linux/hdreg.h>
#include <linux/kdev_t.h>
#include <linux/genhd.h>
#include <linux/blkdev.h>
#include <linux/buffer_head.h>
#include <linux/bio.h>
#include <linux/spinlock.h>
#include <linux/rwsem.h>
#include <linux/version.h>
#include <linux/delay.h>
#include <asm/atomic.h>

#include "kern.h"
#include "hashtbl.h"
#include "control.h"
#include "alldevs.h"
#include "util.h"
#include "logpack.h"
#include "checkpoint.h"
#include "super.h"
#include "io.h"
#include "redo.h"
#include "sysfs.h"
#include "wdev_ioctl.h"
#include "wdev_util.h"
#include "version.h"

#include "walb/ioctl.h"
#include "walb/log_device.h"
#include "walb/sector.h"
#include "walb/logger.h"

/*******************************************************************************
 * Module parameters definition.
 *******************************************************************************/

/**
 * Device major of walb.
 */
int walb_major_ = 0;
module_param_named(walb_major, walb_major_, int, S_IRUGO);

/**
 * Set 1 if you want to sync down superblock in disassemble device.
 * Set 0 if not.
 */
static int is_sync_superblock_ = 1;
module_param_named(is_sync_superblock, is_sync_superblock_, int, S_IRUGO|S_IWUSR);

/**
 * Set Non-zero if you want to sort data IOs
 * before submitting to the data device.
 * The parameter n_io_bulk will work as sort buffer size.
 */
unsigned int is_sort_data_io_ = 1;
module_param_named(is_sort_data_io, is_sort_data_io_, uint, S_IRUGO|S_IWUSR);

/**
 * An executable binary for error notification.
 * When an error ocurred, the exec will be invoked with arguments.
 * argv[1] is minor id of the walb device.
 * argv[2] is event name.
 */
char exec_path_on_error_[EXEC_PATH_ON_ERROR_LEN] = "";
module_param_string(exec_path_on_error, exec_path_on_error_, sizeof(exec_path_on_error_), S_IRUGO|S_IWUSR);

/**
 * Set non-zero if you want walb devices to transit the read-only state
 * where write IO will fail not to overflow ring buffer.
 */
unsigned int is_error_before_overflow_ = 0;
module_param_named(is_error_before_overflow, is_error_before_overflow_, uint, S_IRUGO);

/*******************************************************************************
 * Shared data definition.
 *******************************************************************************/

/**
 * Workqueues.
 */
#define WQ_NORMAL_NAME "walb_wq_normal"
struct workqueue_struct *wq_normal_ = NULL;
#define WQ_NRT_NAME "walb_wq_nrt"
struct workqueue_struct *wq_nrt_ = NULL;
#define WQ_UNBOUND_NAME "walb_wq_unbound"
struct workqueue_struct *wq_unbound_ = NULL;
#define WQ_MISC_NAME "wq_misc"
struct workqueue_struct *wq_misc_ = NULL;

/*******************************************************************************
 * Prototypes of local functions.
 *******************************************************************************/

/* Lock/unlock block device. */
static int walb_lock_bdev(struct block_device **bdevp, dev_t dev);
static void walb_unlock_bdev(struct block_device *bdev);

/* Walb device open/close/ioctl. */
static int walb_open(struct block_device *bdev, fmode_t mode);
static void walb_release(struct gendisk *gd, fmode_t mode);
static int walb_ioctl(struct block_device *bdev, fmode_t mode,
		unsigned int cmd, unsigned long arg);

/* Walblog device open/close/ioctl. */
static int walblog_open(struct block_device *bdev, fmode_t mode);
static void walblog_release(struct gendisk *gd, fmode_t mode);
static int walblog_ioctl(struct block_device *bdev, fmode_t mode,
			unsigned int cmd, unsigned long arg);

/* Workqueues. */
static bool initialize_workqueues(void);
static void finalize_workqueues(void);

/* Prepare/finalize. */
static int walb_prepare_device(
	struct walb_dev *wdev, unsigned int minor, const char *name);
static void walb_finalize_device(struct walb_dev *wdev);
static int walblog_prepare_device(struct walb_dev *wdev, unsigned int minor,
				const char* name);
static void walblog_finalize_device(struct walb_dev *wdev);

static int walb_ldev_initialize(struct walb_dev *wdev);
static void walb_ldev_finalize(struct walb_dev *wdev, bool is_sync);

/* Register/unregister. */
static void walb_register_device(struct walb_dev *wdev);
static void walb_unregister_device(struct walb_dev *wdev);
static void walblog_register_device(struct walb_dev *wdev);
static void walblog_unregister_device(struct walb_dev *wdev);

/* Module init/exit. */
static int __init walb_init(void);
static void walb_exit(void);

/*******************************************************************************
 * Static functions.
 *******************************************************************************/

/**
 * Open and claim underlying block device.
 * @bdevp  pointer to bdev pointer to back.
 * @dev	   device to lock.
 * @return 0 in success.
 */
static int walb_lock_bdev(struct block_device **bdevp, dev_t dev)
{
	int err = 0;
	struct block_device *bdev;
	UNUSED char b[BDEVNAME_SIZE];

	/* Currently the holder is the pointer to walb_lock_bdev(). */
	bdev = blkdev_get_by_dev(dev, FMODE_READ|FMODE_WRITE|FMODE_EXCL, walb_lock_bdev);
	if (IS_ERR(bdev)) { err = PTR_ERR(bdev); goto open_err; }

	*bdevp = bdev;
	return err;

open_err:
	LOGe("open error %s.\n", __bdevname(dev, b));
	return err;
}

/**
 * Release underlying block device.
 * @bdev bdev pointer.
 */
static void walb_unlock_bdev(struct block_device *bdev)
{
	blkdev_put(bdev, FMODE_READ|FMODE_WRITE|FMODE_EXCL);
}

/**
 * Open walb device.
 */
static int walb_open(struct block_device *bdev, fmode_t mode)
{
	struct walb_dev *wdev = get_wdev_from_disk(bdev->bd_disk);
	int n_users;

	n_users = atomic_inc_return(&wdev->n_users);
	if (n_users == 1) {
#if 0
		LOGn("This is the first time to open walb device %d"
			" and check_disk_change() will be called.\n",
			MINOR(wdev->devt));
		check_disk_change(bdev);
#endif
	}
	return 0;
}

/**
 * Release a walb device.
 */
static void walb_release(struct gendisk *gd, fmode_t mode)
{
	struct walb_dev *wdev = get_wdev_from_disk(gd);
	int n_users;

	n_users = atomic_dec_return(&wdev->n_users);
	ASSERT(n_users >= 0);
}

/*
 * The ioctl() implementation
 */
static int walb_ioctl(struct block_device *bdev, fmode_t mode,
		unsigned int cmd, unsigned long arg)
{
	struct hd_geometry geo;
	struct walb_dev *wdev = bdev->bd_disk->private_data;
	int ret = -ENOTTY;
	u32 version;

	LOG_("walb_ioctl begin.\n");
	LOG_("cmd: %08x\n", cmd);

	switch(cmd) {
	case HDIO_GETGEO:
		set_geometry(&geo, wdev->ddev_size);
		if (copy_to_user((void __user *) arg, &geo, sizeof(geo)))
			return -EFAULT;
		ret = 0;
		break;

	case WALB_IOCTL_VERSION:
		version = WALB_VERSION;
		ret = __put_user(version, (int __user *)arg);
		break;

	case WALB_IOCTL_WDEV:

		ret = walb_dispatch_ioctl_wdev(wdev, (void __user *)arg);
		break;
	}

	LOG_("walb_ioctl end.\n");

	return ret;
}

/*
 * The device operations structure.
 */
static struct block_device_operations walb_ops = {
	.owner		 = THIS_MODULE,
	.open		 = walb_open,
	.release	 = walb_release,
	.ioctl		 = walb_ioctl
};

/**
 * Open a walb device.
 */
static int walblog_open(struct block_device *bdev, fmode_t mode)
{
	struct walb_dev *wdev = get_wdev_from_disk(bdev->bd_disk);
	int n_users;

	n_users = atomic_inc_return(&wdev->log_n_users);
	if (n_users == 1) {
#if 0
		LOGn("This is the first time to open walblog device %d"
			" and check_disk_change() will be called.\n",
			MINOR(wdev->devt));
		check_disk_change(bdev);
#endif
	}
	return 0;
}

/**
 * Release a walblog device.
 */
static void walblog_release(struct gendisk *gd, fmode_t mode)
{
	struct walb_dev *wdev = get_wdev_from_disk(gd);
	int n_users;

	n_users = atomic_dec_return(&wdev->log_n_users);
	ASSERT(n_users >= 0);
}

static int walblog_ioctl(struct block_device *bdev, fmode_t mode,
			unsigned int cmd, unsigned long arg)
{
	struct hd_geometry geo;
	struct walb_dev *wdev = bdev->bd_disk->private_data;

	switch(cmd) {
	case HDIO_GETGEO:
		set_geometry(&geo, wdev->ldev_size);
		if (copy_to_user((void __user *) arg, &geo, sizeof(geo)))
			return -EFAULT;
		return 0;
	default:
		return -ENOTTY;
	}
}

static struct block_device_operations walblog_ops = {
	.owner	 = THIS_MODULE,
	.open	 = walblog_open,
	.release = walblog_release,
	.ioctl	 = walblog_ioctl
};

/**
 * Initialize workqueues.
 *
 * RETURN:
 *   true in success, or false.
 */
static bool initialize_workqueues(void)
{
#ifdef MSG
#error
#endif
#define MSG "Failed to allocate the workqueue %s.\n"
	wq_normal_ = alloc_workqueue(WQ_NORMAL_NAME, WQ_MEM_RECLAIM, 0);
	if (!wq_normal_) {
		LOGe(MSG, WQ_NORMAL_NAME);
		goto error0;
	}
	wq_nrt_ = alloc_workqueue(WQ_NRT_NAME,
				WQ_MEM_RECLAIM | WQ_NON_REENTRANT, 0);
	if (!wq_nrt_) {
		LOGe(MSG, WQ_NRT_NAME);
		goto error0;
	}
	wq_unbound_ = alloc_workqueue(WQ_UNBOUND_NAME,
				WQ_MEM_RECLAIM | WQ_UNBOUND, WQ_UNBOUND_MAX_ACTIVE);
	if (!wq_unbound_) {
		LOGe(MSG, WQ_UNBOUND_NAME);
		goto error0;
	}
	wq_misc_ = alloc_workqueue(WQ_MISC_NAME, WQ_MEM_RECLAIM, 0);
	if (!wq_misc_) {
		LOGe(MSG, WQ_MISC_NAME);
		goto error0;
	}
	return true;
#undef MSG
error0:
	finalize_workqueues();
	return false;
}

/**
 * Finalize workqueues.
 */
static void finalize_workqueues(void)
{
	if (wq_misc_) {
		destroy_workqueue(wq_misc_);
		wq_misc_ = NULL;
	}
	if (wq_unbound_) {
		destroy_workqueue(wq_unbound_);
		wq_unbound_ = NULL;
	}
	if (wq_nrt_) {
		destroy_workqueue(wq_nrt_);
		wq_nrt_ = NULL;
	}
	if (wq_normal_) {
		destroy_workqueue(wq_normal_);
		wq_normal_ = NULL;
	}
}

/**
 * Initialize walb block device.
 *
 * @wdev walb_dev.
 * @minor minor id.
 * @name disk name.
 */
static int walb_prepare_device(
	struct walb_dev *wdev, unsigned int minor, const char *name)
{
	struct request_queue *lq, *dq;

	/* Using bio interface */
	wdev->queue = blk_alloc_queue(GFP_KERNEL);
	if (!wdev->queue)
		goto out;
	blk_queue_make_request(wdev->queue, walb_make_request);
	wdev->queue->queuedata = wdev;

	/* Queue limits. */
	blk_set_stacking_limits(&wdev->queue->limits);
	blk_queue_logical_block_size(wdev->queue, LOGICAL_BLOCK_SIZE);
	blk_queue_physical_block_size(wdev->queue, wdev->physical_bs);
	lq = bdev_get_queue(wdev->ldev);
	dq = bdev_get_queue(wdev->ddev);
	blk_queue_stack_limits(wdev->queue, lq);
	blk_queue_stack_limits(wdev->queue, dq);
#if 0
	print_queue_limits(KERN_NOTICE, "lq", &lq->limits);
	print_queue_limits(KERN_NOTICE, "dq", &dq->limits);
	print_queue_limits(KERN_NOTICE, "wdev", &wdev->queue->limits);
#endif

	/* Allocate a gendisk and set parameters. */
	wdev->gd = alloc_disk(1);
	if (!wdev->gd) {
		LOGe("alloc_disk failure.\n");
		goto out_queue;
	}
	wdev->gd->major = walb_major_;
	wdev->gd->first_minor = minor;
	wdev->devt = MKDEV(wdev->gd->major, wdev->gd->first_minor);
	wdev->gd->fops = &walb_ops;
	wdev->gd->queue = wdev->queue;
	wdev->gd->private_data = wdev;
	set_capacity(wdev->gd, wdev->size);

	/* Set a name. */
	snprintf(wdev->gd->disk_name, DISK_NAME_LEN,
		"%s/%s", WALB_DIR_NAME, name);
	LOGd("device path: %s, device name: %s\n",
		wdev->gd->disk_name, name);

	/* Number of users. */
	atomic_set(&wdev->n_users, 0);

	/* Flush support. */
	walb_decide_flush_support(wdev);

	/* Discard support. */
	walb_discard_support(wdev);

	return 0;

#if 0
out_disk:
	if (wdev->gd) {
		put_disk(wdev->gd);
	}
#endif
out_queue:
	if (wdev->queue) {
		blk_cleanup_queue(wdev->queue);
	}
out:
	return -1;
}

/**
 * Finalize walb block device.
 */
static void walb_finalize_device(struct walb_dev *wdev)
{
	if (wdev->gd) {
		put_disk(wdev->gd);
		wdev->gd = NULL;
	}
	if (wdev->queue) {
		blk_cleanup_queue(wdev->queue);
		wdev->queue = NULL;
	}
}

/**
 * Setup walblog device.
 */
static int walblog_prepare_device(struct walb_dev *wdev,
				unsigned int minor, const char* name)
{
	struct request_queue *lq;

	wdev->log_queue = blk_alloc_queue(GFP_KERNEL);
	if (!wdev->log_queue)
		goto error0;

	blk_queue_make_request(wdev->log_queue, walblog_make_request);
	wdev->log_queue->queuedata = wdev;

	/* Queue limits. */
	lq = bdev_get_queue(wdev->ldev);
	blk_set_default_limits(&wdev->log_queue->limits);
	blk_queue_logical_block_size(wdev->log_queue, LOGICAL_BLOCK_SIZE);
	blk_queue_physical_block_size(wdev->log_queue, wdev->physical_bs);
	blk_queue_stack_limits(wdev->log_queue, lq);

	/* Allocate a gendisk and set parameters. */
	wdev->log_gd = alloc_disk(1);
	if (! wdev->log_gd) {
		goto error1;
	}
	wdev->log_gd->major = walb_major_;
	wdev->log_gd->first_minor = minor;
	wdev->log_gd->queue = wdev->log_queue;
	wdev->log_gd->fops = &walblog_ops;
	wdev->log_gd->private_data = wdev;
	set_capacity(wdev->log_gd, wdev->ldev_size);

	/* Set a name. */
	snprintf(wdev->log_gd->disk_name, DISK_NAME_LEN,
		"%s/L%s", WALB_DIR_NAME, name);
	atomic_set(&wdev->log_n_users , 0);
	return 0;

error1:
	if (wdev->log_queue) {
		blk_cleanup_queue(wdev->log_queue);
	}
error0:
	return -1;
}

/**
 * Finalize walblog wrapper device.
 */
static void walblog_finalize_device(struct walb_dev *wdev)
{
	if (wdev->log_gd) {
		put_disk(wdev->log_gd);
		wdev->log_gd = NULL;
	}
	if (wdev->log_queue) {
		blk_cleanup_queue(wdev->log_queue);
		wdev->log_queue = NULL;
	}
}

/**
 * Log device initialization.
 *
 * Read log device metadata
 *    (currently snapshot metadata is not loaded.
 *     super sector0 only...)
 *
 * @wdev walb device struct.
 * @return 0 in success, or -1.
 */
static int walb_ldev_initialize(struct walb_dev *wdev)
{
	struct sector_data *lsuper0_tmp;
	ASSERT(wdev);

	/*
	 * 1. Read log device metadata
	 */
	wdev->lsuper0 = sector_alloc(wdev->physical_bs, GFP_NOIO);
	if (!wdev->lsuper0) {
		LOGe("walb_ldev_init: alloc sector failed.\n");
		goto error0;
	}
	lsuper0_tmp = sector_alloc(wdev->physical_bs, GFP_NOIO);
	if (!lsuper0_tmp) {
		LOGe("walb_ldev_init: alloc sector failed.\n");
		goto error1;
	}

	if (!walb_read_super_sector(wdev->ldev, wdev->lsuper0)) {
		LOGe("walb_ldev_init: read super sector failed.\n");
		goto error2;
	}
	if (!walb_write_super_sector(wdev->ldev, wdev->lsuper0)) {
		LOGe("walb_ldev_init: write super sector failed.\n");
		goto error2;
	}

	if (!walb_read_super_sector(wdev->ldev, lsuper0_tmp)) {
		LOGe("walb_ldev_init: read super sector failed.\n");
		goto error2;
	}

	if (!is_same_sector(wdev->lsuper0, lsuper0_tmp)) {
		LOGe("walb_ldev_init: memcmp NG\n");
		goto error2;
	}

	if (get_super_sector_const(wdev->lsuper0)->physical_bs
		!= wdev->physical_bs) {
		LOGe("Physical block size is different.\n");
		goto error2;
	}

	sector_free(lsuper0_tmp);
	/* Do not forget calling kfree(dev->lsuper0)
	   before releasing the block device. */

	return 0;

error2:
	sector_free(lsuper0_tmp);
error1:
	sector_free(wdev->lsuper0);
error0:
	return -1;
}

/**
 * Finalize log device.
 */
static void walb_ldev_finalize(struct walb_dev *wdev, bool is_sync)
{
	ASSERT(wdev);
	ASSERT(wdev->lsuper0);

	if (!walb_finalize_super_block(wdev, is_sync_superblock_ && is_sync))
		WLOGe(wdev, "finalize super block failed.\n");

	sector_free(wdev->lsuper0);
}

/**
 * Register walb block device.
 */
static void walb_register_device(struct walb_dev *wdev)
{
	ASSERT(wdev);
	ASSERT(wdev->gd);

	add_disk(wdev->gd);
}

/**
 * Unregister walb wrapper device.
 */
static void walb_unregister_device(struct walb_dev *wdev)
{
	LOG_("walb_unregister_device begin.\n");
	if (wdev->gd) {
		del_gendisk(wdev->gd);
		/* Do not assign NULL here. */
	}
	LOG_("walb_unregister_device end.\n");
}

/**
 * Register walblog block device.
 */
static void walblog_register_device(struct walb_dev *wdev)
{
	ASSERT(wdev);
	ASSERT(wdev->log_gd);

	add_disk(wdev->log_gd);
}

/**
 * Unregister walblog wrapper device.
 */
static void walblog_unregister_device(struct walb_dev *wdev)
{
	LOG_("walblog_unregister_device begin.\n");
	if (wdev->log_gd) {
		del_gendisk(wdev->log_gd);
		/* Do not assign NULL here. */
	}
	LOG_("walblog_unregister_device end.\n");
}

static int __init walb_init(void)
{
	LOGn("WALB_VERSION %s\n", WALB_VERSION_STR);

	/* DISK_NAME_LEN assersion */
	ASSERT_DISK_NAME_LEN();

	/*
	 * Get registered.
	 */
	walb_major_ = register_blkdev(walb_major_, WALB_NAME);
	if (walb_major_ <= 0) {
		LOGw("unable to get major number.\n");
		return -EBUSY;
	}
	LOGi("walb_start with major id %d.\n", walb_major_);

	/*
	 * Workqueues.
	 */
	if (!initialize_workqueues()) {
		goto out_register;
	}

	/*
	 * Alldevs.
	 */
	if (alldevs_init() != 0) {
		LOGe("alldevs_init failed.\n");
		goto out_workqueues;
	}

	/*
	 * Init control device.
	 */
	if (walb_control_init() != 0) {
		LOGe("walb_control_init failed.\n");
		goto out_alldevs_exit;
	}

	return 0;

#if 0
out_control_exit:
	walb_control_exit();
#endif
out_alldevs_exit:
	alldevs_exit();
out_workqueues:
	finalize_workqueues();
out_register:
	unregister_blkdev(walb_major_, WALB_NAME);
	return -ENOMEM;
}

static void walb_exit(void)
{
	struct walb_dev *wdev;

	alldevs_lock();
	wdev = alldevs_pop();
	while (wdev) {
		unregister_wdev(wdev);
		finalize_wdev(wdev);
		destroy_wdev(wdev);
		wdev = alldevs_pop();
	}
	alldevs_unlock();

	finalize_workqueues();
	unregister_blkdev(walb_major_, WALB_NAME);
	walb_control_exit();
	alldevs_exit();

	LOGi("walb exit.\n");
}


/*******************************************************************************
 * Global functions.
 *******************************************************************************/

/**
 * Prepare walb device.
 * You must call @register_wdev() after calling this.
 *
 * @minor minor id of the device (must not be WALB_DYNAMIC_MINOR).
 *	  walblog device minor will be (minor + 1).
 * @ldevt device id of log device.
 * @ddevt device id of data device.
 * @param parameters. (this will be updated)
 *
 * @return allocated and prepared walb_dev data, or NULL.
 */
struct walb_dev* prepare_wdev(
	unsigned int minor, dev_t ldevt, dev_t ddevt,
	struct walb_start_param *param)
{
	struct walb_dev *wdev;
	u16 ldev_lbs, ldev_pbs, ddev_lbs, ddev_pbs;
	const char *dev_name;
	struct walb_super_sector *super;
	struct request_queue *lq, *dq;
	bool retb;
	u64 latest_lsid, oldest_lsid;
#ifdef WALB_DEBUG
	u64 completed_lsid, flush_lsid, written_lsid, prev_written_lsid;
#endif

	ASSERT(is_walb_start_param_valid(param));

	/* Minor id check. */
	if (minor == WALB_DYNAMIC_MINOR) {
		LOGe("Do not specify WALB_DYNAMIC_MINOR.\n");
		goto out;
	}

	/*
	 * Initialize walb_dev.
	 */
	wdev = kzalloc(sizeof(struct walb_dev), GFP_KERNEL);
	if (!wdev) {
		LOGe("kmalloc failed.\n");
		goto out;
	}
	spin_lock_init(&wdev->lsid_lock);
	spin_lock_init(&wdev->lsuper0_lock);
	spin_lock_init(&wdev->size_lock);
	wdev->flags = 0;
	mutex_init(&wdev->freeze_lock);
	wdev->freeze_state = FRZ_MELTED;

	/*
	 * Open underlying log device.
	 */
	if (walb_lock_bdev(&wdev->ldev, ldevt) != 0) {
		LOGe("walb_lock_bdev failed (%u:%u for log)\n",
			MAJOR(ldevt), MINOR(ldevt));
		goto out_free;
	}
	wdev->ldev_size = wdev->ldev->bd_part->nr_sects;
	ldev_lbs = bdev_logical_block_size(wdev->ldev);
	ldev_pbs = bdev_physical_block_size(wdev->ldev);
	ASSERT(ldev_lbs == LOGICAL_BLOCK_SIZE);
	LOGi("log disk (%u:%u) size %llu lbs %u pbs %u\n",
		MAJOR(ldevt), MINOR(ldevt),
		wdev->ldev_size,
		ldev_lbs, ldev_pbs);

	/*
	 * Open underlying data device.
	 */
	if (walb_lock_bdev(&wdev->ddev, ddevt) != 0) {
		LOGe("walb_lock_bdev failed (%u:%u for data)\n",
			MAJOR(ddevt), MINOR(ddevt));
		goto out_ldev;
	}
	wdev->ddev_size = wdev->ddev->bd_part->nr_sects;
	ddev_lbs = bdev_logical_block_size(wdev->ddev);
	ddev_pbs = bdev_physical_block_size(wdev->ddev);
	ASSERT(ddev_lbs == LOGICAL_BLOCK_SIZE);
	LOGi("data disk (%u:%u) size %llu lbs %u pbs %u\n",
		MAJOR(ddevt), MINOR(ddevt),
		wdev->ddev_size,
		ddev_lbs, ddev_pbs);

	/* Check compatibility of log device and data device. */
	if (ldev_pbs != ddev_pbs) {
		LOGe("Sector size of data and log must be same.\n");
		goto out_ddev;
	}
	wdev->physical_bs = ldev_pbs;

	/* Load log device metadata. */
	if (walb_ldev_initialize(wdev) != 0) {
		LOGe("ldev init failed.\n");
		goto out_ddev;
	}
	super = get_super_sector(wdev->lsuper0);
	ASSERT(super);
	init_checkpointing(&wdev->cpd);

	/* Set lsids. */
	spin_lock(&wdev->lsid_lock);
	wdev->lsids.oldest = super->oldest_lsid;
	wdev->lsids.prev_written = super->written_lsid;
	wdev->lsids.written = super->written_lsid;
	wdev->lsids.permanent = super->written_lsid;
	wdev->lsids.completed = super->written_lsid;
	wdev->lsids.latest = super->written_lsid;
	spin_unlock(&wdev->lsid_lock);

	wdev->ring_buffer_size = super->ring_buffer_size;
	wdev->ring_buffer_off = get_ring_buffer_offset_2(super);
	wdev->log_checksum_salt = super->log_checksum_salt;
	wdev->size = super->device_size;
	if (wdev->size > wdev->ddev_size) {
		LOGe("device size > underlying data device size.\n");
		goto out_ldev_init;
	}

	/* Set parameters. */
	wdev->max_logpack_pb =
		min(param->max_logpack_kb * 1024 / wdev->physical_bs,
			MAX_TOTAL_IO_SIZE_IN_LOGPACK_HEADER);
	wdev->log_flush_interval_jiffies =
		msecs_to_jiffies(param->log_flush_interval_ms);
	if (wdev->log_flush_interval_jiffies == 0) {
		wdev->log_flush_interval_pb = 0;
	} else {
		wdev->log_flush_interval_pb = param->log_flush_interval_mb
			* (1024 * 1024 / wdev->physical_bs);
	}
	ASSERT(0 < param->min_pending_mb);
	ASSERT(param->min_pending_mb < param->max_pending_mb);
	wdev->max_pending_sectors
		= param->max_pending_mb * 1024 * 1024 / LOGICAL_BLOCK_SIZE;
	wdev->min_pending_sectors
		= param->min_pending_mb * 1024 * 1024 / LOGICAL_BLOCK_SIZE;
	wdev->queue_stop_timeout_jiffies =
		msecs_to_jiffies(param->queue_stop_timeout_ms);
	wdev->n_pack_bulk = 128; /* default value. */
	if (param->n_pack_bulk > 0) { wdev->n_pack_bulk = param->n_pack_bulk; }
	wdev->n_io_bulk = 1024; /* default value. */
	if (param->n_io_bulk > 0) { wdev->n_io_bulk = param->n_io_bulk; }

	lq = bdev_get_queue(wdev->ldev);
	dq = bdev_get_queue(wdev->ddev);
	/* Set chunk size. */
	set_chunk_sectors(&wdev->ldev_chunk_sectors, wdev->physical_bs, lq);
	set_chunk_sectors(&wdev->ddev_chunk_sectors, wdev->physical_bs, dq);

	LOGi("max_logpack_pb: %u "
		"log_flush_interval_jiffies: %u "
		"log_flush_interval_pb: %u "
		"max_pending_sectors: %u "
		"min_pending_sectors: %u "
		"queue_stop_timeout_jiffies: %u "
		"n_pack_bulk: %u n_io_bulk: %u "
		"chunk_sectors ldev %u ddev %u.\n",
		wdev->max_logpack_pb,
		wdev->log_flush_interval_jiffies,
		wdev->log_flush_interval_pb,
		wdev->max_pending_sectors,
		wdev->min_pending_sectors,
		wdev->queue_stop_timeout_jiffies,
		wdev->n_pack_bulk, wdev->n_io_bulk,
		wdev->ldev_chunk_sectors,
		wdev->ddev_chunk_sectors);

	/* Set device name. */
	if (walb_set_name(wdev, minor, param->name) != 0) {
		LOGe("Set device name failed.\n");
		goto out_ldev_init;
	}
	ASSERT_SECTOR_DATA(wdev->lsuper0);
	dev_name = super->name;
	snprintf(param->name, DISK_NAME_LEN, dev_name);

	/*
	 * Prepare walb block device.
	 */
	if (walb_prepare_device(wdev, minor, dev_name) != 0) {
		LOGe("walb_prepare_device() failed.\n");
		goto out_ldev_init;
	}

	/*
	 * Prepare walblog block device.
	 */
	if (walblog_prepare_device(wdev, minor + 1, dev_name) != 0) {
		goto out_walbdev;
	}

	/* Setup iocore data. */
	if (!iocore_initialize(wdev)) {
		LOGe("iocore initialization failed.\n");
		goto out_walblogdev;
	}

	/*
	 * Redo
	 * 1. Read logpacks starting from written_lsid.
	 * 2. Write the corresponding data of the logpacks to data device.
	 * 3. Rewrite the latest logpack if partially valid.
	 * 4. Update written_lsid, latest_lsid, (and completed_lsid).
	 * 5. Sync superblock.
	 */
	retb = execute_redo(wdev);
	if (!retb) {
		LOGe("Redo failed.\n");
		goto out_iocore_init;
	}
	spin_lock(&wdev->lsid_lock);
	latest_lsid = wdev->lsids.latest;
	oldest_lsid = wdev->lsids.oldest;
#ifdef WALB_DEBUG
	completed_lsid = wdev->lsids.completed;
	written_lsid = wdev->lsids.written;
	prev_written_lsid = wdev->lsids.prev_written;
	flush_lsid = wdev->lsids.flush;
#endif
	spin_unlock(&wdev->lsid_lock);
#ifdef WALB_DEBUG
	ASSERT(prev_written_lsid == latest_lsid);
	ASSERT(prev_written_lsid == completed_lsid);
	ASSERT(prev_written_lsid == flush_lsid);
	ASSERT(prev_written_lsid == written_lsid);
#endif

	/* Check the device overflows or not. */
	if (latest_lsid - oldest_lsid > wdev->ring_buffer_size
		|| !walb_check_lsid_valid(wdev, oldest_lsid)) {
		set_bit(WALB_STATE_OVERFLOW, &wdev->flags);
		LOGw("Set overflow flag.\n");
	}

	return wdev;

out_iocore_init:
	iocore_finalize(wdev);
out_walblogdev:
	walblog_finalize_device(wdev);
out_walbdev:
	walb_finalize_device(wdev);
out_ldev_init:
	walb_ldev_finalize(wdev, false);
out_ddev:
	if (wdev->ddev) {
		walb_unlock_bdev(wdev->ddev);
	}
out_ldev:
	if (wdev->ldev) {
		walb_unlock_bdev(wdev->ldev);
	}
out_free:
	kfree(wdev);
out:
	return NULL;
}

/**
 * Finalize a walb device.
 * Remaining write IOs will be completely finished and flushed.
 * You must call @unregister_wdev() before calling this.
 */
void finalize_wdev(struct walb_dev *wdev)
{
	WLOGd(wdev, "finalizing...\n");

	set_bit(WALB_STATE_FINALIZE, &wdev->flags);

	melt_if_frozen(wdev, false);
	iocore_flush(wdev);
	walb_ldev_finalize(wdev, true);

	if (wdev->ddev)
		walb_unlock_bdev(wdev->ddev);
	if (wdev->ldev)
		walb_unlock_bdev(wdev->ldev);

	WLOGd(wdev, "finalized.\n");
}

/**
 * Destroy wdev structure.
 * You must call @finalize_wdev() before calling this.
 */
void destroy_wdev(struct walb_dev *wdev)
{
	const u32 minor = MINOR(wdev->devt);
	int n_users;

	LOGd("destroying %u...\n", minor);

	while ((n_users = atomic_read(&wdev->n_users)) > 0) {
		LOGi("%u: sleep 1000ms to wait for users (%d)\n"
			, minor, n_users);
		msleep(1000);
	}

	walblog_finalize_device(wdev);
	walb_finalize_device(wdev);
	iocore_finalize(wdev);
	kfree(wdev);
	LOGi("%u: destroyed.\n", minor);
}

/**
 * Task runner for deferred calling of destroy_wdev().
 */
void task_destroy_wdev(struct work_struct *task)
{
	struct walb_dev *wdev = container_of(task, struct walb_dev, destroy_task);
	destroy_wdev(wdev);
}

/**
 * Register wdev.
 * You must call @prepare_wdev() before calling this.
 */
bool register_wdev(struct walb_dev *wdev)
{
	ASSERT(wdev);
	ASSERT(wdev->gd);
	ASSERT(wdev->log_gd);

	start_checkpointing(&wdev->cpd);

	walblog_register_device(wdev);
	walb_register_device(wdev);

	if (walb_sysfs_init(wdev)) {
		WLOGe(wdev, "walb_sysfs_init failed.\n");
		goto error;
	}
	return true;

error:
	walb_unregister_device(wdev);
	walblog_unregister_device(wdev);
	stop_checkpointing(&wdev->cpd);
	return false;
}

/**
 * Unregister wdev.
 * You must call @destroy_wdev() after calling this.
 */
void unregister_wdev(struct walb_dev *wdev)
{
	ASSERT(wdev);

	stop_checkpointing(&wdev->cpd);
	walb_sysfs_exit(wdev);

	walblog_unregister_device(wdev);
	walb_unregister_device(wdev);
}

/*******************************************************************************
 * Module definitions.
 *******************************************************************************/

module_init(walb_init);
module_exit(walb_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Block-level WAL");
MODULE_ALIAS(WALB_NAME);
/* MODULE_ALIAS_BLOCKDEV_MAJOR(WALB_MAJOR); */
