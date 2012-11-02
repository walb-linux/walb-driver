/**
 * wrapper_blk.c - Wrapper block device driver for test.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include <linux/blkdev.h>
#include <linux/workqueue.h>
#include <linux/string.h>

#include "walb/common.h"
#include "walb/disk_name.h"
#include "walb/block_size.h"
#include "pack_work.h"
#include "wrapper_blk.h"

/*******************************************************************************
 * Module variable definitions.
 *******************************************************************************/

/* Device number (major). */
int wrapper_blk_major_ = 0;

/* Devices. */
#define MAX_N_DEVICES 32
struct wrdev_devices
{
	struct wrapper_blk_dev *wrdev[MAX_N_DEVICES];
	unsigned int n_active_devices; /* Number of active devices. */
	spinlock_t lock; /* Lock for access to devices_. */

} devices_;

/*******************************************************************************
 * Module parameter definitions.
 *******************************************************************************/

module_param_named(wrapper_blk_major, wrapper_blk_major_, int, S_IRUGO);

/*******************************************************************************
 * Macro definitions.
 *******************************************************************************/

#define ASSERT_WRAPPER_BLK_DEV(wrdev) assert_wrapper_blk_dev(wrdev)

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

/* Block device operations. */
static int wrapper_blk_open(struct block_device *bdev, fmode_t mode);
static int wrapper_blk_release(struct gendisk *gd, fmode_t mode);
static int wrapper_blk_ioctl(struct block_device *bdev, fmode_t mode,
			unsigned int cmd, unsigned long arg);

/* Operations with devices_. */
static void init_devices(void);
static bool add_to_devices(struct wrapper_blk_dev *wrdev);
static struct wrapper_blk_dev* del_from_devices(unsigned int minor);
static struct wrapper_blk_dev* get_from_devices(unsigned int minor);

/* Utilities */
static bool wrdev_register_detail(
	unsigned int minor, u64 capacity, unsigned int pbs,
	make_request_fn *make_request_fn,
	request_fn_proc *request_fn_proc);
static struct wrapper_blk_dev* alloc_and_partial_init_wrdev(
	unsigned int minor, u64 capacity, unsigned int pbs);

static bool init_queue_and_disk(struct wrapper_blk_dev *wrdev);
static void fin_queue_and_disk(struct wrapper_blk_dev *wrdev);
static void assert_wrapper_blk_dev(struct wrapper_blk_dev *wrdev);

/* For exit. */
static void stop_and_unregister_all_devices(void);

/*******************************************************************************
 * Static variables definition.
 *******************************************************************************/

/**
 * The device operations structure.
 */
static struct block_device_operations wrapper_blk_ops_ = {
	.owner		 = THIS_MODULE,
	.open		 = wrapper_blk_open,
	.release	 = wrapper_blk_release,
	.ioctl		 = wrapper_blk_ioctl
};

/*******************************************************************************
 * Exported Global function definitions.
 *******************************************************************************/

/**
 * Register new block device with bio interface.
 */
bool wrdev_register_with_bio(unsigned int minor, u64 capacity,
			unsigned int pbs,
			make_request_fn *make_request_fn)
{
	return wrdev_register_detail(minor, capacity, pbs, make_request_fn, NULL);
}
EXPORT_SYMBOL_GPL(wrdev_register_with_bio);

/**
 * Register new block device with request interface.
 */
bool wrdev_register_with_req(unsigned int minor, u64 capacity,
			unsigned int pbs,
			request_fn_proc *request_fn_proc)
{
	return wrdev_register_detail(minor, capacity, pbs, NULL, request_fn_proc);
}
EXPORT_SYMBOL_GPL(wrdev_register_with_req);

/**
 * Unregister a block device.
 */
bool wrdev_unregister(unsigned int minor)
{
	struct wrapper_blk_dev *wrdev;

	wrdev = del_from_devices(minor);
	if (!wrdev) {
		LOGe("Not found device with minor %u.\n", minor);
		return false;
	}	 
	fin_queue_and_disk(wrdev);
	return true;
}
EXPORT_SYMBOL_GPL(wrdev_unregister);

/**
 * Start a block device.
 * Call this after @wrdev_register_XXX().
 */
bool wrdev_start(unsigned int minor)
{
	struct wrapper_blk_dev *wrdev;
	
	wrdev = get_from_devices(minor);
	if (!wrdev) {
		LOGe("Not found device with minor %u.\n", minor);
		goto error0;
	}
	ASSERT_WRAPPER_BLK_DEV(wrdev);
		
	if (test_and_set_bit(0, &wrdev->is_started)) {
		LOGe("Device with minor %u already started.\n", minor);
		goto error0;
	} else {
		add_disk(wrdev->gd);
		LOGi("Start device with minor %u.\n", minor);
	}
	return true;
error0:
	return false;
}
EXPORT_SYMBOL_GPL(wrdev_start);

/**
 * Stop a block device.
 * Call this before @wrdev_unregister().
 */
bool wrdev_stop(unsigned int minor)
{
	struct wrapper_blk_dev *wrdev;
	
	wrdev = get_from_devices(minor);
	if (!wrdev) {
		LOGe("Not found device with minor %u.\n", minor);
		goto error0;
	}

	ASSERT_WRAPPER_BLK_DEV(wrdev);
	
	if (test_and_clear_bit(0, &wrdev->is_started)) {
		ASSERT(wrdev->gd);
		del_gendisk(wrdev->gd);
		LOGn("Stop device with minor %u.\n", minor);
	} else {
		LOGe("Device wit minor %u is already stopped.\n", minor);
		goto error0;
	}
	return true;
error0:
	return false;
}
EXPORT_SYMBOL_GPL(wrdev_stop);

/**
 * Get major number.
 */
unsigned int wrdev_get_major(void)
{
	return (unsigned int)max(wrapper_blk_major_, 0);
}
EXPORT_SYMBOL_GPL(wrdev_get_major);

/**
 * Get wrdev with a minor number.
 */
struct wrapper_blk_dev* wrdev_get(unsigned int minor)
{
	return get_from_devices(minor);
}
EXPORT_SYMBOL_GPL(wrdev_get);

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * Device operation.
 */
static int wrapper_blk_open(struct block_device *bdev, fmode_t mode)
{
	return 0;
}

/**
 * Device operation.
 */
static int wrapper_blk_release(struct gendisk *gd, fmode_t mode)
{
	return 0;
}

/**
 * Device operation.
 */
static int wrapper_blk_ioctl(struct block_device *bdev, fmode_t mode,
			unsigned int cmd, unsigned long arg)
{
	return -ENOTTY;
}

/**
 * Initialize wrdev_devices and realted data.
 */
static void init_devices(void)
{
	int i;
	
	for (i = 0; i < MAX_N_DEVICES; i++) {
		devices_.wrdev[i] = NULL;
	}
	devices_.n_active_devices = 0;
	spin_lock_init(&devices_.lock);
}

/**
 * Add a specified device.
 */
static bool add_to_devices(struct wrapper_blk_dev *wrdev)
{
	ASSERT(wrdev);

	if (get_from_devices(wrdev->minor)) {
		return false;
	}

	spin_lock(&devices_.lock);
	devices_.wrdev[wrdev->minor] = wrdev;
	devices_.n_active_devices++;
	ASSERT(devices_.n_active_devices >= 0);
	spin_unlock(&devices_.lock);
	return true;
}

/**
 * Delete a specified device.
 */
static struct wrapper_blk_dev* del_from_devices(unsigned int minor)
{
	struct wrapper_blk_dev *wrdev;

	ASSERT(minor < MAX_N_DEVICES);

	spin_lock(&devices_.lock);
	wrdev = devices_.wrdev[minor];
	if (wrdev) {
		devices_.wrdev[minor] = NULL;
		devices_.n_active_devices--;
		ASSERT(devices_.n_active_devices >= 0);
	}
	spin_unlock(&devices_.lock);
	return wrdev;
}

/**
 * Get a specified device.
 *
 * RETURN:
 * Returns NULL if the device does not exist.
 */
static struct wrapper_blk_dev* get_from_devices(unsigned int minor)
{
	struct wrapper_blk_dev *wrdev;
	
	if (minor >= MAX_N_DEVICES) {
		return NULL;
	}
	spin_lock(&devices_.lock);
	wrdev = devices_.wrdev[minor];
	spin_unlock(&devices_.lock);
	return wrdev;
}

/**
 * Create and initialize wrapper_blk_dev data.
 *
 * @minor minor device number.
 * @capacity Device capacity [logical block]
 * RETURN:
 * Created wrdev in success, or NULL.
 * NOTE:
 * Call this before @register_wrdev();
 */
static struct wrapper_blk_dev* alloc_and_partial_init_wrdev(
	unsigned int minor, u64 capacity, unsigned int pbs)
{
	struct wrapper_blk_dev *wrdev;
	
	/* Allocate */
	wrdev = ZALLOC(sizeof(struct wrapper_blk_dev), GFP_KERNEL);
	if (wrdev == NULL) {
		LOGe("memory allocation failed.\n");
		goto error0;
	}
	
	/* Initialize */
	wrdev->minor = minor;
	wrdev->capacity = capacity;
	snprintf(wrdev->name, WRAPPER_BLK_DEV_NAME_MAX_LEN, "%d", minor);
	wrdev->pbs = pbs;

	spin_lock_init(&wrdev->lock);
	wrdev->queue = NULL;
	/* use_make_request_fn is not initialized here. */
	/* make_request_fn and request_fn_proc is not initialized here. */
	wrdev->gd = NULL;
	wrdev->is_started = false;
	wrdev->private_data = NULL;

	return wrdev;
error0:
	return NULL;
}

/**
 * Register a device.
 *
 * @minor minor device number.
 * @capacity capacity [logical block].
 * @pbs physical block size.
 * @make_request_fn callback for bio.
 * @request_fn_proc callback for request.
 *    This is used when make_request_fn is NULL.
 * RETURN:
 * true in success, or false.
 */
static bool wrdev_register_detail(unsigned int minor, u64 capacity,
				unsigned int pbs,
				make_request_fn *make_request_fn,
				request_fn_proc *request_fn_proc)
{
	struct wrapper_blk_dev *wrdev;

	/* Allocate and initialize partially. */
	wrdev = alloc_and_partial_init_wrdev(minor, capacity, pbs);
	if (!wrdev) {
		LOGe("Memory allocation failed.\n");
		goto error0;
	}

	/* Set request callback. */
	if (make_request_fn) {
		wrdev->use_make_request_fn = true;
		wrdev->make_request_fn = make_request_fn;
	} else {
		wrdev->use_make_request_fn = false;
		wrdev->request_fn_proc = request_fn_proc;
	}

	/* Init quene and disk. */
	if (!init_queue_and_disk(wrdev)) {
		LOGe("init_queue_and_disk() failed.\n");
		goto error1;
	}

	/* Add the device to global variables. */
	if (!add_to_devices(wrdev)) {
		LOGe("Already device with minor %u registered.\n", wrdev->minor);
		goto error2;
	}
	return true;

error2:
	fin_queue_and_disk(wrdev);
error1:
	FREE(wrdev);
error0:
	return false;
}

/**
 * Initialize queue and disk data.
 *
 * CONTEXT:
 * Non-IRQ.
 * RETURN:
 * true in success, or false.
 */
static bool init_queue_and_disk(struct wrapper_blk_dev *wrdev)
{
	struct request_queue *q;
	struct gendisk *gd;
	
	ASSERT(wrdev);

	/* Cleanup */
	wrdev->queue = NULL;
	wrdev->gd = NULL;

	/* Allocate and initialize queue. */
	if (wrdev->use_make_request_fn) {
		q = blk_alloc_queue(GFP_KERNEL);
		if (!q) {
			LOGe("blk_alloc_queue failed.\n");
			goto error0;
		}
		blk_queue_make_request(q, wrdev->make_request_fn);
	} else {
		q = blk_init_queue(wrdev->request_fn_proc, &wrdev->lock);
		if (!q) {
			LOGe("blk_init_queue failed.\n");
			goto error0;
		}
		if (elevator_change(q, "noop")) {
			LOGe("changing elevator algorithm failed.\n");
			blk_cleanup_queue(q);
			goto error0;
		}
	}
	/* blk_queue_physical_block_size(q, wrdev->pbs); */
	/* blk_queue_logical_block_size(q, LOGICAL_BLOCK_SIZE); */
	/* blk_queue_io_min(q, wrdev->pbs); */
	/* blk_queue_io_opt(q, wrdev->pbs); */

	/* Accept REQ_DISCARD. */
	/* Do nothing. */

	/* Accept REQ_FLUSH and REQ_FUA. */
	/* Do nothing. */
	
	q->queuedata = wrdev;
	wrdev->queue = q;

	/* Allocate and initialize disk. */
	gd = alloc_disk(1);
	if (!gd) {
		LOGe("alloc_disk failed.\n");
		goto error1;
	}
	gd->major = wrapper_blk_major_;
	gd->first_minor = wrdev->minor;
	
	gd->fops = &wrapper_blk_ops_;
	gd->queue = wrdev->queue;
	gd->private_data = wrdev;
	set_capacity(gd, wrdev->capacity);
	snprintf(gd->disk_name, DISK_NAME_LEN,
		"%s/%s", WRAPPER_BLK_DIR_NAME, wrdev->name);
	wrdev->gd = gd;

	return true;

error1:
	fin_queue_and_disk(wrdev);
error0:
	return false;
}

/**
 * Finalize queue and disk data.
 *
 * CONTEXT:
 * Non-IRQ.
 */
static void fin_queue_and_disk(struct wrapper_blk_dev *wrdev)
{
	ASSERT(wrdev);

	if (wrdev->gd) {
		put_disk(wrdev->gd);
		wrdev->gd = NULL;
	}
	if (wrdev->queue) {
		blk_cleanup_queue(wrdev->queue);
		wrdev->queue = NULL;
	}
}

static void assert_wrapper_blk_dev(struct wrapper_blk_dev *wrdev)
{
	ASSERT(wrdev);
	ASSERT(wrdev->capacity > 0);
	ASSERT_PBS(wrdev->pbs);
	ASSERT(strlen(wrdev->name) > 0);
	ASSERT(wrdev->queue);
	ASSERT(wrdev->gd);
}

static void stop_and_unregister_all_devices(void)
{
	int i;
	struct wrapper_blk_dev *wrdev;
	
	for (i = 0; i < MAX_N_DEVICES; i++) {
		wrdev = get_from_devices(i);
		if (wrdev) {
			wrdev_stop(i);
			wrdev_unregister(i);
			FREE(wrdev);
		}
	}
}

/*******************************************************************************
 * Init/exit functions definition.
 *******************************************************************************/

static int __init wrapper_blk_init(void)
{
	LOGi("Wrapper-blk module init.\n");
	
	/* Register a block device module. */
	wrapper_blk_major_ = register_blkdev(wrapper_blk_major_, WRAPPER_BLK_NAME);
	if (wrapper_blk_major_ <= 0) {
		LOGe("unable to get major device number.\n");
		return -EBUSY;
	}

	/* Init pack_work. */
	if (!pack_work_init()) {
		goto error0;
	}

	/* Initialize devices_. */
	init_devices();

	return 0;
	
error0:
	unregister_blkdev(wrapper_blk_major_, WRAPPER_BLK_NAME);
	return -ENOMEM;
}

static void wrapper_blk_exit(void)
{
	stop_and_unregister_all_devices();
	pack_work_exit();
	unregister_blkdev(wrapper_blk_major_, WRAPPER_BLK_NAME);

	LOGi("Wrapper-blk module exit.\n");
}

module_init(wrapper_blk_init);
module_exit(wrapper_blk_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Simple Wrapper Block Device for Test");
MODULE_ALIAS(WRAPPER_BLK_NAME);
/* MODULE_ALIAS_BLOCKDEV_MAJOR(WRAPPER_BLK_MAJOR); */
