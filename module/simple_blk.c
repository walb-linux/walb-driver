/**
 * simple_blk.c - Simple block device driver for performance test.
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
#include "treemap.h"
#include "hashtbl.h"
#include "simple_blk.h"

/*******************************************************************************
 * Module variable definitions.
 *******************************************************************************/

/* Device number (major). */
int simple_blk_major_ = 0;

/* Devices. */
#define MAX_N_DEVICES 32
struct sdev_devices
{
	struct simple_blk_dev *sdev[MAX_N_DEVICES];
	unsigned int n_active_devices; /* Number of active devices. */
	spinlock_t lock; /* Lock for access to devices_. */

} devices_;

/*******************************************************************************
 * Module parameter definitions.
 *******************************************************************************/

module_param_named(simple_blk_major, simple_blk_major_, int, S_IRUGO);

/*******************************************************************************
 * Macro definitions.
 *******************************************************************************/

#define ASSERT_SIMPLE_BLK_DEV(sdev) assert_simple_blk_dev(sdev)

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

/* Block device operations. */
static int simple_blk_open(struct block_device *bdev, fmode_t mode);
static int simple_blk_release(struct gendisk *gd, fmode_t mode);
static int simple_blk_ioctl(struct block_device *bdev, fmode_t mode,
			unsigned int cmd, unsigned long arg);

/* Operations with devices_. */
static void init_devices(void);
static bool add_to_devices(struct simple_blk_dev *sdev);
static struct simple_blk_dev* del_from_devices(unsigned int minor);
static struct simple_blk_dev* get_from_devices(unsigned int minor);

/* Utilities */
static bool sdev_register_detail(
	unsigned int minor, u64 capacity, unsigned int pbs,
	make_request_fn *make_request_fn,
	request_fn_proc *request_fn_proc);
static struct simple_blk_dev* alloc_and_partial_init_sdev(
	unsigned int minor, u64 capacity, unsigned int pbs);

static bool init_queue_and_disk(struct simple_blk_dev *sdev);
static void fin_queue_and_disk(struct simple_blk_dev *sdev);
static void assert_simple_blk_dev(struct simple_blk_dev *sdev);

/* For exit. */
static void stop_and_unregister_all_devices(void);

/*******************************************************************************
 * Static variables definition.
 *******************************************************************************/

/**
 * The device operations structure.
 */
static struct block_device_operations simple_blk_ops_ = {
	.owner		 = THIS_MODULE,
	.open		 = simple_blk_open,
	.release	 = simple_blk_release,
	.ioctl		 = simple_blk_ioctl
};

/*******************************************************************************
 * Exported Global function definitions.
 *******************************************************************************/

/**
 * Register new block device with bio interface.
 */
bool sdev_register_with_bio(unsigned int minor, u64 capacity, unsigned int pbs,
			make_request_fn *make_request_fn)
{
	return sdev_register_detail(minor, capacity, pbs, make_request_fn, NULL);
}
EXPORT_SYMBOL_GPL(sdev_register_with_bio);

/**
 * Register new block device with request interface.
 */
bool sdev_register_with_req(unsigned int minor, u64 capacity, unsigned int pbs,
			request_fn_proc *request_fn_proc)
{
	return sdev_register_detail(minor, capacity, pbs, NULL, request_fn_proc);
}
EXPORT_SYMBOL_GPL(sdev_register_with_req);

/**
 * Unregister a block device.
 */
bool sdev_unregister(unsigned int minor)
{
	struct simple_blk_dev *sdev;

	sdev = del_from_devices(minor);
	if (!sdev) {
		LOGe("Not found device with minor %u.\n", minor);
		return false;
	}	 
	fin_queue_and_disk(sdev);
	FREE(sdev);
	return true;
}
EXPORT_SYMBOL_GPL(sdev_unregister);

/**
 * Start a block device.
 * Call this after @sdev_register_XXX().
 */
bool sdev_start(unsigned int minor)
{
	struct simple_blk_dev *sdev;
	
	sdev = get_from_devices(minor);
	if (!sdev) {
		LOGe("Not found device with minor %u.\n", minor);
		goto error0;
	}
	ASSERT_SIMPLE_BLK_DEV(sdev);

	if (test_and_set_bit(0, &sdev->is_started)) {
		LOGe("Device with minor %u already started.\n", minor);
		goto error0;
	} else {
		add_disk(sdev->gd);
		LOGi("Start device with minor %u.\n", minor);
	}
	return true;
error0:
	return false;
}
EXPORT_SYMBOL_GPL(sdev_start);

/**
 * Stop a block device.
 * Call this before @sdev_unregister().
 */
bool sdev_stop(unsigned int minor)
{
	struct simple_blk_dev *sdev;
	
	sdev = get_from_devices(minor);
	if (!sdev) {
		LOGe("Not found device with minor %u.\n", minor);
		goto error0;
	}

	ASSERT_SIMPLE_BLK_DEV(sdev);
	
	if (test_and_clear_bit(0, &sdev->is_started)) {
		ASSERT(sdev->gd);
		del_gendisk(sdev->gd);
		LOGi("Stop device with minor %u.\n", minor);
	} else {
		LOGe("Device wit minor %u is already stopped.\n", minor);
		goto error0;
	}
	return true;
error0:
	return false;
}
EXPORT_SYMBOL_GPL(sdev_stop);

/**
 * Get sdev with a minor number.
 */
struct simple_blk_dev* sdev_get(unsigned int minor)
{
	return get_from_devices(minor);
}
EXPORT_SYMBOL_GPL(sdev_get);

/**
 * Create a workqueue with a type.
 */
struct workqueue_struct* create_wq_io(const char *name, enum workqueue_type type)
{
	struct workqueue_struct *wq = NULL;

	switch (type) {
	case WQ_TYPE_SINGLE:
		/* Single thread workqueue. This may be slow. */
		wq = create_singlethread_workqueue(name);
		LOGn("Use workqueue type: SINGLE.\n");
		break;
		
	case WQ_TYPE_UNBOUND:
		/* Worker may not use the same CPU with enqueuer. */
		wq = alloc_workqueue(name, WQ_MEM_RECLAIM | WQ_UNBOUND , 0);
		LOGn("Use workqueue type: UNBOUND.\n");
		break;
		
	case WQ_TYPE_NORMAL:
		/* Default. This is the fastest. */	
		wq = alloc_workqueue(name, WQ_MEM_RECLAIM, 0);
		LOGn("Use workqueue type: NORMAL.\n");
		break;
	default:
		LOGe("Not supported wq_io_type %s.\n", name);
	}
	return wq;
}
EXPORT_SYMBOL_GPL(create_wq_io);

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * Device operation.
 */
static int simple_blk_open(struct block_device *bdev, fmode_t mode)
{
	return 0;
}

/**
 * Device operation.
 */
static int simple_blk_release(struct gendisk *gd, fmode_t mode)
{
	return 0;
}

/**
 * Device operation.
 */
static int simple_blk_ioctl(struct block_device *bdev, fmode_t mode,
			unsigned int cmd, unsigned long arg)
{
	return -ENOTTY;
}

/**
 * Initialize sdev_devices and realted data.
 */
static void init_devices(void)
{
	int i;
	
	for (i = 0; i < MAX_N_DEVICES; i ++) {
		devices_.sdev[i] = NULL;
	}
	devices_.n_active_devices = 0;
	spin_lock_init(&devices_.lock);
}

/**
 * Add a specified device.
 */
static bool add_to_devices(struct simple_blk_dev *sdev)
{
	ASSERT(sdev);

	if (get_from_devices(sdev->minor)) {
		return false;
	}

	spin_lock(&devices_.lock);
	devices_.sdev[sdev->minor] = sdev;
	devices_.n_active_devices ++;
	ASSERT(devices_.n_active_devices >= 0);
	spin_unlock(&devices_.lock);
	return true;
}

/**
 * Delete a specified device.
 */
static struct simple_blk_dev* del_from_devices(unsigned int minor)
{
	struct simple_blk_dev *sdev;

	ASSERT(minor < MAX_N_DEVICES);

	spin_lock(&devices_.lock);
	sdev = devices_.sdev[minor];
	if (sdev) {
		devices_.sdev[minor] = NULL;
		devices_.n_active_devices --;
		ASSERT(devices_.n_active_devices >= 0);
	}
	spin_unlock(&devices_.lock);
	return sdev;
}

/**
 * Get a specified device.
 *
 * RETURN:
 * Returns NULL if the device does not exist.
 */
static struct simple_blk_dev* get_from_devices(unsigned int minor)
{
	struct simple_blk_dev *sdev;
	
	if (minor >= MAX_N_DEVICES) {
		return NULL;
	}
	spin_lock(&devices_.lock);
	sdev = devices_.sdev[minor];
	spin_unlock(&devices_.lock);
	return sdev;
}

/**
 * Create and initialize simple_blk_dev data.
 *
 * @minor minor device number.
 * @capacity Device capacity [logical block]
 * RETURN:
 * Created sdev in success, or NULL.
 * NOTE:
 * Call this before @register_sdev();
 */
static struct simple_blk_dev* alloc_and_partial_init_sdev(
	unsigned int minor, u64 capacity, unsigned int pbs)
{
	struct simple_blk_dev *sdev;
	
	/* Allocate */
	sdev = ZALLOC(sizeof(struct simple_blk_dev), GFP_KERNEL);
	if (sdev == NULL) {
		LOGe("memory allocation failed.\n");
		goto error0;
	}
	
	/* Initialize */
	sdev->minor = minor;
	sdev->capacity = capacity;
	snprintf(sdev->name, SIMPLE_BLK_DEV_NAME_MAX_LEN, "%d", minor);
	sdev->pbs = pbs;

	spin_lock_init(&sdev->lock);
	sdev->queue = NULL;
	/* use_make_request_fn is not initialized here. */
	/* make_request_fn and request_fn_proc is not initialized here. */
	sdev->gd = NULL;
	sdev->is_started = 0;
	sdev->private_data = NULL;

	return sdev;
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
static bool sdev_register_detail(unsigned int minor, u64 capacity,
				unsigned int pbs,
				make_request_fn *make_request_fn,
				request_fn_proc *request_fn_proc)
{
	struct simple_blk_dev *sdev;

	/* Allocate and initialize partially. */
	sdev = alloc_and_partial_init_sdev(minor, capacity, pbs);
	if (!sdev) {
		LOGe("Memory allocation failed.\n");
		goto error0;
	}

	/* Set request callback. */
	if (make_request_fn) {
		sdev->use_make_request_fn = true;
		sdev->make_request_fn = make_request_fn;
	} else {
		sdev->use_make_request_fn = false;
		sdev->request_fn_proc = request_fn_proc;
	}

	/* Init quene and disk. */
	if (!init_queue_and_disk(sdev)) {
		LOGe("init_queue_and_disk() failed.\n");
		goto error1;
	}

	/* Add the device to global variables. */
	if (!add_to_devices(sdev)) {
		LOGe("Already device with minor %u registered.\n", sdev->minor);
		goto error2;
	}
	return true;

error2:
	fin_queue_and_disk(sdev);
error1:
	FREE(sdev);
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
static bool init_queue_and_disk(struct simple_blk_dev *sdev)
{
	struct request_queue *q;
	struct gendisk *gd;
	
	ASSERT(sdev);

	/* Cleanup */
	sdev->queue = NULL;
	sdev->gd = NULL;

	/* Allocate and initialize queue. */
	if (sdev->use_make_request_fn) {
		q = blk_alloc_queue(GFP_KERNEL);
		if (!q) {
			LOGe("blk_alloc_queue failed.\n");
			goto error0;
		}
		blk_queue_make_request(q, sdev->make_request_fn);
	} else {
		q = blk_init_queue(sdev->request_fn_proc, &sdev->lock);
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
	blk_queue_physical_block_size(q, sdev->pbs);
	blk_queue_logical_block_size(q, LOGICAL_BLOCK_SIZE);
	/* blk_queue_io_min(q, sdev->pbs); */
	blk_queue_io_opt(q, sdev->pbs);

	/* Accept REQ_DISCARD. */
	/* Do nothing. */

	/* Accept REQ_FLUSH and REQ_FUA. */
	/* Do nothing. */
	
	q->queuedata = sdev;
	sdev->queue = q;

	/* Allocate and initialize disk. */
	gd = alloc_disk(1);
	if (!gd) {
		LOGe("alloc_disk failed.\n");
		goto error1;
	}
	gd->major = simple_blk_major_;
	gd->first_minor = sdev->minor;
	
	gd->fops = &simple_blk_ops_;
	gd->queue = sdev->queue;
	gd->private_data = sdev;
	set_capacity(gd, sdev->capacity);
	snprintf(gd->disk_name, DISK_NAME_LEN,
		"%s/%s", SIMPLE_BLK_DIR_NAME, sdev->name);
	sdev->gd = gd;

	return true;

error1:
	fin_queue_and_disk(sdev);
error0:
	return false;
}

/**
 * Finalize queue and disk data.
 *
 * CONTEXT:
 * Non-IRQ.
 */
static void fin_queue_and_disk(struct simple_blk_dev *sdev)
{
	ASSERT(sdev);

	if (sdev->gd) {
		put_disk(sdev->gd);
		sdev->gd = NULL;
	}
	if (sdev->queue) {
		blk_cleanup_queue(sdev->queue);
		sdev->queue = NULL;
	}
}

static void assert_simple_blk_dev(struct simple_blk_dev *sdev)
{
	ASSERT(sdev);
	ASSERT(sdev->capacity > 0);
	ASSERT_PBS(sdev->pbs);
	ASSERT(strlen(sdev->name) > 0);
	ASSERT(sdev->queue);
	ASSERT(sdev->gd);
}

static void stop_and_unregister_all_devices(void)
{
	int i;
	struct simple_blk_dev *sdev;
	
	for (i = 0; i < MAX_N_DEVICES; i ++) {
		sdev = get_from_devices(i);
		if (sdev) {
			sdev_stop(i);
			sdev_unregister(i);
		}
	}
}

/*******************************************************************************
 * Init/exit functions definition.
 *******************************************************************************/

static int __init simple_blk_init(void)
{
	ASSERT(!in_interrupt());
	LOGi("Simple-blk module init.\n");
	
	/* Register a block device module. */
	simple_blk_major_ = register_blkdev(simple_blk_major_, SIMPLE_BLK_NAME);
	if (simple_blk_major_ <= 0) {
		LOGe("unable to get major device number.\n");
		goto error0;
	}

	/* Initialize devices_. */
	init_devices();

	return 0;
#if 0
error1:
	unregister_blkdev(simple_blk_major_, SIMPLE_BLK_NAME);
	return -ENOMEM;
#endif
error0:
	return -EBUSY;
}

static void simple_blk_exit(void)
{
	ASSERT(!in_interrupt());
	LOGd("in_atomic: %u.\n", in_atomic());
	
	stop_and_unregister_all_devices();
	unregister_blkdev(simple_blk_major_, SIMPLE_BLK_NAME);

	LOGi("Simple-blk module exit.\n");
}

module_init(simple_blk_init);
module_exit(simple_blk_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Simple Block Device for Test");
MODULE_ALIAS(SIMPLE_BLK_NAME);
/* MODULE_ALIAS_BLOCKDEV_MAJOR(SIMPLE_BLK_MAJOR); */
