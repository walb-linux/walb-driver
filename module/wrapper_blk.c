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
#include "wrapper_blk.h"

/*******************************************************************************
 * Module variable definitions.
 *******************************************************************************/

/* Device number (major). */
int wrapper_blk_major_ = 0;

/* Devices. */
#define MAX_N_DEVICES 32
struct wdev_devices
{
        struct wrapper_blk_dev *wdev[MAX_N_DEVICES];
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

#define ASSERT_WRAPPER_BLK_DEV(wdev) assert_wrapper_blk_dev(wdev)

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
static bool add_to_devices(struct wrapper_blk_dev *wdev);
static struct wrapper_blk_dev* del_from_devices(unsigned int minor);
static struct wrapper_blk_dev* get_from_devices(unsigned int minor);

/* Utilities */
static bool wdev_register_detail(
        unsigned int minor, u64 capacity, unsigned int pbs,
        make_request_fn *make_request_fn,
        request_fn_proc *request_fn_proc);
static struct wrapper_blk_dev* alloc_and_partial_init_wdev(
        unsigned int minor, u64 capacity, unsigned int pbs);

static bool init_queue_and_disk(struct wrapper_blk_dev *wdev);
static void fin_queue_and_disk(struct wrapper_blk_dev *wdev);
static void assert_wrapper_blk_dev(struct wrapper_blk_dev *wdev);

/* For exit. */
static void stop_and_unregister_all_devices(void);

/*******************************************************************************
 * Static variables definition.
 *******************************************************************************/

/**
 * The device operations structure.
 */
static struct block_device_operations wrapper_blk_ops_ = {
	.owner           = THIS_MODULE,
	.open 	         = wrapper_blk_open,
	.release 	 = wrapper_blk_release,
	.ioctl	         = wrapper_blk_ioctl
};

/*******************************************************************************
 * Exported Global function definitions.
 *******************************************************************************/

/**
 * Register new block device with bio interface.
 */
bool wdev_register_with_bio(unsigned int minor, u64 capacity,
			unsigned int pbs,
			make_request_fn *make_request_fn)
{
        return wdev_register_detail(minor, capacity, pbs, make_request_fn, NULL);
}
EXPORT_SYMBOL_GPL(wdev_register_with_bio);

/**
 * Register new block device with request interface.
 */
bool wdev_register_with_req(unsigned int minor, u64 capacity,
			unsigned int pbs,
			request_fn_proc *request_fn_proc)
{
        return wdev_register_detail(minor, capacity, pbs, NULL, request_fn_proc);
}
EXPORT_SYMBOL_GPL(wdev_register_with_req);

/**
 * Unregister a block device.
 */
bool wdev_unregister(unsigned int minor)
{
        struct wrapper_blk_dev *wdev;

        wdev = del_from_devices(minor);
        if (!wdev) {
                LOGe("Not found device with minor %u.\n", minor);
                return false;
        }        
        fin_queue_and_disk(wdev);
        return true;
}
EXPORT_SYMBOL_GPL(wdev_unregister);

/**
 * Start a block device.
 * Call this after @wdev_register_XXX().
 */
bool wdev_start(unsigned int minor)
{
        struct wrapper_blk_dev *wdev;
        
        wdev = get_from_devices(minor);
        if (!wdev) {
                LOGe("Not found device with minor %u.\n", minor);
                goto error0;
        }
        ASSERT_WRAPPER_BLK_DEV(wdev);
                
	if (test_and_set_bit(0, &wdev->is_started)) {
                LOGe("Device with minor %u already started.\n", minor);
                goto error0;
	} else {
                add_disk(wdev->gd);
                LOGi("Start device with minor %u.\n", minor);
	}
        return true;
error0:
        return false;
}
EXPORT_SYMBOL_GPL(wdev_start);

/**
 * Stop a block device.
 * Call this before @wdev_unregister().
 */
bool wdev_stop(unsigned int minor)
{
        struct wrapper_blk_dev *wdev;
        
        wdev = get_from_devices(minor);
        if (!wdev) {
                LOGe("Not found device with minor %u.\n", minor);
                goto error0;
        }

        ASSERT_WRAPPER_BLK_DEV(wdev);
        
	if (test_and_clear_bit(0, &wdev->is_started)) {
		ASSERT(wdev->gd);
		del_gendisk(wdev->gd);
		LOGn("Stop device with minor %u.\n", minor);
	} else {
		LOGe("Device wit minor %u is already stopped.\n", minor);
		goto error0;
        }
        return true;
error0:
        return false;
}
EXPORT_SYMBOL_GPL(wdev_stop);

/**
 * Get wdev with a minor number.
 */
struct wrapper_blk_dev* wdev_get(unsigned int minor)
{
        return get_from_devices(minor);
}
EXPORT_SYMBOL_GPL(wdev_get);

/**
 * Get wdev from a request_queue.
 */
struct wrapper_blk_dev* wdev_get_from_queue(struct request_queue *q)
{
        struct wrapper_blk_dev* wdev;

        ASSERT(q);
        wdev = (struct wrapper_blk_dev *)q->queuedata;
        ASSERT_WRAPPER_BLK_DEV(wdev);
        return wdev;
}
EXPORT_SYMBOL_GPL(wdev_get_from_queue);

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
 * Initialize wdev_devices and realted data.
 */
static void init_devices(void)
{
        int i;
        
        for (i = 0; i < MAX_N_DEVICES; i ++) {
                devices_.wdev[i] = NULL;
        }
        devices_.n_active_devices = 0;
        spin_lock_init(&devices_.lock);
}

/**
 * Add a specified device.
 */
static bool add_to_devices(struct wrapper_blk_dev *wdev)
{
        ASSERT(wdev);

        if (get_from_devices(wdev->minor)) {
                return false;
        }

        spin_lock(&devices_.lock);
        devices_.wdev[wdev->minor] = wdev;
        devices_.n_active_devices ++;
        ASSERT(devices_.n_active_devices >= 0);
        spin_unlock(&devices_.lock);
        return true;
}

/**
 * Delete a specified device.
 */
static struct wrapper_blk_dev* del_from_devices(unsigned int minor)
{
        struct wrapper_blk_dev *wdev;

        ASSERT(minor < MAX_N_DEVICES);

        spin_lock(&devices_.lock);
        wdev = devices_.wdev[minor];
        if (wdev) {
                devices_.wdev[minor] = NULL;
                devices_.n_active_devices --;
                ASSERT(devices_.n_active_devices >= 0);
        }
        spin_unlock(&devices_.lock);
        return wdev;
}

/**
 * Get a specified device.
 *
 * RETURN:
 * Returns NULL if the device does not exist.
 */
static struct wrapper_blk_dev* get_from_devices(unsigned int minor)
{
        struct wrapper_blk_dev *wdev;
        
        if (minor >= MAX_N_DEVICES) {
                return NULL;
        }
        spin_lock(&devices_.lock);
        wdev = devices_.wdev[minor];
        spin_unlock(&devices_.lock);
        return wdev;
}

/**
 * Create and initialize wrapper_blk_dev data.
 *
 * @minor minor device number.
 * @capacity Device capacity [logical block]
 * RETURN:
 * Created wdev in success, or NULL.
 * NOTE:
 * Call this before @register_wdev();
 */
static struct wrapper_blk_dev* alloc_and_partial_init_wdev(
        unsigned int minor, u64 capacity, unsigned int pbs)
{
        struct wrapper_blk_dev *wdev;
        
         /* Allocate */
        wdev = ZALLOC(sizeof(struct wrapper_blk_dev), GFP_KERNEL);
        if (wdev == NULL) {
                LOGe("memory allocation failed.\n");
                goto error0;
        }
        
        /* Initialize */
        wdev->minor = minor;
        wdev->capacity = capacity;
        snprintf(wdev->name, WRAPPER_BLK_DEV_NAME_MAX_LEN, "%d", minor);
	wdev->pbs = pbs;

        spin_lock_init(&wdev->lock);
        wdev->queue = NULL;
        /* use_make_request_fn is not initialized here. */
        /* make_request_fn and request_fn_proc is not initialized here. */
        wdev->gd = NULL;
        wdev->is_started = false;
        wdev->private_data = NULL;

        return wdev;
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
static bool wdev_register_detail(unsigned int minor, u64 capacity,
				unsigned int pbs,
				make_request_fn *make_request_fn,
				request_fn_proc *request_fn_proc)
{
        struct wrapper_blk_dev *wdev;

        /* Allocate and initialize partially. */
        wdev = alloc_and_partial_init_wdev(minor, capacity, pbs);
        if (!wdev) {
                LOGe("Memory allocation failed.\n");
                goto error0;
        }

        /* Set request callback. */
        if (make_request_fn) {
                wdev->use_make_request_fn = true;
                wdev->make_request_fn = make_request_fn;
        } else {
                wdev->use_make_request_fn = false;
                wdev->request_fn_proc = request_fn_proc;
        }

        /* Init quene and disk. */
        if (!init_queue_and_disk(wdev)) {
                LOGe("init_queue_and_disk() failed.\n");
                goto error1;
        }

        /* Add the device to global variables. */
        if (!add_to_devices(wdev)) {
                LOGe("Already device with minor %u registered.\n", wdev->minor);
                goto error2;
        }
        return true;

error2:
        fin_queue_and_disk(wdev);
error1:
        FREE(wdev);
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
static bool init_queue_and_disk(struct wrapper_blk_dev *wdev)
{
        struct request_queue *q;
        struct gendisk *gd;
        
        ASSERT(wdev);

        /* Cleanup */
        wdev->queue = NULL;
        wdev->gd = NULL;

        /* Allocate and initialize queue. */
        if (wdev->use_make_request_fn) {
                q = blk_alloc_queue(GFP_KERNEL);
                if (!q) {
                        LOGe("blk_alloc_queue failed.\n");
                        goto error0;
                }
                blk_queue_make_request(q, wdev->make_request_fn);
        } else {
                q = blk_init_queue(wdev->request_fn_proc, &wdev->lock);
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
        blk_queue_physical_block_size(q, wdev->pbs);
        blk_queue_logical_block_size(q, LOGICAL_BLOCK_SIZE);
        /* blk_queue_io_min(q, wdev->pbs); */
        /* blk_queue_io_opt(q, wdev->pbs); */

        /* Accept REQ_DISCARD. */
        /* Do nothing. */

        /* Accept REQ_FLUSH and REQ_FUA. */
        /* Do nothing. */
        
        q->queuedata = wdev;
        wdev->queue = q;

        /* Allocate and initialize disk. */
        gd = alloc_disk(1);
        if (!gd) {
                LOGe("alloc_disk failed.\n");
                goto error1;
        }
        gd->major = wrapper_blk_major_;
        gd->first_minor = wdev->minor;
        
        gd->fops = &wrapper_blk_ops_;
        gd->queue = wdev->queue;
        gd->private_data = wdev;
        set_capacity(gd, wdev->capacity);
        snprintf(gd->disk_name, DISK_NAME_LEN,
                 "%s/%s", WRAPPER_BLK_DIR_NAME, wdev->name);
        wdev->gd = gd;

        return true;

error1:
        fin_queue_and_disk(wdev);
error0:
        return false;
}

/**
 * Finalize queue and disk data.
 *
 * CONTEXT:
 * Non-IRQ.
 */
static void fin_queue_and_disk(struct wrapper_blk_dev *wdev)
{
        ASSERT(wdev);

        if (wdev->gd) {
                put_disk(wdev->gd);
                wdev->gd = NULL;
        }
        if (wdev->queue) {
                blk_cleanup_queue(wdev->queue);
                wdev->queue = NULL;
        }
}

static void assert_wrapper_blk_dev(struct wrapper_blk_dev *wdev)
{
        ASSERT(wdev);
        ASSERT(wdev->capacity > 0);
        ASSERT_PBS(wdev->pbs);
        ASSERT(strlen(wdev->name) > 0);
        ASSERT(wdev->queue);
        ASSERT(wdev->gd);
}

static void stop_and_unregister_all_devices(void)
{
        int i;
        struct wrapper_blk_dev *wdev;
        
        for (i = 0; i < MAX_N_DEVICES; i ++) {
                wdev = get_from_devices(i);
                if (wdev) {
                        wdev_stop(i);
                        wdev_unregister(i);
			FREE(wdev);
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

        /* Initialize devices_. */
        init_devices();

        return 0;
#if 0
error0:
	unregister_blkdev(wrapper_blk_major_, WRAPPER_BLK_NAME);
#endif
        return -ENOMEM;
}

static void wrapper_blk_exit(void)
{
        stop_and_unregister_all_devices();
        unregister_blkdev(wrapper_blk_major_, WRAPPER_BLK_NAME);

        LOGi("Wrapper-blk module exit.\n");
}

module_init(wrapper_blk_init);
module_exit(wrapper_blk_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Simple Wrapper Block Device for Test");
MODULE_ALIAS(WRAPPER_BLK_NAME);
/* MODULE_ALIAS_BLOCKDEV_MAJOR(WRAPPER_BLK_MAJOR); */
