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
#include <asm/atomic.h>

#include "kern.h"
#include "hashtbl.h"
#include "snapshot.h"
#include "control.h"
#include "alldevs.h"
#include "util.h"
#include "logpack.h"
#include "checkpoint.h"
#include "super.h"
#include "io.h"

#include "walb/ioctl.h"
#include "walb/log_device.h"
#include "walb/sector.h"
#include "walb/snapshot.h"
#include "walb/bitmap.h"

/*******************************************************************************
 * Module parameters definition.
 *******************************************************************************/

/**
 * Device major of walb.
 */
static int walb_major_ = 0;
module_param_named(walb_major, walb_major_, int, S_IRUGO);

/**
 * Set 1 if you want to sync down superblock in disassemble device.
 * Set 0 if not.
 */
static int is_sync_superblock_ = 1;
module_param_named(is_sync_superblock, is_sync_superblock_, int, S_IRUGO|S_IWUSR);

/*******************************************************************************
 * Static data definition.
 *******************************************************************************/

/**
 * Workqueues.
 */
#define WQ_LOGPACK_NAME "wq_logpack"
struct workqueue_struct *wq_logpack_ = NULL;
#define WQ_IO_NAME "wq_io"
struct workqueue_struct *wq_io_ = NULL;
#define WQ_OL_NAME "wq_ol"
struct workqueue_struct *wq_ol_ = NULL;
#define WQ_MISC_NAME "wq_misc"
struct workqueue_struct *wq_misc_ = NULL;

/*******************************************************************************
 * Prototypes of local functions.
 *******************************************************************************/

/* Lock/unlock block device. */
static int walb_lock_bdev(struct block_device **bdevp, dev_t dev);
static void walb_unlock_bdev(struct block_device *bdev);

/* Logpack check function. */
static int walb_check_lsid_valid(struct walb_dev *wdev, u64 lsid);

/* Walb device open/close/ioctl. */
static int walb_open(struct block_device *bdev, fmode_t mode);
static int walb_release(struct gendisk *gd, fmode_t mode);
static int walb_dispatch_ioctl_wdev(struct walb_dev *wdev, void __user *userctl);
static int walb_ioctl(struct block_device *bdev, fmode_t mode,
                      unsigned int cmd, unsigned long arg);

/* Walblog device open/close/ioctl. */
static int walblog_open(struct block_device *bdev, fmode_t mode);
static int walblog_release(struct gendisk *gd, fmode_t mode);
static int walblog_ioctl(struct block_device *bdev, fmode_t mode,
                         unsigned int cmd, unsigned long arg);

/* Utility functions for walb_dev. */
static u64 get_written_lsid(struct walb_dev *wdev);
static u64 get_log_capacity(struct walb_dev *wdev);
static int walb_set_name(struct walb_dev *wdev, unsigned int minor,
                         const char *name);

/* Workqueues. */
static bool initialize_workqueues(void);
static void finalize_workqueues(void);

/* Prepare/finalize. */
static int walb_prepare_device(struct walb_dev *wdev, unsigned int minor,
                               const char *name);
static void walb_finalize_device(struct walb_dev *wdev);
static int walblog_prepare_device(struct walb_dev *wdev, unsigned int minor,
                                  const char* name);
static void walblog_finalize_device(struct walb_dev *wdev);

static int walb_ldev_initialize(struct walb_dev *wdev);
static void walb_ldev_finalize(struct walb_dev *wdev);

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
 * @dev    device to lock.
 * @return 0 in success.
 */
static int walb_lock_bdev(struct block_device **bdevp, dev_t dev)
{
        int err = 0;
        struct block_device *bdev;
        char b[BDEVNAME_SIZE];

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
 * Check logpack of the given lsid exists.
 *
 * @lsid lsid to check.
 * 
 * @return 0 if valid, or -1.
 */
static int walb_check_lsid_valid(struct walb_dev *wdev, u64 lsid)
{
        struct sector_data *sect;
        struct walb_logpack_header *logpack;
        u64 off;

        ASSERT(wdev != NULL);

        sect = sector_alloc(wdev->physical_bs, GFP_NOIO);
        if (sect == NULL) {
                LOGe("walb_check_lsid_valid: alloc sector failed.\n");
                goto error0;
        }
        ASSERT(is_same_size_sector(sect, wdev->lsuper0));
        logpack = get_logpack_header(sect);
        
        off = get_offset_of_lsid_2(get_super_sector(wdev->lsuper0), lsid);
        if (!sector_io(READ, wdev->ldev, off, sect)) {
                LOGe("walb_check_lsid_valid: read sector failed.\n");
                goto error1;
        }

        /* sector type */
        if (logpack->sector_type != SECTOR_TYPE_LOGPACK) {
                goto error1;
        }
        
        /* lsid */
        if (logpack->logpack_lsid != lsid) {
                goto error1;
        }

        /* checksum */
        if (checksum((u8 *)logpack, wdev->physical_bs) != 0) {
                goto error1;
        }

        sector_free(sect);
        return 0;

error1:
        sector_free(sect);
error0:
        return -1;
}

/*
 * Open and close.
 */
static int walb_open(struct block_device *bdev, fmode_t mode)
{
	struct walb_dev *dev = bdev->bd_disk->private_data;

	spin_lock(&dev->lock);
	if (! dev->users) 
		check_disk_change(bdev);
	dev->users++;
	spin_unlock(&dev->lock);
	return 0;
}

static int walb_release(struct gendisk *gd, fmode_t mode)
{
	struct walb_dev *dev = gd->private_data;

	spin_lock(&dev->lock);
	dev->users--;
	spin_unlock(&dev->lock);

	return 0;
}



/**
 * Execute ioctl for WALB_IOCTL_WDEV.
 *
 * return 0 in success, or -EFAULT.
 */
static int walb_dispatch_ioctl_wdev(struct walb_dev *wdev, void __user *userctl)
{
        int ret = -EFAULT;
        struct walb_ctl *ctl;

        u64 oldest_lsid, lsid;
        u32 interval;

        /* Get ctl data. */
        ctl = walb_get_ctl(userctl, GFP_KERNEL);
        if (ctl == NULL) {
                LOGe("walb_get_ctl failed.\n");
                goto error0;
        }
        
        /* Execute each command. */
        switch(ctl->command) {
        
        case WALB_IOCTL_OLDEST_LSID_GET:

                LOGn("WALB_IOCTL_OLDEST_LSID_GET\n");
                spin_lock(&wdev->oldest_lsid_lock);
                oldest_lsid = wdev->oldest_lsid;
                spin_unlock(&wdev->oldest_lsid_lock);

                ctl->val_u64 = oldest_lsid;
                ret = 0;
                break;
                
        case WALB_IOCTL_OLDEST_LSID_SET:
                
                LOGn("WALB_IOCTL_OLDEST_LSID_SET\n");
                lsid = ctl->val_u64;
                
                if (walb_check_lsid_valid(wdev, lsid) == 0) {
                        spin_lock(&wdev->oldest_lsid_lock);
                        wdev->oldest_lsid = lsid;
                        spin_unlock(&wdev->oldest_lsid_lock);
                        
                        walb_sync_super_block(wdev);
                        ret = 0;
                } else {
                        LOGe("lsid %llu is not valid.\n", lsid);
                }
                break;
                
        case WALB_IOCTL_CHECKPOINT_INTERVAL_GET:

                LOGn("WALB_IOCTL_CHECKPOINT_INTERVAL_GET\n");
                ctl->val_u32 = get_checkpoint_interval(wdev);
                ret = 0;
                break;
                
        case WALB_IOCTL_CHECKPOINT_INTERVAL_SET:

                LOGn("WALB_IOCTL_CHECKPOINT_INTERVAL_SET\n");
                interval = ctl->val_u32;
                if (interval <= WALB_MAX_CHECKPOINT_INTERVAL) {
                        set_checkpoint_interval(wdev, interval);
                        ret = 0;
                } else {
                        LOGe("Checkpoint interval is too big.\n");
                }
                break;

        case WALB_IOCTL_WRITTEN_LSID_GET:

                LOGn("WALB_IOCTL_WRITTEN_LSID_GET\n");
                ctl->val_u64 = get_written_lsid(wdev);
                ret = 0;
                break;

        case WALB_IOCTL_LOG_CAPACITY_GET:

                LOGn("WALB_IOCTL_LOG_CAPACITY_GET\n");
                ctl->val_u64 = get_log_capacity(wdev);
                ret = 0;
                break;

        case WALB_IOCTL_SNAPSHOT_CREATE:

                LOGn("WALB_IOCTL_SNAPSHOT_CREATE\n");

                /* (walb_snapshot_record_t *)ctl->u2k.__buf; */
                /* now editing */
                
                break;
                
        case WALB_IOCTL_SNAPSHOT_DELETE:

                LOGn("WALB_IOCTL_SNAPSHOT_DELETE\n");
                break;
                
        case WALB_IOCTL_SNAPSHOT_GET:

                LOGn("WALB_IOCTL_SNAPSHOT_GET\n");
                break;
                
        case WALB_IOCTL_SNAPSHOT_NUM:

                LOGn("WALB_IOCTL_SNAPSHOT_NUM\n");
                break;
                
        case WALB_IOCTL_SNAPSHOT_LIST:

                LOGn("WALB_IOCTL_SNAPSHOT_LIST\n");
                break;
                
        default:
                LOGn("WALB_IOCTL_WDEV %d is not supported.\n",
                         ctl->command);
        }

        /* Put ctl data. */
        if (walb_put_ctl(userctl, ctl) != 0) {
                LOGe("walb_put_ctl failed.\n");
                goto error0;
        }
        
        return ret;

error0:
        return -EFAULT;
}

/*
 * The ioctl() implementation
 */
static int walb_ioctl(struct block_device *bdev, fmode_t mode,
                      unsigned int cmd, unsigned long arg)
{
	long size;
	struct hd_geometry geo;
	struct walb_dev *wdev = bdev->bd_disk->private_data;
        int ret = -ENOTTY;
        u32 version;

        LOGd("walb_ioctl begin.\n");
        LOGd("cmd: %08x\n", cmd);
        
	switch(cmd) {
        case HDIO_GETGEO:
        	/*
		 * Get geometry: since we are a virtual device, we have to make
		 * up something plausible.  So we claim 16 sectors, four heads,
		 * and calculate the corresponding number of cylinders.  We set the
		 * start of data at sector four.
		 */
		size = wdev->ddev_size;
		geo.cylinders = (size & ~0x3f) >> 6;
		geo.heads = 4;
		geo.sectors = 16;
		geo.start = 4;
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
        
        LOGd("walb_ioctl end.\n");

        return ret;
}

/*
 * The device operations structure.
 */
static struct block_device_operations walb_ops = {
	.owner           = THIS_MODULE,
	.open 	         = walb_open,
	.release 	 = walb_release,
	.ioctl	         = walb_ioctl
};

static int walblog_open(struct block_device *bdev, fmode_t mode)
{
	return 0;
}

static int walblog_release(struct gendisk *gd, fmode_t mode)
{
	return 0;
}

static int walblog_ioctl(struct block_device *bdev, fmode_t mode,
                         unsigned int cmd, unsigned long arg)
{
	long size;
	struct hd_geometry geo;
	struct walb_dev *wdev = bdev->bd_disk->private_data;

	switch(cmd) {
        case HDIO_GETGEO:
		size = wdev->ldev_size;
		geo.cylinders = (size & ~0x3f) >> 6;
		geo.heads = 4;
		geo.sectors = 16;
		geo.start = 4;
		if (copy_to_user((void __user *) arg, &geo, sizeof(geo)))
			return -EFAULT;
		return 0;
	}
	return -ENOTTY;
}

static struct block_device_operations walblog_ops = {
        .owner   = THIS_MODULE,
        .open    = walblog_open,
        .release = walblog_release,
        .ioctl   = walblog_ioctl
};

/**
 * Get written lsid of a walb device.
 *
 * @return written_lsid of the walb device.
 */
static u64 get_written_lsid(struct walb_dev *wdev)
{
        u64 ret;

        ASSERT(wdev != NULL);

        spin_lock(&wdev->datapack_list_lock);
        ret = wdev->written_lsid;
        spin_unlock(&wdev->datapack_list_lock);
        
        return ret;
}

/**
 * Get log capacity of a walb device.
 *
 * @return ring_buffer_size of the walb device.
 */
static u64 get_log_capacity(struct walb_dev *wdev)
{
        ASSERT(wdev != NULL);
        ASSERT_SECTOR_DATA(wdev->lsuper0);
        return get_super_sector(wdev->lsuper0)->ring_buffer_size;
}

/**
 * Set device name.
 *
 * @return 0 in success, or -1.
 */
static int walb_set_name(struct walb_dev *wdev,
                         unsigned int minor, const char *name)
{
        int name_len;
        char *dev_name;

        ASSERT(wdev != NULL);
        ASSERT(wdev->lsuper0 != NULL);
        
        name_len = strnlen(name, DISK_NAME_LEN);
        dev_name = get_super_sector(wdev->lsuper0)->name;
        
        if (name == NULL || name_len == 0) {
                if (strnlen(dev_name, DISK_NAME_LEN) == 0) {
                        snprintf(dev_name, DISK_NAME_LEN, "%u", minor / 2);
                }
        } else {
                strncpy(dev_name, name, DISK_NAME_LEN);
        }
        LOGd("minor %u dev_name: %s\n", minor, dev_name);

        name_len = strnlen(dev_name, DISK_NAME_LEN);

        if (name_len > WALB_DEV_NAME_MAX_LEN) {
                LOGe("Device name is too long: %s.\n", name);
                goto error0;
        }
        return 0;
        
error0:
        return -1;
}

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
	wq_logpack_ = alloc_workqueue(WQ_LOGPACK_NAME, WQ_MEM_RECLAIM, 0);
	if (!wq_logpack_) {
		LOGe(MSG, WQ_LOGPACK_NAME);
		goto error;
	}
	wq_io_ = alloc_workqueue(WQ_IO_NAME, WQ_MEM_RECLAIM, 0);
	if (!wq_io_) {
		LOGe(MSG, WQ_IO_NAME);
		goto error;
	}
	wq_ol_ = alloc_workqueue(WQ_OL_NAME, WQ_MEM_RECLAIM, 0);
	if (!wq_ol_) {
		LOGe(MSG, WQ_OL_NAME);
		goto error;
	}
	wq_misc_ = alloc_workqueue(WQ_MISC_NAME, WQ_MEM_RECLAIM, 0);
	if (!wq_misc_) {
		LOGe(MSG, WQ_MISC_NAME);
		goto error;
	}
	return true;
#undef MSG
error:
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
	if (wq_ol_) {
		destroy_workqueue(wq_ol_);
		wq_ol_ = NULL;
	}
	if (wq_io_) {
		destroy_workqueue(wq_io_);
		wq_io_ = NULL;
	}
	if (wq_logpack_) {
		destroy_workqueue(wq_logpack_);
		wq_logpack_ = NULL;
	}
}

/**
 * Initialize walb block device.
 */
static int walb_prepare_device(struct walb_dev *wdev, unsigned int minor,
                               const char *name)
{
#if 1
	/* bio interface */
	wdev->queue = blk_alloc_queue(GFP_KERNEL);
	if (wdev->queue == NULL)
		goto out;
	blk_queue_make_request(wdev->queue, walb_make_request);
#else
	/* request interface */
	wdev->queue = blk_init_queue(walb_full_request2, &wdev->lock);
	if (wdev->queue == NULL)
		goto out;
	if (elevator_change(wdev->queue, "noop"))
		goto out_queue;
#endif	

	blk_queue_logical_block_size(wdev->queue, wdev->logical_bs);
	blk_queue_physical_block_size(wdev->queue, wdev->physical_bs);
	wdev->queue->queuedata = wdev;

        /*
	 * And the gendisk structure.
	 */
	/* dev->gd = alloc_disk(WALB_MINORS); */
        wdev->gd = alloc_disk(1);
	if (! wdev->gd) {
		LOGe("alloc_disk failure.\n");
		goto out_queue;
	}
	wdev->gd->major = walb_major_;
	wdev->gd->first_minor = minor;
        wdev->devt = MKDEV(wdev->gd->major, wdev->gd->first_minor);
	wdev->gd->fops = &walb_ops;
	wdev->gd->queue = wdev->queue;
	wdev->gd->private_data = wdev;
	set_capacity(wdev->gd, wdev->ddev_size);
        
        snprintf(wdev->gd->disk_name, DISK_NAME_LEN,
                 "%s/%s", WALB_DIR_NAME, name);
        LOGd("device path: %s, device name: %s\n",
                 wdev->gd->disk_name, name);

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
        }
        if (wdev->queue) {
		blk_cleanup_queue(wdev->queue);
        }
}

/**
 * Setup walblog device.
 */
static int walblog_prepare_device(struct walb_dev *wdev,
                                  unsigned int minor, const char* name)
{
        wdev->log_queue = blk_alloc_queue(GFP_KERNEL);
        if (wdev->log_queue == NULL)
                goto error0;

        blk_queue_make_request(wdev->log_queue, walblog_make_request);

        blk_queue_logical_block_size(wdev->log_queue, wdev->logical_bs);
        blk_queue_physical_block_size(wdev->log_queue, wdev->physical_bs);
        wdev->log_queue->queuedata = wdev;

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
        snprintf(wdev->log_gd->disk_name, DISK_NAME_LEN,
                 "%s/L%s", WALB_DIR_NAME, name);
        
        return 0;

error1:
        if (wdev->log_queue) {
                kobject_put(&wdev->log_queue->kobj);
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
        }
        if (wdev->log_queue) {
                kobject_put(&wdev->log_queue->kobj);
        }
}

/**
 * Log device initialization.
 *
 * <pre>
 * 1. Read log device metadata
 *    (currently snapshot metadata is not loaded.
 *     super sector0 only...)
 * 2. Redo from written_lsid to avaialble latest lsid.
 * 3. Sync log device super block.
 * </pre>
 *
 * @wdev walb device struct.
 * @return 0 in success, or -1.
 */
static int walb_ldev_initialize(struct walb_dev *wdev)
{
        u64 snapshot_begin_pb, snapshot_end_pb;
        struct sector_data *lsuper0_tmp;
        ASSERT(wdev != NULL);

        /*
         * 1. Read log device metadata
         */
        wdev->lsuper0 = sector_alloc(wdev->physical_bs, GFP_NOIO);
        if (!wdev->lsuper0) {
                LOGe("walb_ldev_init: alloc sector failed.\n");
                goto error0;
        }
	lsuper0_tmp = sector_alloc(wdev->physical_bs, GFP_NOIO);
	if (lsuper0_tmp) {
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

        if (sector_compare(wdev->lsuper0, lsuper0_tmp) != 0) {
                LOGe("walb_ldev_init: memcmp NG\n");
        } else {
                LOGe("walb_ldev_init: memcmp OK\n");
        }
        
        sector_free(lsuper0_tmp);
        /* Do not forget calling kfree(dev->lsuper0)
           before releasing the block device. */

        /*
         * 2. Prepare and initialize snapshot data structure.
         */
        snapshot_begin_pb = get_metadata_offset(wdev->physical_bs);
        snapshot_end_pb = snapshot_begin_pb +
                get_super_sector(wdev->lsuper0)->snapshot_metadata_size;
        LOGd("snapshot offset range: [%"PRIu64",%"PRIu64").\n",
                 snapshot_begin_pb, snapshot_end_pb);
        wdev->snapd = snapshot_data_create
                (wdev->ldev, snapshot_begin_pb, snapshot_end_pb, GFP_KERNEL);
        if (wdev->snapd == NULL) {
                LOGe("snapshot_data_create() failed.\n");
                goto error2;
        }
        /* Initialize snapshot data by scanning snapshot sectors. */
        /* if (snapshot_data_initialize(wdev->snapd) != 0) { */
        /*         LOGe("snapshot_data_initialize() failed.\n"); */
        /*         goto out_snapshot_create; */
        /* } */
        
        /*
         * 3. Redo from written_lsid to avaialble latest lsid.
         *    and set latest_lsid variable.
         */

        /* This feature will be implemented later. */

        
        /*
         * 4. Sync log device super block.
         */

        /* If redo is done, super block should be re-written. */

        
        
        return 0;


/* out_snapshot_init: */
/*         if (wdev->snapd) { */
/*                 snapshot_data_finalize(wdev->snapd); */
/*         } */
#if 0
out_snapshot_create:
        if (wdev->snapd) {
                snapshot_data_destroy(wdev->snapd);
        }
#endif
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
static void walb_ldev_finalize(struct walb_dev *wdev)
{
        ASSERT(wdev != NULL);
        ASSERT(wdev->lsuper0 != NULL);
        ASSERT(wdev->snapd != NULL);

        snapshot_data_finalize(wdev->snapd);
        snapshot_data_destroy(wdev->snapd);
        
        walb_finalize_super_block(wdev, is_sync_superblock_);
        sector_free(wdev->lsuper0);
}

/**
 * Register walb block device.
 */
static void walb_register_device(struct walb_dev *wdev)
{
        ASSERT(wdev != NULL);
        ASSERT(wdev->gd != NULL);
        
        add_disk(wdev->gd);
}

/**
 * Unregister walb wrapper device.
 */
static void walb_unregister_device(struct walb_dev *wdev)
{
        LOGd("walb_unregister_device begin.\n");
        if (wdev->gd) {
                del_gendisk(wdev->gd);
        }
        LOGd("walb_unregister_device end.\n");
}

/**
 * Register walblog block device.
 */
static void walblog_register_device(struct walb_dev *wdev)
{
        ASSERT(wdev != NULL);
        ASSERT(wdev->log_gd != NULL);
        
        add_disk(wdev->log_gd);
}

/**
 * Unregister walblog wrapper device.
 */
static void walblog_unregister_device(struct walb_dev *wdev)
{
        LOGd("walblog_unregister_device begin.\n");
        if (wdev->log_gd) {
                del_gendisk(wdev->log_gd);
        }
        LOGd("walblog_unregister_device end.\n");
}

static int __init walb_init(void)
{
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

        alldevs_write_lock();
        wdev = alldevs_pop();
        while (wdev != NULL) {

                unregister_wdev(wdev);
                destroy_wdev(wdev);
                wdev = alldevs_pop();
        }
        alldevs_write_unlock();

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
 *        walblog device minor will be (minor + 1).
 * @ldevt device id of log device.
 * @ddevt device id of data device.
 * @name name of the device, or NULL to use default.
 *
 * @return allocated and prepared walb_dev data, or NULL.
 */
struct walb_dev* prepare_wdev(unsigned int minor,
                              dev_t ldevt, dev_t ddevt, const char* name)
{
        struct walb_dev *wdev;
        u16 ldev_lbs, ldev_pbs, ddev_lbs, ddev_pbs;
        char *dev_name;

        /* Minor id check. */
        if (minor == WALB_DYNAMIC_MINOR) {
                LOGe("Do not specify WALB_DYNAMIC_MINOR.\n");
                goto out;
        }
        
	/*
	 * Initialize walb_dev.
	 */
        wdev = kzalloc(sizeof(struct walb_dev), GFP_KERNEL);
        if (wdev == NULL) {
                LOGe("kmalloc failed.\n");
                goto out;
        }
	spin_lock_init(&wdev->lock);
        spin_lock_init(&wdev->latest_lsid_lock);
        spin_lock_init(&wdev->lsuper0_lock);
        /* spin_lock_init(&wdev->logpack_list_lock); */
        /* INIT_LIST_HEAD(&wdev->logpack_list); */
        spin_lock_init(&wdev->datapack_list_lock);
        INIT_LIST_HEAD(&wdev->datapack_list);
        atomic_set(&wdev->is_read_only, 0);
	
        /*
         * Open underlying log device.
         */
        if (walb_lock_bdev(&wdev->ldev, ldevt) != 0) {
                LOGe("walb_lock_bdev failed (%u:%u for log)\n",
                         MAJOR(ldevt), MINOR(ldevt));
                goto out_free;
        }
        wdev->ldev_size = get_capacity(wdev->ldev->bd_disk);
        ldev_lbs = bdev_logical_block_size(wdev->ldev);
        ldev_pbs = bdev_physical_block_size(wdev->ldev);
        LOGi("log disk (%u:%u)\n"
                 "log disk size %llu\n"
                 "log logical sector size %u\n"
                 "log physical sector size %u\n",
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
        wdev->ddev_size = get_capacity(wdev->ddev->bd_disk);
        ddev_lbs = bdev_logical_block_size(wdev->ddev);
        ddev_pbs = bdev_physical_block_size(wdev->ddev);
        LOGi("data disk (%d:%d)\n"
                 "data disk size %llu\n"
                 "data logical sector size %u\n"
                 "data physical sector size %u\n",
                 MAJOR(ddevt), MINOR(ddevt),
                 wdev->ddev_size,
                 ddev_lbs, ddev_pbs);

        /* Check compatibility of log device and data device. */
        if (ldev_lbs != ddev_lbs || ldev_pbs != ddev_pbs) {
                LOGe("Sector size of data and log must be same.\n");
                goto out_ddev;
        }
        wdev->logical_bs = ldev_lbs;
        wdev->physical_bs = ldev_pbs;
	wdev->size = wdev->ddev_size * (u64)wdev->logical_bs;

        /* Load log device metadata. */
        if (walb_ldev_initialize(wdev) != 0) {
                LOGe("ldev init failed.\n");
                goto out_ddev;
        }
        wdev->written_lsid = get_super_sector(wdev->lsuper0)->written_lsid;
        wdev->prev_written_lsid = wdev->written_lsid;
        wdev->oldest_lsid = get_super_sector(wdev->lsuper0)->oldest_lsid;

        /* Set device name. */
        if (walb_set_name(wdev, minor, name) != 0) {
                LOGe("Set device name failed.\n");
                goto out_ldev_init;
        }
        ASSERT_SECTOR_DATA(wdev->lsuper0);
        dev_name = get_super_sector(wdev->lsuper0)->name;

        /* For checkpoint */
	init_checkpointing(wdev);
        
        /*
         * Redo
         * 1. Read logpack from written_lsid.
         * 2. Write the corresponding data of the logpack to data device.
         * 3. Update written_lsid and latest_lsid;
         */

        /* Redo feature is not implemented yet. */


        /* latest_lsid is written_lsid after redo. */
        wdev->latest_lsid = wdev->written_lsid;
        
        /* For padding test in the end of ring buffer. */
        /* 64KB ring buffer */
        /* dev->lsuper0->ring_buffer_size = 128; */

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

	return wdev;

/* out_walblogdev: */
/*         walblog_finalize_device(wdev); */
out_walbdev:
        walb_finalize_device(wdev);
out_ldev_init:
        walb_ldev_finalize(wdev);
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
 * Destroy wdev structure.
 * You must call @unregister_wdev() before calling this.
 */
void destroy_wdev(struct walb_dev *wdev)
{
        LOGi("destroy_wdev (wrap %u:%u log %u:%u data %u:%u)\n",
                 MAJOR(wdev->devt),
                 MINOR(wdev->devt),
                 MAJOR(wdev->ldev->bd_dev),
                 MINOR(wdev->ldev->bd_dev),
                 MAJOR(wdev->ddev->bd_dev),
                 MINOR(wdev->ddev->bd_dev));

        walblog_finalize_device(wdev);
        walb_finalize_device(wdev);
        
        snapshot_data_finalize(wdev->snapd);
        walb_ldev_finalize(wdev);
        
        if (wdev->ddev)
                walb_unlock_bdev(wdev->ddev);
        if (wdev->ldev)
                walb_unlock_bdev(wdev->ldev);


        
        kfree(wdev);
        LOGd("destroy_wdev done.\n");
}

/**
 * Register wdev.
 * You must call @prepare_wdev() before calling this.
 */
void register_wdev(struct walb_dev *wdev)
{
        ASSERT(wdev != NULL);
        ASSERT(wdev->gd != NULL);
        ASSERT(wdev->log_gd != NULL);

        start_checkpointing(wdev);

        walblog_register_device(wdev);
        walb_register_device(wdev);
}

/**
 * Unregister wdev.
 * You must call @destroy_wdev() after calling this.
 */
void unregister_wdev(struct walb_dev *wdev)
{
        ASSERT(wdev != NULL);

        stop_checkpointing(wdev);
        
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
