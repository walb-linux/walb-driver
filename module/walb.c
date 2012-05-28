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
#include <linux/slab.h>		/* kmalloc() */
#include <linux/fs.h>		/* everything... */
#include <linux/errno.h>	/* error codes */
/* #include <linux/timer.h> */
#include <linux/types.h>	/* size_t */
#include <linux/fcntl.h>	/* O_ACCMODE */
#include <linux/hdreg.h>	/* HDIO_GETGEO */
#include <linux/kdev_t.h>
/* #include <linux/vmalloc.h> */
#include <linux/genhd.h>
#include <linux/blkdev.h>
#include <linux/buffer_head.h>	/* invalidate_bdev */
#include <linux/bio.h>
#include <linux/spinlock.h>
#include <linux/rwsem.h>
#include <asm/atomic.h>

#include "kern.h"
#include "hashtbl.h"
#include "snapshot.h"
#include "control.h"
#include "alldevs.h"
#include "io.h"
#include "util.h"
#include "logpack.h"
#include "datapack.h"

#include "walb/ioctl.h"
#include "walb/log_device.h"
#include "walb/sector.h"
#include "walb/snapshot.h"
#include "walb/bitmap.h"

/**
 * Device major of walb.
 */
int walb_major = 0;
module_param(walb_major, int, 0);
static int ndevices = 1;
module_param(ndevices, int, 0);

/**
 * Set 1 if you want to sync down superblock in disassemble device.
 * Set 0 if not.
 */
static int is_sync_superblock = 1;
module_param(is_sync_superblock, int, S_IRUGO | S_IWUSR);

/*
 * Underlying devices.
 * ldev (log device) and ddev (data device).
 */
static int ldev_major = 0;
static int ldev_minor = 0;
static int ddev_major = 0;
static int ddev_minor = 0;
module_param(ldev_major, int, 0);
module_param(ldev_minor, int, 0);
module_param(ddev_major, int, 0);
module_param(ddev_minor, int, 0);

static int request_mode = RM_FULL;
module_param(request_mode, int, 0);

static struct walb_dev *Devices = NULL;

/**
 * Workqueues.
 */
struct workqueue_struct *wqs_ = NULL; /* single-thread */
struct workqueue_struct *wqm_ = NULL; /* multi-thread */

/*******************************************************************************
 * Prototypes of local functions.
 *******************************************************************************/

/* Lock/unlock block device. */
static int walb_lock_bdev(struct block_device **bdevp, dev_t dev);
static void walb_unlock_bdev(struct block_device *bdev);

/* Thin wrapper of logpack/datapack. */
static void walb_end_requests(struct request **reqp_ary, int n_req, int error);
static void walb_make_logpack_and_submit_task(struct work_struct *work);
static int walb_make_and_write_logpack(struct walb_dev *wdev,
                                       struct request** reqp_ary, int n_req);

/* Walb device full_request callback. */
static void walb_full_request2(struct request_queue *q);

/* Walblog make_requrest callback. */
static int walblog_make_request(struct request_queue *q, struct bio *bio);

/* Logpack check function. */
static int walb_check_lsid_valid(struct walb_dev *wdev, u64 lsid);

/* Walb device open/close/ioctl. */
static int walb_open(struct block_device *bdev, fmode_t mode);
static int walb_release(struct gendisk *gd, fmode_t mode);
static int walb_dispatch_ioctl_wdev(struct walb_dev *wdev, void __user *userctl);
static int walb_ioctl(struct block_device *bdev, fmode_t mode,
                      unsigned int cmd, unsigned long arg);
/* Walb unplug_fn. */
static void walb_unplug_all(struct request_queue *q);

/* Walblog device open/close/ioctl. */
static int walblog_open(struct block_device *bdev, fmode_t mode);
static int walblog_release(struct gendisk *gd, fmode_t mode);
static int walblog_ioctl(struct block_device *bdev, fmode_t mode,
                         unsigned int cmd, unsigned long arg);
/* Walblog unplug_fn. */
static void walblog_unplug(struct request_queue *q);

/* Super sector functions. */
static int walb_sync_super_block(struct walb_dev *wdev);

/* Utility functions for walb_dev. */
static u64 get_written_lsid(struct walb_dev *wdev);
static u64 get_log_capacity(struct walb_dev *wdev);
static int walb_set_name(struct walb_dev *wdev, unsigned int minor,
                         const char *name);

/* Prepare/finalize. */
static int walb_prepare_device(struct walb_dev *wdev, unsigned int minor,
                               const char *name);
static void walb_finalize_device(struct walb_dev *wdev);
static int walblog_prepare_device(struct walb_dev *wdev, unsigned int minor,
                                  const char* name);
static void walblog_finalize_device(struct walb_dev *wdev);

static int walb_ldev_initialize(struct walb_dev *wdev);
static void walb_ldev_finalize(struct walb_dev *wdev);
static int walb_finalize_super_block(struct walb_dev *wdev);

/* Register/unregister. */
static void walb_register_device(struct walb_dev *wdev);
static void walb_unregister_device(struct walb_dev *wdev);
static void walblog_register_device(struct walb_dev *wdev);
static void walblog_unregister_device(struct walb_dev *wdev);

/* Checkpoint. */
static void do_checkpointing(struct work_struct *work);
static void start_checkpointing(struct walb_dev *wdev);
static void stop_checkpointing(struct walb_dev *wdev);
static u32 get_checkpoint_interval(struct walb_dev *wdev);
static void set_checkpoint_interval(struct walb_dev *wdev, u32 val);

/* Deprecated. */
static int setup_device_tmp(unsigned int minor);

/* Module init/exit. */
static int __init walb_init(void);
static void walb_exit(void);


/*******************************************************************************
 * Local functions.
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
 * Call @blk_end_request_all() for all requests.
 *
 * @reqp_ary array of request pointers.
 * @n_req array size.
 * @error error value. 0 is normally complete.
 */
static void walb_end_requests(struct request **reqp_ary, int n_req, int error)
{
        int i;
        for (i = 0; i < n_req; i ++) {
                blk_end_request_all(reqp_ary[i], error);
        }
}

/**
 * Make log pack and submit related bio(s).
 *
 * @work (struct walb_make_logpack_work *)->work
 */
static void walb_make_logpack_and_submit_task(struct work_struct *work)
{
        struct walb_make_logpack_work *wk;
        struct sector_data *lhead_sect;
        struct walb_logpack_header *lhead;
        int logpack_size;
        u64 logpack_lsid, next_logpack_lsid, oldest_lsid;
        u64 ringbuf_off, ringbuf_size;
        struct walb_dev *wdev;

        wk = container_of(work, struct walb_make_logpack_work, work);
        wdev = wk->wdev;

        LOGd("walb_make_logpack_and_submit_task begin\n");
        ASSERT(wk->n_req <= max_n_log_record_in_sector(wdev->physical_bs));

        LOGd("making log pack (n_req %d)\n", wk->n_req);
        
        /*
         * Allocate memory (sector size) for log pack header.
         */
        lhead_sect = sector_alloc(wdev->physical_bs, GFP_NOIO | __GFP_ZERO);
        if (lhead_sect == NULL) {
                LOGe("walb_alloc_sector() failed\n");
                goto error0;
        }
        lhead = get_logpack_header(lhead_sect);

        /*
         * Get oldest_lsid.
         */
        spin_lock(&wdev->oldest_lsid_lock);
        oldest_lsid = wdev->oldest_lsid;
        spin_unlock(&wdev->oldest_lsid_lock);
        
        /*
         * Fill log records for for each request.
         */
        ringbuf_off = get_ring_buffer_offset_2(get_super_sector(wdev->lsuper0));
        ringbuf_size = get_log_capacity(wdev);
        /*
         * 1. Lock latest_lsid_lock.
         * 2. Get latest_lsid 
         * 3. Calc required number of physical blocks for log pack.
         * 4. Set next latest_lsid.
         * 5. Unlock latest_lsid_lock.
         */
        spin_lock(&wdev->latest_lsid_lock);
        logpack_lsid = wdev->latest_lsid;
        logpack_size = walb_logpack_header_fill
                (lhead, logpack_lsid, wk->reqp_ary, wk->n_req,
                 wdev->physical_bs / wdev->logical_bs, ringbuf_size);
        if (logpack_size < 0) {
                LOGe("walb_logpack_header_fill failed\n");
                spin_unlock(&wdev->latest_lsid_lock);
                goto error0;
        }
        next_logpack_lsid = logpack_lsid + logpack_size;
        if (next_logpack_lsid - oldest_lsid > ringbuf_size) {
                /* Ring buffer overflow. */
                LOGe("There is not enough space to write log for %d:%d !\n",
                         MAJOR(wdev->devt), MINOR(wdev->devt));
                spin_unlock(&wdev->latest_lsid_lock);
                goto error0;
        }
        wdev->latest_lsid = next_logpack_lsid;
        spin_unlock(&wdev->latest_lsid_lock);

        /* Now log records is filled except checksum.
           Calculate and fill checksum for all requests and
           the logpack header. */
#ifdef WALB_DEBUG
        walb_logpack_header_print(KERN_DEBUG, lhead); /* debug */
#endif
        walb_logpack_calc_checksum(lhead, wdev->physical_bs,
                                   wk->reqp_ary, wk->n_req);
#ifdef WALB_DEBUG
        walb_logpack_header_print(KERN_DEBUG, lhead); /* debug */
#endif
        
        /* 
         * Complete log pack header and create its bio.
         *
         * Currnetly walb_logpack_write() is blocked till all bio(s)
         * are completed.
         */
        if (walb_logpack_write(wdev, lhead, wk->reqp_ary) != 0) {
                LOGe("logpack write failed (lsid %llu).\n",
                         lhead->logpack_lsid);
                goto error0;
        }

        /* Clone bio(s) of each request and set offset for log pack.
           Submit prepared bio(s) to log device. */
        if (walb_datapack_write(wdev, lhead, wk->reqp_ary) != 0) {
                LOGe("datapack write failed (lsid %llu). \n",
                         lhead->logpack_lsid);
                goto error0;
        }

        /* Normally completed log/data writes. */
        walb_end_requests(wk->reqp_ary, wk->n_req, 0);

        /* Update written_lsid. */
        spin_lock(&wdev->datapack_list_lock);
        if (next_logpack_lsid <= wdev->written_lsid) {
                LOGe("Logpack/data write order is not kept.\n");
                atomic_set(&wdev->is_read_only, 1);
        } /* This is almost assertion. */
        wdev->written_lsid = next_logpack_lsid;
        spin_unlock(&wdev->datapack_list_lock);

        goto fin;

error0:
        walb_end_requests(wk->reqp_ary, wk->n_req, -EIO);
fin:        
        kfree(wk->reqp_ary);
        kfree(wk);
        sector_free(lhead_sect);

        LOGd("walb_make_logpack_and_submit_task end\n");
}

/**
 * Register work of making log pack.
 *
 * This is executed inside interruption context.
 *
 * @wdev walb device.
 * @reqp_ary array of (request*). This will be deallocated after making log pack really.
 * @n_req number of items in the array.
 *
 * @return 0 in succeeded, or -1.
 */
static int walb_make_and_write_logpack(struct walb_dev *wdev,
                                       struct request** reqp_ary, int n_req)
{
        struct walb_make_logpack_work *wk;

        if (atomic_read(&wdev->is_read_only)) {
                LOGd("Currently read-only mode. write failed.\n");
                goto error0;
        }
        
        wk = kmalloc(sizeof(struct walb_make_logpack_work), GFP_ATOMIC);
        if (! wk) { goto error0; }

        wk->reqp_ary = reqp_ary;
        wk->n_req = n_req;
        wk->wdev = wdev;
        INIT_WORK(&wk->work, walb_make_logpack_and_submit_task);
        queue_work(wqs_, &wk->work);
        
        return 0;

error0:
        return -1;
}

/**
 * Work as a just wrapper of the underlying data device.
 */
static void walb_full_request2(struct request_queue *q)
{
        struct request *req;
        struct walb_dev *wdev = q->queuedata;
        int i;

        struct request **reqp_ary = NULL;
        int n_req = 0;
        const int max_n_req = max_n_log_record_in_sector(wdev->physical_bs);
        /* LOGd("max_n_req: %d\n", max_n_req); */
        
        while ((req = blk_peek_request(q)) != NULL) {

                blk_start_request(req);
                if (req->cmd_type != REQ_TYPE_FS) {
			LOGn("skip non-fs request.\n");
                        __blk_end_request_all(req, -EIO);
                        continue;
                }

                if (req->cmd_flags & REQ_FLUSH) {
                        LOGd("REQ_FLUSH\n");
                }
                if (req->cmd_flags & REQ_FUA) {
                        LOGd("REQ_FUA\n");
                }
                if (req->cmd_flags & REQ_DISCARD) {
                        LOGd("REQ_DISCARD\n");
                }

                if (req->cmd_flags & REQ_WRITE) {
                        /* Write.
                           Make log record and
                           add log pack. */

                        LOGd("WRITE %ld %d\n", blk_rq_pos(req), blk_rq_bytes(req));
                        
                        if (n_req == max_n_req) {
                                if (walb_make_and_write_logpack(wdev, reqp_ary, n_req) != 0) {

                                        for (i = 0; i < n_req; i ++) {
                                                __blk_end_request_all(reqp_ary[i], -EIO);
                                        }
                                        kfree(reqp_ary);
                                        continue;
                                }
                                reqp_ary = NULL;
                                n_req = 0;
                        }
                        if (n_req == 0) {
                                ASSERT(reqp_ary == NULL);
                                reqp_ary = kmalloc(sizeof(struct request *) * max_n_req,
                                                   GFP_ATOMIC);
                        }
                                                  
                        reqp_ary[n_req] = req;
                        n_req ++;
                        
                } else {
                        /* Read.
                           Just forward to data device. */
                        
                        LOGd("READ %ld %d\n", blk_rq_pos(req), blk_rq_bytes(req));

                        switch (1) {
                        case 0:
                                walb_make_ddev_request(wdev->ddev, req);
                                break;
                        case 1:
                                walb_forward_request_to_ddev(wdev->ddev, req);
                                break;
                        case 2:
                                walb_forward_request_to_ddev2(wdev->ddev, req);
                                break;
                        default:
                                BUG();
                        }
                }
        }

        /* If log pack exists(one or more requests are write),
           Enqueue log write task.
        */
        if (n_req > 0) {
                if (walb_make_and_write_logpack(wdev, reqp_ary, n_req) != 0) {
                        for (i = 0; i < n_req; i ++) {
                                __blk_end_request_all(reqp_ary[i], -EIO);
                        }
                        kfree(reqp_ary);
                }
        }
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
static int walblog_make_request(struct request_queue *q, struct bio *bio)
{
        struct walb_dev *wdev = q->queuedata;
        
        if (bio->bi_rw & WRITE) {
                bio_endio(bio, -EIO);
                return 0;
        } else {
                bio->bi_bdev = wdev->ldev;
                return 1;
        }
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
 *
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

                (walb_snapshot_record_t *)ctl->u2k.__buf;
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

/**
 * Unplug walb device.
 *
 * Log -> Data.
 */
static void walb_unplug_all(struct request_queue *q)
{
        struct walb_dev *wdev = q->queuedata;
        struct request_queue *lq, *dq;
        
        ASSERT(wdev != NULL);
        
        generic_unplug_device(q);

        lq = bdev_get_queue(wdev->ldev);
        dq = bdev_get_queue(wdev->ddev);
        if (lq)
                blk_unplug(lq);
        if (dq)
                blk_unplug(dq);
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

/**
 * Unplug walblog device.
 *
 * Just unplug underlying log device.
 */
static void walblog_unplug(struct request_queue *q)
{
        struct walb_dev *wdev = q->queuedata;
        struct request_queue *lq;
        
        ASSERT(wdev != NULL);

        lq = bdev_get_queue(wdev->ldev);
        ASSERT(lq != NULL);
        
        generic_unplug_device(q);
        if (lq)
                blk_unplug(lq);
}

static struct block_device_operations walblog_ops = {
        .owner   = THIS_MODULE,
        .open    = walblog_open,
        .release = walblog_release,
        .ioctl   = walblog_ioctl
};

/**
 * Sync down super block.
 */
static int walb_sync_super_block(struct walb_dev *wdev)
{
        u64 written_lsid, oldest_lsid;

        struct sector_data *lsuper_tmp;
        struct walb_super_sector *sect, *sect_tmp;

        /* Get written lsid. */
        spin_lock(&wdev->datapack_list_lock);
        written_lsid = wdev->written_lsid;
        spin_unlock(&wdev->datapack_list_lock);

        /* Get oldest lsid. */
        spin_lock(&wdev->oldest_lsid_lock);
        oldest_lsid = wdev->oldest_lsid;
        spin_unlock(&wdev->oldest_lsid_lock);

        /* Allocate temporary super block. */
        lsuper_tmp = sector_alloc(wdev->physical_bs, GFP_NOIO);
        if (lsuper_tmp == NULL) {
                goto error0;
        }
        ASSERT_SECTOR_DATA(lsuper_tmp);
        sect_tmp = get_super_sector(lsuper_tmp);

        /* Modify super sector and copy. */
        spin_lock(&wdev->lsuper0_lock);
        ASSERT_SECTOR_DATA(wdev->lsuper0);
        ASSERT(is_same_size_sector(wdev->lsuper0, lsuper_tmp));
        sect = get_super_sector(wdev->lsuper0);
        sect->oldest_lsid = oldest_lsid;
        sect->written_lsid = written_lsid;
        sector_copy(lsuper_tmp, wdev->lsuper0);
        spin_unlock(&wdev->lsuper0_lock);
        
        if (!walb_write_super_sector(wdev->physical_bs, lsuper_tmp)) {
                LOGe("walb_sync_super_block: write super block failed.\n");
                goto error1;
        }

        sector_free(lsuper_tmp);

        /* Update previously written lsid. */
        spin_lock(&wdev->datapack_list_lock);
        wdev->prev_written_lsid = written_lsid;
        spin_unlock(&wdev->datapack_list_lock);
        
        return 0;

error1:
        sector_free(lsuper_tmp);
error0:
        return -1;
}

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
 * Initialize walb block device.
 */
static int walb_prepare_device(struct walb_dev *wdev, unsigned int minor,
                               const char *name)
{
	/*
	 * The I/O queue, depending on whether we are using our own
	 * make_request function or not.
	 */
	switch (request_mode) {
#if 0
        case RM_NOQUEUE:
		wdev->queue = blk_alloc_queue(GFP_KERNEL);
		if (wdev->queue == NULL)
			goto out;
		blk_queue_make_request(wdev->queue, walb_make_request);
#endif
		break;

        case RM_FULL:
		wdev->queue = blk_init_queue(walb_full_request2, &wdev->lock);
		if (wdev->queue == NULL)
			goto out;
                if (elevator_change(wdev->queue, "noop"))
                        goto out_queue;
		break;

        default:
		LOGe("Bad request mode %d.\n", request_mode);
                BUG();
	}
	blk_queue_logical_block_size(wdev->queue, wdev->logical_bs);
	blk_queue_physical_block_size(wdev->queue, wdev->physical_bs);
	wdev->queue->queuedata = wdev;
        /*
         * 1. Bio(s) that can belong to a request should be packed.
         * 2. Parallel (independent) writes should be packed.
         *
         * 'unplug_thresh' is prcatically max requests in a log pack.
         * 'unplug_delay' should be as small as possible to minimize latency.
         */
        wdev->queue->unplug_thresh = 16;
        wdev->queue->unplug_delay = msecs_to_jiffies(1);
        LOGd("1ms = %lu jiffies\n", msecs_to_jiffies(1)); /* debug */
        wdev->queue->unplug_fn = walb_unplug_all;

        /*
	 * And the gendisk structure.
	 */
	/* dev->gd = alloc_disk(WALB_MINORS); */
        wdev->gd = alloc_disk(1);
	if (! wdev->gd) {
		LOGe("alloc_disk failure.\n");
		goto out_queue;
	}
	wdev->gd->major = walb_major;
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

/* out_disk: */
/*         if (wdev->gd) { */
/*                 put_disk(wdev->gd); */
/*         } */
out_queue:
        if (wdev->queue) {
                if (request_mode == RM_NOQUEUE)
                        kobject_put(&wdev->queue->kobj);
                else
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
                if (request_mode == RM_NOQUEUE)
                        kobject_put(&wdev->queue->kobj);
                else
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
        wdev->log_queue->unplug_fn = walblog_unplug;

        wdev->log_gd = alloc_disk(1);
        if (! wdev->log_gd) {
                goto error1;
        }
        wdev->log_gd->major = walb_major;
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
out_snapshot_create:
        if (wdev->snapd) {
                snapshot_data_destroy(wdev->snapd);
        }
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
        
        walb_finalize_super_block(wdev);
        sector_free(wdev->lsuper0);
}

/**
 * Finalize super block.
 *
 * @wdev walb device.
 *
 * @return 0 in success, or -1.
 */
static int walb_finalize_super_block(struct walb_dev *wdev)
{
        /* 
         * 1. Wait for all related IO are finished.
         * 2. Cleanup snapshot metadata and write down.
         * 3. Generate latest super block and write down.
         */
        
        /*
         * Test
         */
        u64 latest_lsid;

        spin_lock(&wdev->latest_lsid_lock);
        latest_lsid = wdev->latest_lsid;
        spin_unlock(&wdev->latest_lsid_lock);
        
        spin_lock(&wdev->datapack_list_lock);
        wdev->written_lsid = latest_lsid;
        spin_unlock(&wdev->datapack_list_lock);

        if (is_sync_superblock) {
                LOGn("is_sync_superblock is on\n");
                if (walb_sync_super_block(wdev) != 0) {
                        goto error0;
                }
        } else {
                LOGn("is_sync_superblock is off\n");
        }
        return 0;

error0:
        return -1;
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


/*******************************************************************************
 * Checkpointing
 *******************************************************************************/

/**
 * Do checkpointing.
 */
static void do_checkpointing(struct work_struct *work)
{
        unsigned long j0, j1;
        unsigned long interval;
        long delay, sync_time, next_delay;
        int ret;
        u64 written_lsid, prev_written_lsid;
        
        struct delayed_work *dwork =
                container_of(work, struct delayed_work, work);
        struct walb_dev *wdev =
                container_of(dwork, struct walb_dev, checkpoint_work);

        LOGd("do_checkpointing called.\n");

        /* Get written_lsid and prev_written_lsid. */
        spin_lock(&wdev->datapack_list_lock);
        written_lsid = wdev->written_lsid;
        prev_written_lsid = wdev->prev_written_lsid;
        spin_unlock(&wdev->datapack_list_lock);

        /* Lock */
        down_write(&wdev->checkpoint_lock);
        interval = wdev->checkpoint_interval;

        ASSERT(interval > 0);
        switch (wdev->checkpoint_state) {
        case CP_STOPPING:
                LOGd("do_checkpointing should stop.\n");
                up_write(&wdev->checkpoint_lock);
                return;
        case CP_WAITING:
                wdev->checkpoint_state = CP_RUNNING;
                break;
        default:
                BUG();
        }
        up_write(&wdev->checkpoint_lock);

        /* Write superblock */
        j0 = jiffies;
        if (written_lsid == prev_written_lsid) {

                LOGd("skip superblock sync.\n");
        } else {
                if (walb_sync_super_block(wdev) != 0) {

                        atomic_set(&wdev->is_read_only, 1);
                        LOGe("superblock sync failed.\n");

                        down_write(&wdev->checkpoint_lock);
                        wdev->checkpoint_state = CP_STOPPED;
                        up_write(&wdev->checkpoint_lock);
                        return;
                }
        }
        j1 = jiffies;

        delay = msecs_to_jiffies(interval);
        sync_time = (long)(j1 - j0);
        next_delay = (long)delay - sync_time;

        LOGd("do_checkpinting: delay %ld sync_time %ld next_delay %ld\n",
                 delay, sync_time, next_delay);

        if (next_delay <= 0) {
                LOGw("Checkpoint interval is too small. "
                         "Should be more than %d.\n", jiffies_to_msecs(sync_time));
                next_delay = 1;
        }
        ASSERT(next_delay > 0);
        
        down_write(&wdev->checkpoint_lock);
        if (wdev->checkpoint_state == CP_RUNNING) {
                /* Register delayed work for next time */
                INIT_DELAYED_WORK(&wdev->checkpoint_work, do_checkpointing);
                ret = queue_delayed_work(wqs_, &wdev->checkpoint_work, next_delay);
                ASSERT(ret);
                wdev->checkpoint_state = CP_WAITING;
        } else {
                /* Do nothing */
                ASSERT(wdev->checkpoint_state == CP_STOPPING);
        }
        up_write(&wdev->checkpoint_lock);
}

/**
 * Start checkpointing.
 *
 * Do nothing if
 *   wdev->is_checkpoint_running is 1 or
 *   wdev->checkpoint_interval is 0.
 */
static void start_checkpointing(struct walb_dev *wdev)
{
        unsigned long delay;
        unsigned long interval;

        down_write(&wdev->checkpoint_lock);
        if (wdev->checkpoint_state != CP_STOPPED) {
                LOGw("Checkpoint state is not stopped.\n");
                up_write(&wdev->checkpoint_lock);
                return;
        }

        interval = wdev->checkpoint_interval;
        if (interval == 0) { /* This is not error. */
                LOGn("checkpoint_interval is 0.\n");
                up_write(&wdev->checkpoint_lock);
                return;
        }
        ASSERT(interval > 0);
        
        delay = msecs_to_jiffies(interval);
        ASSERT(delay > 0);
        INIT_DELAYED_WORK(&wdev->checkpoint_work, do_checkpointing);

        queue_delayed_work(wqs_, &wdev->checkpoint_work, delay);
        wdev->checkpoint_state = CP_WAITING;
        LOGd("state change to CP_WAITING\n");
        up_write(&wdev->checkpoint_lock);
}

/**
 * Stop checkpointing.
 *
 * Do nothing if 
 *   wdev->is_checkpoint_running is not 1 or
 *   wdev->should_checkpoint_stop is not 0.
 */
static void stop_checkpointing(struct walb_dev *wdev)
{
        int ret;
        u8 state;

        down_write(&wdev->checkpoint_lock);
        state = wdev->checkpoint_state;
        if (state != CP_WAITING && state != CP_RUNNING) {
                LOGw("Checkpointing is not running.\n");
                up_write(&wdev->checkpoint_lock);
                return;
        }
        wdev->checkpoint_state = CP_STOPPING;
        LOGd("state change to CP_STOPPING\n");
        up_write(&wdev->checkpoint_lock);

        /* We must unlock before calling this to avoid deadlock. */
        ret = cancel_delayed_work_sync(&wdev->checkpoint_work);
        LOGd("cancel_delayed_work_sync: %d\n", ret);

        down_write(&wdev->checkpoint_lock);
        wdev->checkpoint_state = CP_STOPPED;
        LOGd("state change to CP_STOPPED\n");
        up_write(&wdev->checkpoint_lock);
}

/**
 * Get checkpoint interval
 *
 * @wdev walb device.
 *
 * @return current checkpoint interval.
 */
static u32 get_checkpoint_interval(struct walb_dev *wdev)
{
        u32 interval;
        
        down_read(&wdev->checkpoint_lock);
        interval = wdev->checkpoint_interval;
        up_read(&wdev->checkpoint_lock);

        return interval;
}

/**
 * Set checkpoint interval.
 *
 * @wdev walb device.
 * @val new checkpoint interval.
 */
static void set_checkpoint_interval(struct walb_dev *wdev, u32 val)
{
        down_write(&wdev->checkpoint_lock);
        wdev->checkpoint_interval = val;
        up_write(&wdev->checkpoint_lock);
        
        stop_checkpointing(wdev);
        start_checkpointing(wdev);
}

/*******************************************************************************
 *
 *******************************************************************************/

/**
 * Set up our internal device.
 *
 * @return 0 in success, or -1.
 */
static int setup_device_tmp(unsigned int minor)
{
        dev_t ldevt, ddevt;
        struct walb_dev *wdev;

        ldevt = MKDEV(ldev_major, ldev_minor);
        ddevt = MKDEV(ddev_major, ddev_minor);
        wdev = prepare_wdev(minor, ldevt, ddevt, NULL);
        if (wdev == NULL) {
                goto error0;
        }
        register_wdev(wdev);

        Devices = wdev;
        
        return 0;

error0:
        return -1;
}

static int __init walb_init(void)
{
        /* DISK_NAME_LEN assersion */
        ASSERT_DISK_NAME_LEN();
        
	/*
	 * Get registered.
	 */
	walb_major = register_blkdev(walb_major, WALB_NAME);
	if (walb_major <= 0) {
		LOGw("unable to get major number.\n");
		return -EBUSY;
	}
        LOGi("walb_start with major id %d.\n", walb_major);

        /*
         * Workqueue.
         */
        wqs_ = create_singlethread_workqueue(WALB_WORKQUEUE_SINGLE_NAME);
        if (wqs_ == NULL) {
                LOGe("create single-thread workqueue failed.\n");
                goto out_unregister;
        }
        wqm_ = create_workqueue(WALB_WORKQUEUE_MULTI_NAME);
        if (wqm_ == NULL) {
                LOGe("create multi-thread workqueue failed.\n");
                goto out_workqueue_single;
        }
        
        /*
         * Alldevs.
         */
        if (alldevs_init() != 0) {
                LOGe("alldevs_init failed.\n");
                goto out_workqueue_multi;
        }
        
        /*
         * Init control device.
         */
        if (walb_control_init() != 0) {
                LOGe("walb_control_init failed.\n");
                goto out_alldevs_exit;
        }

	/*
	 * Allocate the device array, and initialize each one.
	 */
#if 0
        if (setup_device_tmp(0) != 0) {
		LOGe("setup_device failed.\n");
                goto out_control_exit;
        }
#endif
	return 0;
        
#if 0
out_control_exit:
        walb_control_exit();
#endif
out_alldevs_exit:
        alldevs_exit();
out_workqueue_multi:
        if (wqm_) { destroy_workqueue(wqm_); }
out_workqueue_single:
        if (wqs_) { destroy_workqueue(wqs_); }
out_unregister:
	unregister_blkdev(walb_major, WALB_NAME);
	return -ENOMEM;
}

static void walb_exit(void)
{
        struct walb_dev *wdev;

#if 0
        wdev = Devices;
        unregister_wdev(wdev);
        destroy_wdev(wdev);
#endif
        
        alldevs_write_lock();
        wdev = alldevs_pop();
        while (wdev != NULL) {

                unregister_wdev(wdev);
                destroy_wdev(wdev);
                
                wdev = alldevs_pop();
        }
        alldevs_write_unlock();

        flush_workqueue(wqm_); /* can omit this? */
        destroy_workqueue(wqm_);
        flush_workqueue(wqs_); /* can omit this? */
        destroy_workqueue(wqs_);
        
	unregister_blkdev(walb_major, WALB_NAME);

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
        init_rwsem(&wdev->checkpoint_lock);
        wdev->checkpoint_interval = WALB_DEFAULT_CHECKPOINT_INTERVAL;
        wdev->checkpoint_state = CP_STOPPED;
        
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
