/**
 * wrapper_blk_walb.c - WalB wrapper block device.
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

#include "walb/walb.h"
#include "walb/block_size.h"
#include "walb/sector.h"
#include "wrapper_blk.h"
#include "wrapper_blk_walb.h"
#include "sector_io.h"

/*******************************************************************************
 * Module variables definition.
 *******************************************************************************/

/* Device size list string. The unit of each size is bytes. */
char *log_device_str_ = "/dev/simple_blk/0";
char *data_device_str_ = "/dev/simple_blk/1";
/* Minor id start. */
int start_minor_ = 0;

/* Physical block size. */
int physical_block_size_ = 4096;

/*******************************************************************************
 * Module parameters definition.
 *******************************************************************************/

module_param_named(log_device_str, log_device_str_, charp, S_IRUGO);
module_param_named(data_device_str, data_device_str_, charp, S_IRUGO);
module_param_named(start_minor, start_minor_, int, S_IRUGO);
module_param_named(pbs, physical_block_size_, int, S_IRUGO);

/*******************************************************************************
 * Static data definition.
 *******************************************************************************/

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

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
	spin_lock_init(&pdata->pending_data_lock);
	INIT_LIST_HEAD(&pdata->writepack_list);
	
        /* open underlying log device. */
        ldev = blkdev_get_by_path(
                log_device_str_, FMODE_READ|FMODE_WRITE|FMODE_EXCL,
                create_private_data);
        if (IS_ERR(ldev)) {
                LOGe("open %s failed.", log_device_str_);
                goto error1;
        }

        /* open underlying data device. */
	ddev = blkdev_get_by_path(
		data_device_str_, FMODE_READ|FMODE_WRITE|FMODE_EXCL,
                create_private_data);
	if (IS_ERR(ddev)) {
		LOGe("open %s failed.", data_device_str_);
		goto error2;
	}

        /* Block size */
        lbs = bdev_logical_block_size(ddev);
        pbs = bdev_physical_block_size(ddev);
        
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
        blk_queue_logical_block_size(wdev->queue, lbs);
        blk_queue_physical_block_size(wdev->queue, pbs);

	/* Prepare pdata. */
	pdata->ldev = ldev;
	pdata->ddev = ddev;
        wdev->private_data = pdata;

	/* Load super block. */
	pdata->lsuper0 = sector_alloc(GFP_KERNEL, pbs);
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
	
        /* capacity */
        wdev->capacity = get_capacity(ddev->bd_disk);
        set_capacity(wdev->gd, wdev->capacity);

	/* Set limit. */
        blk_queue_stack_limits(wdev->queue, bdev_get_queue(ldev));
        blk_queue_stack_limits(wdev->queue, bdev_get_queue(ddev));
	
        return true;

error4:
	sector_free(pdata->lsuper0);
error3:
        blkdev_put(ddev, FMODE_READ|FMODE_WRITE|FMODE_EXCL);
error2:
        blkdev_put(ldev, FMODE_READ|FMODE_WRITE|FMODE_EXCL);
error1:
	kfree(pdata);
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
	kfree(pdata);
	wdev->private_data = NULL;
}

/* Customize wdev after register before start. */
static void customize_wdev(struct wrapper_blk_dev *wdev)
{
        struct request_queue *q, *uq;
	struct pdata *pdata;
        ASSERT(wdev);
        q = wdev->queue;
	pdata = wdev->private_data;

        uq = bdev_get_queue(pdata->ddev);
        /* Accept REQ_FLUSH and REQ_FUA. */
        if (uq->flush_flags & REQ_FLUSH) {
                if (uq->flush_flags & REQ_FUA) {
                        LOGn("Supports REQ_FLUSH | REQ_FUA.");
                        blk_queue_flush(q, REQ_FLUSH | REQ_FUA);
                } else {
                        LOGn("Supports REQ_FLUSH.");
                        blk_queue_flush(q, REQ_FLUSH);
                }
        } else {
                LOGn("Supports REQ_FLUSH (the underlying device does not support).");
		blk_queue_flush(q, REQ_FLUSH);
        }

        if (blk_queue_discard(uq)) {
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

        LOGe("register_dev begin");
        
        /* capacity must be set lator. */
        ret = wdev_register_with_req(get_minor(i), capacity,
				physical_block_size_,
				wrapper_blk_req_request_fn);
                
        if (!ret) {
                goto error;
        }
        wdev = wdev_get(get_minor(i));
        if (!create_private_data(wdev)) {
                goto error;
        }
        customize_wdev(wdev);

        LOGe("register_dev end");

        return true;
error:
        unregister_dev();
        return false;
}

static void unregister_dev(void)
{
        unsigned int i = 0;
        struct wrapper_blk_dev *wdev;
        
        wdev = wdev_get(get_minor(i));
        if (wdev) {
                destroy_private_data(wdev);
        }
        wdev_unregister(get_minor(i));
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

/*******************************************************************************
 * Global function definition.
 *******************************************************************************/

/*******************************************************************************
 * Init/exit definition.
 *******************************************************************************/

static int __init wrapper_blk_init(void)
{
	if (!is_valid_pbs(physical_block_size_)) {
		goto error0;
	}
	
        pre_register();
        
        if (!register_dev()) {
                goto error0;
        }
        if (!start_dev()) {
                goto error1;
        }

        return 0;
#if 0
error2:
        stop_dev();
#endif
error1:
        unregister_dev();
error0:
        return -1;
}

static void wrapper_blk_exit(void)
{
        stop_dev();
        unregister_dev();
        post_unregister();
}

module_init(wrapper_blk_init);
module_exit(wrapper_blk_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Walb block req device for Test");
MODULE_ALIAS("wrapper_blk_req");
