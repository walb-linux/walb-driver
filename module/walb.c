/**
 * walb.c - Block-level WAL
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * Written by: Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
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

#include "walb_kern.h"
#include "../include/walb_log_device.h"

static int walb_major = 0;
module_param(walb_major, int, 0);
static int ndevices = 1;
module_param(ndevices, int, 0);

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

        bdev = open_by_devnum(dev, FMODE_READ|FMODE_WRITE);
        if (IS_ERR(bdev)) { err = PTR_ERR(bdev); goto open_err; }

        err = bd_claim(bdev, walb_lock_bdev);
        if (err) { goto claim_err; }
        
        *bdevp = bdev;
        return err;

claim_err:
        printk_e("bd_claim error %s.\n", __bdevname(dev, b));
        blkdev_put(bdev, FMODE_READ|FMODE_WRITE);
        return err;
open_err:
        printk_e("open error %s.\n", __bdevname(dev, b));
        return err;
}

/**
 * Release underlying block device.
 * @bdev bdev pointer.
 */
static void walb_unlock_bdev(struct block_device *bdev)
{
        bd_release(bdev);
        blkdev_put(bdev, FMODE_READ|FMODE_WRITE);
}

/*
 * Handle an I/O request.
 */
static void walb_transfer(struct walb_dev *dev, unsigned long sector,
                          unsigned long nbytes, char *buffer, int write)
{
	unsigned long offset = sector * dev->logical_bs;

	if ((offset + nbytes) > dev->size) {
		printk_n("Beyond-end write (%ld %ld)\n", offset, nbytes);
		return;
	}
	if (write)
		memcpy(dev->data + offset, buffer, nbytes);
	else
		memcpy(buffer, dev->data + offset, nbytes);
}

/*
 * Transfer a single BIO.
 */
static int walb_xfer_bio(struct walb_dev *dev, struct bio *bio)
{
	int i;
	struct bio_vec *bvec;
	sector_t sector = bio->bi_sector;

	/* Do each segment independently. */
	bio_for_each_segment(bvec, bio, i) {
		char *buffer = __bio_kmap_atomic(bio, i, KM_USER0);
		walb_transfer(dev, sector, bio_cur_bytes(bio),
                              buffer, bio_data_dir(bio) == WRITE);
		sector += bio_cur_bytes(bio) / dev->logical_bs;
		__bio_kunmap_atomic(bio, KM_USER0);
	}
	return 0; /* Always "succeed" */
}

static int walb_xfer_segment(struct walb_dev *dev,
                             struct req_iterator *iter)
{
        struct bio *bio = iter->bio;
        int i = iter->i;
        sector_t sector = bio->bi_sector;
        
        char *buffer = __bio_kmap_atomic(bio, i, KM_USER0);
        walb_transfer(dev, sector, bio_cur_bytes(bio),
                      buffer, bio_data_dir(bio) == WRITE);
        sector += bio_cur_bytes(bio) / dev->logical_bs;
        __bio_kunmap_atomic(bio, KM_USER0);
        return 0;
}

/*
 * Transfer a full request.
 */
static int walb_xfer_request(struct walb_dev *dev, struct request *req)
{
	struct bio *bio;
	int nsect = 0;
        struct req_iterator iter;
        struct bio_vec *bvec;

        if (0) {
                __rq_for_each_bio(bio, req) {
                        walb_xfer_bio(dev, bio);
                        nsect += bio->bi_size/dev->logical_bs;
                }
        } else {
                rq_for_each_segment(bvec, req, iter) {
                        walb_xfer_segment(dev, &iter);
                        nsect += bio_iovec_idx(iter.bio, iter.i)->bv_len
                                / dev->logical_bs;
                }
        }
	return nsect;
}


/*
 * Smarter request function that "handles clustering".
 */
static void walb_full_request(struct request_queue *q)
{
	struct request *req;
	int sectors_xferred;
	struct walb_dev *dev = q->queuedata;
        int error;

	while ((req = blk_peek_request(q)) != NULL) {
                blk_start_request(req);
		if (req->cmd_type != REQ_TYPE_FS) {
			printk_n("Skip non-fs request\n");
			__blk_end_request_all(req, -EIO);
			continue;
		}
		sectors_xferred = walb_xfer_request(dev, req);
                
                error = (sectors_xferred * dev->logical_bs ==
                         blk_rq_bytes(req)) ? 0 : -EIO;
                __blk_end_request_all(req, error);
	}
}


/**
 * End io with completion.
 *
 * bio->bi_private must be (struct walb_bio_with_completion *).
 */
static void walb_end_io_with_completion(struct bio *bio, int error)
{
        struct walb_bio_with_completion *bioc;
        bioc = bio->bi_private;

        ASSERT(bioc->status == WALB_BIO_INIT);
        if (error || ! test_bit(BIO_UPTODATE, &bio->bi_flags)) {
                printk_e("walb_end_io_with_completion: error %d bi_flags %lu\n",
                         error, bio->bi_flags);
                bioc->status = WALB_BIO_ERROR;
        } else {
                bioc->status = WALB_BIO_END;
        }
        complete(&bioc->wait);
}

/**
 * The bio is walb_ddev_bio's bio.
 *
 */
static void walb_ddev_end_io(struct bio *bio, int error)
{
        struct walb_ddev_bio *dbio = bio->bi_private;
        struct request *req = dbio->req;
        struct walb_ddev_bio *tmp, *next;
        struct list_head *head;
        struct walb_submit_bio_work *wq;

        int is_last = 1;
        int is_err = 0;
        
        printk_d("ddev_end_io() called\n");
        printk_d("bio %ld %d\n",
               (long)bio->bi_sector, bio->bi_size);
        
        BUG_ON(! dbio);
        head = dbio->head;
        BUG_ON(! head);
        
        if (error || ! test_bit(BIO_UPTODATE, &bio->bi_flags)) {
                printk_e("IO failed error=%d, uptodate=%d\n",
                       error, test_bit(BIO_UPTODATE, &bio->bi_flags));
                
                dbio->status = WALB_BIO_ERROR;
        }

        dbio->status = WALB_BIO_END;
        bio_put(bio);
        dbio->bio = NULL;

        /* Check whether it's the last bio in the request finished
           or error or not finished. */
        wq = container_of(head, struct walb_submit_bio_work, list);
        spin_lock(&wq->lock);
        list_for_each_entry_safe(tmp, next, head, list) {

                /* printk_d("status: %d\n", tmp->status); */
                switch (tmp->status) {
                case WALB_BIO_END:
                        /* do nothing */
                        break;
                case WALB_BIO_ERROR:
                        is_err ++;
                        break;
                case WALB_BIO_INIT:
                        is_last = 0;
                        break;
                default:
                        BUG();
                }

                if (is_err) {
                        break;
                }
        }
        spin_unlock(&wq->lock);

        /* Finalize the request of wrapper device. */
        if (is_last) {
                if (is_err) {
                        printk_d("walb blk_end_request_all() -EIO\n");
                        blk_end_request_all(req, -EIO);
                } else {
                        printk_d("walb blk_end_request_all() 0\n");
                        blk_end_request_all(req, 0);
                }

                spin_lock(&wq->lock);
                list_for_each_entry_safe(tmp, next, head, list) {
                        BUG_ON(tmp->bio != NULL);
                        BUG_ON(tmp->status == WALB_BIO_INIT);
                        list_del(&tmp->list);
                        kfree(tmp);
                }
                /* confirm the list is empty */
                if (! list_empty(&wq->list)) {
                        printk_e("wq->list must be empty.\n");
                        BUG();
                }
                spin_unlock(&wq->lock);

                kfree(wq);
        }

        printk_d("ddev_end_io() end\n");
}


/**
 * Task to call submit_bio in a process context.
 */
static void walb_submit_bio_task(struct work_struct *work)
{
        struct walb_submit_bio_work *wq;
        struct walb_ddev_bio *dbio, *next;

        printk_d("submit_bio_task begin\n");
        
        wq = container_of(work, struct walb_submit_bio_work, work);
        
        if (list_empty(&wq->list)) {
                printk_w("list is empty\n");
        }
        
        list_for_each_entry_safe(dbio, next, &wq->list, list) {

                printk_d("submit_bio_task %ld %d\n",
                       (long)dbio->bio->bi_sector, dbio->bio->bi_size);
                submit_bio(dbio->bio->bi_rw, dbio->bio);
        }

        printk_d("submit_bio_task end\n");
}

/**
 * Make log record of the request.
 *
 * @wdev walb device.
 * @req request of the wrapper block device.
 */
static void walb_make_log_record(struct walb_dev *wdev, struct request *req)
{

        /* now editing */

}

/**
 * Make log pack and submit related bio(s).
 */
static void walb_make_log_pack_and_submit_task(struct work_struct *work)
{
        struct walb_make_log_pack_work *wk;

        wk = container_of(work, struct walb_make_log_pack_work, work);

        /* Allocate memory (sector size) for log pack header. */

        
        /* Fill log records for for each request. */
        

        /* Complete log pack header and create its bio. */
        

        /* Clone bio(s) of each request and set offset for log pack. */


        /* Submit prepared bio(s) to log device. */



        /* Now editing */
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
static int walb_make_and_write_log_pack(struct walb_dev *wdev,
                                         struct request** reqp_ary, int n_req)
{
        struct walb_make_log_pack_work *wk;

        wk = kmalloc(sizeof(struct walb_make_log_pack_work), GFP_ATOMIC);
        if (! wk) { goto error0; }

        wk->reqp_ary = reqp_ary;
        spin_lock_init(&wk->lock);
        
        INIT_WORK(&wk->work, walb_make_log_pack_and_submit_task);
        schedule_work(&wk->work);
        
        return 0;

error0:
        return -1;
}

/**
 * Convert request for data device and put IO task to workqueue.
 * This is executed in interruption context.
 *
 * @blk_start_request() has been already called.
 *
 * @wdev walb device.
 * @req request of the wrapper block device.
 */
static void walb_make_ddev_request(struct walb_dev *wdev, struct request *req)
{
        struct bio *bio;
        struct walb_ddev_bio *dbio, *next;
        int bio_nr = 0;
        struct walb_submit_bio_work *wq;

        printk_d("make_ddev_request() called\n");

        wq = kmalloc(sizeof(struct walb_submit_bio_work), GFP_ATOMIC);
        if (! wq) { goto error0; }
        INIT_LIST_HEAD(&wq->list);
        spin_lock_init(&wq->lock);
        
        __rq_for_each_bio(bio, req) {

                dbio = kmalloc(sizeof(struct walb_ddev_bio), GFP_ATOMIC);
                if (! dbio) { goto error1; }
                
                walb_init_ddev_bio(dbio);
                dbio->bio = bio_clone(bio, GFP_ATOMIC);
                dbio->bio->bi_bdev = wdev->ddev;
                dbio->bio->bi_end_io = walb_ddev_end_io;
                dbio->bio->bi_private = dbio;
                dbio->req = req;
                dbio->status = WALB_BIO_INIT;
                dbio->head = &wq->list;
                
                list_add_tail(&dbio->list, &wq->list);
                bio_nr ++;
                printk_d("dbio->status: %d\n", dbio->status);
        }

        printk_d("bio_nr: %d\n", bio_nr);
        ASSERT(! list_empty(&wq->list));

        INIT_WORK(&wq->work, walb_submit_bio_task);
        schedule_work(&wq->work);
        
        printk_d("make_ddev_request() end\n");
        return;

error1:
        list_for_each_entry_safe(dbio, next, &wq->list, list) {
                bio_put(dbio->bio);
                kfree(dbio);
        }
        kfree(wq);
error0:
        printk_e("make_ddev_request failed\n");
        __blk_end_request_all(req, -EIO);
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
        
        while ((req = blk_peek_request(q)) != NULL) {

                blk_start_request(req);
                if (req->cmd_type != REQ_TYPE_FS) {
			printk_n("skip non-fs request.\n");
                        __blk_end_request_all(req, -EIO);
                        continue;
                }

                if (req->cmd_flags & REQ_WRITE) {
                        /* Write.
                           Make log record and
                           add log pack.
                         */
                        if (n_req == max_n_req) {
                                if (walb_make_and_write_log_pack(wdev, reqp_ary, n_req) != 0) {

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
                        walb_make_ddev_request(wdev, req);
                }
        }

        /* If log pack exists(one or more requests are write),
           Enqueue log write task.
         */
        if (n_req > 0) {
                if (walb_make_and_write_log_pack(wdev, reqp_ary, n_req) != 0) {
                        for (i = 0; i < n_req; i ++) {
                                __blk_end_request_all(reqp_ary[i], -EIO);
                        }
                        kfree(reqp_ary);
                }
        }
}


/*
 * The direct make request version.
 */
static int walb_make_request(struct request_queue *q, struct bio *bio)
{
	struct walb_dev *dev = q->queuedata;
	int status;

	status = walb_xfer_bio(dev, bio);
	bio_endio(bio, status);
	return 0;
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

/*
 * The ioctl() implementation
 */

int walb_ioctl(struct block_device *bdev, fmode_t mode,
               unsigned int cmd, unsigned long arg)
{
	long size;
	struct hd_geometry geo;
	struct walb_dev *dev = bdev->bd_disk->private_data;

	switch(cmd) {
        case HDIO_GETGEO:
        	/*
		 * Get geometry: since we are a virtual device, we have to make
		 * up something plausible.  So we claim 16 sectors, four heads,
		 * and calculate the corresponding number of cylinders.  We set the
		 * start of data at sector four.
		 */
		size = dev->size * (dev->physical_bs / dev->logical_bs);
		geo.cylinders = (size & ~0x3f) >> 6;
		geo.heads = 4;
		geo.sectors = 16;
		geo.start = 4;
		if (copy_to_user((void __user *) arg, &geo, sizeof(geo)))
			return -EFAULT;
		return 0;
	}

	return -ENOTTY; /* unknown command */
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


/**
 * Allocate sector.
 *
 * @dev walb device.
 * @flag GFP flag.
 *
 * @return pointer to allocated memory or 
 */
static void* walb_alloc_sector(struct walb_dev *dev, gfp_t gfp_mask)
{
        void *ret;
        ret = kmalloc(dev->physical_bs, gfp_mask);
        return ret;
}

/**
 * Read/write physical sector from/to block device.
 * This is blocked operation.
 * Do not call this function in interuption handlers.
 *
 * @rw READ or WRITE (as same as 1st arg of submit_bio(rw, bio).).
 * @bdev block device, which is already opened.
 * @buf pointer to buffer of physical sector size.
 * @off offset in the block device [physical sector].
 *
 * @return 0 in success, or -1.
 */
static int walb_io_sector(int rw, struct block_device *bdev, void* buf, u64 off)
{
        struct bio *bio;
        int pbs, lbs;
        struct page *page;
        struct walb_bio_with_completion *bioc;

        printk_d("walb_io_sector begin\n");

        ASSERT(rw == READ || rw == WRITE);
        
        lbs = bdev_logical_block_size(bdev);
        pbs = bdev_physical_block_size(bdev);

        bioc = kmalloc(sizeof(struct walb_bio_with_completion), GFP_NOIO);
        if (bioc == NULL) {
                goto error0;
        }
        init_completion(&bioc->wait);
        bioc->status = WALB_BIO_INIT;
        
        /* Alloc bio */
        bio = bio_alloc(GFP_NOIO, 1);
        if (bio == NULL) {
                printk_e("bio_alloc failed\n");
                goto error1;
        }
        page = virt_to_page(buf);

        printk_d("walb_io_sector: sector %lu "
                 "page %p buf %p sectorsize %d offset %lu rw %d\n",
                 (unsigned long)(off * (pbs / lbs)),
                 virt_to_page(buf), buf,
                 pbs, offset_in_page(buf), rw);

        bio->bi_bdev = bdev;
        bio->bi_sector = off * (pbs / lbs);
        bio->bi_end_io = walb_end_io_with_completion;
        bio->bi_private = bioc;
        bio_add_page(bio, page, pbs, offset_in_page(buf));

        /* Submit and wait to complete. */
        submit_bio(rw, bio);
        wait_for_completion(&bioc->wait);

        /* Check result. */
        if (bioc->status != WALB_BIO_END) {
                printk_e("walb_io_sector: read sector failed\n");
                goto error2;
        }

        /* Cleanup allocated bio and memory. */
        bio_put(bio);
        kfree(bioc);

        printk_d("walb_io_sector end\n");
        return 0;

error2:
        bio_put(bio);
error1:
        kfree(bioc);
error0:
        return -1;
}


/**
 * Read super sector.
 * Currently super sector 0 will be read only (not super sector 1).
 *
 * @dev walb device.
 *
 * @return super sector (internally allocated) or NULL.
 */
static walb_super_sector_t* walb_read_super_sector(struct walb_dev *dev)
{
        u64 off0;
        walb_super_sector_t *lsuper0;

        printk_d("walb_read_super_sector begin\n");
        
        lsuper0 = walb_alloc_sector(dev, GFP_NOIO);
        if (lsuper0 == NULL) {
                goto error0;
        }

        /* Really read. */
        off0 = get_super_sector0_offset(dev->physical_bs);
        /* off1 = get_super_sector1_offset(dev->physical_bs, dev->n_snapshots); */
        if (walb_io_sector(READ, dev->ldev, lsuper0, off0) != 0) {
                printk_e("read super sector0 failed\n");
                goto error1;
        }

        /* Validate checksum. */
        if (checksum((u8 *)lsuper0, dev->physical_bs) != 0) {
                printk_e("walb_read_super_sector: checksum check failed.\n");
                goto error1;
        }
        
        printk_d("walb_read_super_sector end\n");
        return lsuper0;

error1:
        kfree(lsuper0);
error0:
        return NULL;
}

/**
 * Write super sector.
 * Currently only super sector 0 will be written. (super sector 1 is not.)
 *
 * @dev walb device.
 *
 * @return 0 in success, or -1.
 */
static int walb_write_super_sector(struct walb_dev *dev)
{
        u64 off0;
        u32 csum;

        printk_d("walb_write_super_sector begin\n");
        
        ASSERT(dev->lsuper0 != NULL);

        /* Generate checksum. */
        dev->lsuper0->checksum = 0;
        csum = checksum((u8 *)dev->lsuper0, dev->physical_bs);
        dev->lsuper0->checksum = csum;

        /* Really write. */
        off0 = get_super_sector0_offset(dev->physical_bs);
        if (walb_io_sector(WRITE, dev->ldev, dev->lsuper0, off0) != 0) {
                printk_e("write super sector0 failed\n");
                goto error0;
        }

        printk_d("walb_write_super_sector end\n");
        return 0;

error0:
        return -1;
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
 * @dev walb device struct.
 * @return 0 in success, or -1.
 */
static int walb_ldev_init(struct walb_dev *dev)
{
        walb_super_sector_t *lsuper0_tmp;
        ASSERT(dev != NULL);

        /*
         * 1. Read log device metadata
         */
        
        dev->lsuper0 = walb_read_super_sector(dev);
        if (dev->lsuper0 == NULL) {
                printk_e("walb_ldev_init: read super sector failed\n");
                goto error0;
        }

        if (walb_write_super_sector(dev) != 0) {
                printk_e("walb_ldev_init: write super sector failed\n");
                goto error1;
        }

        lsuper0_tmp = walb_read_super_sector(dev);
        if (lsuper0_tmp == NULL) {
                printk_e("walb_ldev_init: read lsuper0_tmp failed\n");
                kfree(lsuper0_tmp);
                goto error1;
        }

        if (memcmp(dev->lsuper0, lsuper0_tmp, dev->physical_bs) != 0) {
                printk_e("walb_ldev_init: memcmp NG\n");
        } else {
                printk_e("walb_ldev_init: memcmp OK\n");
        }

        kfree(lsuper0_tmp);
        /* Do not forget calling kfree(dev->lsuper0)
           before releasing the block device. */

        /* now editing */

        
        /*
         * 2. Redo from written_lsid to avaialble latest lsid.
         */

        /* This feature will be implemented later. */

        
        /*
         * 3. Sync log device super block.
         */

        /* If redo is done, super block should be re-written. */

        
        
        return 0;

error1:
        kfree(dev->lsuper0);
error0:
        return -1;
}

/*
 * Set up our internal device.
 *
 * @return 0 in success, or -1.
 */
static int setup_device(struct walb_dev *dev, int which)
{
        dev_t ldevt, ddevt;
        u16 ldev_lbs, ldev_pbs, ddev_lbs, ddev_pbs;

	/*
	 * Initialize walb_dev.
	 */
	memset(dev, 0, sizeof (struct walb_dev));
	spin_lock_init(&dev->lock);
        spin_lock_init(&dev->lsuper0_lock);
	
        /*
         * Open underlying log device.
         */
        ldevt = MKDEV(ldev_major, ldev_minor);
        if (walb_lock_bdev(&dev->ldev, ldevt) != 0) {
                printk_e("walb_lock_bdev failed (%d:%d for log)\n",
                         ldev_major, ldev_minor);
                return -1;
        }
        dev->ldev_size = get_capacity(dev->ldev->bd_disk);
        ldev_lbs = bdev_logical_block_size(dev->ldev);
        ldev_pbs = bdev_physical_block_size(dev->ldev);
        printk_i("log disk (%d:%d)\n", ldev_major, ldev_minor);
        printk_i("log disk size %llu\n", dev->ldev_size);
        printk_i("log logical sector size %u\n", ldev_lbs);
        printk_i("log physical sector size %u\n", ldev_pbs);
        
        /*
         * Open underlying data device.
         */
        ddevt = MKDEV(ddev_major, ddev_minor);
        if (walb_lock_bdev(&dev->ddev, ddevt) != 0) {
                printk_e("walb_lock_bdev failed (%d:%d for data)\n",
                         ddev_major, ddev_minor);
                goto out_ldev;
        }
        dev->ddev_size = get_capacity(dev->ddev->bd_disk);
        ddev_lbs = bdev_logical_block_size(dev->ddev);
        ddev_pbs = bdev_physical_block_size(dev->ddev);
        printk_i("data disk (%d:%d)\n", ddev_major, ddev_minor);
        printk_i("data disk size %llu\n", dev->ddev_size);
        printk_i("data logical sector size %u\n", ddev_lbs);
        printk_i("data physical sector size %u\n", ddev_pbs);

        /* Check compatibility of log device and data devic. */
        if (ldev_lbs != ddev_lbs || ldev_pbs != ddev_pbs) {
                printk_e("Sector size of data and log must be same.\n");
                goto out_ldev;
        }
        dev->logical_bs = ldev_lbs;
        dev->physical_bs = ldev_pbs;
	dev->size = dev->ddev_size * (u64)dev->logical_bs;

        /* Load log device metadata. */
        if (walb_ldev_init(dev) != 0) {
                printk_e("ldev init failed.\n");
                goto out_ldev;
        }
        
	/*
	 * The I/O queue, depending on whether we are using our own
	 * make_request function or not.
	 */
	switch (request_mode) {
        case RM_NOQUEUE:
		dev->queue = blk_alloc_queue(GFP_KERNEL);
		if (dev->queue == NULL)
			goto out_ddev;
		blk_queue_make_request(dev->queue, walb_make_request);
		break;

        case RM_FULL:
		dev->queue = blk_init_queue(walb_full_request2, &dev->lock);
		if (dev->queue == NULL)
			goto out_ddev;
                if (elevator_change(dev->queue, "noop"))
                        goto out_queue;
		break;

        default:
		printk_i("Bad request mode %d, using simple\n", request_mode);
                BUG();
	}
	blk_queue_logical_block_size(dev->queue, dev->logical_bs);
	blk_queue_physical_block_size(dev->queue, dev->physical_bs);
	dev->queue->queuedata = dev;
	/*
	 * And the gendisk structure.
	 */
	/* dev->gd = alloc_disk(WALB_MINORS); */
        dev->gd = alloc_disk(1);
	if (! dev->gd) {
		printk_n("alloc_disk failure\n");
		goto out_queue;
	}
	dev->gd->major = walb_major;
	dev->gd->first_minor = which * WALB_MINORS;
        dev->devt = MKDEV(dev->gd->major, dev->gd->first_minor);
	dev->gd->fops = &walb_ops;
	dev->gd->queue = dev->queue;
	dev->gd->private_data = dev;
	snprintf (dev->gd->disk_name, 32, "walb%d", which);
	set_capacity(dev->gd, dev->ddev_size);
	add_disk(dev->gd);
	return 0;

out_queue:
        if (dev->queue) {
                if (request_mode == RM_NOQUEUE)
                        kobject_put(&dev->queue->kobj);
                else
                        blk_cleanup_queue(dev->queue);
        }
out_ddev:
        if (dev->ddev) {
                walb_unlock_bdev(dev->ddev);
        }
out_ldev:
        if (dev->ldev) {
                walb_unlock_bdev(dev->ldev);
        }
        return -1;
}



static int __init walb_init(void)
{
        int ret = 0;
	int i;
	/*
	 * Get registered.
	 */
	walb_major = register_blkdev(walb_major, "walb");
	if (walb_major <= 0) {
		printk_w("unable to get major number\n");
		return -EBUSY;
	}
        printk_i("walb_start with major id %d\n", walb_major);
        
	/*
	 * Allocate the device array, and initialize each one.
	 */
	Devices = kmalloc(ndevices*sizeof (struct walb_dev), GFP_KERNEL);
	if (Devices == NULL)
		goto out_unregister;
	for (i = 0; i < ndevices; i++) {
                ret = setup_device(Devices + i, i);
        }
        if (ret) {
                printk_e("setup_device failed\n");
                goto out_unregister;
        }
    
	return 0;

out_unregister:
	unregister_blkdev(walb_major, "walb");
	return -ENOMEM;
}

static void walb_exit(void)
{
	int i;

	for (i = 0; i < ndevices; i++) {
		struct walb_dev *dev = Devices + i;

		if (dev->gd) {
			del_gendisk(dev->gd);
			put_disk(dev->gd);
		}
		if (dev->queue) {
			if (request_mode == RM_NOQUEUE)
                                kobject_put(&dev->queue->kobj);
			else
				blk_cleanup_queue(dev->queue);
		}
                if (dev->ddev)
                        walb_unlock_bdev(dev->ddev);
                if (dev->ldev)
                        walb_unlock_bdev(dev->ldev);

                kfree(dev->lsuper0);

                printk_i("walb stop (wrap %d:%d log %d:%d data %d:%d)\n",
                         MAJOR(dev->devt),
                         MINOR(dev->devt),
                         MAJOR(dev->ldev->bd_dev),
                         MINOR(dev->ldev->bd_dev),
                         MAJOR(dev->ddev->bd_dev),
                         MINOR(dev->ddev->bd_dev));
	}
	unregister_blkdev(walb_major, "walb");
	kfree(Devices);

        printk_i("walb exit.\n");
}
	
module_init(walb_init);
module_exit(walb_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Block-level WAL");
MODULE_ALIAS("walb");
/* MODULE_ALIAS_BLOCKDEV_MAJOR(WALB_MAJOR); */

