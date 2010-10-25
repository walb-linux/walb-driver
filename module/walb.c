/*
 * walb.c - Block-level WAL
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * Written by: Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 */

#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include <linux/sched.h>
#include <linux/kernel.h>	/* printk() */
#include <linux/slab.h>		/* kmalloc() */
#include <linux/fs.h>		/* everything... */
#include <linux/errno.h>	/* error codes */
#include <linux/timer.h>
#include <linux/types.h>	/* size_t */
#include <linux/fcntl.h>	/* O_ACCMODE */
#include <linux/hdreg.h>	/* HDIO_GETGEO */
#include <linux/kdev_t.h>
#include <linux/vmalloc.h>
#include <linux/genhd.h>
#include <linux/blkdev.h>
#include <linux/buffer_head.h>	/* invalidate_bdev */
#include <linux/bio.h>

#include "walb.h"

static int walb_major = 0;
module_param(walb_major, int, 0);
static int hardsect_size = 512;
module_param(hardsect_size, int, 0);
static int nsectors = 1024;	/* How big the drive is */
/* module_param(nsectors, int, 0); */
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
        printk(KERN_ERR "walb: bd_claim error %s.\n", __bdevname(dev, b));
        blkdev_put(bdev, FMODE_READ|FMODE_WRITE);
        return err;
open_err:
        printk(KERN_ERR "walb: open error %s.\n", __bdevname(dev, b));
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
	unsigned long offset = sector*KERNEL_SECTOR_SIZE;

	if ((offset + nbytes) > dev->size) {
		printk (KERN_NOTICE "Beyond-end write (%ld %ld)\n", offset, nbytes);
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
		sector += bio_cur_bytes(bio) / KERNEL_SECTOR_SIZE;
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
        sector += bio_cur_bytes(bio) / KERNEL_SECTOR_SIZE;
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
                        nsect += bio->bi_size/KERNEL_SECTOR_SIZE;
                }
        } else {
                rq_for_each_segment(bvec, req, iter) {
                        walb_xfer_segment(dev, &iter);
                        nsect += bio_iovec_idx(iter.bio, iter.i)->bv_len
                                / KERNEL_SECTOR_SIZE;
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
			printk (KERN_NOTICE "Skip non-fs request\n");
			__blk_end_request_all(req, -EIO);
			continue;
		}
		sectors_xferred = walb_xfer_request(dev, req);
                
                error = (sectors_xferred * KERNEL_SECTOR_SIZE ==
                         blk_rq_bytes(req)) ? 0 : -EIO;
                __blk_end_request_all(req, error);
	}
}

/**
 *
 */
/* static walb_ddev_bio* walb_make_ddev_bio(bio) */
/* { */


/* } */


/**
 *
 */
static void walb_submit_all_bios(struct walb_ddev_bio bios)
{
        


        
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
        
        printk(KERN_INFO "walb: ddev_end_io() called\n");
        
        BUG_ON(! dbio);
        head = dbio->head;
        BUG_ON(! head);
        
        if (error || ! test_bit(BIO_UPTODATE, &bio->bi_flags)) {
                printk(KERN_ERR "walb: IO failed error=%d, uptodate=%d\n",
                       error, test_bit(BIO_UPTODATE, &bio->bi_flags));
                
                dbio->status = WALB_BIO_ERROR;
        }

        dbio->status = WALB_BIO_END;
        bio_put(bio);
        dbio->bio = NULL;

        /* Check whether it's the last bio in the request finished
           or error or not finished. */
        list_for_each_entry_safe(tmp, next, head, list) {

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
                default: BUG();
                }

                if (is_err) {
                        break;
                }
        }

        /* Finalize the request of wrapper device. */
        if (is_last) {
                if (is_err) {
                        printk(KERN_INFO "walb blk_end_request_all() -EIO\n");
                        blk_end_request_all(req, -EIO);
                } else {
                        printk(KERN_INFO "walb blk_end_request_all() 0\n");
                        blk_end_request_all(req, 0);
                }

                list_for_each_entry_safe(tmp, next, head, list) {
                        BUG_ON(tmp->bio != NULL);
                        BUG_ON(tmp->status == WALB_BIO_INIT);
                        list_del(&tmp->list);
                        kfree(tmp);
                }
        }

        
        wq = container_of(head, struct walb_submit_bio_work, list);
        BUG_ON(! wq);
        kfree(wq);
        
        printk(KERN_INFO "walb: ddev_end_io() end\n");
}


/**
 * Task to call submit_bio in a process context.
 */
static void walb_submit_bio_task(struct work_struct *work)
{
        struct walb_submit_bio_work *wq;
        struct walb_ddev_bio *dbio, *next;

        printk(KERN_INFO "walb: submit_bio_task begin\n");
        
        wq = container_of(work, struct walb_submit_bio_work, work);
        BUG_ON(! wq);
        
        if (list_empty(&wq->list)) {
                printk(KERN_WARNING "walb: list is empty\n");
        }
        
        list_for_each_entry_safe(dbio, next, &wq->list, list) {

                printk(KERN_INFO "walb: submit_bio_task\n");
                submit_bio(dbio->bio->bi_rw, dbio->bio);
        }

        printk(KERN_INFO "walb: submit_bio_task end\n");
}


/**
 * Convert request for data device.
 * @blk_start_request() has been already called.
 *
 * @req request of the wrapper block device.
 */
static void walb_make_ddev_request(struct walb_dev *wdev, struct request *req)
{
        struct bio *bio;
        struct walb_ddev_bio *dbio;
        int bio_nr = 0;
        struct walb_submit_bio_work *wq;

        printk(KERN_INFO "walb: make_ddev_request() called\n");

        wq = kmalloc(sizeof(struct walb_submit_bio_work), GFP_NOIO);
        if (! wq) { goto out; }
        INIT_LIST_HEAD(&wq->list);
        
        __rq_for_each_bio(bio, req) {

                dbio = kmalloc(sizeof(struct walb_ddev_bio), GFP_NOIO);
                if (! dbio) { goto out; }
                
                walb_init_ddev_bio(dbio);
                dbio->bio = bio_clone(bio, GFP_NOIO);
                dbio->bio->bi_bdev = wdev->ddev;
                dbio->bio->bi_end_io = walb_ddev_end_io;
                dbio->bio->bi_private = dbio;
                dbio->req = req;
                dbio->status = WALB_BIO_INIT;
                dbio->head = &wq->list;
                
                list_add_tail(&dbio->list, &wq->list);
                bio_nr ++;
        }

        printk(KERN_INFO "bio_nr: %d\n", bio_nr);

        if (list_empty(&wq->list)) {
                printk(KERN_WARNING "walb: list empty.\n");
        }
        
        INIT_WORK(&wq->work, walb_submit_bio_task);
        schedule_work(&wq->work);
        
        /* Make bios corresponding to the given request. */
        /* ddev_bio = walb_make_ddev_bios(req); */
        
        /* Submit all the bios. */
        /* walb_submit_all_bios(ddev_bio); */
        
        printk(KERN_INFO "walb: make_ddev_request() end\n");
        
        return;
        
out:
        printk(KERN_ERR "walb: make_ddev_request failed\n");
        __blk_end_request_all(req, -EIO);
}


/**
 * Work as a just wrapper of the underlying data device.
 */
static void walb_full_request2(struct request_queue *q)
{
        struct request *req;
        struct walb_dev *wdev = q->queuedata;

        while ((req = blk_peek_request(q)) != NULL) {

                blk_start_request(req);
                if (req->cmd_type != REQ_TYPE_FS) {
			printk (KERN_NOTICE "walb: skip non-fs request.\n");
                        __blk_end_request_all(req, -EIO);
                        continue;
                }

                walb_make_ddev_request(wdev, req);
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

	del_timer_sync(&dev->timer);
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

	if (!dev->users) {
		dev->timer.expires = jiffies + INVALIDATE_DELAY;
		add_timer(&dev->timer);
	}
	spin_unlock(&dev->lock);

	return 0;
}

/*
 * Look for a (simulated) media change.
 */
int walb_media_changed(struct gendisk *gd)
{
	struct walb_dev *dev = gd->private_data;
	
	return dev->media_change;
}

/*
 * Revalidate.  WE DO NOT TAKE THE LOCK HERE, for fear of deadlocking
 * with open.  That needs to be reevaluated.
 */
int walb_revalidate(struct gendisk *gd)
{
	struct walb_dev *dev = gd->private_data;
	
	if (dev->media_change) {
		dev->media_change = 0;
		memset (dev->data, 0, dev->size);
	}
	return 0;
}

/*
 * The "invalidate" function runs out of the device timer; it sets
 * a flag to simulate the removal of the media.
 */
void walb_invalidate(unsigned long ldev)
{
	struct walb_dev *dev = (struct walb_dev *) ldev;

	spin_lock(&dev->lock);
	if (dev->users || !dev->data) 
		printk (KERN_WARNING "walb: timer sanity check failed\n");
	else
		dev->media_change = 1;
	spin_unlock(&dev->lock);
}

/*
 * The ioctl() implementation
 */

int walb_ioctl (struct block_device *bdev, fmode_t mode,
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
		size = dev->size*(hardsect_size/KERNEL_SECTOR_SIZE);
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
	/* .media_changed   = walb_media_changed, */
	.revalidate_disk = walb_revalidate,
	.ioctl	         = walb_ioctl
};


/*
 * Set up our internal device.
 *
 * @return 0 in success, or -1.
 */
static int setup_device(struct walb_dev *dev, int which)
{
	/*
	 * Get some memory.
	 */
	memset (dev, 0, sizeof (struct walb_dev));
	dev->size = nsectors*hardsect_size;
	dev->data = vmalloc(dev->size);
	if (dev->data == NULL) {
		printk (KERN_NOTICE "walb: vmalloc failure.\n");
		return -1;
	}
	spin_lock_init(&dev->lock);
	
	/*
	 * The timer which "invalidates" the device.
	 */
	init_timer(&dev->timer);
	dev->timer.data = (unsigned long) dev;
	dev->timer.function = walb_invalidate;

        /*
         * Setup underlying data device.
         */
        dev->devt = MKDEV(ddev_major, ddev_minor);
        if (walb_lock_bdev(&dev->ddev, dev->devt) != 0) {
                printk(KERN_ERR "walb: walb_lock_bdev failed\n");
                goto out_vfree;
        }
        nsectors = get_capacity(dev->ddev->bd_disk);
        printk(KERN_INFO "walb: underlying disk size %d\n", nsectors);

	/*
	 * The I/O queue, depending on whether we are using our own
	 * make_request function or not.
	 */
	switch (request_mode) {
        case RM_NOQUEUE:
		dev->queue = blk_alloc_queue(GFP_KERNEL);
		if (dev->queue == NULL)
			goto out_blkdev;
		blk_queue_make_request(dev->queue, walb_make_request);
		break;

        case RM_FULL:
		dev->queue = blk_init_queue(walb_full_request2, &dev->lock);
		if (dev->queue == NULL)
			goto out_blkdev;
                if (elevator_change(dev->queue, "noop"))
                        goto out_queue;
		break;

        default:
		printk(KERN_NOTICE "Bad request mode %d, using simple\n", request_mode);
        	/* fall into.. */
	}
	blk_queue_logical_block_size(dev->queue, hardsect_size);
	blk_queue_physical_block_size(dev->queue, hardsect_size);
	dev->queue->queuedata = dev;
	/*
	 * And the gendisk structure.
	 */
	/* dev->gd = alloc_disk(WALB_MINORS); */
        dev->gd = alloc_disk(1);
	if (! dev->gd) {
		printk (KERN_NOTICE "walb: alloc_disk failure\n");
		goto out_queue;
	}
	dev->gd->major = walb_major;
	dev->gd->first_minor = which * WALB_MINORS;
	dev->gd->fops = &walb_ops;
	dev->gd->queue = dev->queue;
	dev->gd->private_data = dev;
	snprintf (dev->gd->disk_name, 32, "walb_%c", which + 'a');
	set_capacity(dev->gd, nsectors*(hardsect_size/KERNEL_SECTOR_SIZE));
	add_disk(dev->gd);
	return 0;

out_queue:
        if (dev->queue) {
                if (request_mode == RM_NOQUEUE)
                        kobject_put(&dev->queue->kobj);
                else
                        blk_cleanup_queue(dev->queue);
        }
out_blkdev:
        if (dev->ddev) {
                walb_unlock_bdev(dev->ddev);
        }
out_vfree:
	if (dev->data) {
		vfree(dev->data);
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
		printk(KERN_WARNING "walb: unable to get major number\n");
		return -EBUSY;
	}
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
                printk(KERN_ERR "walb: setup_device failed\n");
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

		del_timer_sync(&dev->timer);
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
		if (dev->data)
			vfree(dev->data);
	}
	unregister_blkdev(walb_major, "walb");
	kfree(Devices);
}
	
module_init(walb_init);
module_exit(walb_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Block-level WAL");
MODULE_ALIAS("walb");
/* MODULE_ALIAS_BLOCKDEV_MAJOR(WALB_MAJOR); */

