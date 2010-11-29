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
#include "../include/bitmap.h"

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

/* Prototypes */
static void* walb_alloc_sector(struct walb_dev *dev, gfp_t gfp_mask);
static void walb_free_sector(void *p);
static int walb_rq_count_bio(struct request *req);


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
        unsigned long irq_flags;

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
        spin_lock_irqsave(&wq->lock, irq_flags);
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
        spin_unlock_irqrestore(&wq->lock, irq_flags);

        /* Finalize the request of wrapper device. */
        if (is_last) {
                if (is_err) {
                        printk_d("walb blk_end_request_all() -EIO\n");
                        blk_end_request_all(req, -EIO);
                } else {
                        printk_d("walb blk_end_request_all() 0\n");
                        blk_end_request_all(req, 0);
                }

                spin_lock_irqsave(&wq->lock, irq_flags);
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
                spin_unlock_irqrestore(&wq->lock, irq_flags);

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
 * Debug print of logpack header.
 *
 */
static void walb_logpack_header_print(const char *level,
                                      struct walb_logpack_header *lhead)
{
        int i;
        printk("%s*****logpack header*****\n"
               "checksum: %08x\n"
               "n_records: %u\n"
               "total_io_size: %u\n",
               level,
               lhead->checksum,
               lhead->n_records,
               lhead->total_io_size);
        for (i = 0; i < lhead->n_records; i ++) {
                printk("%srecord %d\n"
                       "  checksum: %08x\n"
                       "  lsid: %llu\n"
                       "  lsid_local: %u\n"
                       "  io_size: %u\n"
                       "  is_exist: %u\n"
                       "  offset: %llu\n",
                       level, i,
                       lhead->record[i].checksum,
                       lhead->record[i].lsid,
                       lhead->record[i].lsid_local,
                       lhead->record[i].io_size,
                       lhead->record[i].is_exist,
                       lhead->record[i].offset);
                printk("%slogpack lsid: %llu\n", level,
                       lhead->record[i].lsid - lhead->record[i].lsid_local);
        }
}


/**
 * Add record to log pack.
 *
 * @lhead header of log pack.
 * @logpack_lsid lsid of the log pack.
 * @reqp_ary requests to add.
 * @n_req number of requests.
 * @n_lb_in_pb number of logical blocks in a physical block
 *
 * @return physical sectors of the log pack in success, or -1.
 */
static int walb_logpack_header_fill(struct walb_logpack_header *lhead,
                                   u64 logpack_lsid,
                                   struct request** reqp_ary, int n_req,
                                   int n_lb_in_pb)
{
        struct request* req;
        int total_lb, req_lb, req_padding_lb, i, logpack_size;

        printk_d("walb_logpack_header_fill begin\n");

        printk_d("logpack_lsid %llu n_req %d n_lb_in_pb %d\n",
                 logpack_lsid, n_req, n_lb_in_pb);
        
        total_lb = 0;
        for (i = 0; i < n_req; i ++) {
                req = reqp_ary[i];
                req_lb = blk_rq_sectors(req);
                lhead->record[i].io_size = req_lb;

                /* Calc size of padding logical sectors
                   for physical sector alignment. */
                if (req_lb % n_lb_in_pb == 0) {
                        req_padding_lb = req_lb;
                } else {
                        req_padding_lb = ((req_lb / n_lb_in_pb) + 1) * n_lb_in_pb;
                }
                
                /* set lsid_local and io_size and offset. */
                lhead->record[i].offset = blk_rq_pos(req);
                lhead->record[i].is_exist = 1;
                lhead->record[i].lsid_local = total_lb / n_lb_in_pb + 1;
                lhead->record[i].lsid = logpack_lsid + lhead->record[i].lsid_local;
                
                total_lb += req_padding_lb;
                ASSERT(total_lb % n_lb_in_pb == 0);
        }
        lhead->n_records = n_req;
        ASSERT(total_lb % n_lb_in_pb == 0);
        printk_d("total_lb: %d\n", total_lb); /* debug */
        lhead->total_io_size = total_lb / n_lb_in_pb;
        lhead->logpack_lsid = logpack_lsid;
        
        logpack_size = lhead->total_io_size + 1;

        printk_d("walb_logpack_header_fill end\n");
        return logpack_size;
}


/**
 * Count number of bio(s) in the request.
 *
 * @req request
 *
 * @return number of bio(s).
 */
static int walb_rq_count_bio(struct request *req)
{
        struct bio *bio;
        int n = 0;

        __rq_for_each_bio(bio, req) {
                n ++;
        }
        return n;
}

/**
 * Create logpack request entry and its substructure.
 *
 * @logpack completed log pack.
 * @idx idx of the request in the log pack.
 *
 * @return Pointer to created entry, or NULL.
 *         This must be destroyed with @walb_destroy_logpack_request_entry().
 */
static struct walb_logpack_request_entry*
walb_create_logpack_request_entry(
        struct walb_logpack_entry *logpack_entry,
        struct walb_logpack_header *logpack,
        int idx)
{
        struct walb_logpack_request_entry *entry;
        /* int n_bio; */

        printk_d("walb_create_logpack_request_entry begin\n");
        
        ASSERT(idx < logpack->n_records);
        entry = kmalloc(sizeof(struct walb_logpack_request_entry), GFP_NOIO);
        if (entry == NULL) { goto error0; }

        entry->head = &logpack_entry->req_list;
        entry->logpack_entry = logpack_entry;
        entry->req_orig = logpack_entry->reqp_ary[idx];
        /* spin_lock_init(&entry->bmp_lock); */

        /* n_bio = walb_rq_count_bio(entry->req_orig); */

        /* entry->io_submitted_bmp = walb_bitmap_create(n_bio, GFP_NOIO); */
        /* if (entry->io_submitted_bmp == NULL) { goto error1; } */
        /* walb_bitmap_clear(entry->io_submitted_bmp); */
        
        /* entry->io_end_bmp = walb_bitmap_create(n_bio, GFP_NOIO); */
        /* if (entry->io_end_bmp == NULL) { goto error2; } */
        /* walb_bitmap_clear(entry->io_end_bmp); */
        
        /* entry->io_success_bmp = walb_bitmap_create(n_bio, GFP_NOIO); */
        /* if (entry->io_success_bmp == NULL) { goto error3; } */
        /* walb_bitmap_clear(entry->io_success_bmp); */

        INIT_LIST_HEAD(&entry->bioc_list);
        
        printk_d("walb_create_logpack_request_entry end\n");
        return entry;
        
/* error3: */
/*         walb_bitmap_free(entry->io_end_bmp); */
/* error2: */
/*         walb_bitmap_free(entry->io_submitted_bmp); */
/* error1: */
/*         kfree(entry); */
error0:
        return NULL;
}


/**
 * Destroy logpack request entry.
 *
 * @entry logpack request entry to destroy (and deallocated).
 */
static void walb_destroy_logpack_request_entry(
        struct walb_logpack_request_entry *entry)
{
        ASSERT(entry != NULL);
        
        /* walb_bitmap_free(entry->io_success_bmp); */
        /* walb_bitmap_free(entry->io_end_bmp); */
        /* walb_bitmap_free(entry->io_submitted_bmp); */

        ASSERT(list_empty(&entry->bioc_list));
        kfree(entry);
}


/**
 * Create logpack entry and its substructure.
 *
 * @wdev walb device
 * @logpack completed logpack data.
 * 
 * @return Pointer to created logpack entry or NULL.
 *         This must be destroyed with @walb_destroy_logpack_entry().
 *
 */
static struct walb_logpack_entry* walb_create_logpack_entry(
        struct walb_dev *wdev,
        struct walb_logpack_header *logpack,
        struct request** reqp_ary
        )
{
        struct walb_logpack_entry *entry;
        struct walb_logpack_request_entry *req_entry, *tmp_req_entry;
        int i;


        printk_d("walb_create_logpack_entry begin\n");
        
        /*
         * Allocate and initialize logpack entry.
         */
        entry = kmalloc(sizeof(struct walb_logpack_entry), GFP_NOIO);
        if (entry == NULL) { goto error0; }

        entry->head = &wdev->logpack_list;
        entry->wdev = wdev;
        entry->logpack = logpack;
        INIT_LIST_HEAD(&entry->req_list);
        entry->reqp_ary = reqp_ary;

        /*
         * Create request entry and add tail to the request entry list.
         */
        for (i = 0; i < logpack->n_records; i ++) {
                req_entry = walb_create_logpack_request_entry(entry, logpack, i);
                if (req_entry == NULL) { goto error1; }
                list_add_tail(&req_entry->list, &entry->req_list);
        }

        printk_d("walb_create_logpack_entry end\n");
        return entry;

error1:
        list_for_each_entry_safe(req_entry, tmp_req_entry, &entry->req_list, list) {
                list_del(&req_entry->list);
                walb_destroy_logpack_request_entry(req_entry);
        }
        kfree(entry);
error0:
        return NULL;
}

/**
 * Destory logpack entry and its substructure created by
 * @walb_create_logpack_entry().
 *
 * @entry logpack entry. Call this after deleted from wdev->logpack_list.
 */
static void walb_destroy_logpack_entry(struct walb_logpack_entry *entry)
{
        struct walb_logpack_request_entry *req_entry, *tmp_req_entry;

        ASSERT(entry != NULL);
        
        list_for_each_entry_safe(req_entry, tmp_req_entry, &entry->req_list, list) {
                list_del(&req_entry->list);
                walb_destroy_logpack_request_entry(req_entry);
        }
        kfree(entry);
}


/**
 * End io callback of bio of logpack header write.
 *
 * @bio completed bio, bio->bi_private must be (struct walb_logpack_header_bio)
 */
#if 0
static void walb_logpack_header_write_end_io_callback(struct bio *bio, int error0)
{

        /* now editing */
}
#endif

/**
 * End_io callback of bio related to logpack write (except logpack header)
 *
 * @bio completed bio, bio->bi_private must be (struct walb_logpack_bio).
 */
#if 0
static void walb_logpack_request_write_end_io_callback(struct bio *bio, int error)
{
        struct walb_logpack_bio *lbio;

        lbio = bio->bi_private;
        
        ASSERT(lbio->status == WALB_BIO_INIT);

        if (error || ! test_bit(BIO_UPTODATE, &bio->bi_flags)) {
                printk_e("walb_logpack_request_write_end_io_callback: "
                         " error %d bi_flags %lu\n",
                         error, bio->bi_flags);
                lbio->status = WALB_BIO_ERROR;
        } else {
                lbio->status = WALB_BIO_END;
        }

        
        /* now editing */



        
        kfree(lbio);
        bio_put(bio);
}
#endif

/**
 * Call end_request and 
 * 
 * @logpack_entry logpack entry.
 *                must be deleted from wdev->logpack_entry_list,
 *                all related bio(s) for log device are all done.
 *
 */
#if 0
static void walb_logpack_write_fail(struct walb_logpack_entry *logpack_entry)
{
        int is_submitted_header, is_end_header, is_success_header;
        
        ASSERT(logpack_entry != NULL);
        is_submitted_header = atomic_read(&logpack_entry->is_submitted_header);        
        is_end_header = atomic_read(&logpack_entry->is_submitted_header);        
        is_success_header = atomic_read(&logpack_entry->is_submitted_header);

        /* Check all bio(s) are done */
        /* now editing */
        
        /* Free all resources */
        walb_destroy_logpack_entry(logpack_entry);
}
#endif

/**
 * Clone and submit given bio of the request entry.
 *
 * @req_entry request entry.
 * @bio bio to clone and submit to log device.
 * @idx index of the bio inside the request.a
 * @bio_offset offset of the bio inside the whole request [logical block].
 *
 * @return walb_bio_with_completion or NULL.
 */
static struct walb_bio_with_completion* walb_submit_logpack_bio_to_ldev
(struct walb_logpack_request_entry *req_entry, struct bio *bio, int idx, int bio_offset)
{
        struct request *req;
        /* unsigned long irq_flags; */
        struct walb_bio_with_completion *bioc;
        struct bio *cbio;
        struct walb_dev *wdev;
        u64 off_pb, off_lb;
        /* u64 logpack_lsid; */
        /* struct walb_logpack_request_entry *req_entry1, *req_entry2; */
        /* struct walb_logpack_entry *logpack_entry2; */

        req = req_entry->req_orig;
        wdev = req_entry->logpack_entry->wdev;

        bioc = kmalloc(sizeof(*bioc), GFP_NOIO);
        if (bioc == NULL) {
                printk_e("kmalloc failed\n");
                goto error0;
        }
        init_completion(&bioc->wait);
        bioc->status = WALB_BIO_INIT;
        
        cbio = bio_clone(bio, GFP_NOIO);
        if (bio == NULL) {
                printk_e("bio_clone() failed\n");
                goto error1;
        }
        cbio->bi_bdev = wdev->ldev;
        cbio->bi_end_io = walb_end_io_with_completion;
        cbio->bi_private = bioc;

        /* wdev should have copy of ring buffer offset
           not to lock lsuper0. */
        spin_lock(&wdev->lsuper0_lock);
        off_pb = get_offset_of_lsid_2(wdev->lsuper0,
                                      req_entry->logpack_entry->logpack->record[idx].lsid);
        spin_unlock(&wdev->lsuper0_lock);
        off_lb = off_pb * (wdev->physical_bs / wdev->logical_bs);
        cbio->bi_sector = off_lb + bio_offset;
        
        bioc->bio = cbio;
                        
        ASSERT(cbio->bi_rw & WRITE);
        submit_bio(cbio->bi_rw, cbio);

        return bioc;

error1:
        kfree(bioc);
error0:
        return NULL;

#if 0
        /* Below code will executed after returned? */
        
        /* Failed clone or submit. */
        logpack_lsid = req_entry->logpack_entry->logpack->record[0].lsid - 
                req_entry->logpack_entry->logpack->record[0].lsid_local;

        spin_lock_irqsave(&req_entry->bmp_lock, irq_flags);
        walb_bitmap_on(req_entry->io_end_bmp, idx);
        walb_bitmap_off(req_entry->io_succeeded_bmp, idx);
        spin_unlock_irqrestore(&req_entry->bmp_lock, irq_flags);

        
        /* Check all bio(s) is not submitted and all bio(s) end */
        spin_lock_irqsave(&wdev->logpack_list_lock, irq_flags);
        logpack_entry2 = walb_search_logpack_list(wdev, lsid);

        if (logpack_entry2 != NULL) {
                /* If null, logpack is already freed by end_io callback. */

                is_all_failed = 1;
                list_for_each_entry_safe(req_entry1, req_entry2, 
                                         req_entry->logpack_entry->req_list,
                                         list) {
                        if (walb_bitmap_is_any_off(req_entry1->io_end_bmp) ||
                            walb_bitmap_is_any_on(req_entry1->io_submitted_bmp)) {

                                is_all_failed = 0;
                                break;
                        }
                }
                if (is_all_failed) {

                        printk_e("walb_submit_logpack_bio_to_ldev:"
                                 " all bio failed\n");

                        /* Read only mode */
                        atomic_set(&wdev->is_read_only, 1);

                        list_del(logpack_entry2);
                        walb_logpack_write_fail(logpack_entry);
                        
                        /* now editing */
                }

        }
        spin_unlock_irqrestore(&wdev->logpack_list_lock, irq_flags);


        /* blk_end_request_all(wk->reqp_ary[i], -EIO); */

        
        /* now editing */
#endif        
}


/**
 * Clone each bio in the logpack request entry
 * for log device write and submit it.
 *
 * @req_entry logpack request entry.
 *
 * @return 0 in success, or -1.
 */
static int walb_submit_logpack_request_to_ldev
(struct walb_logpack_request_entry *req_entry)
{
        struct bio *bio;
        int idx;
        int off_lb;
        struct request* req;
        /* unsigned long irq_flags; */
        int lbs = req_entry->logpack_entry->wdev->logical_bs;
        struct walb_bio_with_completion *bioc;
        
        ASSERT(req_entry != NULL);
        req = req_entry->req_orig;

        idx = 0;
        off_lb = 0;
        __rq_for_each_bio(bio, req) {

                bioc = walb_submit_logpack_bio_to_ldev(req_entry, bio, idx, off_lb);
                if (bioc) {
                        list_add_tail(&bioc->list, &req_entry->bioc_list);
                } else {
                        printk_e("walb_submit_logpack_bio_to_ldev() faild\n");
                        goto error0;
                }
                idx ++;
                ASSERT(bio->bi_size % lbs == 0);
                off_lb += bio->bi_size / lbs;
        }

        return 0;
error0:
        return -1;
}

/**
 * Clone each bio in the logpack entry for log device write and submit it.
 *
 * @logpack_entry logpack entry to issue.
 * 
 * @return 0 in success, or -1.
 */
static int walb_submit_logpack_to_ldev(struct walb_logpack_entry* logpack_entry)
{
        int n_req, i, is_fail;
        struct bio *bio;
        struct walb_logpack_header *logpack;
        struct walb_logpack_request_entry *req_entry, *tmp_req_entry;
        struct page *page;
        u64 logpack_lsid, off_pb, off_lb;
        u32 pbs, lbs;
        struct walb_bio_with_completion *bioc, *tmp_bioc;
        struct walb_dev *wdev;

        printk_d("walb_submit_logpack_to_ldev begin\n");
        
        ASSERT(logpack_entry != NULL);

        logpack = logpack_entry->logpack;
        n_req = logpack->n_records;
        logpack_lsid = logpack_entry->logpack->logpack_lsid;
        ASSERT(logpack_lsid ==
               logpack_entry->logpack->record[0].lsid -
               logpack_entry->logpack->record[0].lsid_local);
        wdev = logpack_entry->wdev;
        lbs = wdev->logical_bs;
        pbs = wdev->physical_bs;

        /* Create bio for logpack header and submit. */
        bioc = kmalloc(sizeof(struct walb_bio_with_completion), GFP_NOIO);
        if (! bioc) { goto error0; }
        init_completion(&bioc->wait);
        bioc->status = WALB_BIO_INIT;
        
        bio = bio_alloc(GFP_NOIO, 1);
        if (! bio) { goto error1; }

        /* This is bad for non-64bit architecture.
           Use map() and unmap(). */
        ASSERT(virt_addr_valid(logpack_entry->logpack));
        page = virt_to_page(logpack_entry->logpack);

        bio->bi_bdev = wdev->ldev;
        
        /* We should use lock free data structure and algorithm. */
        spin_lock(&wdev->lsuper0_lock);
        off_pb = get_offset_of_lsid_2(wdev->lsuper0, logpack_lsid);
        spin_unlock(&wdev->lsuper0_lock);
        off_lb = off_pb * (pbs / lbs);
        bio->bi_sector = off_lb;

        bio->bi_end_io = walb_end_io_with_completion;
        bio->bi_private = bioc;
        bio_add_page(bio, page, pbs, offset_in_page(logpack_entry->logpack));
        bioc->bio = bio;

        submit_bio(WRITE, bio);

        /* Clone bio and submit for each bio of each request. */
        is_fail = 0;
        i = 0;
        list_for_each_entry_safe(req_entry, tmp_req_entry,
                                 &logpack_entry->req_list, list) {

                if (walb_submit_logpack_request_to_ldev(req_entry) != 0) {
                        printk_e("walb_submit_logpack_request_to_ldev() failed\n");
                        is_fail = 1;
                }
                i ++;
        }

        
        /*
         * Wait for completion of all bio(s).
         */

        /* Wait completion of logpack header IO. */
        wait_for_completion(&bioc->wait);
        bio_put(bioc->bio);
        kfree(bioc);

        /* for each request entry */
        list_for_each_entry_safe(req_entry, tmp_req_entry,
                                 &logpack_entry->req_list, list) {
                /* for each bioc */
                list_for_each_entry_safe(bioc, tmp_bioc,
                                         &req_entry->bioc_list, list) {
                        
                        wait_for_completion(&bioc->wait);
                        if (bioc->status != WALB_BIO_END) {
                                is_fail = 1;
                        }
                        bio_put(bioc->bio);
                        list_del(&bioc->list);
                        kfree(bioc);
                }
                ASSERT(list_empty(&req_entry->bioc_list));
        }

        /* Now all bio is done. */

        if (is_fail) {
                for (i = 0; i < n_req; i ++) {
                        blk_end_request_all(logpack_entry->reqp_ary[i], -EIO);
                }
                goto error0;
        }
        
        /*
         * Write requests to data device.
         */
        
        /* now editing */

        /* temporary */
        for (i = 0; i < logpack_entry->logpack->n_records; i ++) {
                blk_end_request_all(logpack_entry->reqp_ary[i], 0);
        }
        

        printk_d("walb_submit_logpack_to_ldev end\n");
        return 0;


/* error2: */
/*         bio_put(bioc->bio); */
error1:
        kfree(bioc);
error0:
        return -1;
}


/**
 * Clone each bio for log pack and write log header and its contents
 * to log device.
 *
 * @wdev walb device (to access logpack_list)
 * @logpack logpack header
 * @reqp_ary request pointer array.
 *
 * @return 0 in success, or -1.
 */
static int walb_logpack_write(struct walb_dev *wdev,
                              struct walb_logpack_header *logpack,
                              struct request** reqp_ary)
{
        struct walb_logpack_entry *logpack_entry;

        printk_d("walb_logpack_write begin\n");
        
        /* Create logpack entry for IO to log device. */
        logpack_entry = walb_create_logpack_entry(wdev, logpack, reqp_ary);
        if (logpack_entry == NULL) { goto error0; }

        /* Alloc/clone related bio(s) and submit them.
           Currently this function waits for end of all bio(s). */
        walb_submit_logpack_to_ldev(logpack_entry);


        /* now editing */
        walb_destroy_logpack_entry(logpack_entry);

        printk_d("walb_logpack_write end\n");
        
        return 0;
        
error0:
        return -1;
}


/**
 * Calc checksum of each requests and log header and set it.
 *
 * @lhead log pack header.
 * @physical_bs physical sector size (allocated size as lhead).
 * @reqp_ary requests to add.
 * @n_req number of requests.
 *
 * @return 0 in success, or -1.
 */
static int walb_logpack_calc_checksum(struct walb_logpack_header *lhead,
                                      int physical_bs,
                                      struct request** reqp_ary, int n_req)
{
        int i;
        struct request *req;
        struct req_iterator iter;
        struct bio_vec *bvec;
        u64 sum;
        
        for (i = 0; i < n_req; i ++) {
                sum = 0;
                req = reqp_ary[i];

                ASSERT(req->cmd_flags & REQ_WRITE);

                rq_for_each_segment(bvec, req, iter) {
                        
                        sum = checksum_partial
                                (sum,
                                 kmap(bvec->bv_page) + bvec->bv_offset,
                                 bvec->bv_len);
                        kunmap(bvec->bv_page);
                }

                lhead->record[i].checksum = checksum_finish(sum);
        }

        ASSERT(lhead->checksum == 0);
        lhead->checksum = checksum((u8 *)lhead, physical_bs);
        ASSERT(checksum((u8 *)lhead, physical_bs) == 0);
        
        return 0;
}

/**
 * Make log pack and submit related bio(s).
 */
static void walb_make_logpack_and_submit_task(struct work_struct *work)
{
        struct walb_make_logpack_work *wk;
        struct walb_logpack_header *lhead;
        /* int i; */
        int logpack_size;
        u64 logpack_lsid;

        wk = container_of(work, struct walb_make_logpack_work, work);

        printk_d("walb_make_logpack_and_submit_task begin\n");
        ASSERT(wk->n_req <= max_n_log_record_in_sector(wk->wdev->physical_bs));

        printk_d("making log pack (n_req %d)\n", wk->n_req);
        
        /*
         * Allocate memory (sector size) for log pack header.
         */
        lhead = walb_alloc_sector(wk->wdev, GFP_NOIO);
        if (lhead == NULL) {
                printk_e("walb_alloc_sector() failed\n");
                goto fin;
        }
        memset(lhead, 0, wk->wdev->physical_bs);

        /*
         * Fill log records for for each request.
         */

        /*
         * 1. Lock latest_lsid_lock.
         * 2. Get latest_lsid 
         * 3. Calc required number of physical blocks for log pack.
         * 4. Set next latest_lsid.
         * 5. Unlock latest_lsid_lock.
         */
        spin_lock(&wk->wdev->latest_lsid_lock);
        logpack_lsid = wk->wdev->latest_lsid;
        logpack_size = walb_logpack_header_fill
                (lhead, logpack_lsid, wk->reqp_ary, wk->n_req,
                 wk->wdev->physical_bs / wk->wdev->logical_bs);
        if (logpack_size < 0) {
                printk_e("walb_logpack_header_fill failed\n");
                spin_unlock(&wk->wdev->latest_lsid_lock);
                goto fin;
        }
        wk->wdev->latest_lsid += logpack_size;
        spin_unlock(&wk->wdev->latest_lsid_lock);

        /* Now log records is filled except checksum.
           Calculate and fill checksum for all requests and
           the logpack header. */
#ifdef WALB_DEBUG
        walb_logpack_header_print(KERN_DEBUG, lhead); /* debug */
#endif
        walb_logpack_calc_checksum(lhead, wk->wdev->physical_bs,
                                   wk->reqp_ary, wk->n_req);
#ifdef WALB_DEBUG
        walb_logpack_header_print(KERN_DEBUG, lhead); /* debug */
#endif
        
        /* Complete log pack header and create its bio. */
        walb_logpack_write(wk->wdev, lhead, wk->reqp_ary);

        /* now editing */        

        /* Clone bio(s) of each request and set offset for log pack. */

        /* Submit prepared bio(s) to log device. */



fin:        
        /* temporarl deallocation */
        /* msleep(100); */
        /* for (i = 0; i < wk->n_req; i ++) { */
        /*         blk_end_request_all(wk->reqp_ary[i], 0); */
        /* } */
        kfree(wk->reqp_ary);
        kfree(wk);
        walb_free_sector(lhead);

        printk_d("walb_make_logpack_and_submit_task end\n");
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

        wk = kmalloc(sizeof(struct walb_make_logpack_work), GFP_ATOMIC);
        if (! wk) { goto error0; }

        wk->reqp_ary = reqp_ary;
        wk->n_req = n_req;
        wk->wdev = wdev;
        INIT_WORK(&wk->work, walb_make_logpack_and_submit_task);
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
        /* printk_d("max_n_req: %d\n", max_n_req); */
        
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

                        printk_d("walb: WRITE\n");
                        
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

                        printk_d("walb: READ\n");
                        walb_make_ddev_request(wdev, req);
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
 * Deallocate sector.
 *
 * This must be used for memory allocated with @walb_alloc_sector().
 */
static void walb_free_sector(void *p)
{
        kfree(p);
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
        ASSERT(virt_addr_valid(buf));
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
 * Sprint uuid.
 *
 * @buf buffer to store result. Its size must be 16 * 2 + 1.
 * @uuid uuid ary. Its size must be 16.
 */
static void walb_sprint_uuid(char *buf, const u8 *uuid)
{
#ifdef WALB_DEBUG
        char tmp[3];
        int i;

        buf[0] = '\0';
        for (i = 0; i < 16; i ++) {
                sprintf(tmp, "%02x", uuid[i]);
                strcat(buf, tmp);
        }
#endif
}

/**
 * Print super sector for debug.
 *
 * @lsuper0 super sector.
 */
static void walb_print_super_sector(walb_super_sector_t *lsuper0)
{
#ifdef WALB_DEBUG
        char uuidstr[16 * 2 + 1];
        walb_sprint_uuid(uuidstr, lsuper0->uuid);
        
        printk_d("-----super block------\n"
                 "checksum %08x\n"
                 "logical_bs %u\n"
                 "physical_bs %u\n"
                 "snapshot_metadata_size %u\n"
                 "uuid: %s\n"         
                 "start_offset %llu\n"
                 "ring_buffer_size %llu\n"
                 "oldest_lsid %llu\n"
                 "written_lsid %llu\n"
                 "device_size %llu\n"
                 "----------\n",
                 lsuper0->checksum,
                 lsuper0->logical_bs,
                 lsuper0->physical_bs,
                 lsuper0->snapshot_metadata_size,
                 uuidstr,
                 lsuper0->start_offset,
                 lsuper0->ring_buffer_size,
                 lsuper0->oldest_lsid,
                 lsuper0->written_lsid,
                 lsuper0->device_size);
#endif
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

#ifdef WALB_DEBUG
        walb_print_super_sector(lsuper0);
#endif
        
        printk_d("walb_read_super_sector end\n");
        return lsuper0;

error1:
        walb_free_sector(lsuper0);
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

        walb_free_sector(lsuper0_tmp);
        /* Do not forget calling kfree(dev->lsuper0)
           before releasing the block device. */

        /* now editing */

        
        /*
         * 2. Redo from written_lsid to avaialble latest lsid.
         *    and set latest_lsid variable.
         */

        /* This feature will be implemented later. */

        
        /*
         * 3. Sync log device super block.
         */

        /* If redo is done, super block should be re-written. */

        
        
        return 0;

error1:
        walb_free_sector(dev->lsuper0);
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
        spin_lock_init(&dev->latest_lsid_lock);
        spin_lock_init(&dev->lsuper0_lock);
        spin_lock_init(&dev->logpack_list_lock);
        INIT_LIST_HEAD(&dev->logpack_list);
        atomic_set(&dev->is_read_only, 0);
	
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
        dev->queue->unplug_thresh = 8; /* will be max requests in a log pack. */
        dev->queue->unplug_delay = msecs_to_jiffies(1); /* 1 ms */
        printk_d("1ms = %d jiffies\n", msecs_to_jiffies(1)); /* debug */
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

