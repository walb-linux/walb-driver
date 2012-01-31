/**
 * io.c - Walb IO operations.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/slab.h>
#include <linux/bio.h>
#include <linux/blkdev.h>

#include "util.h"
#include "walb/sector.h"
#include "walb/bitmap.h"

#include "io.h"

/**
 * Workqueues defined in walb.c.
 */
extern struct workqueue_struct *wqs_;
extern struct workqueue_struct *wqm_;

/*******************************************************************************
 * Global functions.
 *******************************************************************************/

/**
 * Count number of bio(s) in the request.
 *
 * @req request
 *
 * @return number of bio(s).
 */
int walb_rq_count_bio(struct request *req)
{
        struct bio *bio;
        int n = 0;
        ASSERT(req != NULL);
        
        __rq_for_each_bio(bio, req) {
                n ++;
        }
        return n;
}

/**
 * Read/write sector from/to block device.
 * This is blocked operation.
 * Do not call this function in interuption handlers.
 *
 * @rw READ or WRITE (as same as 1st arg of submit_bio(rw, bio).).
 * @bdev block device, which is already opened.
 * @off offset in the block device [physical sector].
 * @sect sector data.
 *
 * @return 0 in success, or -1.
 */
int sector_io(int rw, struct block_device *bdev,
              u64 off, struct sector_data *sect)
{
        struct bio *bio;
        int pbs, lbs;
        struct page *page;
        struct walb_bio_with_completion *bioc;
        u8 *buf;

        LOGd("walb_sector_io begin\n");

        ASSERT(rw == READ || rw == WRITE);
        ASSERT_SECTOR_DATA(sect);
        buf = sect->data;
        ASSERT(buf != NULL);
        
        lbs = bdev_logical_block_size(bdev);
        pbs = bdev_physical_block_size(bdev);
        
        if (sect->size != pbs) {
                LOGe("Sector size is invalid %d %d.\n", sect->size, pbs);
                goto error0;
        }

        bioc = kmalloc(sizeof(struct walb_bio_with_completion), GFP_NOIO);
        if (bioc == NULL) {
                goto error0;
        }
        init_completion(&bioc->wait);
        bioc->status = WALB_BIO_INIT;
        
        /* Alloc bio */
        bio = bio_alloc(GFP_NOIO, 1);
        if (bio == NULL) {
                LOGe("bio_alloc failed.\n");
                goto error1;
        }
        ASSERT(virt_addr_valid(buf));
        page = virt_to_page(buf);

        LOGd("sector %lu "
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
                LOGe("sector io failed.\n");
                goto error2;
        }

        /* Cleanup allocated bio and memory. */
        bio_put(bio);
        kfree(bioc);

        LOGd("walb_sector_io end\n");
        return 0;

error2:
        bio_put(bio);
error1:
        kfree(bioc);
error0:
        return -1;
}

/**
 * End io with completion.
 *
 * bio->bi_private must be (struct walb_bio_with_completion *).
 */
void walb_end_io_with_completion(struct bio *bio, int error)
{
        struct walb_bio_with_completion *bioc;
        bioc = bio->bi_private;

        ASSERT(bioc->status == WALB_BIO_INIT);
        if (error || ! test_bit(BIO_UPTODATE, &bio->bi_flags)) {
                LOGe("walb_end_io_with_completion: error %d bi_flags %lu\n",
                         error, bio->bi_flags);
                bioc->status = WALB_BIO_ERROR;
        } else {
                bioc->status = WALB_BIO_END;
        }
        complete(&bioc->wait);
}

/**
 * Convert request for data device and put IO task to workqueue.
 * This is executed in interruption context.
 *
 * @blk_start_request() has been already called.
 *
 * @bdev data block device.
 * @req request of the wrapper block device.
 */
void walb_make_ddev_request(struct block_device *bdev, struct request *req)
{
        struct bio *bio;
        struct walb_ddev_bio *dbio, *next;
        int bio_nr = 0;
        struct walb_submit_bio_work *wk;

        LOGd("make_ddev_request() called\n");

        wk = kmalloc(sizeof(struct walb_submit_bio_work), GFP_ATOMIC);
        if (! wk) { goto error0; }
        INIT_LIST_HEAD(&wk->list);
        spin_lock_init(&wk->lock);
        
        __rq_for_each_bio(bio, req) {

                dbio = kmalloc(sizeof(struct walb_ddev_bio), GFP_ATOMIC);
                if (! dbio) { goto error1; }
                
                walb_init_ddev_bio(dbio);
                dbio->bio = bio_clone(bio, GFP_ATOMIC);
                dbio->bio->bi_bdev = bdev;
                dbio->bio->bi_end_io = walb_ddev_end_io;
                dbio->bio->bi_private = dbio;
                dbio->req = req;
                dbio->status = WALB_BIO_INIT;
                dbio->head = &wk->list;
                
                list_add_tail(&dbio->list, &wk->list);
                bio_nr ++;
                LOGd("dbio->status: %d\n", dbio->status);
        }

        LOGd("bio_nr: %d\n", bio_nr);
        ASSERT(! list_empty(&wk->list));

        INIT_WORK(&wk->work, walb_submit_bio_task);
        queue_work(wqm_, &wk->work);
        
        LOGd("make_ddev_request() end\n");
        return;

error1:
        list_for_each_entry_safe(dbio, next, &wk->list, list) {
                bio_put(dbio->bio);
                kfree(dbio);
        }
        kfree(wk);
error0:
        LOGe("make_ddev_request failed\n");
        __blk_end_request_all(req, -EIO);
}

/**
 * Task to call submit_bio in a process context.
 */
void walb_submit_bio_task(struct work_struct *work)
{
        struct walb_submit_bio_work *wk;
        struct walb_ddev_bio *dbio, *next;

        LOGd("submit_bio_task begin\n");
        
        wk = container_of(work, struct walb_submit_bio_work, work);
        
        if (list_empty(&wk->list)) {
                LOGw("list is empty\n");
        }
        
        list_for_each_entry_safe(dbio, next, &wk->list, list) {

                LOGd("submit_bio_task %ld %d\n",
                       (long)dbio->bio->bi_sector, dbio->bio->bi_size);
                submit_bio(dbio->bio->bi_rw, dbio->bio);
        }

        LOGd("submit_bio_task end\n");
}

/**
 * The bio is walb_ddev_bio's bio.
 */
void walb_ddev_end_io(struct bio *bio, int error)
{
        struct walb_ddev_bio *dbio = bio->bi_private;
        struct request *req = dbio->req;
        struct walb_ddev_bio *tmp, *next;
        struct list_head *head;
        struct walb_submit_bio_work *wk;
        unsigned long irq_flags;

        int is_last = 1;
        int is_err = 0;
        
        LOGd("ddev_end_io() called\n");
        LOGd("bio %ld %d\n",
               (long)bio->bi_sector, bio->bi_size);
        
        BUG_ON(! dbio);
        head = dbio->head;
        BUG_ON(! head);
        
        if (error || ! test_bit(BIO_UPTODATE, &bio->bi_flags)) {
                LOGe("IO failed error=%d, uptodate=%d\n",
                       error, test_bit(BIO_UPTODATE, &bio->bi_flags));
                
                dbio->status = WALB_BIO_ERROR;
        }

        dbio->status = WALB_BIO_END;
        bio_put(bio);
        dbio->bio = NULL;

        /* Check whether it's the last bio in the request finished
           or error or not finished. */
        wk = container_of(head, struct walb_submit_bio_work, list);
        spin_lock_irqsave(&wk->lock, irq_flags);
        list_for_each_entry_safe(tmp, next, head, list) {

                /* LOGd("status: %d\n", tmp->status); */
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
        spin_unlock_irqrestore(&wk->lock, irq_flags);

        /* Finalize the request of wrapper device. */
        if (is_last) {
                if (is_err) {
                        LOGd("walb blk_end_request_all() -EIO\n");
                        blk_end_request_all(req, -EIO);
                } else {
                        LOGd("walb blk_end_request_all() 0\n");
                        blk_end_request_all(req, 0);
                }

                spin_lock_irqsave(&wk->lock, irq_flags);
                list_for_each_entry_safe(tmp, next, head, list) {
                        BUG_ON(tmp->bio != NULL);
                        BUG_ON(tmp->status == WALB_BIO_INIT);
                        list_del(&tmp->list);
                        kfree(tmp);
                }
                /* confirm the list is empty */
                if (! list_empty(&wk->list)) {
                        LOGe("wk->list must be empty.\n");
                        BUG();
                }
                spin_unlock_irqrestore(&wk->lock, irq_flags);

                kfree(wk);
        }

        LOGd("ddev_end_io() end\n");
}

/**
 * Initialize struct walb_ddev_bio.
 */
void walb_init_ddev_bio(struct walb_ddev_bio *dbio)
{
        ASSERT(dbio != NULL);
        
        dbio->req = NULL;
        INIT_LIST_HEAD(&dbio->list);
        dbio->status = WALB_BIO_INIT;
        dbio->bio = NULL;
}

/**
 * Just forward request to ddev.
 *
 * Context:
 *     Interrupted. Queue lock held.
 */
void walb_forward_request_to_ddev(struct block_device *bdev,
                                  struct request *req)
{
        struct walb_bios_work *wk;
        wk = walb_create_bios_work(bdev, req, GFP_ATOMIC);
        if (wk == NULL) {
                __blk_end_request_all(req, -EIO);
        }

        INIT_WORK(&wk->work, walb_bios_work_task);
        queue_work(wqm_, &wk->work);
}

/**
 * Task of walb_bios_work.
 */
void walb_bios_work_task(struct work_struct *work)
{
        struct walb_bios_work *wk;

        wk = container_of(work, struct walb_bios_work, work);

        if (walb_fill_bios_work(wk, GFP_NOIO) != 0) {
                goto error0;
        }        
        if (walb_clone_bios_work(wk, GFP_NOIO) != 0) {
                goto error0;
        }
        walb_submit_bios_work(wk);
        return;
        
error0:
        blk_end_request_all(wk->req_orig, -EIO);
        walb_destroy_bios_work(wk);
}

/**
 * End IO callback for walb_bios_work.
 */
void walb_bios_work_end_io(struct bio *bio, int error)
{
        int idx, is_all_on, req_error;
        unsigned long flags;
        struct walb_bios_work* wk = bio->bi_private;

        bio_put(bio);

        /* Get index */
        for (idx = 0; idx < wk->n_bio; idx ++) {
                if (wk->biop_ary[idx] == bio) {
                        break;
                }
        }
        ASSERT(idx < wk->n_bio);

        if (error) {
                atomic_inc(&wk->is_fail);
        }

        spin_lock_irqsave(&wk->end_bmp->lock, flags);
        walb_bitmap_on(wk->end_bmp, idx);
        is_all_on = walb_bitmap_is_all_on(wk->end_bmp);
        spin_unlock_irqrestore(&wk->end_bmp->lock, flags);
        
        if (is_all_on) {
                /* All bio(s) done */
                if (atomic_read(&wk->is_fail)) {
                        req_error = -EIO;
                } else {
                        req_error = 0;
                }
                blk_end_request_all(wk->req_orig, req_error);

                walb_destroy_bios_work(wk);
        }
}

/**
 * Create walb_bios_work.
 *
 * @return NULL in failure.
 */
struct walb_bios_work*
walb_create_bios_work(struct block_device *bdev,
                      struct request *req_orig,
                      gfp_t gfp_mask)
{
        struct walb_bios_work *wk;

        wk = kzalloc(sizeof(struct walb_bios_work), gfp_mask);
        if (wk == NULL) { goto error0; }
        
        wk->bdev = bdev;
        wk->req_orig = req_orig;

        ASSERT(wk->n_bio == 0);
        ASSERT(wk->end_bmp == NULL);
        ASSERT(wk->biop_ary == NULL);
        ASSERT(atomic_read(&wk->is_fail) == 0);

        return wk;

error0:
        return NULL;
}

/**
 * Destroy walb_bios_work.
 */
void walb_destroy_bios_work(struct walb_bios_work* wk)
{
        ASSERT(wk != NULL);
        
        ASSERT(wk->biop_ary);
        kfree(wk->biop_ary);

        ASSERT(wk->end_bmp);
        walb_bitmap_free(wk->end_bmp);

        kfree(wk);
}

/**
 * Allocate internal data structure and fill them.
 *
 * @return 0 in success, or -1.
 */
int walb_fill_bios_work(struct walb_bios_work* wk, gfp_t gfp_mask)
{
        int n_bio;
        
        ASSERT(wk != NULL && wk->req_orig != NULL);

        n_bio = walb_rq_count_bio(wk->req_orig);
        
        wk->end_bmp = walb_bitmap_create(n_bio, gfp_mask);
        if (wk->end_bmp == NULL) { goto error0; }
        ASSERT(walb_bitmap_is_all_off(wk->end_bmp));
        
        wk->biop_ary = kzalloc(sizeof(struct bio *) * n_bio, gfp_mask);
        if (wk->biop_ary == NULL) { goto error1; }

        return 0;
        
error1:
        walb_bitmap_free(wk->end_bmp);
error0:
        return -1;
}

/**
 * Clone bio(s) and submit.
 */
int walb_clone_bios_work(struct walb_bios_work* wk, gfp_t gfp_mask)
{
        int i;
        struct bio *bio, *cbio;

        wk->n_bio = 0;
        __rq_for_each_bio(bio, wk->req_orig) {
                
                cbio = bio_clone(bio, gfp_mask);
                if (cbio == NULL) { goto error0; }
                cbio->bi_bdev = wk->bdev;
                cbio->bi_end_io = walb_bios_work_end_io;
                cbio->bi_private = wk;
                wk->biop_ary[wk->n_bio] = cbio;
                
                wk->n_bio ++;
        }
        ASSERT(walb_rq_count_bio(wk->req_orig) == wk->n_bio);
        return 0;

error0:
        for (i = 0; i < wk->n_bio; i ++) {
                bio_put(wk->biop_ary[i]);
        }
        return -1;
}

/**
 * Submit all bio(s) inside walb_bios_work.
 */
void walb_submit_bios_work(struct walb_bios_work* wk)
{
        int i;
        struct bio *bio;

        for (i = 0; i < wk->n_bio; i ++) {
                bio = wk->biop_ary[i];
                submit_bio(bio->bi_rw, bio);
        }
}

/**
 * Just forward request to ddev.
 * Using completion.
 *
 * Context:
 * Interrupted. Queue lock held.
 */
void walb_forward_request_to_ddev2(struct block_device *bdev, struct request *req)
{
        struct walb_bioclist_work *wk;
        wk = walb_create_bioclist_work(bdev, req, GFP_ATOMIC);
        if (wk == NULL) {
                __blk_end_request_all(req, -EIO);
        } else {
                INIT_WORK(&wk->work, walb_bioclist_work_task);
                queue_work(wqm_, &wk->work);
        }
}

/**
 * Work task with struct walb_bioclist_work.
 */
void walb_bioclist_work_task(struct work_struct *work)
{
        struct walb_bioclist_work *wk;
        struct request *req;
        struct list_head bioc_list;
        struct bio *bio, *cbio;
        struct walb_bio_with_completion *bioc, *bioc_next;
        int is_fail, req_error;
        
        wk = container_of(work, struct walb_bioclist_work, work);
        req = wk->req_orig;
        
        INIT_LIST_HEAD(&bioc_list);

        is_fail = 0;
        __rq_for_each_bio(bio, req) {

                bioc = kmalloc(sizeof(struct walb_bio_with_completion),
                               GFP_NOIO);
                if (bioc == NULL) {
                        is_fail ++;
                        break;
                }
                init_completion(&bioc->wait);
                bioc->status = WALB_BIO_INIT;
                
                cbio = bio_clone(bio, GFP_NOIO);
                if (cbio == NULL) {
                        is_fail ++;
                        break;
                }
                cbio->bi_bdev = wk->bdev;
                cbio->bi_private = bioc;
                cbio->bi_end_io = walb_end_io_with_completion;

                bioc->bio = cbio;
                list_add_tail(&bioc->list, &bioc_list);
                submit_bio(cbio->bi_rw, cbio);
        }

        list_for_each_entry_safe(bioc, bioc_next, &bioc_list, list) {

                wait_for_completion(&bioc->wait);
                if (bioc->status != WALB_BIO_END) {
                        LOGe("walb_bioclist_work_task: read error.\n");
                        is_fail ++;
                }
                bio_put(bioc->bio);
                list_del(&bioc->list);
                kfree(bioc);
        }
        ASSERT(list_empty(&bioc_list));

        if (is_fail) {
                req_error = -EIO;
        } else {
                req_error = 0;
        }
        blk_end_request_all(req, req_error);
        walb_destroy_bioclist_work(wk);
}

/**
 * Create bioclist_work.
 */
struct walb_bioclist_work*
walb_create_bioclist_work(struct block_device *bdev,
                          struct request *req,
                          gfp_t gfp_mask)
{
        struct walb_bioclist_work *wk;
        
        wk = kzalloc(sizeof(struct walb_bioclist_work), gfp_mask);
        if (wk == NULL) { goto error0; }
        
        wk->bdev = bdev;
        wk->req_orig = req;

        return wk;

error0:
        return NULL;
}

/**
 * Destroy bioclist_work.
 */
void walb_destroy_bioclist_work(struct walb_bioclist_work *wk)
{
        kfree(wk);
}

/*******************************************************************************
 * Deprecated functions.
 *******************************************************************************/

#if 0

/*
 * Handle an I/O request.
 */
void walb_transfer(struct walb_dev *dev, unsigned long sector,
                   unsigned long nbytes, char *buffer, int write)
{
	unsigned long offset = sector * dev->logical_bs;

	if ((offset + nbytes) > dev->size) {
		LOGn("Beyond-end write (%ld %ld)\n", offset, nbytes);
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
int walb_xfer_bio(struct walb_dev *dev, struct bio *bio)
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

/**
 * Transfer a single segment.
 */
int walb_xfer_segment(struct walb_dev *dev,
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

/**
 * Transfer a full request.
 */
int walb_xfer_request(struct walb_dev *dev, struct request *req)
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

/**
 * Smarter request function that "handles clustering".
 */
void walb_full_request(struct request_queue *q)
{
	struct request *req;
	int sectors_xferred;
	struct walb_dev *dev = q->queuedata;
        int error;

	while ((req = blk_peek_request(q)) != NULL) {
                blk_start_request(req);
		if (req->cmd_type != REQ_TYPE_FS) {
			LOGn("Skip non-fs request\n");
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
 * The direct make request version.
 */
int walb_make_request(struct request_queue *q, struct bio *bio)
{
	struct walb_dev *dev = q->queuedata;
	int status;

	status = walb_xfer_bio(dev, bio);
	bio_endio(bio, status);
	return 0;
}

#endif /* #if 0 */

MODULE_LICENSE("Dual BSD/GPL");
