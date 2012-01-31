/**
 * logpack.c - Logpack operations.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/blkdev.h>

#include "check_kernel.h"
#include "logpack.h"

/**
 * Debug print of logpack header.
 *
 */
void walb_logpack_header_print(const char *level,
                               struct walb_logpack_header *lhead)
{
        int i;
        printk("%s*****logpack header*****\n"
               "checksum: %08x\n"
               "n_records: %u\n"
               "n_padding: %u\n"
               "total_io_size: %u\n"
               "logpack_lsid: %llu",
               level,
               lhead->checksum,
               lhead->n_records,
               lhead->n_padding,
               lhead->total_io_size,
               lhead->logpack_lsid);
        for (i = 0; i < lhead->n_records; i ++) {
                printk("%srecord %d\n"
                       "  checksum: %08x\n"
                       "  lsid: %llu\n"
                       "  lsid_local: %u\n"
                       "  is_padding: %u\n"
                       "  io_size: %u\n"
                       "  is_exist: %u\n"
                       "  offset: %llu\n",
                       level, i,
                       lhead->record[i].checksum,
                       lhead->record[i].lsid,
                       lhead->record[i].lsid_local,
                       lhead->record[i].is_padding,
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
 * @ring_buffer_offset ring buffer offset [physical block]
 * @ring_buffer_size ring buffer size [physical block]
 *
 * @return physical sectors of the log pack in success, or -1.
 */
int walb_logpack_header_fill(struct walb_logpack_header *lhead,
                             u64 logpack_lsid,
                             struct request** reqp_ary, int n_req,
                             int n_lb_in_pb,
                             u64 ring_buffer_offset,
                             u64 ring_buffer_size)
{
        struct request* req;
        int total_lb, req_lb, req_padding_lb, req_padding_pb;
        int i, logpack_size;
        int n_padding;
        u64 cur_lsid;
       
        LOGd("walb_logpack_header_fill begin\n");

        LOGd("logpack_lsid %llu n_req %d n_lb_in_pb %d\n",
                 logpack_lsid, n_req, n_lb_in_pb);

        total_lb = 0;
        n_padding = 0;
        i = 0;
        while (i < n_req + n_padding) {

                LOGd("walb_logpack_header_fill: i %d n_req %d n_padding %d\n",
                         i, n_req, n_padding); /* debug */
                
                req = reqp_ary[i - n_padding];
                req_lb = blk_rq_sectors(req);
                lhead->record[i].io_size = req_lb;

                /* Calc size of padding logical sectors
                   for physical sector alignment. */
                if (req_lb % n_lb_in_pb == 0) {
                        req_padding_lb = req_lb;
                } else {
                        req_padding_lb = ((req_lb / n_lb_in_pb) + 1) * n_lb_in_pb;
                        ASSERT(req_padding_lb % n_lb_in_pb == 0);
                }

                cur_lsid = logpack_lsid + total_lb / n_lb_in_pb + 1;
                req_padding_pb = req_padding_lb / n_lb_in_pb;

                if ((u64)req_padding_pb > ring_buffer_size) {
                        LOGe("IO request size (%llu) > ring buffer size (%llu).\n",
                                 (u64)req_padding_pb, ring_buffer_size);
                        return -1;
                }
                
                if (cur_lsid % ring_buffer_size + req_padding_pb > ring_buffer_size) {
                        /* Log of this request will cross the end of ring buffer.
                           So padding is required. */
                        lhead->record[i].is_padding = 1;
                        lhead->record[i].offset = 0;

                        req_padding_lb =
                                (ring_buffer_size
                                 - (cur_lsid % ring_buffer_size))
                                * n_lb_in_pb;
                        lhead->record[i].io_size = req_padding_lb;
                        n_padding ++;

                        LOGd("padding req_padding_lb: %d %u\n",
                                 req_padding_lb, lhead->record[i].io_size); /* debug */
                } else {
                        lhead->record[i].is_padding = 0;
                        lhead->record[i].offset = blk_rq_pos(req);
                }
                lhead->record[i].is_exist = 1;
                lhead->record[i].lsid_local = total_lb / n_lb_in_pb + 1;
                lhead->record[i].lsid = logpack_lsid + lhead->record[i].lsid_local;
                
                total_lb += req_padding_lb;
                ASSERT(total_lb % n_lb_in_pb == 0);
                i ++;
        }
        ASSERT(n_padding <= 1);
        lhead->n_padding = n_padding;
        lhead->n_records = n_req + n_padding;
        ASSERT(total_lb % n_lb_in_pb == 0);
        LOGd("total_lb: %d\n", total_lb); /* debug */
        lhead->total_io_size = total_lb / n_lb_in_pb;
        lhead->logpack_lsid = logpack_lsid;
        lhead->sector_type = SECTOR_TYPE_LOGPACK;
        
        logpack_size = lhead->total_io_size + 1;

        LOGd("walb_logpack_header_fill end\n");
        return logpack_size;
}

/**
 * Create logpack request entry and its substructure.
 *
 * @logpack_entry parent logpack entry.
 * @idx record idx of the request in the log pack.
 *
 * @return Pointer to created entry, or NULL.
 *         This must be destroyed with @walb_destroy_logpack_request_entry().
 */
struct walb_logpack_request_entry*
walb_create_logpack_request_entry(
        struct walb_logpack_entry *logpack_entry, int idx)
{
        struct walb_logpack_request_entry *entry;
        struct walb_logpack_header *logpack = logpack_entry->logpack;
        int n_padding;
        int i;
        /* int n_bio; */
        
        LOGd("walb_create_logpack_request_entry begin\n");

        ASSERT(idx < logpack->n_records);
        ASSERT(logpack->record[idx].is_padding == 0);
        
        entry = kmalloc(sizeof(struct walb_logpack_request_entry), GFP_NOIO);
        if (entry == NULL) { goto error0; }

        entry->head = &logpack_entry->req_list;
        entry->logpack_entry = logpack_entry;
        entry->idx = idx;

        /* Calc padding record. */
        n_padding = 0;
        i = 0;
        while (i < idx) {
                if (logpack->record[i].is_padding) {
                        n_padding ++;
                }
                i ++;
        }
        
        entry->req_orig = logpack_entry->reqp_ary[idx - n_padding];
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
        
        LOGd("walb_create_logpack_request_entry end\n");
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
void walb_destroy_logpack_request_entry(
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
 * @logpack filled logpack data.
 * @reqp_ary original request array.
 * 
 * @return Pointer to created logpack entry or NULL.
 *         This must be destroyed with @walb_destroy_logpack_entry().
 *
 */
struct walb_logpack_entry* walb_create_logpack_entry(
        struct walb_dev *wdev,
        struct walb_logpack_header *logpack,
        struct request** reqp_ary)
{
        struct walb_logpack_entry *entry;
        struct walb_logpack_request_entry *req_entry, *tmp_req_entry;
        int i, n_padding;


        LOGd("walb_create_logpack_entry begin\n");
        
        /*
         * Allocate and initialize logpack entry.
         */
        entry = kmalloc(sizeof(struct walb_logpack_entry), GFP_NOIO);
        if (entry == NULL) { goto error0; }

        /* entry->head = &wdev->logpack_list; */
        entry->wdev = wdev;
        entry->logpack = logpack;
        INIT_LIST_HEAD(&entry->req_list);
        entry->reqp_ary = reqp_ary;

        /*
         * Create request entry and add tail to the request entry list.
         */
        n_padding = 0;
        for (i = 0; i < logpack->n_records; i ++) {
                if (logpack->record[i].is_padding) {
                        n_padding ++;
                } else {
                        req_entry = walb_create_logpack_request_entry(entry, i);
                        if (req_entry == NULL) { goto error1; }
                        list_add_tail(&req_entry->list, &entry->req_list);
                }
        }
        ASSERT(n_padding <= 1);

        LOGd("walb_create_logpack_entry end\n");
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
 * @entry logpack entry.
 */
void walb_destroy_logpack_entry(struct walb_logpack_entry *entry)
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
 * Clone and submit given bio of the request entry for log device.
 *
 * @req_entry request entry.
 * @bio original bio to clone and submit to log device.
 * @ldev_offset log device offset to write log for the request [physical block].
 * @bio_offset offset of the bio inside the whole request [logical block].
 *
 * @return walb_bio_with_completion or NULL.
 */
struct walb_bio_with_completion*
walb_submit_logpack_bio_to_ldev(
        struct walb_logpack_request_entry *req_entry, struct bio *bio,
        u64 ldev_offset, int bio_offset)
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

        LOGd("walb_submit_logpack_bio_to_ldev begin\n");
        
        req = req_entry->req_orig;
        wdev = req_entry->logpack_entry->wdev;

        bioc = kmalloc(sizeof(*bioc), GFP_NOIO);
        if (bioc == NULL) {
                LOGe("kmalloc failed\n");
                goto error0;
        }
        init_completion(&bioc->wait);
        bioc->status = WALB_BIO_INIT;
        
        cbio = bio_clone(bio, GFP_NOIO);
        if (cbio == NULL) {
                LOGe("bio_clone() failed\n");
                goto error1;
        }
        cbio->bi_bdev = wdev->ldev;
        cbio->bi_end_io = walb_end_io_with_completion;
        cbio->bi_private = bioc;

        /* wdev should have copy of ring buffer offset
           not to lock lsuper0. */
        /* spin_lock(&wdev->lsuper0_lock); */
        /* off_pb = get_offset_of_lsid_2(wdev->lsuper0, */
        /*                               req_entry->logpack_entry->logpack->record[idx].lsid); */
        /* spin_unlock(&wdev->lsuper0_lock); */

        off_pb = ldev_offset;
        off_lb = off_pb * (wdev->physical_bs / wdev->logical_bs);
        cbio->bi_sector = off_lb + bio_offset;
        bioc->bio = cbio;

        LOGd("submit logpack data bio: off %llu size %u\n",
                 (u64)cbio->bi_sector, bio_cur_bytes(cbio));
        
        ASSERT(cbio->bi_rw & WRITE);
        submit_bio(cbio->bi_rw, cbio);

        LOGd("walb_submit_logpack_bio_to_ldev end\n");
        return bioc;

error1:
        kfree(bioc);
error0:
        return NULL;
}


/**
 * Clone each bio in the logpack request entry
 * for log device write and submit it.
 *
 * @req_entry logpack request entry.
 *
 * @return 0 in success, or -1.
 */
int walb_submit_logpack_request_to_ldev(
        struct walb_logpack_request_entry *req_entry)
{
        struct bio *bio;
        int off_lb;
        struct request* req;
        /* unsigned long irq_flags; */
        int lbs = req_entry->logpack_entry->wdev->logical_bs;
        struct walb_bio_with_completion *bioc;
        u64 ldev_off_pb;

        LOGd("walb_submit_logpack_request_to_ldev begin\n");
        
        ASSERT(req_entry != NULL);
        req = req_entry->req_orig;

        ldev_off_pb = get_offset_of_lsid_2
                (get_super_sector(req_entry->logpack_entry->wdev->lsuper0),
                 req_entry->logpack_entry->logpack->record[req_entry->idx].lsid);
        
        off_lb = 0;
        __rq_for_each_bio(bio, req) {

                bioc = walb_submit_logpack_bio_to_ldev
                        (req_entry, bio, ldev_off_pb, off_lb);
                if (bioc) {
                        list_add_tail(&bioc->list, &req_entry->bioc_list);
                } else {
                        LOGe("walb_submit_logpack_bio_to_ldev() failed\n");
                        goto error0;
                }
                ASSERT(bio->bi_size % lbs == 0);
                off_lb += bio->bi_size / lbs;
        }

        LOGd("walb_submit_logpack_request_to_ldev end\n");
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
int walb_submit_logpack_to_ldev(struct walb_logpack_entry* logpack_entry)
{
        int i, is_fail;
        int n_req, n_padding;
        struct bio *bio;
        struct walb_logpack_header *logpack;
        struct walb_logpack_request_entry *req_entry, *tmp_req_entry;
        struct page *page;
        u64 logpack_lsid, off_pb, off_lb;
        u32 pbs, lbs;
        struct walb_bio_with_completion *bioc, *tmp_bioc;
        struct walb_dev *wdev;

        LOGd("walb_submit_logpack_to_ldev begin\n");
        
        ASSERT(logpack_entry != NULL);

        logpack = logpack_entry->logpack;
        n_req = logpack->n_records;
        n_padding = logpack->n_padding;
        logpack_lsid = logpack->logpack_lsid;
        ASSERT(logpack_lsid ==
               logpack->record[0].lsid -
               logpack->record[0].lsid_local);
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

        ASSERT(virt_addr_valid(logpack_entry->logpack));
        page = virt_to_page(logpack_entry->logpack);

        bio->bi_bdev = wdev->ldev;
        
        /* We should use lock free data structure and algorithm. */
        /* spin_lock(&wdev->lsuper0_lock); */
        off_pb = get_offset_of_lsid_2(get_super_sector(wdev->lsuper0),
                                      logpack_lsid);
        /* spin_unlock(&wdev->lsuper0_lock); */
        off_lb = off_pb * (pbs / lbs);
        bio->bi_sector = off_lb;

        bio->bi_end_io = walb_end_io_with_completion;
        bio->bi_private = bioc;
        bio_add_page(bio, page, pbs, offset_in_page(logpack_entry->logpack));
        bioc->bio = bio;

        LOGd("submit logpack header bio: off %llu size %u\n",
                 (u64)bio->bi_sector, bio_cur_bytes(bio));
        submit_bio(WRITE, bio);

        /* Clone bio and submit for each bio of each request. */
        is_fail = 0;
        i = 0;
        list_for_each_entry_safe(req_entry, tmp_req_entry,
                                 &logpack_entry->req_list, list) {

                if (walb_submit_logpack_request_to_ldev(req_entry) != 0) {
                        LOGe("walb_submit_logpack_request_to_ldev() failed\n");
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
                goto error0;
        }
        
        LOGd("walb_submit_logpack_to_ldev end\n");
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
int walb_logpack_write(struct walb_dev *wdev,
                       struct walb_logpack_header *logpack,
                       struct request** reqp_ary)
{
        struct walb_logpack_entry *logpack_entry;

        LOGd("walb_logpack_write begin\n");
        
        /* Create logpack entry for IO to log device. */
        logpack_entry = walb_create_logpack_entry(wdev, logpack, reqp_ary);
        if (logpack_entry == NULL) { goto error0; }

        /* Alloc/clone related bio(s) and submit them.
           Currently this function waits for end of all bio(s). */
        if (walb_submit_logpack_to_ldev(logpack_entry) != 0) {
                goto error1;
        }

        walb_destroy_logpack_entry(logpack_entry);
        LOGd("walb_logpack_write end\n");
        return 0;

error1:
        walb_destroy_logpack_entry(logpack_entry);
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
int walb_logpack_calc_checksum(struct walb_logpack_header *lhead,
                               int physical_bs,
                               struct request** reqp_ary, int n_req)
{
        int i;
        struct request *req;
        struct req_iterator iter;
        struct bio_vec *bvec;
        u64 sum;
        int n_padding;

        n_padding = 0;
        i = 0;
        while (i < n_req + n_padding) {

                if (lhead->record[i].is_padding) {
                        n_padding ++;
                        i ++;
                        continue;
                }
                
                sum = 0;
                req = reqp_ary[i - n_padding];

                ASSERT(req->cmd_flags & REQ_WRITE);

                rq_for_each_segment(bvec, req, iter) {
                        
                        sum = checksum_partial
                                (sum,
                                 kmap(bvec->bv_page) + bvec->bv_offset,
                                 bvec->bv_len);
                        kunmap(bvec->bv_page);
                }

                lhead->record[i].checksum = checksum_finish(sum);
                i ++;
        }
        ASSERT(n_padding <= 1);
        ASSERT(n_padding == lhead->n_padding);
        ASSERT(n_req + n_padding == lhead->n_records);
        ASSERT(lhead->checksum == 0);
        lhead->checksum = checksum((u8 *)lhead, physical_bs);
        ASSERT(checksum((u8 *)lhead, physical_bs) == 0);
        
        return 0;
}

MODULE_LICENSE("Dual BSD/GPL");
