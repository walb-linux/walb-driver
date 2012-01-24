/**
 * datapack.c - Datapack operations.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/blkdev.h>

#include "check_kernel.h"
#include "datapack.h"

/**
 * Create datapack request entry and its substructure.
 *
 * @datapack_entry parent datapack entry.
 * @idx record idx of the request in the logpack.
 *
 * @return Pointer to create entry, or NULL.
 *         This must be destroyed with @walb_destroy_datapack_request_entry().
 */
struct walb_datapack_request_entry*
walb_create_datapack_request_entry(
        struct walb_datapack_entry *datapack_entry, int idx)
{
        struct walb_datapack_request_entry *entry;
        struct walb_logpack_header *logpack = datapack_entry->logpack;
        int i, n_padding;
        
        printk_d("walb_create_datapack_request_entry begin\n");

        ASSERT(idx < logpack->n_records);
        ASSERT(logpack->record[idx].is_padding == 0);

        entry = kmalloc(sizeof(struct walb_datapack_request_entry), GFP_NOIO);
        if (entry == NULL) { goto error0; }

        entry->head = &datapack_entry->req_list;
        entry->datapack_entry = datapack_entry;
        entry->idx = idx;

        n_padding = 0;
        i = 0;
        while (i < idx) {
                if (logpack->record[i].is_padding) {
                        n_padding ++;
                }
                i ++;
        }
        entry->req_orig = datapack_entry->reqp_ary[idx - n_padding];

        INIT_LIST_HEAD(&entry->bioc_list);

        printk_d("walb_create_datapack_request_entry end\n");
        return entry;
        
error0:
        return NULL;
}

/**
 * Destroy datapack request entry.
 *
 * @entry datapack request entry to destroy (and deallocated).
 */
void walb_destroy_datapack_request_entry(
        struct walb_datapack_request_entry *entry)
{
        ASSERT(entry != NULL);

        ASSERT(list_empty(&entry->bioc_list));
        kfree(entry);
}

/**
 * Create datapack entry and its substructure.
 *
 * @wdev walb device.
 * @logpack corresponding logpack data.
 * @req_ary original request array.
 *
 * @return Pointer to created datapack entry or NULL.
 *         This must be destroyed with @walb_destroy_datapack_entry();
 */
struct walb_datapack_entry* walb_create_datapack_entry(
        struct walb_dev *wdev,
        struct walb_logpack_header *logpack,
        struct request** reqp_ary)
{
        struct walb_datapack_entry *entry;
        struct walb_datapack_request_entry *req_entry, *tmp_req_entry;
        int i, n_padding;

        printk_d("walb_create_datapack_entry begin\n");

        entry = kmalloc(sizeof(struct walb_datapack_entry), GFP_NOIO);
        if (entry == NULL) { goto error0; }

        entry->wdev = wdev;
        entry->logpack = logpack;
        INIT_LIST_HEAD(&entry->req_list);
        entry->reqp_ary = reqp_ary;

        n_padding = 0;
        for (i = 0; i < logpack->n_records; i ++) {
                if (logpack->record[i].is_padding) {
                        n_padding ++;
                } else {
                        req_entry = walb_create_datapack_request_entry(entry, i);
                        if (req_entry == NULL) { goto error1; }
                        list_add_tail(&req_entry->list, &entry->req_list);
                }                
        }
        ASSERT(n_padding <= 1);
        printk_d("walb_create_datapack_entry end\n");
        return entry;

error1:
        list_for_each_entry_safe(req_entry, tmp_req_entry, &entry->req_list, list) {
                list_del(&req_entry->list);
                walb_destroy_datapack_request_entry(req_entry);
        }        
error0:
        return NULL;
}

/**
 * Destroy datapack entry and its substructure created by
 * @walb_create_datapack_entry().
 *
 * @entry datapack entry.
 */
void walb_destroy_datapack_entry(struct walb_datapack_entry *entry)
{
        struct walb_datapack_request_entry *req_entry, *tmp_req_entry;

        ASSERT(entry != NULL);

        list_for_each_entry_safe(req_entry, tmp_req_entry, &entry->req_list, list) {

                list_del(&req_entry->list);
                walb_destroy_datapack_request_entry(req_entry);
        }
        kfree(entry);
}

/**
 * Clone and submit given bio of the request entry for data device.
 *
 * @req_entry request entry.
 * @bio original bio to clone and submit to data device.
 *
 * @return struct walb_bio_with_completion or NULL.
 */
struct walb_bio_with_completion* walb_submit_datapack_bio_to_ddev(
        struct walb_datapack_request_entry *req_entry,
        struct bio *bio)
{
        struct request *req;
        struct walb_dev *wdev;
        struct walb_bio_with_completion *bioc;
        struct bio *cbio;
        
        printk_d("walb_submit_datapack_bio_to_ddev begin\n");

        req = req_entry->req_orig;
        wdev = req_entry->datapack_entry->wdev;

        bioc = kmalloc(sizeof(*bioc), GFP_NOIO);
        if (bioc == NULL) { goto error0; }
        init_completion(&bioc->wait);
        bioc->status = WALB_BIO_INIT;

        cbio = bio_clone(bio, GFP_NOIO);
        if (cbio == NULL) { goto error1; }
        cbio->bi_bdev = wdev->ddev;
        cbio->bi_end_io = walb_end_io_with_completion;
        cbio->bi_private = bioc;
        bioc->bio = cbio;

        /* Block address is the same as original bio. */

        printk_d("submit datapack bio: off %llu size %u\n",
                 (u64)cbio->bi_sector, bio_cur_bytes(cbio));
        ASSERT(cbio->bi_rw & WRITE);
        submit_bio(cbio->bi_rw, cbio);
        
        printk_d("walb_submit_datapack_bio_to_ddev end\n");
        return bioc;

error1:        
        kfree(bioc);
error0:
        return NULL;
}

/**
 * Clone each bio in the datapack request entry
 * for data device write and submit it.
 *
 * @req_entry datapack request entry.
 *
 * @return 0 in success, or -1.
 */
int walb_submit_datapack_request_to_ddev(
        struct walb_datapack_request_entry *req_entry)
{
        struct request *req;
        struct bio *bio;
        struct walb_bio_with_completion *bioc;

        printk_d("walb_submit_datapack_request_to_ddev begin\n");

        ASSERT(req_entry != NULL);
        req = req_entry->req_orig;
        
        __rq_for_each_bio(bio, req) {

                bioc = walb_submit_datapack_bio_to_ddev
                        (req_entry, bio);
                if (bioc) {
                        list_add_tail(&bioc->list, &req_entry->bioc_list);
                } else {
                        printk_e("walb_submit_datapack_bio_to_ddev() failed\n");
                        goto error0;
                }
        }

        printk_d("walb_submit_datapack_request_to_ddev end\n");
        return 0;

error0:
        return -1;
}

/**
 * Clone each bio in the datapack entry for data device write and submit it.
 *
 * @datapack_entry datapack_entry to issue.
 *
 * @return 0 in success, or -1.
 */
int walb_submit_datapack_to_ddev(struct walb_datapack_entry* datapack_entry)
{
        struct walb_logpack_header *logpack;
        int n_req, n_padding, i, is_fail;
        u64 logpack_lsid;
        struct walb_dev *wdev;
        u32 pbs, lbs;
        struct walb_datapack_request_entry *req_entry, *tmp_req_entry;
        struct walb_bio_with_completion *bioc, *tmp_bioc;
        
        printk_d("walb_submit_datapack_to_ddev begin\n");

        ASSERT(datapack_entry != NULL);

        logpack = datapack_entry->logpack;
        n_req = logpack->n_records;
        n_padding = logpack->n_padding;
        logpack_lsid = logpack->logpack_lsid;
        ASSERT(logpack_lsid ==
               logpack->record[0].lsid -
               logpack->record[0].lsid_local);
        wdev = datapack_entry->wdev;
        lbs = wdev->logical_bs;
        pbs = wdev->physical_bs;
        
        /* Create bio and submit for datapack. */
        is_fail = 0;
        i = 0;
        list_for_each_entry_safe(req_entry, tmp_req_entry,
                                 &datapack_entry->req_list, list) {

                if (walb_submit_datapack_request_to_ddev(req_entry) != 0) {
                        printk_e("walb_submit_datapack_request_to_ddev() failed\n");
                        is_fail = 1;
                }
                i ++;
        }

        /* Wait bioc completion */
        list_for_each_entry_safe(req_entry, tmp_req_entry,
                                 &datapack_entry->req_list, list) {

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

        if (is_fail) {
                goto error0;
        }

        printk_d("walb_submit_datapack_to_ddev end\n");
        return 0;
        
error0:
        return -1;
}


/**
 * Clone each bio for data and write its contents
 * to data device.
 *
 * @wdev walb device.
 * @logpack corresponding logpack.
 * @reqp_ary request array.
 *           array size is logpack->
 *
 * @return 0 in success, or -1.
 */
int walb_datapack_write(struct walb_dev *wdev,
                        struct walb_logpack_header *logpack,
                        struct request **reqp_ary)
{
        struct walb_datapack_entry *datapack_entry;

        printk_d("walb_datapack_write begin\n");

        datapack_entry = walb_create_datapack_entry(wdev, logpack, reqp_ary);
        if (datapack_entry == NULL) {
                goto error0;
        }
        if (walb_submit_datapack_to_ddev(datapack_entry) != 0) {
                goto error1;
        }
        walb_destroy_datapack_entry(datapack_entry);
        printk_d("walb_datapack_write end\n");
        return 0;

error1:
        walb_destroy_datapack_entry(datapack_entry);
error0:
        return -1;
}

MODULE_LICENSE("Dual BSD/GPL");
