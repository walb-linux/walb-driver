/**
 * walb.c - Block-level WAL
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

#include "walb_kern.h"
#include "hashtbl.h"
#include "walb_control.h"
#include "walb_alldevs.h"
#include "snapshot_kern.h"
#include "walb_io.h"

#include "../include/walb_ioctl.h"
#include "../include/walb_log_device.h"
#include "../include/walb_sector.h"
#include "../include/walb_snapshot.h"
#include "../include/bitmap.h"

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
 * Workqueue for read/write.
 */
struct workqueue_struct *wq_ = NULL;


/*******************************************************************************
 * Prototypes of local functions.
 *******************************************************************************/

/* Lock/unlock block device. */
static int walb_lock_bdev(struct block_device **bdevp, dev_t dev);
static void walb_unlock_bdev(struct block_device *bdev);

/* Logpack functions. */
static void walb_logpack_header_print(const char *level,
                                      struct walb_logpack_header *lhead);
static int walb_logpack_header_fill(struct walb_logpack_header *lhead,
                                    u64 logpack_lsid,
                                    struct request** reqp_ary, int n_req,
                                    int n_lb_in_pb,
                                    u64 ring_buffer_offset,
                                    u64 ring_buffer_size);
static struct walb_logpack_request_entry* walb_create_logpack_request_entry(
        struct walb_logpack_entry *logpack_entry, int idx);
static void walb_destroy_logpack_request_entry(
        struct walb_logpack_request_entry *entry);
static struct walb_logpack_entry* walb_create_logpack_entry(
        struct walb_dev *wdev,
        struct walb_logpack_header *logpack,
        struct request** reqp_ary);
static void walb_destroy_logpack_entry(struct walb_logpack_entry *entry);
static struct walb_bio_with_completion* walb_submit_logpack_bio_to_ldev(
        struct walb_logpack_request_entry *req_entry,
        struct bio *bio,
        u64 ldev_offset, int bio_offset);
static int walb_submit_logpack_request_to_ldev(
        struct walb_logpack_request_entry *req_entry);
static int walb_submit_logpack_to_ldev(
        struct walb_logpack_entry* logpack_entry);
static int walb_logpack_write(struct walb_dev *wdev,
                              struct walb_logpack_header *logpack,
                              struct request** reqp_ary);
static int walb_logpack_calc_checksum(struct walb_logpack_header *lhead,
                                      int physical_bs,
                                      struct request** reqp_ary, int n_req);
/* Datapack functions. */
static struct walb_datapack_request_entry* walb_create_datapack_request_entry(
        struct walb_datapack_entry *datapack_entry, int idx);
static void walb_destroy_datapack_request_entry(
        struct walb_datapack_request_entry *entry);
static struct walb_datapack_entry* walb_create_datapack_entry(
        struct walb_dev *wdev,
        struct walb_logpack_header *logpack,
        struct request** reqp_ary);
static void walb_destroy_datapack_entry(struct walb_datapack_entry *entry);
static struct walb_bio_with_completion* walb_submit_datapack_bio_to_ddev(
        struct walb_datapack_request_entry *req_entry,
        struct bio *bio);
static int walb_submit_datapack_request_to_ddev(
        struct walb_datapack_request_entry *req_entry);
static int walb_submit_datapack_to_ddev(
        struct walb_datapack_entry* datapack_entry);
static int walb_datapack_write(struct walb_dev *wdev,
                               struct walb_logpack_header *logpack,
                               struct request **reqp_ary);

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
static void walb_print_super_sector(struct walb_super_sector *lsuper0);
static struct sector_data* walb_read_super_sector(struct walb_dev *dev);
static int walb_write_super_sector(struct walb_dev *dev,
                                   struct sector_data *lsuper);
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
static int walb_logpack_header_fill(struct walb_logpack_header *lhead,
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
       
        printk_d("walb_logpack_header_fill begin\n");

        printk_d("logpack_lsid %llu n_req %d n_lb_in_pb %d\n",
                 logpack_lsid, n_req, n_lb_in_pb);

        total_lb = 0;
        n_padding = 0;
        i = 0;
        while (i < n_req + n_padding) {

                printk_d("walb_logpack_header_fill: i %d n_req %d n_padding %d\n",
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
                        printk_e("IO request size (%llu) > ring buffer size (%llu).\n",
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

                        printk_d("padding req_padding_lb: %d %u\n",
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
        printk_d("total_lb: %d\n", total_lb); /* debug */
        lhead->total_io_size = total_lb / n_lb_in_pb;
        lhead->logpack_lsid = logpack_lsid;
        lhead->sector_type = SECTOR_TYPE_LOGPACK;
        
        logpack_size = lhead->total_io_size + 1;

        printk_d("walb_logpack_header_fill end\n");
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
static struct walb_logpack_request_entry*
walb_create_logpack_request_entry(
        struct walb_logpack_entry *logpack_entry, int idx)
{
        struct walb_logpack_request_entry *entry;
        struct walb_logpack_header *logpack = logpack_entry->logpack;
        int n_padding;
        int i;
        /* int n_bio; */
        
        printk_d("walb_create_logpack_request_entry begin\n");

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
 * @logpack filled logpack data.
 * @reqp_ary original request array.
 * 
 * @return Pointer to created logpack entry or NULL.
 *         This must be destroyed with @walb_destroy_logpack_entry().
 *
 */
static struct walb_logpack_entry* walb_create_logpack_entry(
        struct walb_dev *wdev,
        struct walb_logpack_header *logpack,
        struct request** reqp_ary)
{
        struct walb_logpack_entry *entry;
        struct walb_logpack_request_entry *req_entry, *tmp_req_entry;
        int i, n_padding;


        printk_d("walb_create_logpack_entry begin\n");
        
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
 * @entry logpack entry.
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
 * Clone and submit given bio of the request entry for log device.
 *
 * @req_entry request entry.
 * @bio original bio to clone and submit to log device.
 * @ldev_offset log device offset to write log for the request [physical block].
 * @bio_offset offset of the bio inside the whole request [logical block].
 *
 * @return walb_bio_with_completion or NULL.
 */
static struct walb_bio_with_completion*
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

        printk_d("walb_submit_logpack_bio_to_ldev begin\n");
        
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
        if (cbio == NULL) {
                printk_e("bio_clone() failed\n");
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

        printk_d("submit logpack data bio: off %llu size %u\n",
                 (u64)cbio->bi_sector, bio_cur_bytes(cbio));
        
        ASSERT(cbio->bi_rw & WRITE);
        submit_bio(cbio->bi_rw, cbio);

        printk_d("walb_submit_logpack_bio_to_ldev end\n");
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
static int walb_submit_logpack_request_to_ldev(
        struct walb_logpack_request_entry *req_entry)
{
        struct bio *bio;
        int off_lb;
        struct request* req;
        /* unsigned long irq_flags; */
        int lbs = req_entry->logpack_entry->wdev->logical_bs;
        struct walb_bio_with_completion *bioc;
        u64 ldev_off_pb;

        printk_d("walb_submit_logpack_request_to_ldev begin\n");
        
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
                        printk_e("walb_submit_logpack_bio_to_ldev() failed\n");
                        goto error0;
                }
                ASSERT(bio->bi_size % lbs == 0);
                off_lb += bio->bi_size / lbs;
        }

        printk_d("walb_submit_logpack_request_to_ldev end\n");
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

        printk_d("walb_submit_logpack_to_ldev begin\n");
        
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

        printk_d("submit logpack header bio: off %llu size %u\n",
                 (u64)bio->bi_sector, bio_cur_bytes(bio));
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
                goto error0;
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
        if (walb_submit_logpack_to_ldev(logpack_entry) != 0) {
                goto error1;
        }

        walb_destroy_logpack_entry(logpack_entry);
        printk_d("walb_logpack_write end\n");
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
static int walb_logpack_calc_checksum(struct walb_logpack_header *lhead,
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

/**
 * Create datapack request entry and its substructure.
 *
 * @datapack_entry parent datapack entry.
 * @idx record idx of the request in the logpack.
 *
 * @return Pointer to create entry, or NULL.
 *         This must be destroyed with @walb_destroy_datapack_request_entry().
 */
static struct walb_datapack_request_entry*
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
static void walb_destroy_datapack_request_entry(
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
static struct walb_datapack_entry* walb_create_datapack_entry(
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
static void walb_destroy_datapack_entry(struct walb_datapack_entry *entry)
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
static struct walb_bio_with_completion* walb_submit_datapack_bio_to_ddev(
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
static int walb_submit_datapack_request_to_ddev(
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
static int walb_submit_datapack_to_ddev(struct walb_datapack_entry* datapack_entry)
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
static int walb_datapack_write(struct walb_dev *wdev,
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

        printk_d("walb_make_logpack_and_submit_task begin\n");
        ASSERT(wk->n_req <= max_n_log_record_in_sector(wdev->physical_bs));

        printk_d("making log pack (n_req %d)\n", wk->n_req);
        
        /*
         * Allocate memory (sector size) for log pack header.
         */
        lhead_sect = sector_alloc(wdev->physical_bs, GFP_NOIO | __GFP_ZERO);
        if (lhead_sect == NULL) {
                printk_e("walb_alloc_sector() failed\n");
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
                 wdev->physical_bs / wdev->logical_bs,
                 ringbuf_off, ringbuf_size);
        if (logpack_size < 0) {
                printk_e("walb_logpack_header_fill failed\n");
                spin_unlock(&wdev->latest_lsid_lock);
                goto error0;
        }
        next_logpack_lsid = logpack_lsid + logpack_size;
        if (next_logpack_lsid - oldest_lsid > ringbuf_size) {
                /* Ring buffer overflow. */
                printk_e("There is not enough space to write log for %d:%d !\n",
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
                printk_e("logpack write failed (lsid %llu).\n",
                         lhead->logpack_lsid);
                goto error0;
        }

        /* Clone bio(s) of each request and set offset for log pack.
           Submit prepared bio(s) to log device. */
        if (walb_datapack_write(wdev, lhead, wk->reqp_ary) != 0) {
                printk_e("datapack write failed (lsid %llu). \n",
                         lhead->logpack_lsid);
                goto error0;
        }

        /* Normally completed log/data writes. */
        walb_end_requests(wk->reqp_ary, wk->n_req, 0);

        /* Update written_lsid. */
        spin_lock(&wdev->datapack_list_lock);
        if (next_logpack_lsid <= wdev->written_lsid) {
                printk_e("Logpack/data write order is not kept.\n");
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

        if (atomic_read(&wdev->is_read_only)) {
                printk_d("Currently read-only mode. write failed.\n");
                goto error0;
        }
        
        wk = kmalloc(sizeof(struct walb_make_logpack_work), GFP_ATOMIC);
        if (! wk) { goto error0; }

        wk->reqp_ary = reqp_ary;
        wk->n_req = n_req;
        wk->wdev = wdev;
        INIT_WORK(&wk->work, walb_make_logpack_and_submit_task);
        queue_work(wq_, &wk->work);
        
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
        /* printk_d("max_n_req: %d\n", max_n_req); */
        
        while ((req = blk_peek_request(q)) != NULL) {

                blk_start_request(req);
                if (req->cmd_type != REQ_TYPE_FS) {
			printk_n("skip non-fs request.\n");
                        __blk_end_request_all(req, -EIO);
                        continue;
                }

                if (req->cmd_flags & REQ_FLUSH) {
                        printk_d("REQ_FLUSH\n");
                }
                if (req->cmd_flags & REQ_DISCARD) {
                        printk_d("REQ_DISCARD\n");
                }

                if (req->cmd_flags & REQ_WRITE) {
                        /* Write.
                           Make log record and
                           add log pack. */

                        printk_d("WRITE %ld %d\n", blk_rq_pos(req), blk_rq_bytes(req));
                        
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
                        
                        printk_d("READ %ld %d\n", blk_rq_pos(req), blk_rq_bytes(req));

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
                printk_e("walb_check_lsid_valid: alloc sector failed.\n");
                goto error0;
        }
        ASSERT(is_same_size_sector(sect, wdev->lsuper0));
        logpack = get_logpack_header(sect);
        
        off = get_offset_of_lsid_2(get_super_sector(wdev->lsuper0), lsid);
        if (sector_io(READ, wdev->ldev, off, sect) != 0) {
                printk_e("walb_check_lsid_valid: read sector failed.\n");
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
                printk_e("walb_get_ctl failed.\n");
                goto error0;
        }
        
        /* Execute each command. */
        switch(ctl->command) {
        
        case WALB_IOCTL_OLDEST_LSID_GET:

                printk_n("WALB_IOCTL_OLDEST_LSID_GET\n");
                spin_lock(&wdev->oldest_lsid_lock);
                oldest_lsid = wdev->oldest_lsid;
                spin_unlock(&wdev->oldest_lsid_lock);

                ctl->val_u64 = oldest_lsid;
                ret = 0;
                break;
                
        case WALB_IOCTL_OLDEST_LSID_SET:
                
                printk_n("WALB_IOCTL_OLDEST_LSID_SET\n");
                lsid = ctl->val_u64;
                
                if (walb_check_lsid_valid(wdev, lsid) == 0) {
                        spin_lock(&wdev->oldest_lsid_lock);
                        wdev->oldest_lsid = lsid;
                        spin_unlock(&wdev->oldest_lsid_lock);
                        
                        walb_sync_super_block(wdev);
                        ret = 0;
                } else {
                        printk_e("lsid %llu is not valid.\n", lsid);
                }
                break;
                
        case WALB_IOCTL_CHECKPOINT_INTERVAL_GET:

                printk_n("WALB_IOCTL_CHECKPOINT_INTERVAL_GET\n");
                ctl->val_u32 = get_checkpoint_interval(wdev);
                ret = 0;
                break;
                
        case WALB_IOCTL_CHECKPOINT_INTERVAL_SET:

                printk_n("WALB_IOCTL_CHECKPOINT_INTERVAL_SET\n");
                interval = ctl->val_u32;
                if (interval <= WALB_MAX_CHECKPOINT_INTERVAL) {
                        set_checkpoint_interval(wdev, interval);
                        ret = 0;
                } else {
                        printk_e("Checkpoint interval is too big.\n");
                }
                break;

        case WALB_IOCTL_WRITTEN_LSID_GET:

                printk_n("WALB_IOCTL_WRITTEN_LSID_GET\n");
                ctl->val_u64 = get_written_lsid(wdev);
                ret = 0;
                break;

        case WALB_IOCTL_LOG_CAPACITY_GET:

                printk_n("WALB_IOCTL_LOG_CAPACITY_GET\n");
                ctl->val_u64 = get_log_capacity(wdev);
                ret = 0;
                break;

        case WALB_IOCTL_SNAPSHOT_CREATE:

                printk_n("WALB_IOCTL_SNAPSHOT_CREATE\n");

                (walb_snapshot_record_t *)ctl->u2k.__buf;
                /* now editing */
                
                break;
                
        case WALB_IOCTL_SNAPSHOT_DELETE:

                printk_n("WALB_IOCTL_SNAPSHOT_DELETE\n");
                break;
                
        case WALB_IOCTL_SNAPSHOT_GET:

                printk_n("WALB_IOCTL_SNAPSHOT_GET\n");
                break;
                
        case WALB_IOCTL_SNAPSHOT_NUM:

                printk_n("WALB_IOCTL_SNAPSHOT_NUM\n");
                break;
                
        case WALB_IOCTL_SNAPSHOT_LIST:

                printk_n("WALB_IOCTL_SNAPSHOT_LIST\n");
                break;
                
        default:
                printk_n("WALB_IOCTL_WDEV %d is not supported.\n",
                         ctl->command);
        }

        /* Put ctl data. */
        if (walb_put_ctl(userctl, ctl) != 0) {
                printk_e("walb_put_ctl failed.\n");
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

        printk_d("walb_ioctl begin.\n");
        printk_d("cmd: %08x\n", cmd);
        
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
        
        printk_d("walb_ioctl end.\n");

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
 * Print super sector for debug.
 *
 * @lsuper0 super sector.
 */
static void walb_print_super_sector(struct walb_super_sector *lsuper0)
{
#ifdef WALB_DEBUG
        const int str_size = 16 * 3 + 1;
        char uuidstr[str_size];
        sprint_uuid(uuidstr, str_size, lsuper0->uuid);
        
        printk_d("-----super block------\n"
                 "checksum %08x\n"
                 "logical_bs %u\n"
                 "physical_bs %u\n"
                 "snapshot_metadata_size %u\n"
                 "uuid: %s\n"
                 "sector_type: %04x\n"
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
                 lsuper0->sector_type,
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
 * @wdev walb device.
 *
 * @return super sector (internally allocated) or NULL.
 */
static struct sector_data* walb_read_super_sector(struct walb_dev *wdev)
{
        u64 off0;
        struct sector_data *lsuper0;
        struct walb_super_sector *sect;

        printk_d("walb_read_super_sector begin\n");
        
        lsuper0 = sector_alloc(wdev->physical_bs, GFP_NOIO);
        if (lsuper0 == NULL) {
                goto error0;
        }
        ASSERT_SECTOR_DATA(lsuper0);
        sect = get_super_sector(lsuper0);

        /* Really read. */
        off0 = get_super_sector0_offset(wdev->physical_bs);
        /* off1 = get_super_sector1_offset(wdev->physical_bs, wdev->n_snapshots); */
        if (sector_io(READ, wdev->ldev, off0, lsuper0) != 0) {
                printk_e("read super sector0 failed\n");
                goto error1;
        }

        /* Validate checksum. */
        if (checksum((u8 *)sect, lsuper0->size) != 0) {
                printk_e("walb_read_super_sector: checksum check failed.\n");
                goto error1;
        }

        /* Validate sector type */
        if (sect->sector_type != SECTOR_TYPE_SUPER) {
                printk_e("walb_read_super_sector: sector type check failed.\n");
                goto error1;
        }

#ifdef WALB_DEBUG
        walb_print_super_sector(sect);
#endif
        
        printk_d("walb_read_super_sector end\n");
        return lsuper0;

error1:
        sector_free(lsuper0);
error0:
        return NULL;
}

/**
 * Write super sector.
 * Currently only super sector 0 will be written. (super sector 1 is not.)
 *
 * @wdev walb device.
 * @lsuper super sector to write.
 *
 * @return 0 in success, or -1.
 */
static int walb_write_super_sector(struct walb_dev *wdev,
                                   struct sector_data *lsuper)
{
        u64 off0;
        u32 csum;
        struct walb_super_sector *sect;

        printk_d("walb_write_super_sector begin\n");

        ASSERT(wdev != NULL);
        ASSERT_SECTOR_DATA(lsuper);
        ASSERT(wdev->physical_bs == lsuper->size);
        sect = get_super_sector(lsuper);
        
        /* Set sector_type. */
        sect->sector_type = SECTOR_TYPE_SUPER;
        
        /* Generate checksum. */
        sect->checksum = 0;
        csum = checksum((u8 *)sect, wdev->physical_bs);
        sect->checksum = csum;

        /* Really write. */
        off0 = get_super_sector0_offset(wdev->physical_bs);
        if (sector_io(WRITE, wdev->ldev, off0, lsuper) != 0) {
                printk_e("write super sector0 failed\n");
                goto error0;
        }

        printk_d("walb_write_super_sector end\n");
        return 0;

error0:
        return -1;
}

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
        
        if (walb_write_super_sector(wdev, lsuper_tmp) != 0) {
                printk_e("walb_sync_super_block: write super block failed.\n");
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
        printk_d("minor %u dev_name: %s\n", minor, dev_name);

        name_len = strnlen(dev_name, DISK_NAME_LEN);

        if (name_len > WALB_DEV_NAME_MAX_LEN) {
                printk_e("Device name is too long: %s.\n", name);
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
		printk_e("Bad request mode %d.\n", request_mode);
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
        printk_d("1ms = %lu jiffies\n", msecs_to_jiffies(1)); /* debug */
        wdev->queue->unplug_fn = walb_unplug_all;

        /*
	 * And the gendisk structure.
	 */
	/* dev->gd = alloc_disk(WALB_MINORS); */
        wdev->gd = alloc_disk(1);
	if (! wdev->gd) {
		printk_e("alloc_disk failure.\n");
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
        printk_d("device path: %s, device name: %s\n",
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
        
        wdev->lsuper0 = walb_read_super_sector(wdev);
        if (wdev->lsuper0 == NULL) {
                printk_e("walb_ldev_init: read super sector failed\n");
                goto error0;
        }

        if (walb_write_super_sector(wdev, wdev->lsuper0) != 0) {
                printk_e("walb_ldev_init: write super sector failed\n");
                goto error1;
        }

        lsuper0_tmp = walb_read_super_sector(wdev);
        if (lsuper0_tmp == NULL) {
                printk_e("walb_ldev_init: read lsuper0_tmp failed\n");
                sector_free(lsuper0_tmp);
                goto error1;
        }

        if (sector_compare(wdev->lsuper0, lsuper0_tmp) != 0) {
                printk_e("walb_ldev_init: memcmp NG\n");
        } else {
                printk_e("walb_ldev_init: memcmp OK\n");
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
        printk_d("snapshot offset range: [%"PRIu64",%"PRIu64").\n",
                 snapshot_begin_pb, snapshot_end_pb);
        wdev->snapd = snapshot_data_create
                (wdev->ldev, snapshot_begin_pb, snapshot_end_pb, GFP_KERNEL);
        if (wdev->snapd == NULL) {
                printk_e("snapshot_data_create() failed.\n");
                goto error1;
        }
        /* Initialize snapshot data by scanning snapshot sectors. */
        if (snapshot_data_initialize(wdev->snapd) != 0) {
                printk_e("snapshot_data_initialize() failed.\n");
                goto out_snapshot_create;
        }
        
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
                printk_n("is_sync_superblock is on\n");
                if (walb_sync_super_block(wdev) != 0) {
                        goto error0;
                }
        } else {
                printk_n("is_sync_superblock is off\n");
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
        printk_d("walb_unregister_device begin.\n");
        if (wdev->gd) {
                del_gendisk(wdev->gd);
        }
        printk_d("walb_unregister_device end.\n");
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
        printk_d("walblog_unregister_device begin.\n");
        if (wdev->log_gd) {
                del_gendisk(wdev->log_gd);
        }
        printk_d("walblog_unregister_device end.\n");
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

        printk_d("do_checkpointing called.\n");

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
                printk_d("do_checkpointing should stop.\n");
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

                printk_d("skip superblock sync.\n");
        } else {
                if (walb_sync_super_block(wdev) != 0) {

                        atomic_set(&wdev->is_read_only, 1);
                        printk_e("superblock sync failed.\n");

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

        printk_d("do_checkpinting: delay %ld sync_time %ld next_delay %ld\n",
                 delay, sync_time, next_delay);

        if (next_delay <= 0) {
                printk_w("Checkpoint interval is too small. "
                         "Should be more than %d.\n", jiffies_to_msecs(sync_time));
                next_delay = 1;
        }
        ASSERT(next_delay > 0);
        
        down_write(&wdev->checkpoint_lock);
        if (wdev->checkpoint_state == CP_RUNNING) {
                /* Register delayed work for next time */
                INIT_DELAYED_WORK(&wdev->checkpoint_work, do_checkpointing);
                ret = queue_delayed_work(wq_, &wdev->checkpoint_work, next_delay);
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
                printk_w("Checkpoint state is not stopped.\n");
                up_write(&wdev->checkpoint_lock);
                return;
        }

        interval = wdev->checkpoint_interval;
        if (interval == 0) { /* This is not error. */
                printk_n("checkpoint_interval is 0.\n");
                up_write(&wdev->checkpoint_lock);
                return;
        }
        ASSERT(interval > 0);
        
        delay = msecs_to_jiffies(interval);
        ASSERT(delay > 0);
        INIT_DELAYED_WORK(&wdev->checkpoint_work, do_checkpointing);

        queue_delayed_work(wq_, &wdev->checkpoint_work, delay);
        wdev->checkpoint_state = CP_WAITING;
        printk_d("state change to CP_WAITING\n");
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
                printk_w("Checkpointing is not running.\n");
                up_write(&wdev->checkpoint_lock);
                return;
        }
        wdev->checkpoint_state = CP_STOPPING;
        printk_d("state change to CP_STOPPING\n");
        up_write(&wdev->checkpoint_lock);

        /* We must unlock before calling this to avoid deadlock. */
        ret = cancel_delayed_work_sync(&wdev->checkpoint_work);
        printk_d("cancel_delayed_work_sync: %d\n", ret);

        down_write(&wdev->checkpoint_lock);
        wdev->checkpoint_state = CP_STOPPED;
        printk_d("state change to CP_STOPPED\n");
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
		printk_w("unable to get major number.\n");
		return -EBUSY;
	}
        printk_i("walb_start with major id %d.\n", walb_major);

        /*
         * Workqueue.
         */
        wq_ = create_singlethread_workqueue(WALB_WORKQUEUE_NAME);
        if (wq_ == NULL) {
                printk_e("create workqueue failed.\n");
                goto out_unregister;
        }
        
        /*
         * Alldevs.
         */
        if (alldevs_init() != 0) {
                printk_e("alldevs_init failed.\n");
                goto out_workqueue;
        }
        
        /*
         * Init control device.
         */
        if (walb_control_init() != 0) {
                printk_e("walb_control_init failed.\n");
                goto out_alldevs_exit;
        }

	/*
	 * Allocate the device array, and initialize each one.
	 */
#if 0
        if (setup_device_tmp(0) != 0) {
		printk_e("setup_device failed.\n");
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
out_workqueue:
        if (wq_) { destroy_workqueue(wq_); }
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

        flush_workqueue(wq_); /* can omit this? */
        destroy_workqueue(wq_);
        
	unregister_blkdev(walb_major, WALB_NAME);

        walb_control_exit();
        alldevs_exit();
        
        printk_i("walb exit.\n");
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
                printk_e("Do not specify WALB_DYNAMIC_MINOR.\n");
                goto out;
        }
        
	/*
	 * Initialize walb_dev.
	 */
        wdev = kzalloc(sizeof(struct walb_dev), GFP_KERNEL);
        if (wdev == NULL) {
                printk_e("kmalloc failed.\n");
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
                printk_e("walb_lock_bdev failed (%u:%u for log)\n",
                         MAJOR(ldevt), MINOR(ldevt));
                goto out_free;
        }
        wdev->ldev_size = get_capacity(wdev->ldev->bd_disk);
        ldev_lbs = bdev_logical_block_size(wdev->ldev);
        ldev_pbs = bdev_physical_block_size(wdev->ldev);
        printk_i("log disk (%u:%u)\n"
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
                printk_e("walb_lock_bdev failed (%u:%u for data)\n",
                         MAJOR(ddevt), MINOR(ddevt));
                goto out_ldev;
        }
        wdev->ddev_size = get_capacity(wdev->ddev->bd_disk);
        ddev_lbs = bdev_logical_block_size(wdev->ddev);
        ddev_pbs = bdev_physical_block_size(wdev->ddev);
        printk_i("data disk (%d:%d)\n"
                 "data disk size %llu\n"
                 "data logical sector size %u\n"
                 "data physical sector size %u\n",
                 MAJOR(ddevt), MINOR(ddevt),
                 wdev->ddev_size,
                 ddev_lbs, ddev_pbs);

        /* Check compatibility of log device and data device. */
        if (ldev_lbs != ddev_lbs || ldev_pbs != ddev_pbs) {
                printk_e("Sector size of data and log must be same.\n");
                goto out_ddev;
        }
        wdev->logical_bs = ldev_lbs;
        wdev->physical_bs = ldev_pbs;
	wdev->size = wdev->ddev_size * (u64)wdev->logical_bs;

        /* Load log device metadata. */
        if (walb_ldev_initialize(wdev) != 0) {
                printk_e("ldev init failed.\n");
                goto out_ddev;
        }
        wdev->written_lsid = get_super_sector(wdev->lsuper0)->written_lsid;
        wdev->prev_written_lsid = wdev->written_lsid;
        wdev->oldest_lsid = get_super_sector(wdev->lsuper0)->oldest_lsid;

        /* Set device name. */
        if (walb_set_name(wdev, minor, name) != 0) {
                printk_e("Set device name failed.\n");
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
                printk_e("walb_prepare_device() failed.\n");
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
        printk_i("destroy_wdev (wrap %u:%u log %u:%u data %u:%u)\n",
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
        printk_d("destroy_wdev done.\n");
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
