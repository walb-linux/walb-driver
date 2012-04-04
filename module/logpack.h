/**
 * logpack.h - Header for logpack.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_LOGPACK_H_KERNEL
#define WALB_LOGPACK_H_KERNEL

#include <linux/blkdev.h>
#include <linux/bio.h>
#include <linux/workqueue.h>
#include <linux/list.h>

#include "check_kernel.h"
#include "kern.h"

/*******************************************************************************
 * Data definitions.
 *******************************************************************************/

/**
 * Work to create logpack.
 */
struct walb_make_logpack_work
{
        struct request** reqp_ary; /* This is read only. */
        int n_req; /* array size */
        struct walb_dev *wdev;
        struct work_struct work;
};


/**
 * Bio wrapper for logpack write.
 */
struct walb_logpack_bio {

        struct request *req_orig; /* corresponding wrapper-level request */
        struct bio *bio_orig;     /* corresponding wrapper-level bio */
        
        int status; /* bio_for_log status */
        struct bio *bio_for_log; /* inside logpack */
        /* struct bio *bio_for_data; */ /* for data device */

        /* pointer to belonging logpack request entry */
        struct walb_logpack_request_entry *req_entry;
        int idx; /* idx'th bio in the request. */
};

/**
 * Logpack list entry.
 */
struct walb_logpack_entry {

        struct list_head *head; /* pointer to wdev->logpack_list */
        struct list_head list;

        struct walb_dev *wdev; /* belonging walb device. */
        struct walb_logpack_header *logpack;

        /* list of walb_logpack_request_entry */
        struct list_head req_list;
        /* array of pointer of original request */
        struct request **reqp_ary;

        /* Logpack header block flags. */
        /* atomic_t is_submitted_header; */
        /* atomic_t is_end_header; */
        /* atomic_t is_success_header; */
};

/**
 * Logpack request entry.
 *
 * A logpack may have several requests.
 * This struct is corresponding to each request.
 */
struct walb_logpack_request_entry {

        /* pointer to walb_logpack_entry->req_list */
        struct list_head *head;
        struct list_head list;
        
        struct walb_logpack_entry *logpack_entry; /* belonging logpack entry. */
        struct request *req_orig; /* corresponding original request. */
        int idx; /* Record index inside logpack header. */
        
        /* size must be number of bio(s) inside the req_orig. */
        /* spinlock_t bmp_lock; */
        /* struct walb_bitmap *io_submitted_bmp; */
        /* struct walb_bitmap *io_end_bmp; */
        /* struct walb_bitmap *io_success_bmp; */

        /* bio_completion list */
        struct list_head bioc_list;
};


/*******************************************************************************
 * Functions prototype.
 *******************************************************************************/

void walb_logpack_header_print(const char *level,
                               struct walb_logpack_header *lhead);
int walb_logpack_header_fill(struct walb_logpack_header *lhead,
                             u64 logpack_lsid,
                             struct request** reqp_ary, int n_req,
                             int n_lb_in_pb,
                             u64 ring_buffer_offset,
                             u64 ring_buffer_size);
struct walb_logpack_request_entry* walb_create_logpack_request_entry(
        struct walb_logpack_entry *logpack_entry, int idx);
void walb_destroy_logpack_request_entry(
        struct walb_logpack_request_entry *entry);
struct walb_logpack_entry* walb_create_logpack_entry(
        struct walb_dev *wdev,
        struct walb_logpack_header *logpack,
        struct request** reqp_ary);
void walb_destroy_logpack_entry(struct walb_logpack_entry *entry);
struct walb_bio_with_completion* walb_submit_logpack_bio_to_ldev(
        struct walb_logpack_request_entry *req_entry,
        struct bio *bio,
        u64 ldev_offset, int bio_offset);
int walb_submit_logpack_request_to_ldev(
        struct walb_logpack_request_entry *req_entry);
int walb_submit_logpack_to_ldev(
        struct walb_logpack_entry* logpack_entry);
int walb_logpack_write(struct walb_dev *wdev,
                       struct walb_logpack_header *logpack,
                       struct request** reqp_ary);
int walb_logpack_calc_checksum(struct walb_logpack_header *lhead,
                               int physical_bs,
                               struct request** reqp_ary, int n_req);


#endif /* WALB_LOGPACK_H_KERNEL */
