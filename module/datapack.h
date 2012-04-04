/**
 * datapack.h - Header for datapack.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_DATAPACK_H_KERNEL
#define WALB_DATAPACK_H_KERNEL

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
 * Work to create datapack.
 */
struct walb_make_datapack_work
{
        struct request** reqp_ary; /* This is read only. */
        int n_req; /* array size */
        struct walb_dev *wdev;
        struct work_struct work;
};

/**
 * Bio wrapper for datapack write.
 * Almost the same walb_logpack_bio.
 */
struct walb_datapack_bio {

        struct request *req_orig;
        struct bio *bio_orig;

        int status;
        struct bio *bio_for_data;

        struct walb_datapack_request_entry *req_entry;
        int idx;
};

/**
 * wdev->datapack_list_lock is already locked.
 */
struct walb_datapack_entry {

        struct list_head *head;
        struct list_head list;

        struct walb_dev *wdev;
        struct walb_logpack_header *logpack;

        struct list_head req_list;
        struct request **reqp_ary;
};

/**
 * Datapack request entry.
 *
 * A datapack may have several requests.
 * This struct is corresponding to each request.
 */
struct walb_datapack_request_entry {

        struct list_head *head;
        struct list_head list;

        struct walb_datapack_entry *datapack_entry;
        struct request *req_orig;
        int idx;

        struct list_head bioc_list;
};

/*******************************************************************************
 * Functions prototype.
 *******************************************************************************/

struct walb_datapack_request_entry* walb_create_datapack_request_entry(
        struct walb_datapack_entry *datapack_entry, int idx);
void walb_destroy_datapack_request_entry(
        struct walb_datapack_request_entry *entry);
struct walb_datapack_entry* walb_create_datapack_entry(
        struct walb_dev *wdev,
        struct walb_logpack_header *logpack,
        struct request** reqp_ary);
void walb_destroy_datapack_entry(struct walb_datapack_entry *entry);
struct walb_bio_with_completion* walb_submit_datapack_bio_to_ddev(
        struct walb_datapack_request_entry *req_entry,
        struct bio *bio);
int walb_submit_datapack_request_to_ddev(
        struct walb_datapack_request_entry *req_entry);
int walb_submit_datapack_to_ddev(
        struct walb_datapack_entry* datapack_entry);
int walb_datapack_write(struct walb_dev *wdev,
                        struct walb_logpack_header *logpack,
                        struct request **reqp_ary);


#endif /* WALB_DATAPACK_H_KERNEL */
