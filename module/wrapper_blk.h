/**
 * wrapper_blk.h - Definition for wrapper_blk driver.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_WRAPPER_BLK_H_KERNEL
#define WALB_WRAPPER_BLK_H_KERNEL

#include "check_kernel.h"

#include <linux/blkdev.h>
#include <linux/genhd.h>

#include "walb/common.h"
#include "walb/disk_name.h"

/*******************************************************************************
 * Macros definition.
 *******************************************************************************/

#define WRAPPER_BLK_NAME "wrapper_blk"
#define WRAPPER_BLK_DIR_NAME "wrapper_blk"
#define WRAPPER_BLK_DEV_NAME_MAX_LEN                                         \
        (DISK_NAME_LEN - sizeof(WRAPPER_BLK_DIR_NAME) - sizeof("/dev//"))

#define WRAPPER_BLK_SINGLE_WQ_NAME "wrapper_blk_s"
#define WRAPPER_BLK_MULTI_WQ_NAME "wrapper_blk_m"

/*******************************************************************************
 * Data definition.
 *******************************************************************************/

/**
 * Memory block device.
 */
struct wrapper_blk_dev
{
        unsigned int minor; /* Minor device id. */
        u64 capacity; /* Device capacity [logical block] */

        /* name of the device. terminated by '\0'. */
        char name[WRAPPER_BLK_DEV_NAME_MAX_LEN];

	unsigned int pbs; /* physical block size. */
	
        spinlock_t lock; /* Lock data for this struct and queue if need. */
        struct request_queue *queue; /* request queue */
        bool use_make_request_fn; /* true if using wdev_register_with_bio(). */
        union {
                /* for bio. */
                make_request_fn *make_request_fn;
                /* for request. */
                request_fn_proc *request_fn_proc;
        };
        
        struct gendisk *gd; /* disk */
        unsigned long is_started; /* If started, bit 0 is on, or off. */
        
        void *private_data; /* You can use this for any purpose. */
};

/*******************************************************************************
 * Exported functions prototype.
 *******************************************************************************/

/* (Un)register a new block device. */
bool wdev_register_with_bio(
        unsigned int minor, u64 capacity, unsigned int pbs,
        make_request_fn *make_request_fn);
bool wdev_register_with_req(
        unsigned int minor, u64 capacity, unsigned int pbs,
        request_fn_proc *request_fn_proc);
bool wdev_unregister(unsigned int minor);

/* Start/stop a registered device. */
bool wdev_start(unsigned int minor);
bool wdev_stop(unsigned int minor);

/* Get a device. */
struct wrapper_blk_dev* wdev_get(unsigned minor);

/* Get a device from a queue. */
struct wrapper_blk_dev* wdev_get_from_queue(struct request_queue *q);

#endif /* WALB_WRAPPER_BLK_H_KERNEL */
