/**
 * memblk.h - Definition for memblk driver.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_MEMBLK_H_KERNEL
#define _WALB_MEMBLK_H_KERNEL

#include "check_kernel.h"

#include <linux/blkdev.h>
#include <linux/genhd.h>

#include "walb/common.h"
#include "walb/disk_name.h"
#include "treemap.h"
#include "block_size.h"

#define MEMBLK_NAME "memblk"
#define MEMBLK_DIR_NAME "memblk"
#define MEMBLK_DEV_NAME_MAX_LEN                                         \
        (DISK_NAME_LEN - sizeof(MEMBLK_DIR_NAME) - sizeof("/dev//"))

#define MEMBLK_SINGLE_WQ_NAME "memblk_s"
#define MEMBLK_MULTI_WQ_NAME "memblk_m"

/**
 * Memory block device.
 */
struct memblk_dev
{
        char name[DISK_NAME_LEN]; /* name of the device. terminated by '\0'. */
        u64 capacity; /* Device capacity [logical block] */
        unsigned int minor; /* minor device number. */
        struct block_size_op bs_op;

        dev_t devt;

        /* Key: physical address,
           Value: pointer to allocated memory with physical block size. */
        map_t *index; 

        /* Queue and disk. */
        struct request_queue *queue; /* request queue */
        struct gendisk *gd; /* disk */

        make_request_fn *make_request_fn;
};

#endif /* _WALB_MEMBLK_H_KERNEL */
