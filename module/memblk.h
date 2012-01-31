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

/* Make request name */
#define MEMBLK_MAKE_REQUEST_NAME_MAX_LEN 16

/**
 * Memory block device.
 */
struct memblk_dev
{
        spinlock_t lock; /* Lock data for this struct. */
        
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


#if 0

/**
 * A wrapper block device.
 */
struct wrapper_block_device
{
        spinlock_t lock; /* Lock data for this struct.
                            Used for queue lock also if request_fn_proc is used. */
        
        char name[DISK_NAME_LEN];
        u64 capacity; /* Capacity [logical block] */
        dev_t devt; /* Device id */

        struct request_queue *queue;
        struct gendisk *gd;
        make_request_fn *make_request_fn;
        request_fn_proc *requets_fn_proc; /* Used when make_request_fn is NULL. */
        
        void *private_data; /* Can be used for any purpose. */
};

u8* get_physical_sector(struct memblk_dev *mdev, u64 physical_sector_id);

/* Register a memblk_dev. */
struct memblk_dev* create_memblk_dev(int minor, );
void destroy_memblk_dev(struct memblk_dev* mdev);
int register_memblk_dev();
void unregister_memblk_dev();

void start_memblk_dev();
void stop_memblk_dev();

bool register_memblk_make_request(const char* name, make_request_fn *make_request_fn);
void unregister_memblk_make_request(const char* name);

#endif /* #if 0 */

#endif /* _WALB_MEMBLK_H_KERNEL */
