/**
 * General definitions for Walb for kernel code.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_KERN_H
#define _WALB_KERN_H

#include <linux/workqueue.h>
#include <linux/bio.h>
#include <linux/spinlock.h>
#include <linux/kernel.h>

#include "../include/walb_log_device.h"

/**
 * For debug
 */
#if defined(WALB_DEBUG)
#define printk_d(fmt, args...) \
        printk(KERN_DEBUG "walb: " fmt, ##args)
#else
#define printk_d(fmt, args...)
#endif

#define printk_e(fmt, args...)                  \
        printk(KERN_ERR "walb: " fmt, ##args)
#define printk_w(fmt, args...)                  \
        printk(KERN_WARNING "walb: " fmt, ##args)
#define printk_n(fmt, args...)                  \
        printk(KERN_NOTICE "walb: " fmt, ##args)
#define printk_i(fmt, args...)                  \
        printk(KERN_INFO "walb: " fmt, ##args)


/*
 * The different "request modes" we can use.
 */
enum {
	RM_FULL    = 0,	/* The full-blown version */
	RM_NOQUEUE = 1,	/* Use make_request */
};

/*
 * Minor number and partition management.
 */
#define WALB_MINORS	  16
#define WALB_MINORS_SHIFT  4
#define DEVNUM(kdevnum)	(MINOR(kdev_t_to_nr(kdevnum)) >> MINOR_SHIFT

/*
 * We can tweak our hardware sector size, but the kernel talks to us
 * in terms of small sectors, always.
 */
/* #define KERNEL_SECTOR_SIZE	512 */

/*
 * The internal representation of our device.
 */
struct walb_dev {
        u64 size;                       /* Device size in bytes */
        u8 *data;                       /* The data array */
        int users;                      /* How many users */
        spinlock_t lock;                /* For mutual exclusion */
        struct request_queue *queue;    /* The device request queue */
        struct gendisk *gd;             /* The gendisk structure */

        /* Max number of snapshots.
           This is const after log device is initialized. */
        u32 n_snapshots;
        
        /* Size of underlying devices. [logical block] */
        u64 ldev_size;
        u64 ddev_size;
        
        /* You can get sector size with
           bdev_logical_block_size(bdev) and
           bdev_physical_block_size(bdev).

           Those of underlying log device and data device
           must be same.
        */
        u16 logical_bs;
        u16 physical_bs;

        /* Wrapper device id. */
        dev_t devt;
        
        /* Underlying block devices */
        struct block_device *ldev;
        struct block_device *ddev;

        /* Latest lsid and its lock. */
        spinlock_t latest_lsid_lock;
        u64 latest_lsid;

        /* Spinlock for lsuper0 access. */
        spinlock_t lsuper0_lock;
        /* Super sector of log device. */
        walb_super_sector_t *lsuper0;
        /* walb_super_sector_t *lsuper1; */
};


#define WALB_BIO_INIT    0
#define WALB_BIO_END     1
#define WALB_BIO_ERROR   2

struct walb_ddev_bio {

        struct request *req; /* wrapper-level request */

        struct list_head *head; /* list head */
        struct list_head list;
        
        /* sector_t offset; /\* io offset *\/ */
        /* int iosize;      /\* io size *\/ */

        int status;
        
        struct bio *bio; /* bio for underlying device */

};

struct walb_submit_bio_work
{
        struct list_head list; /* list of walb_ddev_bio */
        spinlock_t lock; /* lock for the list */
        struct work_struct work;
};

struct walb_bio_with_completion
{
        struct bio *bio;
        struct completion wait;
        int status;
};

static inline void walb_init_ddev_bio(struct walb_ddev_bio *dbio)
{
        dbio->req = NULL;
        INIT_LIST_HEAD(&dbio->list);
        dbio->status = WALB_BIO_INIT;
        dbio->bio = NULL;
}

/**
 * Work to create log pack.
 */
struct walb_make_log_pack_work
{
        struct request** reqp_ary;
        int n_req; /* array size */
        spinlock_t lock; /* lock for the ary. */
        struct walb_dev *wdev;
        struct work_struct work;
};

#endif /* _WALB_KERN_H */
