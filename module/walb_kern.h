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
        spinlock_t lock;                /* For mutual exclusion.
                                           Use spin_lock() */
        struct request_queue *queue;    /* The device request queue */
        struct gendisk *gd;             /* The gendisk structure */

        atomic_t is_read_only;          /* Write always fails if true */

        
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

        /* Spinlock for lsuper0 access.
           Irq handler must not lock this.
           Use spin_lock().
         */
        spinlock_t lsuper0_lock;
        /* Super sector of log device. */
        walb_super_sector_t *lsuper0;
        /* walb_super_sector_t *lsuper1; */

        /* Log pack list.
           Use spin_lock_irqsave(). */
        /* spinlock_t logpack_list_lock; */
        /* struct list_head logpack_list; */

        /* Data pack list.
           Use spin_lock() */
        spinlock_t datapack_list_lock;
        struct list_head datapack_list;
        u64 written_lsid;
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
        struct list_head list;
};

static inline void walb_init_ddev_bio(struct walb_ddev_bio *dbio)
{
        dbio->req = NULL;
        INIT_LIST_HEAD(&dbio->list);
        dbio->status = WALB_BIO_INIT;
        dbio->bio = NULL;
}

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

#endif /* _WALB_KERN_H */
