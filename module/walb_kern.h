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
#include "walb_util.h"
#include "walb_io.h"

/**
 * Walb device major.
 */
extern int walb_major;

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

/**
 * Workqueue name.
 */
#define WALB_WORKQUEUE_NAME "walb_wq"

/*
 * Default checkpoint interval [ms]
 */
#define WALB_DEFAULT_CHECKPOINT_INTERVAL 10000
#define WALB_MAX_CHECKPOINT_INTERVAL (24 * 60 * 60 * 1000) /* 1 day */

/*
 * We can tweak our hardware sector size, but the kernel talks to us
 * in terms of small sectors, always.
 */
/* #define KERNEL_SECTOR_SIZE	512 */

/**
 * Checkpointing state.
 *
 * Permitted state transition:
 *   stoppped -> waiting @start_checkpionting()
 *   waiting -> running  @do_checkpointing()
 *   running -> waiting  @do_checkpointing()
 *   waiting -> stopped  @do_checkpointing()
 *   waiting -> stopping @stop_checkpointing()
 *   running -> stopping @stop_checkpointing()
 *   stopping -> stopped @stop_checkpointing()
 */
enum {
        CP_STOPPED = 0,
        CP_STOPPING,
        CP_WAITING,
        CP_RUNNING,
};

/**
 * The internal representation of walb and walblog device.
 */
struct walb_dev {
        u64 size;                       /* Device size in bytes */
        u8 *data;                       /* The data array */
        int users;                      /* How many users */
        spinlock_t lock;                /* For queue access. */
        struct request_queue *queue;    /* The device request queue */
        struct gendisk *gd;             /* The gendisk structure */

        atomic_t is_read_only;          /* Write always fails if true */

        struct list_head list; /* member of all_wdevs_ */
        
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
        u64 prev_written_lsid; /* previously sync down lsid. */

        spinlock_t oldest_lsid_lock;
        u64 oldest_lsid;

        /*
         * For wrapper log device.
         */
        /* spinlock_t log_queue_lock; */
        struct request_queue *log_queue;
        struct gendisk *log_gd;

        /*
         * For checkpointing.
         *
         * start_checkpointing(): register handler.
         * stop_checkpointing():  unregister handler.
         * do_checkpointing():    checkpoint handler.
         *
         * checkpoint_lock is used to access 
         *   checkpoint_interval,
         *   checkpoint_state.
         *
         * checkpoint_work accesses are automatically
         * serialized by checkpoint_state.
         */
        struct rw_semaphore checkpoint_lock;
        u32 checkpoint_interval; /* [ms]. 0 means never do checkpointing. */
        u8 checkpoint_state;
        struct delayed_work checkpoint_work;


        /*
         * For snapshotting.
         */
        struct snapshot_data *snapd;
};


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

/**
 * Prototypes defined in walb.c
 */
struct walb_dev* prepare_wdev(unsigned int minor,
                              dev_t ldevt, dev_t ddevt, const char* name);
void destroy_wdev(struct walb_dev *wdev);
void register_wdev(struct walb_dev *wdev);
void unregister_wdev(struct walb_dev *wdev);


#endif /* _WALB_KERN_H */
