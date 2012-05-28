/**
 * kern.h - Common definitions for Walb kernel code.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_KERN_H_KERNEL
#define WALB_KERN_H_KERNEL

#include "check_kernel.h"

#include <linux/workqueue.h>
#include <linux/bio.h>
#include <linux/spinlock.h>
#include <linux/kernel.h>

#include "walb/log_device.h"
#include "walb/sector.h"
#include "util.h"
#include "io.h"

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
#define WALB_WORKQUEUE_SINGLE_NAME "walb_wqs"
#define WALB_WORKQUEUE_MULTI_NAME "walb_wqm"

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
        struct sector_data *lsuper0;

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

/*******************************************************************************
 * Prototypes defined in walb.c
 *******************************************************************************/

struct walb_dev* prepare_wdev(unsigned int minor,
                              dev_t ldevt, dev_t ddevt, const char* name);
void destroy_wdev(struct walb_dev *wdev);
void register_wdev(struct walb_dev *wdev);
void unregister_wdev(struct walb_dev *wdev);


#endif /* WALB_KERN_H_KERNEL */
