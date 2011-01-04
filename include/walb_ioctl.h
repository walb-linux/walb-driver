/**
 * ioctl header for Walb.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_IOCTL_H
#define _WALB_IOCTL_H

#include "walb.h"

#ifdef __KERNEL__
#include <linux/kernel.h>
#include <linux/ioctl.h>
#else /* __KERNEL__ */
#include <stdio.h>
#include <sys/ioctl.h>
#endif /* __KERNEL__ */

/**
 * If you want to assign device minor automatically, specify this.
 */
#define WALB_DYNAMIC_MINOR (-1U)

/**
 * Data structure for walb ioctl.
 */
struct walb_ctl_data {

        unsigned int wmajor; /* walb device major. */
        unsigned int wminor; /* walb device minor.
                                walblog device minor is (wminor + 1). */
        unsigned int lmajor;  /* log device major. */
        unsigned int lminor;  /* log device minor. */
        unsigned int dmajor;  /* data device major. */
        unsigned int dminor;  /* data device minor. */
        
        /* These are used for other struct for each control command. */
        size_t buf_size; /* buffer size. */
#ifdef __KERNEL__
        void __user *buf;
#else
        void *buf; /* buffer pointer if data_size > 0. */
#endif
        void *__buf; /* used inside kernel. */
} __attribute__((packed));

/**
 * Data structure for walb ioctl to /dev/walb/control.
 */
struct walb_ctl {

        /* Command id. */
        int command;

        /* Used for integer value transfer. */
        int val_int;
        u32 val_u32;
        u64 val_u64;

        int error; /* error no. */
        
        /* For userland --> kernel. */
        struct walb_ctl_data u2k;

        /* For kernel --> userland. */
        struct walb_ctl_data k2u;
} __attribute__((packed));

/**
 * Print walb_ctl data for debug.
 */
static inline void print_walb_ctl(const struct walb_ctl *ctl)
{
#ifdef __KERNEL__
#define PRINT_WALB_CTL printk_d
#else
#define PRINT_WALB_CTL printf
#endif
        PRINT_WALB_CTL("***** walb_ctl *****\n"
                       "command: %d\n"
                       "val_int: %d\n"
                       "val_u32: %u\n"
                       "val_u64: %"PRIu64"\n"
                       "error: %d\n"
                 
                       "u2k.wdevt: (%u:%u)\n"
                       "u2k.ldevt: (%u:%u)\n"
                       "u2k.ddevt: (%u:%u)\n"
                       "u2k.buf_size: %zu\n"
                 
                       "k2u.wdevt: (%u:%u)\n"
                       "k2u.ldevt: (%u:%u)\n"
                       "k2u.ddevt: (%u:%u)\n"
                       "k2u.buf_size: %zu\n",
                       ctl->command,
                       ctl->val_int,
                       ctl->val_u32,
                       ctl->val_u64,
                       ctl->error,
                 
                       ctl->u2k.wmajor, ctl->u2k.wminor,
                       ctl->u2k.lmajor, ctl->u2k.lminor,
                       ctl->u2k.dmajor, ctl->u2k.dminor,
                       ctl->u2k.buf_size,
                 
                       ctl->k2u.wmajor, ctl->k2u.wminor,
                       ctl->k2u.lmajor, ctl->k2u.lminor,
                       ctl->k2u.dmajor, ctl->k2u.dminor,
                       ctl->k2u.buf_size);
#undef PRINT_WALB_CTL
}

/**
 * Ioctl magic word for walb.
 */
#define WALB_IOCTL_ID 0xfe

/**
 * Ioctl command id.
 */
enum {
        WALB_IOCTL_VERSION_CMD = 0,
        WALB_IOCTL_CONTROL_CMD,
        WALB_IOCTL_WDEV_CMD,
};

/**
 * Ioctl command id.
 *
 * WALB_IOCTL_CONTROL is for /dev/walb/control device.
 * WALB_IOCTL_WDEV is for each walb device.
 * WALB_IOCTL_VERSION is for both. (currently each walb device only.)
 */
#define WALB_IOCTL_CONTROL _IOWR(WALB_IOCTL_ID, WALB_IOCTL_CONTROL_CMD, struct walb_ctl)
#define WALB_IOCTL_WDEV    _IOWR(WALB_IOCTL_ID, WALB_IOCTL_WDEV_CMD,    struct walb_ctl)
#define WALB_IOCTL_VERSION  _IOR(WALB_IOCTL_ID, WALB_IOCTL_VERSION_CMD, u32)

/**
 * For walb_ctl.command.
 */
enum {
        WALB_IOCTL_DUMMY = 0,

        /*
         * For WALB_IOCTL_CONTROL
         */
        WALB_IOCTL_DEV_START, /* NIY */
        WALB_IOCTL_DEV_STOP, /* NIY */
        WALB_IOCTL_DEV_LIST, /* NIY */
        WALB_IOCTL_NUM_DEV_GET, /* NIY */

        /*
         * For WALB_IOCTL_WDEV
         */
        WALB_IOCTL_OLDEST_LSID_GET,
        WALB_IOCTL_OLDEST_LSID_SET,
        WALB_IOCTL_LSID_SEARCH, /* NIY */
        WALB_IOCTL_STATUS_GET, /* NIY */

        WALB_IOCTL_SNAPSHOT_CREATE, /* NIY */
        WALB_IOCTL_SNAPSHOT_DELETE, /* NIY */
        WALB_IOCTL_SNAPSHOT_GET, /* NIY */

        WALB_IOCTL_CHECKPOINT_INTERVAL_GET,
        WALB_IOCTL_CHECKPOINT_INTERVAL_SET,
};


#endif /* _WALB_IOCTL_H */
