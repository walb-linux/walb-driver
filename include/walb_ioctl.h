/**
 * ioctl header for Walb.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_IOCTL_H
#define _WALB_IOCTL_H

#include "walb.h"

#ifdef __KERNEL__
#include <linux/ioctl.h>
#else /* __KERNEL__ */
#include <sys/ioctl.h>
#endif /* __KERNEL__ */

/**
 * Data structure for walb ioctl.
 */
struct walb_ctl_data {

        dev_t walb_devt; /* walb device.
                            walblog device is walb_devt + 1. */
        
        dev_t log_devt;  /* log device. */
        dev_t data_devt; /* data device. */
        
        unsigned int minor; /* minor id of walb device. */
        
        /* These are used for other struct for each control command. */
        size_t buf_size; /* buffer size. */
        void *buf; /* buffer pointer if data_size > 0. */
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

        /* For userland --> kernel. */
        struct walb_ctl_data u2k;

        /* For kernel --> userland. */
        struct walb_ctl_data k2u;
} __attribute__((packed));

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
};


/**
 * Prorotypes.
 * These are implemented in walb_control.c
 */
void* walb_alloc_and_copy_from_user(
        void __user *userbuf,
        size_t buf_size,
        gfp_t gfp_mask);
int walb_copy_to_user_and_free(
        void __user *userbuf,
        void *buf,
        size_t buf_size);
struct walb_ctl* walb_get_ctl(void __user *userctl, gfp_t gfp_mask);
int walb_put_ctl(void __user *userctl, struct walb_ctl *ctl);



#endif /* _WALB_IOCTL_H */
