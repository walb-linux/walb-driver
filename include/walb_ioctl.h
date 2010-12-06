/**
 * ioctl header for Walb.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_IOCTL_H
#define _WALB_IOCTL_H

#ifdef __KERNEL__
#include <linux/ioctl.h>
#else /* __KERNEL__ */
#include <sys/ioctl.h>
#endif /* __KERNEL__ */

enum {
        WALB_IOCTL_VERSION_CMD = 0,
        WALB_IOCTL_STATUS_CMD,
        WALB_IOCTL_GET_OLDESTLSID_CMD,
        WALB_IOCTL_SET_OLDESTLSID_CMD,
        WALB_IOCTL_SEARCH_LSID_CMD,

        /* not implemented yet */
        WALB_IOCTL_SNAPSHOT_CREATE_CMD,
        WALB_IOCTL_SNAPSHOT_DELETE_CMD,
        WALB_IOCTL_SNAPSHOT_GET_CMD
};

#define WALB_IOCTL_ID 0xfe

#define WALB_IOCTL_VERSION _IOR(WALB_IOCTL_ID, WALB_IOCTL_VERSION_CMD, u32)
/*
#define WALB_IOCTL_STATUS _IOR(WALB_IOCTL_ID, WALB_IOCTL_STATUS_CMD, struct walb_status)
*/
#define WALB_IOCTL_GET_OLDESTLSID _IOR(WALB_IOCTL_ID, WALB_IOCTL_GET_OLDESTLSID_CMD, u64)
#define WALB_IOCTL_SET_OLDESTLSID _IOW(WALB_IOCTL_ID, WALB_IOCTL_SET_OLDESTLSID_CMD, u64)
#define WALB_IOCTL_SEARCH_LSID _IOWR(WALB_IOCTL_ID, WALB_IOCTL_SEARCH_LSID_CMD, u64)


#endif /* _WALB_IOCTL_H */
