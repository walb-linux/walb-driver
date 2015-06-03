/**
 * disk_name.h - Header for disk name.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_DISK_NAME_H
#define WALB_DISK_NAME_H

#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Disk name length.
 *
 * DISK_NAME_LEN_USER must be the same as
 * DISK_NAME_LEN defined in linux/genhd.h file.
 */
#define DISK_NAME_LEN_USER 32
#ifdef __KERNEL__
#include <linux/genhd.h>
#else
#define DISK_NAME_LEN DISK_NAME_LEN_USER
#endif
#define ASSERT_DISK_NAME_LEN() ASSERT(DISK_NAME_LEN == DISK_NAME_LEN_USER)

/**
 * Disk data.
 * This is used by ioctl to list walb devices.
 */
struct walb_disk_data
{
	/* Device name */
	char name[DISK_NAME_LEN];

	/* Device major/minor id. */
	unsigned int major;
	unsigned int minor;

} __attribute__((packed));

#ifdef __cplusplus
}
#endif

#endif /* WALB_DISK_NAME_H */
