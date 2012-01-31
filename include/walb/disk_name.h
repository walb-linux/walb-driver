/**
 * disk_name.h - Header for disk name.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_DISK_NAME_H
#define _WALB_DISK_NAME_H

#include "common.h"

/**
 * Disk name length.
 */
#define DISK_NAME_LEN_USER 32
#ifdef __KERNEL__
#include <linux/genhd.h>
#else
#define DISK_NAME_LEN DISK_NAME_LEN_USER
#endif
#define ASSERT_DISK_NAME_LEN() ASSERT(DISK_NAME_LEN == DISK_NAME_LEN_USER)

#endif /* _WALB_DISK_NAME_H */
