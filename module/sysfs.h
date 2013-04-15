/**
 * sysfs.h - sysfs related header.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_SYSFS_H_KERNEL
#define WALB_SYSFS_H_KERNEL

#include "kern.h"

int walb_sysfs_init(struct walb_dev *wdev);
void walb_sysfs_exit(struct walb_dev *wdev);
void walb_sysfs_notify(struct walb_dev *wdev, const char *attr_name);

#endif /* WALB_SYSFS_H_KERNEL */
