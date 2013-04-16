/**
 * wdev_ioctl.h - walb device ioctl.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_DEV_IOCTL_H_KERNEL
#define WALB_DEV_IOCTL_H_KERNEL

#include "check_kernel.h"
#include "kern.h"

int walb_dispatch_ioctl_wdev(struct walb_dev *wdev, void __user *userctl);

#endif /* WALB_DEV_IOCTL_H_KERNEL */
