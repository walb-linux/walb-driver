/**
 * test_bdev.c - test block_device.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include <linux/mm.h>
#include <linux/blkdev.h>
#include "linux/walb/util.h"
#include "linux/walb/logger.h"
#include "build_date.h"

unsigned int major_ = UINT_MAX;
unsigned int minor_ = UINT_MAX;
char *path_ = NULL;

module_param_named(major, major_, uint, S_IRUGO);
module_param_named(minor, minor_, uint, S_IRUGO);
module_param_named(path, path_, charp, S_IRUGO);

static void lock_(void)
{
}

static int __init test_init(void)
{
	dev_t dev;
	int err = 0;
	struct block_device *bdev;

	LOGe("BUILD_DATE %s\n", BUILD_DATE);

	if (path_) {
		bdev = blkdev_get_by_path(
			path_, FMODE_READ|FMODE_WRITE|FMODE_EXCL, lock_);
		if (IS_ERR(bdev)) {
			err = PTR_ERR(bdev);
			goto error0;
		}
		blkdev_put(bdev, FMODE_READ|FMODE_WRITE|FMODE_EXCL);
	}

	if (major_ != UINT_MAX && minor_ != UINT_MAX) {
		dev = MKDEV(major_, minor_);
		bdev = blkdev_get_by_dev(
			dev, FMODE_READ|FMODE_WRITE|FMODE_EXCL, lock_);
		if (IS_ERR(bdev)) {
			err = PTR_ERR(bdev);
			goto error0;
		}
		blkdev_put(bdev, FMODE_READ|FMODE_WRITE|FMODE_EXCL);
	}
	LOGn("succeeded.\n");
	return -1;

error0:
	LOGn("failed %d.\n", err);
	return -1;
}

static void test_exit(void)
{
}

module_init(test_init);
module_exit(test_exit);
MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Test of bdev.");
MODULE_ALIAS("test_bdev");
/* MODULE_ALIAS_BLOCKDEV_MAJOR(MEMBLK_MAJOR); */
