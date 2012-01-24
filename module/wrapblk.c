/**
 * wrapblk.c - Wrapper block device module for test.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */

#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include <linux/workqueue.h>

#include "wrapblk.h"


/**
 * Device number (major).
 */
int wrapblk_major = 0;
module_param(wrapblk_major, int, 0);
static int ndevices = 1;
module_param(ndevices, int, 0);

/**
 * Underlying devices.
 * ldev(log device) and ddev(data device).
 */
static int ldev_major = 0;
static int ldev_minor = 0;
static int ddev_major = 0;
static int ddev_minor = 0;
module_param(ldev_major, int, 0);
module_param(ldev_minor, int, 0);
module_param(ddev_major, int, 0);
module_param(ddev_minor, int, 0);

/* static int request_mode = RM_FULL; */
/* module_param(request_mode, int, 0); */

/* static struct walb_dev *Devices = NULL; */

/**
 * Workqueue for read/write.
 */
struct workqueue_struct *wq_ = NULL;

/*******************************************************************************
 * Prototypes of local functions.
 *******************************************************************************/

/* Module init/exit. */
static int __init wrapblk_init(void);
static void wrapblk_exit(void);

/*******************************************************************************
 * Implementation.
 *******************************************************************************/

static int __init wrapblk_init(void)
{
        return 0;
}

static void wrapblk_exit(void)
{
}

/*******************************************************************************
 * Module definitions.
 *******************************************************************************/

module_init(wrapblk_init);
module_exit(wrapblk_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Wrapper Block Device for Test");
MODULE_ALIAS(WRAPBLK_NAME);
/* MODULE_ALIAS_BLOCKDEV_MAJOR(WRAPBLK_MAJOR); */
