/**
 * test_memblk_data.c - test functonalities of memblk_data.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include "memblk_data.h"

#include <linux/mm.h>
#include <linux/scatterlist.h>
#include "walb/util.h"
#include "build_date.h"

static int __init test_init(void)
{
        int i;
        const int loop = 10;

        LOGe("BUILD_DATE %s\n", BUILD_DATE);

        /* for (i = 0; i < loop; i ++) { */
        /*         test_memblk_data_simple(128, 512); */
        /*         test_memblk_data_simple(128, 1024); */
        /*         test_memblk_data_simple(128, 2048); */
        /*         test_memblk_data_simple(128, 4096); */
        /* } */

	mdata_init();
        for (i = 0; i < loop; i ++) {
                test_memblk_data(128, 512);
                test_memblk_data(128, 1024);
                test_memblk_data(128, 2048);
                test_memblk_data(128, 4096);
        }
	mdata_exit();
        return -1;
}

static void test_exit(void)
{
}

module_init(test_init);
module_exit(test_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Test of memblk_data.");
MODULE_ALIAS("test_memblk_data");
/* MODULE_ALIAS_BLOCKDEV_MAJOR(MEMBLK_MAJOR); */
