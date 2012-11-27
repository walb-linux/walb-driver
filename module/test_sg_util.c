/**
 * test_sg.c - test scatterlist and its utility.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include <linux/mm.h>
#include <linux/scatterlist.h>
#include "walb/util.h"
#include "sg_util.h"
#include "build_date.h"

static int __init test_init(void)
{
	int i;

	LOGe("BUILD_DATE %s\n", BUILD_DATE);

	test_scatterlist(8, 32);
	test_scatterlist(8, 4096);
	test_scatterlist(128, 32);
	test_scatterlist(128, 4096);
	test_scatterlist(256, 4096);
	test_scatterlist(1024, 4096);

	for (i = 0; i < 100; i++) {
		test_sg_pos();
	}

	for (i = 0; i < 100; i++) {
		test_sg_util();
	}

	return -1;
}

static void test_exit(void)
{
}

module_init(test_init);
module_exit(test_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Test of scatterlist.");
MODULE_ALIAS("test_sg");
/* MODULE_ALIAS_BLOCKDEV_MAJOR(MEMBLK_MAJOR); */
