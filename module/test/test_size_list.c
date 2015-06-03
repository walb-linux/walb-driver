/**
 * test_size_list.c - test size_list.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include "linux/walb/logger.h"
#include "linux/walb/util.h"
#include "build_date.h"
#include "../proto/size_list.h"

static int __init test_init(void)
{
	LOGe("BUILD_DATE %s\n", BUILD_DATE);

	test_sizlist();
	return -1;
}

static void test_exit(void)
{
}

module_init(test_init);
module_exit(test_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Test of size list.");
MODULE_ALIAS("test_size_list");
