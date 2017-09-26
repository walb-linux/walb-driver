/**
 * test_vmalloc.c - test vmalloc.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include <linux/mm.h>
#include <linux/vmalloc.h>
#include <linux/time.h>
#include "linux/walb/logger.h"
#include "linux/walb/util.h"
#include "build_date.h"

static int __init test_init(void)
{
	size_t i, kbytes;
	struct timespec ts_bgn, ts_end, ts_time;
	u8 *p;

	LOGe("BUILD_DATE %s\n", BUILD_DATE);

	for (kbytes = 1; kbytes < 1024 * 32 + 1; kbytes *= 2) {

		getnstimeofday(&ts_bgn);
		p = vmalloc(kbytes * 1024);
		if (!p) {
			LOGe("allocation error %zu KB\n", kbytes);
			continue;
		}
		for (i = 0; i < kbytes * 1024; i++) {
			p[i] = 0;
		}
		getnstimeofday(&ts_end);

		ts_time = timespec_sub(ts_end, ts_bgn);
		LOGn("vmalloc %zu KB and fill: %ld.%09ld secs\n",
			kbytes, ts_time.tv_sec, ts_time.tv_nsec);

		vfree(p);
	}

	return -1;
}

static void test_exit(void)
{
}

module_init(test_init);
module_exit(test_exit);
MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Test of vmalloc.");
MODULE_ALIAS("test_vmalloc");
