/**
 * test_vmalloc.c - test vmalloc.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>
#include <linux/delay.h>

#include "linux/walb/logger.h"
#include "bio_entry.h"
#include "bio_wrapper.h"

static unsigned int obj_size_ = 1;
module_param_named(obj_size, obj_size_, uint, S_IRUGO);

struct test_struct
{
	int a;
	int b;
	void *p;
};

static int __init test_init(void)
{
	struct kmem_cache *cache;

#if 0
	bio_entry_init();
	bio_entry_exit();
#endif

#if 0
	bio_wrapper_init();
	bio_wrapper_exit();
#endif

	LOGn("sizeof bio_entry %zu bio_wrapper %zu\n",
		sizeof(struct bio_entry),
		sizeof(struct bio_wrapper));

	cache = kmem_cache_create(
		"test_bio_entry_cache", obj_size_, 0, 0, NULL);
	if (cache) {
		LOGn("kmem_cache_create size %u success.\n", obj_size_);
		msleep(1);
		kmem_cache_destroy(cache);
	} else {
		LOGn("kmem_cache_create size %u failed.\n", obj_size_);
	}

	return -1;
}

static void test_exit(void)
{
}

module_init(test_init);
module_exit(test_exit);
MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Test of bio_entry.");
MODULE_ALIAS("test_bio_entry");
