/**
 * test_kmem_cache.c - test kmem_cache.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include <linux/slab.h>

#include "linux/walb/common.h"
#include "linux/walb/util.h"

struct kmem_cache *cache_;

struct some_data
{
	int a;
	int b;
};

void init_some_data(void *p)
{
	struct some_data *d = (struct some_data*)p;
	d->a = 1;
	d->b = 2;
}


void test_kmem_cache(void)
{
	int i;
	struct some_data *some_data[10];

	cache_ = kmem_cache_create("test_kmem_cache_some_data",
				sizeof(struct some_data), 0, 0, init_some_data);

	for (i = 0; i < 10; i++) {
		some_data[i] = (struct some_data *)kmem_cache_alloc(cache_, GFP_KERNEL);
	}
	for (i = 0; i < 10; i++) {
		ASSERT(some_data[i]->a == 1);
		ASSERT(some_data[i]->b == 2);
	}

	for (i = 0; i < 10; i++) {
		kmem_cache_free(cache_, some_data[i]);
	}
	kmem_cache_destroy(cache_);
}

static int __init test_init(void)
{
	test_kmem_cache();
	return -1;
}

static void test_exit(void)
{
}

module_init(test_init);
module_exit(test_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Test kmem_cache.");
MODULE_ALIAS("test_kmem_cache");
