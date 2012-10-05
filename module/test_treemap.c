/**
 * test_treemap.c - test_treemap module.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include "treemap.h"


struct treemap_memory_manager mmgr_;

static bool initialize(void)
{
	bool ret;
	ret = initialize_treemap_memory_manager(
		&mmgr_, 1,
		"test_node_cache",
		"test_cell_head_cache",
		"test_cell_cache");
	return ret;
}

static void finalize(void)
{
	finalize_treemap_memory_manager(&mmgr_);
}

static int __init test_treemap_init(void)
{
	printk(KERN_INFO "test_treemap_init begin\n");

	if (!initialize()) {
		printk(KERN_ERR "initialize() failed.\n");
		goto error;
	}
	
	/* Treemap test for debug. */
	if (map_test()) {
		printk(KERN_ERR "map_test() failed.\n");
		goto error;
	}
	if (map_cursor_test()) {
		printk(KERN_ERR "map_cursor_test() failed.\n");
		goto error;
	}
	if (multimap_test()) {
		printk(KERN_ERR "multimap_test() failed.\n");
		goto error;
	}
	if (multimap_cursor_test()) {
		printk(KERN_ERR "multimap_cursor_test() failed.\n");
		goto error;
	}
	
	finalize();
	printk(KERN_INFO "test_treemap_init end\n");
	
error:
	return -1;
}

static void test_treemap_exit(void)
{
}


module_init(test_treemap_init);
module_exit(test_treemap_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Test treemap module");
MODULE_ALIAS("test_treemap");
