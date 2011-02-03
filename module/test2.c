/**
 * test2.c - test2 module.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include "treemap.h"

static int __init test2_init(void)
{
        printk(KERN_INFO "test2_init begin\n");
        
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

        printk(KERN_INFO "test2_init end\n");

error:
        return -1;
}

static void test2_exit(void)
{
}


module_init(test2_init);
module_exit(test2_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Test module 2");
MODULE_ALIAS("test2");
