/**
 * test.c - Test module.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include "hashtbl.h"

static int __init test_init(void)
{
        printk(KERN_INFO "test_init begin\n");
        
        /* Hashmap test for debug. */
        if (hashtbl_test()) {
                printk(KERN_ERR "hashtbl_test() failed.\n");
                goto error;
        }

        if (hashtbl_cursor_test()) {
                printk(KERN_ERR "hashtbl_cursor_test() failed.\n");
                goto error;
        }

        printk(KERN_INFO "test_init end\n");
        
error:
        return -1;
}

static void test_exit(void)
{
}


module_init(test_init);
module_exit(test_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Test module");
MODULE_ALIAS("test");
