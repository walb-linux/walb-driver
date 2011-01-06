#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include "treemap.h"

static int __init test2_init(void)
{
        printk(KERN_INFO "test2_init begin\n");
        
        /* Hashmap test for debug. */
        if (treemap_test()) {
                printk(KERN_ERR "treemap_test() failed.\n");
                goto error;
        }

        printk(KERN_INFO "test2_init end\n");

        return 0;

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
