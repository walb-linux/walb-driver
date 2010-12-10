#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include "hashmap.h"

static int __init test_init(void)
{
        printk(KERN_INFO "test_init begin\n");
        
        /* Hashmap test for debug. */
        if (hashmap_test()) {
                printk(KERN_ERR "hashmap_test() failed.\n");
        }

        printk(KERN_INFO "test_init end\n");

        return 0;
}

static void test_exit(void)
{
}


module_init(test_init);
module_exit(test_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Test module");
MODULE_ALIAS("test");
