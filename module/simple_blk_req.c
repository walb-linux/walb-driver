/**
 * simple_blk_req.c - Simple block device with req interface.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include "block_size.h"
#include "size_list.h"
#include "simple_blk.h"
#include "simple_blk_req.h"

/*******************************************************************************
 * Module variables definition.
 *******************************************************************************/

/* Device size list string. The unit of each size is bytes. */
char *device_size_list_str_ = "1m";
/* Minor id start. */
int start_minor_ = 0;

/* Logical block size is 512. */
#define LOGICAL_BLOCK_SIZE 512
/* Physical block size. */
int physical_block_size_ = 4096;

/* Block sizes. */
struct block_sizes blksiz_;

/* Number of devices. */
unsigned int n_devices_ = 0;

/* Sleep in ms between seconds. */
int sleep_ms_ = 0;

/*******************************************************************************
 * Module parameters definition.
 *******************************************************************************/

module_param_named(device_size_list, device_size_list_str_, charp, S_IRUGO);
module_param_named(start_minor, start_minor_, int, S_IRUGO);
module_param_named(pbs, physical_block_size_, int, S_IRUGO);
module_param_named(sleep, sleep_ms_, int, S_IRUGO | S_IWUSR);

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

static unsigned int get_minor(unsigned int id);
static bool register_alldevs(void);
static void unregister_alldevs(void);
static bool start_alldevs(void);
static void stop_alldevs(void);

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

static unsigned int get_minor(unsigned int id)
{
        return (unsigned int)start_minor_ + id;
}

static bool register_alldevs(void)
{
        unsigned int i;
        u64 capacity;
        bool ret;
        struct simple_blk_dev *sdev;
        
        for (i = 0; i < n_devices_; i ++) {
                capacity = sizlist_nth_size(device_size_list_str_, i)
                        / LOGICAL_BLOCK_SIZE;
                ASSERT(capacity > 0);

                ret = sdev_register_with_req(get_minor(i), capacity, &blksiz_,
                                             simple_blk_req_request_fn);
                
                if (!ret) {
                        goto error;
                }
                sdev = sdev_get(get_minor(i));
                if (!create_private_data(sdev)) {
                        goto error;
                }
                customize_sdev(sdev);
        }
        return true;
error:
        unregister_alldevs();
        return false;
}

static void unregister_alldevs(void)
{
        unsigned int i;
        struct simple_blk_dev *sdev;
        
        ASSERT(n_devices_ > 0);
        
        for (i = 0; i < n_devices_; i ++) {

                sdev = sdev_get(get_minor(i));
                if (sdev) {
                        destroy_private_data(sdev);
                }
                sdev_unregister(get_minor(i));
        }
}

static bool start_alldevs(void)
{
        unsigned int i;

        ASSERT(n_devices_ > 0);
        for (i = 0; i < n_devices_; i ++) {
                if (!sdev_start(get_minor(i))) {
                        goto error;
                }
        }
        return true;
error:
        stop_alldevs();
        return false;
}


static void stop_alldevs(void)
{
        unsigned int i;
        ASSERT(n_devices_ > 0);
        
        for (i = 0; i < n_devices_; i ++) {
                sdev_stop(get_minor(i));
        }
}

/*******************************************************************************
 * Init/exit definition.
 *******************************************************************************/

static int __init simple_blk_init(void)
{
        blksiz_init(&blksiz_, LOGICAL_BLOCK_SIZE, physical_block_size_);

        n_devices_ = sizlist_length(device_size_list_str_);
        ASSERT(n_devices_ > 0);
        ASSERT(start_minor_ >= 0);

        pre_register();
        
        if (!register_alldevs()) {
                goto error0;
        }
        if (!start_alldevs()) {
                goto error1;
        }

        return 0;
#if 0
error2:
        stop_alldevs();
#endif
error1:
        unregister_alldevs();
error0:
        return -1;
}

static void simple_blk_exit(void)
{
        stop_alldevs();
        unregister_alldevs();
        post_unregister();
}

module_init(simple_blk_init);
module_exit(simple_blk_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Simple block req device for Test");
MODULE_ALIAS("simple_blk_req");
