/**
 * memblk.c - Memory block device driver for performance test.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include <linux/blkdev.h>
#include <linux/workqueue.h>
#include <linux/string.h>

#include "block_size.h"
#include "treemap.h"

/*  Device number (major). */
int memblk_major_ = 0;

/* Block size */
int logical_bs_ = 512;
int physical_bs_ = 512;

/**
 * Device definitions.
 * Size in bytes separated by comma.
 * "1m,2m,4m" means three devices, which size is 1m, 2m and 4m respectively.
 * Size suffix is permitted 'k' for kilo, 'm' for mega, and 'g' for giga.
 * Size unit is logical block size (which is ordinary 512).
 * Device files will be created as '0', '1', and '2' in /dev/memblk directory.
 */
char *memblk_devices_str_ = "1m";

/* Workqueues */
struct workqueue *wqs_; /* single-thread */
struct workqueue *wqm_; /* multi-thread */

/* Module parameters */
module_param_named(memblk_major, memblk_major_, int, 0);
module_param_named(logical_bs, logical_bs_, int, 0);
module_param_named(physical_bs, physical_bs_, int, 0);
module_param_named(devices, memblk_devices_str_, charp, 0);

/**
 * Memory block device.
 */
struct memblk_dev
{
        int is_active; /* Non-zero for active. */
        int device_id; /* 0 <= device_id < MAX_N_DEVICES */
        u64 capacity; /* Device capacity [logical block] */
        struct block_size_op bs_op;
        
        map_t *index; /* Key: physical address,
                         Value: pointer to allocated memory with physical block size. */
};

/* Devices. */
#define MAX_N_DEVICES 16
struct memblk_dev devices_[MAX_N_DEVICES];
int n_devices_ = 0; /* Number of active devices. */

/**
 * Get number of entries in devices_str.
 * It returns 3 for "1g,2g,3g".
 */
static int devices_str_get_n_devices(const char* devices_str)
{
        int n;
        int i, n;
        int len = strlen(devices_str);

        if (len == 0) {
                return 0; /* No data. */
        }
        n = 1;
        for (i = 0; i < len; i ++) {
                if (devices_str[i] == ',' && 
                    devices_str[i + 1] != '\0') {
                        n ++;
                }
        }
        ASSERT(n > 0);
        return n;
}

/**
 * Get capacity 
 * @n 0 <= n < devices_str_get_n_devices.
 * RETURN:
 * Size in logical blocks.
 */
static u64 devices_str_get_capacity_of_nth_dev(const char* devices_str, int n, int logical_bs)
{
        int i;
        char *p = devices_str;
        char *p_next;
        int len;
        u64 capacity;

        ASSERT(n >= 0);

        /* Skip ',' for n times. */
        for (i = 0; i < n; i ++) {
                p = strchr(p, ',') + 1;
        }
        ASSERT(p != NULL);

        /* Get length of the entry string. */
        p_next = strchr(p ',');
        if (p_next) {
                len = p_next - p;
        } else {
                len = strlen(p);
        }

        /* Parse number (with suffix). */
        capacity = 0;
        for (i = 0; i < len; i ++) {

                if ('0' <= p[i] && p[i] <= '9') {
                        capacity *= 10;
                        capacity += (u64)(p[i] - '0');
                } else {
                        switch (p[i]) {
                        case 't':
                                capacity *= 1024;
                        case 'g':
                                capacity *= 1024;
                        case 'm':
                                capacity *= 1024;
                        case 'k':
                                capacity *= 1024;
                                break;
                        default:
                                ASSERT(0);
                        }
                }
        }
        return capacity;
}

void assert_memblk_dev(struct memblk_dev *mdev)
{
        ASSERT(mdev);
        ASSERT(mdev->is_active);
        ASSERT(mdev->capacity > 0);
        ASSERT(mdev->bs_op);
}
#define ASSERT_MEMBLK_DEV(mdev) assert_memblk_dev(mdev)

/**
 * Initialize memblk_dev data.
 * 
 * RETURN:
 * Non-zero in success, or 0.
 */
static int init_memblk_dev(struct memblk_dev *mdev, int device_id)
{
        u64 addr;
        u64 n_pb;
        u8 *memblk;
        
        /* Initialize */
        mdev->is_active = 1;
        mdev->device_id = device_id;
        mdev->capacity = devices_str_get_capacity_of_nth_dev(
                memblk_devices_str_, device_id, logical_bs_);
        init_block_size_op(&mdev->bs_op, logical_bs_, physical_bs);
        assert_memblk_dev(mdev);

        /* Allocate index */
        mdev->index = map_create(GFP_KERNEL);
        if (!mdev->index) { goto error0; }

        /* Allocate blocks */
        n_pb = MCALL(&mdev->bs_op, required_n_pb, mdev->capacity);
        for (addr = 0; addr < n_pb; addr ++) {

                memblk = MALLOC(mdev->bs_op.physical_bs, GFP_KERNEL);
                if (!memblk) { goto error1; }
                if (map_add(mdev->index, addr, memblk, GFP_KERNEL)) {
                        FREE(memblk);
                        goto error1;
                }
        }
        return 1;

error1:
        exit_memblk_dev(mdev);
error0:
        return 0;
}

/**
 * Exit a memblk_dev.
 */
static void exit_memblk_dev(struct memblk_dev *mdev)
{
        u64 addr, n_pb;
        unsigned long val;
        
        if (mdev) {
                if (mdev->index) {
                        /* Free all allocated blocks. */
                        n_pb = MCALL(&mdev->bs_op, required_n_pb, mdev->capacity);
                        for (addr = 0; addr < n_pb; addr ++) {
                                val = map_del(mdev->index, addr);
                                if (val == TREEMAP_INVALID_VAL) {
                                        break;
                                } else {
                                        FREE(val);
                                }
                        }
                        /* Destory map. */
                        map_destroy(mdev->index);
                        mdev->index = NULL;
                }
        }
}

/**
 * Create memory block devices.
 *
 * CONTEXT:
 * Non-IRQ. Using Global variables.
 * RETURN:
 * Non-zero in success, or 0.
 */
static int create_all_memblk_devices()
{
        int i;
        u64 capacity;

        /* Clean up devices_ data. */
        for (i = 0; i < MAX_N_DEVICES; i ++ ) {
                memset(&devices_[i], 0, sizeof(struct memblk_dev));
        }

        /* Initialize each memblk_dev. */
        n_devices_ = devices_str_get_n_devices(memblk_devices_str_);
        ASSERT(n_devices_ > 0);
        ASSERT(n_devices_ <= MAX_N_DEVICES);
        for (i = 0; i < n_devices_; i ++) {
                if (! init_memblk_dev(&devices_[i], i)) {
                        LOGe("Initialize device %d failed.\n", i);
                        goto error0;
                }
        }
        return 1;

error0:
        return 0;
}

/**
 * Destroy memory block devices.
 */
static void destroy_all_memblk_devices()
{
        for (i = 0; i < n_devices_; i ++) {
                exit_memblk_dev(&devices_[i]);
        }
}

/**
 * Create mdev.
 * Call this before @register_mdev();
 */
static memblk_dev* create_mdev(unsigned int minor, const char* name)
{
        /* now editing */

        return NULL;
}

/**
 * Destroy mdev.
 * Call this after @unregister_mdev();
 */
static void destroy_mdev(struct memblk_dev *mdev)
{
        /* now editing */
}

/**
 * Register mdev.
 * Call this after @create_mdev().
 */
static void register_mdev(struct memblk_dev *mdev)
{
        /* now editing */
}

/**
 * Unregister mdev.
 * Call this before @destroy_mdev().
 */
static void unregister_mdev(struct memblk_dev *mdev)
{
        /* now editing */
}


static int __init memblk_init(void)
{
        /* Register. */
        memblk_major = register_blkdev(memblk_major, "memblk");
        if (memblk_major <= 0) {
                printk_e("unable to get major device number.\n");
                return -EBUSY;
        }

        /* Workqueue. */
        wqs_ = create_singlethread_workqueue("memblk_s");
        if (wqs_ == NULL) {
                printk_e("create single-thread workqueue failed.\n");
                goto error0;
        }
        wqm_ = create_workqueue("memblk_m");
        if (wqm_ == NULL) {
                printk_e("create multi-thread workqueue failed.\n");
                goto error1;
        }

        /* Allocate memory. */
        if (!create_all_memblk_devices()) {
                printk_e("create all memblk devices failed.\n");
                goto error2;
        }

        /* Create disks */

        
        
        /* now editing */

        return 0;
#if 0
error3:
        destroy_all_memblk_devices());
#endif
error2:
        if (wqm_) { destroy_workqueue(wqm_); }
error1:
        if (wqs_) { destroy_workqueue(wqs_); }
error0:
	unregister_blkdev(memblk_major, "memblk");
        return -ENOMEM;
}

static void memblk_exit(void)
{
}

module_init(memblk_init);
module_exit(memblk_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Memory Block Device for Test");
MODULE_ALIAS("memblk");
/* MODULE_ALIAS_BLOCKDEV_MAJOR(MEMBLK_MAJOR); */
