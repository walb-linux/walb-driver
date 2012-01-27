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

#include "walb/common.h"
#include "walb/disk_name.h"
#include "block_size.h"
#include "treemap.h"
#include "block_size.h"
#include "memblk.h"
#include "memblk_io.h"

/*******************************************************************************
 * Module variable definitions.
 *******************************************************************************/

/*  Device number (major). */
int memblk_major_ = 0;

/* Block size */
int logical_bs_ = 512;
int physical_bs_ = 4096;

/**
 * Each device capacity.
 * Size in bytes separated by comma.
 * "1m,2m,4m" means three devices, which size is 1m, 2m and 4m respectively.
 * Size suffix is permitted 'k' for kilo, 'm' for mega, and 'g' for giga.
 * Size unit is logical block size (which is ordinary 512).
 * Device files will be created as '0', '1', and '2' in /dev/memblk directory.
 */
char *memblk_devices_str_ = "1m";

/* Workqueues */
struct workqueue_struct *wqs_; /* single-thread */
struct workqueue_struct *wqm_; /* multi-thread */

/* Devices. */
#define MAX_N_DEVICES 16
struct memblk_dev *devices_[MAX_N_DEVICES];
int n_devices_ = 0; /* Number of active devices. */

/*******************************************************************************
 * Module parameter definitions.
 *******************************************************************************/

module_param_named(memblk_major, memblk_major_, int, S_IRUGO);
module_param_named(logical_bs, logical_bs_, int, S_IRUGO);
module_param_named(physical_bs, physical_bs_, int, S_IRUGO);
module_param_named(devices, memblk_devices_str_, charp, S_IRUGO);

/*******************************************************************************
 * Macro definitions.
 *******************************************************************************/

#define ASSERT_MEMBLK_DEV(mdev) assert_memblk_dev(mdev)

/*******************************************************************************
 * Function prototypes.
 *******************************************************************************/

static int memblk_open(struct block_device *bdev, fmode_t mode);
static int memblk_release(struct gendisk *gd, fmode_t mode);
static int memblk_ioctl(struct block_device *bdev, fmode_t mode,
                        unsigned int cmd, unsigned long arg);

static int devices_str_get_n_devices(const char* devices_str);
static u64 devices_str_get_capacity_of_nth_dev(const char* devices_str, int n, u32 logical_bs);

static void assert_memblk_dev(struct memblk_dev *mdev);
static struct memblk_dev* create_mdev(unsigned int minor, u64 capacity);
static void destroy_mdev(struct memblk_dev *mdev);
static int create_all_mdevs(void);
static void destroy_all_mdevs(void);
static int init_queue_and_disk(struct memblk_dev *mdev);
static void fin_queue_and_disk(struct memblk_dev *mdev);
static void register_mdev(struct memblk_dev *mdev);
static void unregister_mdev(struct memblk_dev *mdev);
static void register_all_mdevs(void);
static void unregister_all_mdevs(void);

/*******************************************************************************
 * Static function definitions.
 *******************************************************************************/

/**
 * Device operation.
 */
static int memblk_open(struct block_device *bdev, fmode_t mode)
{
        return 0;
}

/**
 * Device operation.
 */
static int memblk_release(struct gendisk *gd, fmode_t mode)
{
        return 0;
}

/**
 * Device operation.
 */
static int memblk_ioctl(struct block_device *bdev, fmode_t mode,
                        unsigned int cmd, unsigned long arg)
{
        return -ENOTTY;
}

/**
 * The device operations structure.
 */
static struct block_device_operations memblk_ops_ = {
	.owner           = THIS_MODULE,
	.open 	         = memblk_open,
	.release 	 = memblk_release,
	.ioctl	         = memblk_ioctl
};

/**
 * Get number of entries in devices_str.
 * It returns 3 for "1g,2g,3g".
 */
static int devices_str_get_n_devices(const char* devices_str)
{
        int n;
        int i;
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
static u64 devices_str_get_capacity_of_nth_dev(const char* devices_str, int n, u32 logical_bs)
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
        p_next = strchr(p, ',');
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

static void assert_memblk_dev(struct memblk_dev *mdev)
{
        ASSERT(mdev);
        ASSERT(mdev->capacity > 0);
        ASSERT(mdev->index);
        ASSERT(strlen(mdev->name) > 0);
        ASSERT(mdev->queue);
        ASSERT(mdev->gd);
}

/**
 * Create and initialize memblk_dev data.
 *
 * @minor minor device number.
 * @capacity Device capacity [logical block]
 * RETURN:
 * Created mdev in success, or NULL.
 * NOTE:
 * Call this before @register_mdev();
 */
static struct memblk_dev* create_mdev(unsigned int minor, u64 capacity)
{
        struct memblk_dev *mdev;
        u64 addr;
        u64 n_pb;
        u8 *memblk;
        
         /* Allocate */
        mdev = ZALLOC(sizeof(struct memblk_dev), GFP_KERNEL);
        if (mdev == NULL) {
                LOGe("memory allocation failed.\n");
                goto error0;
        }
        
        /* Initialize */
        mdev->minor = minor;
        mdev->capacity = capacity;
        init_block_size_op(&mdev->bs_op, logical_bs_, physical_bs_);
        mdev->index = NULL;
        mdev->make_request_fn = memblk_make_request;
        mdev->queue = NULL;
        mdev->gd = NULL;
        snprintf(mdev->name, MEMBLK_DEV_NAME_MAX_LEN, "%d", minor);

        /* Create an index. */
        mdev->index = map_create(GFP_KERNEL);
        if (!mdev->index) { goto error1; }

        /* Allocate blocks */
        n_pb = MCALL(&mdev->bs_op, required_n_pb, mdev->capacity);
        for (addr = 0; addr < n_pb; addr ++) {

                memblk = MALLOC(mdev->bs_op.physical_bs, GFP_KERNEL);
                if (!memblk) { goto error1; }
                if (map_add(mdev->index, addr, (unsigned long)memblk, GFP_KERNEL)) {
                        FREE(memblk);
                        goto error1;
                }
        }

        if (!init_queue_and_disk(mdev)) {
                goto error1;
        }

        ASSERT_MEMBLK_DEV(mdev);
        return mdev;

error1:
        destroy_mdev(mdev);
error0:
        return NULL;
}

/**
 * Destroy a memblk_dev data.
 *
 * @mdev mdev to destroy.
 * NOTE:
 * Call this after @unregister_mdev();
 */
static void destroy_mdev(struct memblk_dev *mdev)
{
        u64 addr, n_pb;
        unsigned long val;

        if (!mdev) { return; }
        
        fin_queue_and_disk(mdev);
        if (mdev->index) {
                /* Free all allocated blocks. */
                n_pb = MCALL(&mdev->bs_op, required_n_pb, mdev->capacity);
                for (addr = 0; addr < n_pb; addr ++) {
                        val = map_del(mdev->index, addr);
                        if (val == TREEMAP_INVALID_VAL) {
                                break;
                        } else {
                                FREE((u8*)val);
                        }
                }
                /* Destory the index. */
                map_destroy(mdev->index);
                mdev->index = NULL;
        }
        FREE(mdev);
}

/**
 * Create memory block devices.
 *
 * CONTEXT:
 * Non-IRQ. Using Global variables.
 * RETURN:
 * Non-zero in success, or 0.
 */
static int create_all_mdevs(void)
{
        int i;
        u64 capacity;

        /* Initialize devices_ data. */
        n_devices_ = devices_str_get_n_devices(memblk_devices_str_);
        ASSERT(n_devices_ > 0);
        ASSERT(n_devices_ <= MAX_N_DEVICES);
        memset(devices_, 0, sizeof(struct memblk_dev *) * MAX_N_DEVICES);

        /* Create each memblk_dev. */
        for (i = 0; i < n_devices_; i ++) {
                capacity = devices_str_get_capacity_of_nth_dev(memblk_devices_str_, i, logical_bs_);
                devices_[i] = create_mdev(i, capacity);
                if (! devices_[i]) {
                        LOGe("Create device %d failed.\n", i);
                        goto error0;
                }
        }
        return 1;

error0:
        destroy_all_mdevs();
        return 0;
}

/**
 * Destroy memory block devices.
 * CONTEXT:
 * Non-IRQ. Using Global variables.
 */
static void destroy_all_mdevs(void)
{
        int i;
        for (i = 0; i < n_devices_; i ++) {
                if (devices_[i]) {
                        destroy_mdev(devices_[i]);
                } else {
                        break;
                }
        }
}

/**
 * Initialize queue and disk data.
 *
 * CONTEXT:
 * Non-IRQ.
 * RETURN:
 * Non-zero in success, or 0.
 */
static int init_queue_and_disk(struct memblk_dev *mdev)
{
        struct request_queue *q;
        struct gendisk *gd;
        
        ASSERT(mdev);

        /* Cleanup */
        mdev->queue = NULL;
        mdev->gd = NULL;

        /* Allocate and initialize queue. */
        q = blk_alloc_queue(GFP_KERNEL);
        if (!q) {
                LOGe("blk_alloc_queue failed.\n");
                goto error0;
        }
        blk_queue_make_request(q, mdev->make_request_fn);

        blk_queue_logical_block_size(q, logical_bs_);
        blk_queue_physical_block_size(q, physical_bs_);
        q->queuedata = mdev;
        mdev->queue = q;

        /* Allocate and initialize disk. */
        gd = alloc_disk(1);
        if (!gd) {
                LOGe("alloc_disk failed.\n");
                goto error1;
        }
        gd->major = memblk_major_;
        gd->first_minor = mdev->minor;
        
        gd->fops = &memblk_ops_;
        gd->queue = mdev->queue;
        gd->private_data = mdev;
        set_capacity(gd, mdev->capacity);
        snprintf(gd->disk_name, DISK_NAME_LEN,
                 "%s/%s", MEMBLK_DIR_NAME, mdev->name);
        mdev->gd = gd;

        /* Set device id. */
        mdev->devt = MKDEV(gd->major, gd->first_minor);        

        return 1;

error1:
        fin_queue_and_disk(mdev);
error0:
        return 0;
}

/**
 * Finalize queue and disk data.
 *
 * CONTEXT:
 * Non-IRQ.
 */
static void fin_queue_and_disk(struct memblk_dev *mdev)
{
        ASSERT(mdev);

        if (mdev->gd) {
                put_disk(mdev->gd);
                mdev->gd = NULL;
        }
        if (mdev->queue) {
                blk_cleanup_queue(mdev->queue);
                mdev->queue = NULL;
        }
}

/**
 * Register mdev.
 * Call this after @create_mdev().
 */
static void register_mdev(struct memblk_dev *mdev)
{
        ASSERT(mdev);
        ASSERT(mdev->gd != NULL);

        add_disk(mdev->gd);
}

/**
 * Unregister mdev.
 * Call this before @destroy_mdev().
 */
static void unregister_mdev(struct memblk_dev *mdev)
{
        if (mdev->gd) {
                del_gendisk(mdev->gd);
        }
}

static void register_all_mdevs(void)
{
        int i;

        for (i = 0; i < n_devices_; i ++) {
                if (devices_[i]) {
                        register_mdev(devices_[i]);
                } else {
                        break;
                }
        }
}

static void unregister_all_mdevs(void)
{
        int i;
        
        for (i = 0; i < n_devices_; i ++) {
                if (devices_[i]) {
                        unregister_mdev(devices_[i]);
                } else {
                        break;
                }
        }

}



/*******************************************************************************
 * Global function definitions.
 *******************************************************************************/







/*******************************************************************************
 * Init/exit function definitions.
 *******************************************************************************/

static int __init memblk_init(void)
{
        /* Register. */
        memblk_major_ = register_blkdev(memblk_major_, MEMBLK_NAME);
        if (memblk_major_ <= 0) {
                LOGe("unable to get major device number.\n");
                return -EBUSY;
        }

        /* Workqueue. */
        wqs_ = create_singlethread_workqueue(MEMBLK_SINGLE_WQ_NAME);
        if (wqs_ == NULL) {
                LOGe("create single-thread workqueue failed.\n");
                goto error0;
        }
        wqm_ = create_workqueue(MEMBLK_MULTI_WQ_NAME);
        if (wqm_ == NULL) {
                LOGe("create multi-thread workqueue failed.\n");
                goto error1;
        }

        /* Create all devices. */
        if (!create_all_mdevs()) {
                LOGe("create all memblk devices failed.\n");
                goto error2;
        }

        /* Register all devices. */
        register_all_mdevs();

        return 0;
#if 0
error3:
        destroy_all_mdevs();
#endif
error2:
        if (wqm_) { destroy_workqueue(wqm_); }
error1:
        if (wqs_) { destroy_workqueue(wqs_); }
error0:
	unregister_blkdev(memblk_major_, MEMBLK_NAME);
        return -ENOMEM;
}

static void memblk_exit(void)
{
        unregister_all_mdevs();
        destroy_all_mdevs();
        flush_workqueue(wqm_);
        flush_workqueue(wqs_);
        destroy_workqueue(wqm_);
        destroy_workqueue(wqs_);
        unregister_blkdev(memblk_major_, MEMBLK_NAME);
}

module_init(memblk_init);
module_exit(memblk_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Memory Block Device for Test");
MODULE_ALIAS(MEMBLK_NAME);
/* MODULE_ALIAS_BLOCKDEV_MAJOR(MEMBLK_MAJOR); */
