/**
 * walb_alldevs.c - for all devices management.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * Written by: Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 */

#include <linux/list.h>
#include <linux/rwsem.h>

#include "hashtbl.h"
#include "walb_alldevs.h"

/**
 * Lock to access all functions declared in this header.
 */
static struct rw_semaphore all_wdevs_lock_;

/**
 * List of struct walb_dev.
 * This is used just for 'for all' operations.
 */
static struct list_head all_wdevs_;

/**
 * Hash tables to get wdev by name or uuid.
 *
 * htbl_minor's key size is sizeof(unsigned int).
 *             value is pointer to struct walb_dev.
 * htbl_name's key size is 64.
 *             value is pointer to struct walb_dev.
 * htbl_uuid's key size is 16.
 *             value is pointer to struct walb_dev.
 */
static struct hash_tbl *htbl_minor_;
static struct hash_tbl *htbl_name_;
static struct hash_tbl *htbl_uuid_;

/**
 * For debug.
 */
static atomic_t is_available_ = ATOMIC_INIT(0);

#define START()                                                        \
        do {                                                           \
                if (atomic_inc_return(&is_available_) != 1) { BUG(); } \
        } while (0)

#define STOP()                                                         \
        do {                                                           \
                if (atomic_dec_return(&is_available_) != 0) { BUG(); } \
        } while (0)

#define CHECK()                                                   \
        do {                                                      \
                if (atomic_read(&is_available_) != 1) { BUG(); }  \
        } while (0)                             

/*******************************************************************************
 * Static functions.
 *******************************************************************************/


/**
 * Get length of walb device.
 *
 * @return 0 to WALB_DEV_NAME_MAX_LEN - 1.
 */
static size_t get_wdev_name_len(const struct walb_dev *wdev)
{
        ASSERT(wdev != NULL);
        ASSERT(wdev->lsuper0 != NULL);
        return strnlen(wdev->lsuper0->name, WALB_DEV_NAME_MAX_LEN - 1);
}


/*******************************************************************************
 * Global functions.
 *******************************************************************************/

/**
 * Initialize alldevs functionality.
 *
 * @return 0 in success, or -ENOMEM.
 */
int alldevs_init(void)
{
        INIT_LIST_HEAD(&all_wdevs_);

        htbl_name_ = hashtbl_create(HASHTBL_MAX_BUCKET_SIZE, GFP_KERNEL);
        if (htbl_name_ == NULL) { goto error0; }
        
        htbl_uuid_ = hashtbl_create(HASHTBL_MAX_BUCKET_SIZE, GFP_KERNEL);
        if (htbl_uuid_ == NULL) { goto error1; }

        htbl_minor_ = hashtbl_create(HASHTBL_MAX_BUCKET_SIZE, GFP_KERNEL);
        if (htbl_minor_ == NULL) { goto error2; }
        
        START();
        
        return 0;

error2:
        hashtbl_destroy(htbl_uuid_);
error1:
        hashtbl_destroy(htbl_name_);
error0:
        return -ENOMEM;
}

/**
 * Exit alldevs functionality.
 */
void alldevs_exit(void)
{
        STOP();
        
        /* Call this after all walb devices has stopped. */

        ASSERT(list_empty(&all_wdevs_));
        ASSERT(hashtbl_is_empty(htbl_minor_));
        ASSERT(hashtbl_is_empty(htbl_uuid_));
        ASSERT(hashtbl_is_empty(htbl_name_));
        
        hashtbl_destroy(htbl_minor_);
        hashtbl_destroy(htbl_uuid_);
        hashtbl_destroy(htbl_name_);
}

/**
 * Search wdev with device minor.
 * Traversing wdev list.
 * Read lock is required.
 */
#if 0
struct walb_dev* search_wdev_with_minor(unsigned int minor)
{
        struct walb_dev *wdev, *wdev_next;
        dev_t wdevt;
        
        /* odd -> even */
        if (minor % 2 == 1) { -- minor; }

        wdevt = MKDEV(walb_major, minor);

        list_for_each_entry_safe(wdev, wdev_next, &all_wdevs_, list) {
                
                if (wdev->devt == wdevt) { return wdev; }
        }

        return NULL; /* not found. */
}
#endif

/**
 * Search wdev with device minor id.
 * Using htbl_minor_.
 *
 * @LOCK read lock is required.
 */
struct walb_dev* search_wdev_with_minor(unsigned int minor)
{
        CHECK();
        return hashtbl_lookup(htbl_minor_,
                              (const u8 *)&minor, sizeof(unsigned int));
}

/**
 * Search wdev with device name.
 * Using htbl_name_.
 *
 * @LOCK read lock is required.
 */
struct walb_dev* search_wdev_with_name(const char* name)
{
        size_t len;

        CHECK();
        len = strnlen(name, WALB_DEV_NAME_MAX_LEN - 1);
        return hashtbl_lookup(htbl_name_, (const u8 *)name, len);
}

/**
 * Search wdev with device uuid.
 * Using htbl_uuid_.
 * 
 * @LOCK read lock is required.
 */
struct walb_dev* search_wdev_with_uuid(const u8* uuid)
{
        CHECK();
        return hashtbl_lookup(htbl_uuid_, uuid, 16);
}


/**
 * Add walb device alldevs list and hash tables.
 *
 * @wdev walb device to add.
 *
 * @return 0 in success,
 *         -ENOMEM in memory allocation failure,
 *         -EPERM in name or
 *
 * @LOCK write lock is required.
 */
int alldevs_add(struct walb_dev* wdev)
{
        size_t len;
        int ret;
        char buf[16 * 2 + 1];
        unsigned int minor;

        CHECK();
        minor = MINOR(wdev->devt);
        ret = hashtbl_add(htbl_minor_,
                          (const u8 *)&minor, sizeof(unsigned int),
                          wdev, GFP_KERNEL);
        if (ret != 0) {
                if (ret == -EPERM) {
                        printk_e("alldevs_add: minor %u is already registered.\n",
                                 MINOR(wdev->devt));
                }
                goto error0;
        }
        
        len = get_wdev_name_len(wdev);
        ret = hashtbl_add(htbl_name_,
                          wdev->lsuper0->name, len,
                          wdev, GFP_KERNEL);
        if (ret != 0) {
                if (ret == -EPERM) {
                        printk_e("alldevs_add: name %s is already registered.\n",
                                 wdev->lsuper0->name);
                }
                goto error1;
        }

        ret = hashtbl_add(htbl_uuid_,
                          wdev->lsuper0->uuid, 16,
                          wdev, GFP_KERNEL);
        if (ret != 0) {
                if (ret == -EPERM) {
                        sprint_uuid(buf, wdev->lsuper0->uuid);
                        printk_e("alldevs_add: uuid %s is already registered.\n",
                                 buf);
                }
                goto error2;
        }
        
        list_add_tail(&wdev->list, &all_wdevs_);
        return 0;

/* error3: */
/*         hashtbl_del(htbl_uuid_, wdev->lsuper0->uuid, 16); */
error2:
        hashtbl_del(htbl_name_, wdev->lsuper0->name, len);
error1:
        hashtbl_del(htbl_minor_, (const u8 *)&minor, sizeof(unsigned int));
error0:
        return ret;
}

/**
 * Delete walb device from alldevs list and hash tables.
 *
 * @wdev walb device to del.
 *
 * @LOCK write lock required.
 */
void alldevs_del(struct walb_dev* wdev)
{
        size_t len;
        struct walb_dev *tmp0, *tmp1, *tmp2;
        unsigned int wminor;

        CHECK();

        len = get_wdev_name_len(wdev);
        wminor = MINOR(wdev->devt);
        
        tmp0 = hashtbl_del(htbl_uuid_, wdev->lsuper0->uuid, 16);
        tmp1 = hashtbl_del(htbl_name_, wdev->lsuper0->name, len);
        tmp2 = hashtbl_del(htbl_minor_, (const u8 *)&wminor, sizeof(unsigned int));

        ASSERT(wdev == tmp0);
        ASSERT(wdev == tmp1);
        ASSERT(wdev == tmp2);
        list_del(&wdev->list);
}

/**
 * Return any of walb devices in the list and
 * delete it from alldevs list and hash tables.
 *
 * @return
 *
 * @LOCK write lock required.
 */
struct walb_dev* alldevs_pop(void)
{
        struct walb_dev *wdev;
        
        CHECK();
        if (list_empty(&all_wdevs_)) {
                return NULL;
        }

        wdev = list_first_entry(&all_wdevs_, struct walb_dev, list);
        alldevs_del(wdev);
        
        return wdev;
}

/**
 * Get free minor id.
 * This is not efficient implementation.
 *
 * @LOCK read lock required.
 */
unsigned int get_free_minor()
{
        unsigned int minor = 0;

        CHECK();
        while (hashtbl_lookup(htbl_minor_,
                              (const u8 *)&minor, sizeof(unsigned int))) {
                minor += 2;
        }
        return minor;
}

/**
 * Read lock.
 */
void alldevs_read_lock(void)
{
        CHECK();
        down_read(&all_wdevs_lock_);
}

/**
 * Read unlock.
 */
void alldevs_read_unlock(void)
{
        CHECK();
        up_read(&all_wdevs_lock_);
}

/**
 * Write lock.
 */
void alldevs_write_lock(void)
{
        CHECK();
        down_write(&all_wdevs_lock_);
}

/**
 * Write unlock.
 */
void alldevs_write_unlock(void)
{
        CHECK();
        up_write(&all_wdevs_lock_);
}
