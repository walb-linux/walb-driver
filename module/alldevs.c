/**
 * alldevs.c - for multiple devices management.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/list.h>
#include <linux/rwsem.h>

#include "treemap.h"
#include "hashtbl.h"
#include "alldevs.h"

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
 * Number of devices.
 */
static unsigned int n_devices_;

/**
 * Tree map or hash tables to get wdev by minor, name or uuid.
 *
 * map_minor's key size is unsigned int minor.
 *	       value is pointer to struct walb_dev.
 * htbl_name's key size is 64.
 *	       value is pointer to struct walb_dev.
 * htbl_uuid's key size is 16.
 *	       value is pointer to struct walb_dev.
 */
static struct map *map_minor_;
static struct hash_tbl *htbl_name_;
static struct hash_tbl *htbl_uuid_;

/**
 * Memory manager for this module.
 */
static struct treemap_memory_manager mmgr_;

/**
 * For debug.
 */
static atomic_t is_available_ = ATOMIC_INIT(0);

#define CHECK_START() do {					\
		if (atomic_inc_return(&is_available_) != 1) {	\
			BUG();					\
		}						\
	} while (0)

#define CHECK_STOP() do {					\
		if (atomic_dec_return(&is_available_) != 0) {	\
			BUG();					\
		}						\
	} while (0)

#define CHECK_RUNNING() do {				\
		if (atomic_read(&is_available_) != 1) { \
			BUG();				\
		}					\
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
/*
	なぜ-1をしているのか
	値チェックは外部入力時に行われているのだからstrlen()でよいだろう
*/
	return strnlen(get_super_sector(wdev->lsuper0)->name,
		WALB_DEV_NAME_MAX_LEN - 1);
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
	bool ret;

	INIT_LIST_HEAD(&all_wdevs_);

	ret = initialize_treemap_memory_manager_kmalloc(&mmgr_, 1);
	if (!ret) { goto error0; }

	htbl_name_ = hashtbl_create(HASHTBL_MAX_BUCKET_SIZE, GFP_KERNEL);
	if (!htbl_name_) { goto error1; }

	htbl_uuid_ = hashtbl_create(HASHTBL_MAX_BUCKET_SIZE, GFP_KERNEL);
	if (!htbl_uuid_) { goto error2; }

	map_minor_ = map_create(GFP_KERNEL, &mmgr_);
	if (!map_minor_) { goto error3; }

	init_rwsem(&all_wdevs_lock_);

	CHECK_START();

	return 0;

#if 0
error4:
	map_destroy(map_minor_);
	map_minor_ = NULL;
#endif
error3:
	hashtbl_destroy(htbl_uuid_);
	htbl_uuid_ = NULL;
error2:
	hashtbl_destroy(htbl_name_);
	htbl_name_ = NULL;
error1:
	finalize_treemap_memory_manager(&mmgr_);
error0:
	return -ENOMEM;
}

/**
 * Exit alldevs functionality.
 */
void alldevs_exit(void)
{
	CHECK_STOP();

	/* Call this after all walb devices has stopped. */

	ASSERT(list_empty(&all_wdevs_));
	ASSERT(map_is_empty(map_minor_));
	ASSERT(hashtbl_is_empty(htbl_uuid_));
	ASSERT(hashtbl_is_empty(htbl_name_));

	map_destroy(map_minor_);
	map_minor_ = NULL;
	hashtbl_destroy(htbl_uuid_);
	htbl_uuid_ = NULL;
	hashtbl_destroy(htbl_name_);
	htbl_name_ = NULL;

	finalize_treemap_memory_manager(&mmgr_);
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
	if (minor % 2 == 1) { minor--; }

	wdevt = MKDEV(walb_major_, minor);

	list_for_each_entry_safe(wdev, wdev_next, &all_wdevs_, list) {

		if (wdev->devt == wdevt) { return wdev; }
	}

	return NULL; /* not found. */
}
#endif

/**
 * Search wdev with device minor id.
 * Using map_minor_.
 *
 * @LOCK read lock is required.
 */
struct walb_dev* search_wdev_with_minor(unsigned int minor)
{
	unsigned long p;

	CHECK_RUNNING();

	p = map_lookup(map_minor_, (u64)minor);
	if (p == TREEMAP_INVALID_VAL) {
		return NULL;
	} else {
		ASSERT(p);
		return (struct walb_dev *)p;
	}
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
	unsigned long p;

	CHECK_RUNNING();

	/*
		ここも-1は必要? strlen()でよい?
	*/
	len = strnlen(name, WALB_DEV_NAME_MAX_LEN - 1);

	p = hashtbl_lookup(htbl_name_, (const u8 *)name, len);
	ASSERT(p != 0);

	if (p == HASHTBL_INVALID_VAL) {
		return NULL;
	} else {
		return (struct walb_dev *)p;
	}
}

/**
 * Search wdev with device uuid.
 * Using htbl_uuid_.
 *
 * @LOCK read lock is required.
 */
struct walb_dev* search_wdev_with_uuid(const u8* uuid)
{
	unsigned long p;

	CHECK_RUNNING();

	p = hashtbl_lookup(htbl_uuid_, uuid, 16);
	ASSERT(p != 0);

	if (p == HASHTBL_INVALID_VAL) {
		return NULL;
	} else {
		return (struct walb_dev *)p;
	}
}

/**
 * Listing walb devices.
 *
 * @ddata_k pointer to buffer to store results (kernel space).
 * @ddata_u pointer to buffer to store results (user space).
 *   If both ddata_k and ddata_u are NULL,
 *   this function will just count the number of devices.
 * @n buffer size [walb_disk_data].
 * @minor0 lower bound of minor0.
 * @minor1 upper bound of minor1.
 *   The range is minor0 <= minor < minor1.
 *
 * RETURN:
 *   Number of stored devices (0 <= return <= n) in success, or -1.
 */
int get_wdev_list_range(
	struct walb_disk_data *ddata_k,
	struct walb_disk_data __user *ddata_u,
	size_t n,
	unsigned int minor0, unsigned int minor1)
{
	struct walb_disk_data ddata_t; /* whileの中で宣言 */
	int ret;
	struct walb_dev *wdev; /* whileの中で宣言 */
	unsigned long val; /* whileの中で宣言 */
	unsigned int minor;
	size_t remaining = n;
	struct map_cursor cur_t;

	ASSERT(n > 0);
	ASSERT(minor0 < minor1);

	map_cursor_init(map_minor_, &cur_t);
	ret = map_cursor_search(&cur_t, (u64)minor0, MAP_SEARCH_GE);
	if (ret) {
		minor = map_cursor_key(&cur_t);
	} else {
		minor = (-1U);
	}
	while (remaining > 0 && ret && minor < (u64)minor1) {
		/* Get walb_dev. */
		val = map_cursor_val(&cur_t);
		ASSERT(val != TREEMAP_INVALID_VAL);
		wdev = (struct walb_dev *)val;
		ASSERT(wdev);

		/* Make walb_disk_data. */
		ASSERT(wdev->gd);
		strncpy(ddata_t.name, wdev->gd->disk_name, DISK_NAME_LEN);
		ddata_t.major = walb_major_;
		ddata_t.minor = minor;

		/* Copy to the result buffer. */
		if (ddata_u) {
			if (copy_to_user(ddata_u, &ddata_t, sizeof(struct walb_disk_data))) {
				LOGe("copy_to_user failed.\n");
				return -1;
			}
			ddata_u++;
		}
		if (ddata_k) {
			/*
				*ddata_k = ddata_t;の方がわかりやすい。
			*/
			memcpy(ddata_k, &ddata_t, sizeof(struct walb_disk_data));
			ddata_k++;
		}
		remaining--;
		ret = map_cursor_next(&cur_t);
		minor = (unsigned int)map_cursor_key(&cur_t);
	}
	ASSERT(n - remaining <= (size_t)INT_MAX);
	return (int)(n - remaining);
}

/**
 * Get number of walb devices.
 *
 * RETURN:
 *   Number of walb devices.
 */
unsigned int get_n_devices(void)
{
	return n_devices_;
}

/**
 * Add walb device alldevs list and hash tables.
 *
 * @wdev walb device to add.
 *
 * @return 0 in success,
 *	   -ENOMEM in memory allocation failure,
 *	   -EPERM in name or
 *
 * @LOCK write lock is required.
 */
int alldevs_add(struct walb_dev* wdev)
{
	size_t len;
	int ret;
	/*
		この数値はあちこちに定義されているので #define UUID_SIZEみたいにグローバルに定義すべき
	*/
	const int buf_size = 16 * 3 + 1;
	char buf[buf_size];
	unsigned int minor;

	CHECK_RUNNING();
	minor = MINOR(wdev->devt);

	ret = map_add(map_minor_, (u64)minor, (unsigned long)wdev, GFP_KERNEL);
	if (ret) {
		if (ret == -EEXIST) {
			LOGe("alldevs_add: minor %u is already registered.\n",
				MINOR(wdev->devt));
		}
		goto error0;
	}

	len = get_wdev_name_len(wdev);
	ret = hashtbl_add(htbl_name_,
			get_super_sector(wdev->lsuper0)->name, len,
			(unsigned long)wdev, GFP_KERNEL);
	if (ret != 0) {
		if (ret == -EPERM) {
			LOGe("alldevs_add: name %s is already registered.\n",
				get_super_sector(wdev->lsuper0)->name);
		}
		goto error1;
	}

	ret = hashtbl_add(htbl_uuid_,
			get_super_sector(wdev->lsuper0)->uuid, 16,
			(unsigned long)wdev, GFP_KERNEL);
	if (ret != 0) {
		if (ret == -EPERM) {
			sprint_uuid(buf, buf_size, get_super_sector(wdev->lsuper0)->uuid);
			LOGe("alldevs_add: uuid %s is already registered.\n",
				buf);
		}
		goto error2;
	}

	list_add_tail(&wdev->list, &all_wdevs_);
	n_devices_++;
	return 0;

#if 0
error3:
	hashtbl_del(htbl_uuid_, get_super_sector(wdev->lsuper0)->uuid, 16);
#endif
error2:
	hashtbl_del(htbl_name_, get_super_sector(wdev->lsuper0)->name, len);
error1:
	map_del(map_minor_, (u64)minor);
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

	CHECK_RUNNING();

	len = get_wdev_name_len(wdev);
	wminor = MINOR(wdev->devt);

	tmp0 = (struct walb_dev *)
		hashtbl_del(htbl_uuid_, get_super_sector(wdev->lsuper0)->uuid, 16);
	tmp1 = (struct walb_dev *)
		hashtbl_del(htbl_name_, get_super_sector(wdev->lsuper0)->name, len);
	tmp2 = (struct walb_dev *)map_del(map_minor_, (u64)wminor);

	ASSERT(wdev == tmp0);
	ASSERT(wdev == tmp1);
	ASSERT(wdev == tmp2);
	list_del(&wdev->list);
	n_devices_--;
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

	CHECK_RUNNING();
	if (list_empty(&all_wdevs_)) {
		return NULL;
	}

	wdev = list_first_entry(&all_wdevs_, struct walb_dev, list);
	alldevs_del(wdev);

	return wdev;
}

/**
 * Update uuid of a walb device.
 *
 * RETURN:
 *   0 in success, or -1.
 *
 * @LOCK write lock required.
 */
int alldevs_update_uuid(
	const u8 *old_uuid, const u8 *new_uuid)
{
	struct walb_dev *wdev;
	const int buf_size = 16 * 3 + 1;
	char buf[buf_size];
	int ret;

	wdev = (struct walb_dev *)hashtbl_del(
		htbl_uuid_, old_uuid, 16);
	if (!wdev) {
		LOGe("Specified uuid not found.\n");
		goto error0; /* return -1; */
	}
	ret = hashtbl_add(
		htbl_uuid_, new_uuid, 16,
		(unsigned long)wdev, GFP_KERNEL);
	/*
		if (ret == 0) return 0;
		にすればこのあとgoto erro1は不要
	*/
	if (ret != 0) {
		if (ret == -EPERM) {
			sprint_uuid(buf, buf_size, new_uuid);
			LOGe("alldevs_add: uuid %s is already registered.\n",
				buf);
		}
		goto error1;
	}
	return 0;

error1:
	ret = hashtbl_add(
		htbl_uuid_, old_uuid, 16, (unsigned long)wdev, GFP_KERNEL);
	if (ret != 0) {
		LOGe("Failed to re-add.\n");
	}
error0:
	return -1;
}

/**
 * Get free minor id.
 *
 * @LOCK read lock required.
 */
unsigned int get_free_minor()
{
	unsigned int minor;
	struct map_cursor cur_t;

	CHECK_RUNNING();

	map_cursor_init(map_minor_, &cur_t);
	map_cursor_end(&cur_t);

	if (map_cursor_prev(&cur_t)) {
		minor = (unsigned int)map_cursor_key(&cur_t);
		minor += 2;
	} else {
		ASSERT(map_is_empty(map_minor_));
		minor = 0;
	}
	return minor;
}

/**
 * Read lock.
 */
void alldevs_read_lock(void)
{
	CHECK_RUNNING();
	down_read(&all_wdevs_lock_);
}

/**
 * Read unlock.
 */
void alldevs_read_unlock(void)
{
	CHECK_RUNNING();
	up_read(&all_wdevs_lock_);
}

/**
 * Write lock.
 */
void alldevs_write_lock(void)
{
	CHECK_RUNNING();
	down_write(&all_wdevs_lock_);
}

/**
 * Write unlock.
 */
void alldevs_write_unlock(void)
{
	CHECK_RUNNING();
	up_write(&all_wdevs_lock_);
}

MODULE_LICENSE("Dual BSD/GPL");
