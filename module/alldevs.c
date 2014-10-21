/**
 * alldevs.c - for multiple devices management.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/idr.h>
#include <linux/version.h>

#include "alldevs.h"

/**
 * Lock to access almost all the functions declared in this header.
 */
static struct mutex all_wdevs_lock_;

/**
 * All items of struct walb_dev.
 * key: (wdev's minor id) / 2
 * val: (struct wdev *)
 */
static struct idr all_wdevs_;

/**
 * Constants.
 */
#define ALL_WDEVS_KEY_MAX ((1 << MINORBITS) >> 1)
#define ALL_WDEVS_PREALLOCED ((struct walb_dev *)-1)

/**
 * Number of devices.
 */
static atomic_t nr_devs_ = ATOMIC_INIT(0);

/**
 * For debug.
 */
static atomic_t is_available_ = ATOMIC_INIT(0);

#define CHECK_START() {						\
		if (atomic_inc_return(&is_available_) != 1)	\
			BUG();					\
	}
#define CHECK_STOP() {						\
		if (atomic_dec_return(&is_available_) != 0)	\
			BUG();					\
	}
#define CHECK_RUNNING() {				\
		if (atomic_read(&is_available_) != 1)	\
			BUG();				\
	}

/**
 * Static functions.
 */

static inline int get_key_from_wdev(const struct walb_dev* wdev)
{
	return MINOR(wdev->devt) / 2;
}

/*******************************************************************************
 * Global functions.
 *******************************************************************************/

/**
 * Initialize alldevs functionality.
 *
 * RETURN:
 *   0 in success, or -ENOMEM.
 */
int alldevs_init(void)
{
	idr_init(&all_wdevs_);
	mutex_init(&all_wdevs_lock_);
	CHECK_START();
	return 0;
}

/**
 * Exit alldevs functionality.
 */
void alldevs_exit(void)
{
	CHECK_STOP();
	ASSERT(atomic_read(&nr_devs_) == 0);
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 15, 0)
	ASSERT(idr_is_empty(&all_wdevs_));
#endif
	idr_destroy(&all_wdevs_);
}

/**
 * Search wdev with device minor id.
 *
 * LOCK:
 *   required.
 */
struct walb_dev* search_wdev_with_minor(uint minor)
{
	struct walb_dev *wdev;

	CHECK_RUNNING();

	wdev = idr_find(&all_wdevs_, minor / 2);
	ASSERT(wdev);
	return wdev;
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
 *
 * LOCK:
 *   required.
 */
int get_wdev_list_range(
	struct walb_disk_data *ddata_k,
	struct walb_disk_data __user *ddata_u,
	size_t n,
	uint minor0, uint minor1)
{
	size_t remaining = n;
	struct walb_dev *wdev;
	int key0 = minor0 / 2;
	int key1 = minor1 / 2;
	int key;

	ASSERT(n > 0);
	ASSERT(minor0 < minor1);

	if (key0 == key1)
		key1 = key0 + 1;
	ASSERT(key0 < key1);

	key = key0;
	while (remaining > 0 && key < key1) {
		struct walb_disk_data ddata_t;
		uint minor;

		/* Get next wdev. */
		wdev = idr_get_next(&all_wdevs_, &key);
		if (!wdev)
			break;
		minor = MINOR(wdev->devt);
		ASSERT(minor == key * 2);

		/* Make walb_disk_data. */
		memset(ddata_t.name, 0, DISK_NAME_LEN);
		snprintf(ddata_t.name, DISK_NAME_LEN, "%s", wdev->gd->disk_name);
		ddata_t.major = walb_major_;
		ddata_t.minor = minor;

		/* Copy to the result buffer. */
		if (ddata_u) {
			if (copy_to_user(ddata_u, &ddata_t, sizeof(ddata_t))) {
				LOGe("copy_to_user failed.\n");
				return -1;
			}
			ddata_u++;
		}
		if (ddata_k) {
			*ddata_k = ddata_t;
			ddata_k++;
		}

		remaining--;
		key++;
	}
	ASSERT(n - remaining <= (size_t)INT_MAX);
	return (int)(n - remaining);
}

/**
 * Get number of walb devices.
 *
 * RETURN:
 *   Number of walb devices.
 *
 * LOCK:
 *   not required.
 */
uint get_n_devices(void)
{
	const int n = atomic_read(&nr_devs_);
	ASSERT(n >= 0);
	return n;
}

/**
 * Add walb device alldevs list.
 *
 * @wdev walb device to add.
 *   Its minor id must be preallocated using
 *   alloc_any_minor() or alloc_specific_minor().
 *
 * RETURN:
 *   true in success.
 *   false if a walb device already exist with the same name.
 *
 * LOCK:
 *   required.
 *   You must call alloc_xxx_minor() and all_devs_add()
 *   in the same critical section not to reveal
 *   invalid pointers to other threads.
 */
bool alldevs_add(struct walb_dev* wdev)
{
	const int key = get_key_from_wdev(wdev);
	struct walb_dev *old_ptr, *wdev_tmp;
	int key_tmp;

	CHECK_RUNNING();

	idr_for_each_entry(&all_wdevs_, wdev_tmp, key_tmp) {
		if (wdev_tmp == ALL_WDEVS_PREALLOCED)
			continue;
		if (!strncmp(wdev_tmp->gd->disk_name, wdev->gd->disk_name, DISK_NAME_LEN)) {
			LOGe("walb device already exist: %s\n", wdev_tmp->gd->disk_name);
			return false;
		}
	}

	old_ptr = idr_replace(&all_wdevs_, wdev, key);
	ASSERT(old_ptr == ALL_WDEVS_PREALLOCED);
	atomic_inc(&nr_devs_);
	return true;
}

/**
 * Delete walb device from alldevs list.
 *
 * @wdev walb device to del.
 *
 * LOCK:
 *   required.
 */
void alldevs_del(struct walb_dev* wdev)
{
	const int key = get_key_from_wdev(wdev);
	struct walb_dev *old_ptr;

	CHECK_RUNNING();

	old_ptr = idr_replace(&all_wdevs_, ALL_WDEVS_PREALLOCED, key);
	ASSERT(old_ptr == wdev);
	idr_remove(&all_wdevs_, key);
	atomic_dec(&nr_devs_);
}

/**
 * Return any of walb devices in the list and
 * delete it from alldevs structure.
 *
 * LOCK:
 *   required.
 */
struct walb_dev* alldevs_pop(void)
{
	struct walb_dev *wdev;
	int key = 0;

	CHECK_RUNNING();

	wdev = idr_get_next(&all_wdevs_, &key);
	if (!wdev)
		return NULL;

	ASSERT(get_key_from_wdev(wdev) == key);
	alldevs_del(wdev);
	return wdev;
}

/**
 * Allocate an any unused minor id.
 *
 * RETURN:
 *   minor id in success. The returned minor is even due to walb limitation.
 *   uint(-1) in failure.
 *
 * LOCK:
 *   required.
 */
uint alloc_any_minor()
{
	int key;

	CHECK_RUNNING();

	key = idr_alloc(&all_wdevs_, ALL_WDEVS_PREALLOCED,
			0, ALL_WDEVS_KEY_MAX, GFP_KERNEL);
	if (key < 0 || key >= ALL_WDEVS_KEY_MAX)
		return -1;

	return key * 2;
}

/**
 * Allocate an specified minor id.
 *
 * RETURN:
 *   minor id in success. The returned minor is even due to walb limitation.
 *   uint(-1) in failure.
 *
 * LOCK:
 *   required.
 */
uint alloc_specific_minor(uint minor)
{
	int key = minor / 2;

	CHECK_RUNNING();

	key = idr_alloc(&all_wdevs_, ALL_WDEVS_PREALLOCED,
			key, key + 1, GFP_KERNEL);
	if (key != minor / 2)
		return -1;

	return key * 2;
}

/**
 * Free an allocated minor id.
 *
 * LOCK:
 *   required.
 */
void free_minor(uint minor)
{
	const int key = minor / 2;

	ASSERT(key < ALL_WDEVS_KEY_MAX);
	CHECK_RUNNING();

	idr_remove(&all_wdevs_, key);
}

/**
 * RETURN:
 *   true if the device is already used as walb underlying device.
 *
 * LOCK:
 *   required.
 */
bool alldevs_is_already_used(dev_t devt)
{
	struct walb_dev *wdev;
	int key;

	idr_for_each_entry(&all_wdevs_, wdev, key) {
		ASSERT(get_key_from_wdev(wdev) == key);
		if (devt == wdev->ldev->bd_dev || devt == wdev->ddev->bd_dev)
			return true;
	}
	return false;
}

void alldevs_lock(void)
{
	CHECK_RUNNING();
	mutex_lock(&all_wdevs_lock_);
}

void alldevs_unlock(void)
{
	CHECK_RUNNING();
	mutex_unlock(&all_wdevs_lock_);
}

MODULE_LICENSE("Dual BSD/GPL");
