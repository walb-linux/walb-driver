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
 * The list is sorted by device minor number.
 */
static struct list_head all_wdevs_;

/**
 * Number of devices.
 */
static unsigned int n_devices_;

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
	init_rwsem(&all_wdevs_lock_);
	CHECK_START();
	return 0;
}

/**
 * Exit alldevs functionality.
 */
void alldevs_exit(void)
{
	CHECK_STOP();
	ASSERT(list_empty(&all_wdevs_));
}

/**
 * Search wdev with device minor id.
 *
 * @LOCK read lock is required.
 */
struct walb_dev* search_wdev_with_minor(unsigned int minor)
{
	struct walb_dev *wdev;

	CHECK_RUNNING();
	list_for_each_entry(wdev, &all_wdevs_, list) {
		if (MINOR(wdev->devt) == minor)
			return wdev;

		/* assume the list is sorted. */
		if (MINOR(wdev->devt) > minor)
			return NULL;
	}
	return NULL;
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
	size_t remaining = n;
	struct walb_dev *wdev;
	ASSERT(n > 0);
	ASSERT(minor0 < minor1);

	list_for_each_entry(wdev, &all_wdevs_, list) {
		struct walb_disk_data ddata_t;
		const uint minor = MINOR(wdev->devt);
		if (minor < minor0)
			continue;
		if (minor >= minor1 || remaining == 0)
			break;

		/* Make walb_disk_data. */
		ASSERT(wdev->gd);
		memset(ddata_t.name, 0, DISK_NAME_LEN);
		ASSERT(strnlen(wdev->gd->disk_name, DISK_NAME_LEN) < DISK_NAME_LEN);
		snprintf(ddata_t.name, DISK_NAME_LEN, "%s", wdev->gd->disk_name);
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
			*ddata_k = ddata_t;
			ddata_k++;
		}
		remaining--;
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
 * Add walb device alldevs list.
 *
 * @wdev walb device to add.
 *
 * @LOCK write lock is required.
 */
void alldevs_add(struct walb_dev* wdev)
{
	bool added = false;
	struct walb_dev *wdev0, *wdev1;
#ifdef WALB_DEBUG
	uint minor;
#endif
	CHECK_RUNNING();

	/* Insert sort. */
	if (list_empty(&all_wdevs_)) {
		list_add_tail(&wdev->list, &all_wdevs_);
		goto fin;
	}
	wdev0 = list_last_entry(&all_wdevs_, struct walb_dev, list);
	if (MINOR(wdev->devt) > MINOR(wdev0->devt)) {
		list_add_tail(&wdev->list, &all_wdevs_);
		goto fin;
	}
	list_for_each_entry_safe_reverse(wdev0, wdev1, &all_wdevs_, list) {
		if (MINOR(wdev->devt) > MINOR(wdev0->devt)) {
			list_add(&wdev->list, &wdev0->list);
			added = true;
			break;
		}
	}
	if (!added)
		list_add(&wdev->list, &all_wdevs_);

fin:
#ifdef WALB_DEBUG
	minor = 0;
	list_for_each_entry(wdev0, &all_wdevs_, list) {
		if (minor == 0)
			ASSERT(minor <= MINOR(wdev0->devt));
		else
			ASSERT(minor < MINOR(wdev0->devt));

		minor = MINOR(wdev0->devt);
	}
#endif
	n_devices_++;
}

/**
 * Delete walb device from alldevs list.
 *
 * @wdev walb device to del.
 *
 * @LOCK write lock required.
 */
void alldevs_del(struct walb_dev* wdev)
{
	CHECK_RUNNING();
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
 * Get free minor id.
 *
 * @LOCK read lock required.
 */
unsigned int get_free_minor()
{
	struct walb_dev *wdev;

	CHECK_RUNNING();

	if (list_empty(&all_wdevs_))
		return 0;

	wdev = list_last_entry(&all_wdevs_, struct walb_dev, list);
	return MINOR(wdev->devt) + 2;
}

/**
 * RETURN:
 *   true if the device is already used as walb underlying device.
 *
 * @LOCK read lock required.
 */
bool alldevs_is_already_used(dev_t devt)
{
	struct walb_dev *wdev;
	list_for_each_entry(wdev, &all_wdevs_, list) {
		if (devt == wdev->ldev->bd_dev || devt == wdev->ddev->bd_dev)
			return true;
	}
	return false;
}

void alldevs_read_lock(void)
{
	CHECK_RUNNING();
	down_read(&all_wdevs_lock_);
}

void alldevs_read_unlock(void)
{
	CHECK_RUNNING();
	up_read(&all_wdevs_lock_);
}

void alldevs_write_lock(void)
{
	CHECK_RUNNING();
	down_write(&all_wdevs_lock_);
}

void alldevs_write_unlock(void)
{
	CHECK_RUNNING();
	up_write(&all_wdevs_lock_);
}

MODULE_LICENSE("Dual BSD/GPL");
