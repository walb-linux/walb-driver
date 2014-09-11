/**
 * wdev_ioctl.c - walb device ioctl.
 *
 * (C) 2013, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include "walb/logger.h"
#include "wdev_ioctl.h"
#include "wdev_util.h"
#include "kern.h"
#include "io.h"
#include "super.h"
#include "alldevs.h"
#include "control.h"

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

/* Ioctl details. */
static int ioctl_wdev_get_oldest_lsid(struct walb_dev *wdev, struct walb_ctl *ctl);
static int ioctl_wdev_set_oldest_lsid(struct walb_dev *wdev, struct walb_ctl *ctl);
static int ioctl_wdev_status(struct walb_dev *wdev, struct walb_ctl *ctl); /* NYI */
static int ioctl_wdev_take_checkpoint(struct walb_dev *wdev, struct walb_ctl *ctl);
static int ioctl_wdev_get_checkpoint_interval(struct walb_dev *wdev, struct walb_ctl *ctl);
static int ioctl_wdev_set_checkpoint_interval(struct walb_dev *wdev, struct walb_ctl *ctl);
static int ioctl_wdev_get_written_lsid(struct walb_dev *wdev, struct walb_ctl *ctl);
static int ioctl_wdev_get_permanent_lsid(struct walb_dev *wdev, struct walb_ctl *ctl);
static int ioctl_wdev_get_completed_lsid(struct walb_dev *wdev, struct walb_ctl *ctl);
static int ioctl_wdev_get_log_usage(struct walb_dev *wdev, struct walb_ctl *ctl);
static int ioctl_wdev_get_log_capacity(struct walb_dev *wdev, struct walb_ctl *ctl);
static int ioctl_wdev_is_flush_capable(struct walb_dev *wdev, struct walb_ctl *ctl);
static int ioctl_wdev_resize(struct walb_dev *wdev, struct walb_ctl *ctl);
static int ioctl_wdev_clear_log(struct walb_dev *wdev, struct walb_ctl *ctl);
static int ioctl_wdev_is_log_overflow(struct walb_dev *wdev, struct walb_ctl *ctl);
static int ioctl_wdev_freeze(struct walb_dev *wdev, struct walb_ctl *ctl);
static int ioctl_wdev_is_frozen(struct walb_dev *wdev, struct walb_ctl *ctl);
static int ioctl_wdev_melt(struct walb_dev *wdev, struct walb_ctl *ctl);

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * Get oldest_lsid.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0.
 */
static int ioctl_wdev_get_oldest_lsid(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	LOGd("WALB_IOCTL_GET_OLDEST_LSID\n");
	ASSERT(ctl->command == WALB_IOCTL_GET_OLDEST_LSID);

	ctl->val_u64 = get_oldest_lsid(wdev);
	return 0;
}

/**
 * Set oldest_lsid.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0 in success, or -EFAULT.
 */
static int ioctl_wdev_set_oldest_lsid(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	u64 lsid, oldest_lsid, prev_written_lsid;
	bool is_valid;

	LOGd("WALB_IOCTL_SET_OLDEST_LSID_SET\n");

	lsid = ctl->val_u64;

	spin_lock(&wdev->lsid_lock);
	prev_written_lsid = wdev->lsids.prev_written;
	oldest_lsid = wdev->lsids.oldest;
	spin_unlock(&wdev->lsid_lock);

	is_valid = lsid == prev_written_lsid;
	if (!is_valid)
		is_valid = oldest_lsid <= lsid && lsid < prev_written_lsid
			&& walb_check_lsid_valid(wdev, lsid);

	if (!is_valid) {
		LOGe("lsid %"PRIu64" is not valid.\n", lsid);
		LOGe("You shoud specify valid logpack header lsid"
			" (oldest_lsid (%"PRIu64") <= lsid <= prev_written_lsid (%"PRIu64").\n",
			oldest_lsid, prev_written_lsid);
		return -EFAULT;
	}

	spin_lock(&wdev->lsid_lock);
	wdev->lsids.oldest = lsid;
	spin_unlock(&wdev->lsid_lock);

	if (!walb_sync_super_block(wdev))
		return -EFAULT;

	return 0;
}

/**
 * Get status.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0 in success, or -EFAULT.
 */
static int ioctl_wdev_status(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	/* not yet implemented */

	LOGd("WALB_IOCTL_STATUS is not supported currently.\n");
	return -EFAULT;
}

/**
 * Take a checkpoint immedicately.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0 in success, or -EFAULT.
 */
static int ioctl_wdev_take_checkpoint(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	LOGd("WALB_IOCTL_TAKE_CHECKPOINT\n");
	ASSERT(ctl->command == WALB_IOCTL_TAKE_CHECKPOINT);

	stop_checkpointing(&wdev->cpd);
#ifdef WALB_DEBUG
	down_write(&wdev->cpd.lock);
	ASSERT(wdev->cpd.state == CP_STOPPED);
	up_write(&wdev->cpd.lock);
#endif
	if (!take_checkpoint(&wdev->cpd))
		return -EFAULT;

	start_checkpointing(&wdev->cpd);
	return 0;
}

/**
 * Get checkpoint interval.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0.
 */
static int ioctl_wdev_get_checkpoint_interval(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	LOGd("WALB_IOCTL_GET_CHECKPOINT_INTERVAL\n");
	ASSERT(ctl->command == WALB_IOCTL_GET_CHECKPOINT_INTERVAL);

	ctl->val_u32 = get_checkpoint_interval(&wdev->cpd);
	return 0;
}

/**
 * Set checkpoint interval.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0 in success, or -EFAULT.
 */
static int ioctl_wdev_set_checkpoint_interval(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	u32 interval;

	LOGd("WALB_IOCTL_SET_CHECKPOINT_INTERVAL\n");
	ASSERT(ctl->command == WALB_IOCTL_SET_CHECKPOINT_INTERVAL);

	interval = ctl->val_u32;
	if (interval > WALB_MAX_CHECKPOINT_INTERVAL) {
		LOGe("Checkpoint interval is too big.\n");
		return -EFAULT;
	}
	set_checkpoint_interval(&wdev->cpd, interval);
	return 0;
}

/**
 * Get written_lsid.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0.
 */
static int ioctl_wdev_get_written_lsid(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	LOGd("WALB_IOCTL_GET_WRITTEN_LSID\n");
	ASSERT(ctl->command == WALB_IOCTL_GET_WRITTEN_LSID);

	ctl->val_u64 = get_written_lsid(wdev);
	return 0;
}

/**
 * Get permanent_lsid.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0.
 */
static int ioctl_wdev_get_permanent_lsid(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	LOGd("WALB_IOCTL_GET_PERMANENT_LSID\n");
	ASSERT(ctl->command == WALB_IOCTL_GET_PERMANENT_LSID);

	ctl->val_u64 = get_permanent_lsid(wdev);
	return 0;
}

/**
 * Get completed_lsid.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0.
 */
static int ioctl_wdev_get_completed_lsid(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	LOGd("WALB_IOCTL_GET_COMPLETED_LSID\n");
	ASSERT(ctl->command == WALB_IOCTL_GET_COMPLETED_LSID);

	ctl->val_u64 = get_completed_lsid(wdev);
	return 0;
}

/**
 * Get log usage.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0.
 */
static int ioctl_wdev_get_log_usage(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	LOGd("WALB_IOCTL_GET_LOG_USAGE\n");
	ASSERT(ctl->command == WALB_IOCTL_GET_LOG_USAGE);

	ctl->val_u64 = walb_get_log_usage(wdev);
	return 0;
}

/**
 * Get log capacity.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0.
 */
static int ioctl_wdev_get_log_capacity(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	LOGd("WALB_IOCTL_GET_LOG_CAPACITY\n");
	ASSERT(ctl->command == WALB_IOCTL_GET_LOG_CAPACITY);

	ctl->val_u64 = walb_get_log_capacity(wdev);
	return 0;
}

/**
 * Get flush request capable or not.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0.
 */
static int ioctl_wdev_is_flush_capable(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	LOGd("WALB_IOCTL_IS_FLUAH_CAPABLE");
	ASSERT(ctl->command == WALB_IOCTL_IS_FLUSH_CAPABLE);

	ctl->val_int = (wdev->queue->flush_flags & REQ_FLUSH) != 0;
	return 0;
}

/**
 * Resize walb device.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0 in success, or -EFAULT.
 */
static int ioctl_wdev_resize(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	u64 ddev_size;
	u64 new_size;
	u64 old_size;

	LOGd("WALB_IOCTL_RESIZE.\n");
	ASSERT(ctl->command == WALB_IOCTL_RESIZE);

	old_size = get_capacity(wdev->gd);
	new_size = ctl->val_u64;
	ddev_size = wdev->ddev->bd_part->nr_sects;

	if (new_size == 0) {
		new_size = ddev_size;
	}
	if (new_size < old_size) {
		LOGe("Shrink size from %"PRIu64" to %"PRIu64" is not supported.\n",
			old_size, new_size);
		return -EFAULT;
	}
	if (new_size > ddev_size) {
		LOGe("new_size %"PRIu64" > data device capacity %"PRIu64".\n",
			new_size, ddev_size);
		return -EFAULT;
	}
	if (new_size == old_size) {
		LOGi("No need to resize.\n");
		return 0;
	}

	spin_lock(&wdev->size_lock);
	wdev->size = new_size;
	wdev->ddev_size = ddev_size;
	spin_unlock(&wdev->size_lock);

	if (!resize_disk(wdev->gd, new_size)) {
		return -EFAULT;
	}

	/* Sync super block for super->device_size */
	if (!walb_sync_super_block(wdev))
		return -EFAULT;

	return 0;
}

/**
 * Clear log and detect resize of log device.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0 in success, or -EFAULT.
 */
static int ioctl_wdev_clear_log(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	u64 new_ldev_size, old_ldev_size;
	u8 new_uuid[UUID_SIZE], old_uuid[UUID_SIZE];
	unsigned int pbs = wdev->physical_bs;
	bool is_grown = false;
	int ret;
	struct walb_super_sector *super;
	u64 lsid0_off;
	struct lsid_set lsids;
	u64 old_ring_buffer_size;
	u32 new_salt;

	ASSERT(ctl->command == WALB_IOCTL_CLEAR_LOG);
	LOGd("WALB_IOCTL_CLEAR_LOG.\n");

	/* Freeze iocore and checkpointing.  */
	iocore_freeze(wdev);
	stop_checkpointing(&wdev->cpd);

	/* Get old/new log device size. */
	old_ldev_size = wdev->ldev_size;
	new_ldev_size = wdev->ldev->bd_part->nr_sects;

	if (old_ldev_size > new_ldev_size) {
		LOGe("Log device shrink not supported.\n");
		goto error0;
	}

	/* Backup variables. */
	old_ring_buffer_size = wdev->ring_buffer_size;
	backup_lsid_set(wdev, &lsids);

	/* Initialize lsid(s). */
	spin_lock(&wdev->lsid_lock);
	wdev->lsids.latest = 0;
	wdev->lsids.flush = 0;
	wdev->lsids.completed = 0;
	wdev->lsids.permanent = 0;
	wdev->lsids.written = 0;
	wdev->lsids.prev_written = 0;
	wdev->lsids.oldest = 0;
	spin_unlock(&wdev->lsid_lock);

	/* Grow the walblog device. */
	if (old_ldev_size < new_ldev_size) {
		LOGi("Detect log device size change.\n");

		/* Grow the disk. */
		is_grown = true;
		if (!resize_disk(wdev->log_gd, new_ldev_size)) {
			LOGe("grow disk failed.\n");
			set_bit(WALB_STATE_READ_ONLY, &wdev->flags);
			goto error1;
		}
		LOGi("Grown log device size from %"PRIu64" to %"PRIu64".\n",
			old_ldev_size, new_ldev_size);
		wdev->ldev_size = new_ldev_size;

		/* Recalculate ring buffer size. */
		wdev->ring_buffer_size =
			addr_pb(pbs, new_ldev_size)
			- get_ring_buffer_offset(pbs);
	}

	/* Generate new uuid and salt. */
	get_random_bytes(new_uuid, 16);
	get_random_bytes(&new_salt, sizeof(new_salt));
	wdev->log_checksum_salt = new_salt;

	/* Update superblock image. */
	spin_lock(&wdev->lsuper0_lock);
	super = get_super_sector(wdev->lsuper0);
	memcpy(old_uuid, super->uuid, UUID_SIZE);
	memcpy(super->uuid, new_uuid, UUID_SIZE);
	super->ring_buffer_size = wdev->ring_buffer_size;
	super->log_checksum_salt = new_salt;
	/* super->metadata_size; */
	lsid0_off = get_offset_of_lsid_2(super, 0);
	spin_unlock(&wdev->lsuper0_lock);

	/* Sync super sector. */
	if (!walb_sync_super_block(wdev))
		goto error2;

	/* Update uuid index of alldev data. */
	alldevs_write_lock();
	ret = alldevs_update_uuid(old_uuid, new_uuid);
	alldevs_write_unlock();
	if (ret) {
		LOGe("Update alldevs index failed.\n");
		set_bit(WALB_STATE_READ_ONLY, &wdev->flags);
		goto error2;
	}

	/* Invalidate first logpack */
	if (!invalidate_lsid(wdev, 0)) {
		LOGe("invalidate lsid 0 failed.\n");
		set_bit(WALB_STATE_READ_ONLY, &wdev->flags);
		goto error2;
	}

	/* Clear log overflow. */
	clear_bit(WALB_STATE_OVERFLOW, &wdev->flags);

	/* Melt iocore and checkpointing. */
	start_checkpointing(&wdev->cpd);
	iocore_melt(wdev);

	return 0;

error2:
	restore_lsid_set(wdev, &lsids);
	wdev->ring_buffer_size = old_ring_buffer_size;
#if 0
	wdev->ldev_size = old_ldev_size;
	if (!resize_disk(wdev->log_gd, old_ldev_size)) {
		LOGe("resize_disk to shrink failed.\n");
	}
#endif
error1:
	start_checkpointing(&wdev->cpd);
	iocore_melt(wdev);
error0:
	return -EFAULT;
}

/**
 * Check log space overflow.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0.
 */
static int ioctl_wdev_is_log_overflow(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	ASSERT(ctl->command == WALB_IOCTL_IS_LOG_OVERFLOW);
	LOGd("WALB_IOCTL_IS_LOG_OVERFLOW.\n");

	ctl->val_int = test_bit(WALB_STATE_OVERFLOW, &wdev->flags);
	return 0;
}

/**
 * Freeze a walb device.
 * Currently write IOs will be frozen but read IOs will not.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0 in success, or -EFAULT.
 */
static int ioctl_wdev_freeze(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	u32 timeout_sec;

	ASSERT(ctl->command == WALB_IOCTL_FREEZE);
	LOGd("WALB_IOCTL_FREEZE\n");

	/* Clip timeout value. */
	timeout_sec = ctl->val_u32;
	if (timeout_sec > 86400) {
		timeout_sec = 86400;
		LOGi("Freeze timeout has been cut to %"PRIu32" seconds.\n",
			timeout_sec);
	}

	cancel_melt_work(wdev);
	if (freeze_if_melted(wdev, timeout_sec)) {
		return 0;
	}
	return -EFAULT;
}

/**
 * Check whether the device is frozen or not.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0.
 */
static int ioctl_wdev_is_frozen(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	ASSERT(ctl->command == WALB_IOCTL_IS_FROZEN);
	LOGd("WALB_IOCTL_IS_FROZEN\n");

	mutex_lock(&wdev->freeze_lock);
	ctl->val_int = (wdev->freeze_state == FRZ_MELTED) ? 0 : 1;
	mutex_unlock(&wdev->freeze_lock);

	return 0;
}

/**
 * Melt a frozen device.
 *
 * @wdev walb dev.
 * @ctl ioctl data.
 * RETURN:
 *   0 in success, or -EFAULT.
 */
static int ioctl_wdev_melt(struct walb_dev *wdev, struct walb_ctl *ctl)
{
	ASSERT(ctl->command == WALB_IOCTL_MELT);
	LOGd("WALB_IOCTL_MELT\n");

	cancel_melt_work(wdev);
	if (melt_if_frozen(wdev, true)) {
		return 0;
	}
	return -EFAULT;
}

/*******************************************************************************
 * Global functions.
 *******************************************************************************/

/**
 * Execute ioctl for WALB_IOCTL_WDEV.
 *
 * return 0 in success, or -EFAULT.
 */
int walb_dispatch_ioctl_wdev(struct walb_dev *wdev, void __user *userctl)
{
	int ret = -EFAULT;
	struct walb_ctl *ctl;

	/* Get ctl data. */
	ctl = walb_get_ctl(userctl, GFP_KERNEL);
	if (!ctl) {
		LOGe("walb_get_ctl failed.\n");
		return -EFAULT;
	}

	/* Execute each command. */
	switch(ctl->command) {
	case WALB_IOCTL_GET_OLDEST_LSID:
		ret = ioctl_wdev_get_oldest_lsid(wdev, ctl);
		break;
	case WALB_IOCTL_SET_OLDEST_LSID:
		ret = ioctl_wdev_set_oldest_lsid(wdev, ctl);
		break;
	case WALB_IOCTL_TAKE_CHECKPOINT:
		ret = ioctl_wdev_take_checkpoint(wdev, ctl);
		break;
	case WALB_IOCTL_GET_CHECKPOINT_INTERVAL:
		ret = ioctl_wdev_get_checkpoint_interval(wdev, ctl);
		break;
	case WALB_IOCTL_SET_CHECKPOINT_INTERVAL:
		ret = ioctl_wdev_set_checkpoint_interval(wdev, ctl);
		break;
	case WALB_IOCTL_GET_WRITTEN_LSID:
		ret = ioctl_wdev_get_written_lsid(wdev, ctl);
		break;
	case WALB_IOCTL_GET_PERMANENT_LSID:
		ret = ioctl_wdev_get_permanent_lsid(wdev, ctl);
		break;
	case WALB_IOCTL_GET_COMPLETED_LSID:
		ret = ioctl_wdev_get_completed_lsid(wdev, ctl);
		break;
	case WALB_IOCTL_GET_LOG_USAGE:
		ret = ioctl_wdev_get_log_usage(wdev, ctl);
		break;
	case WALB_IOCTL_GET_LOG_CAPACITY:
		ret = ioctl_wdev_get_log_capacity(wdev, ctl);
		break;
	case WALB_IOCTL_IS_FLUSH_CAPABLE:
		ret = ioctl_wdev_is_flush_capable(wdev, ctl);
		break;
	case WALB_IOCTL_STATUS:
		ret = ioctl_wdev_status(wdev, ctl);
		break;
	case WALB_IOCTL_RESIZE:
		ret = ioctl_wdev_resize(wdev, ctl);
		break;
	case WALB_IOCTL_CLEAR_LOG:
		ret = ioctl_wdev_clear_log(wdev, ctl);
		break;
	case WALB_IOCTL_IS_LOG_OVERFLOW:
		ret = ioctl_wdev_is_log_overflow(wdev, ctl);
		break;
	case WALB_IOCTL_FREEZE:
		ret = ioctl_wdev_freeze(wdev, ctl);
		break;
	case WALB_IOCTL_MELT:
		ret = ioctl_wdev_melt(wdev, ctl);
		break;
	case WALB_IOCTL_IS_FROZEN:
		ret = ioctl_wdev_is_frozen(wdev, ctl);
		break;
	default:
		LOGw("WALB_IOCTL_WDEV %d is not supported.\n",
			ctl->command);
	}

	/* Put ctl data. */
	if (walb_put_ctl(userctl, ctl) != 0) {
		LOGe("walb_put_ctl failed.\n");
		return -EFAULT;
	}
	return ret;
}

MODULE_LICENSE("Dual BSD/GPL");
