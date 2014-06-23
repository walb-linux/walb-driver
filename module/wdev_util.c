/**
 * wdev_util.c - walb device utilities.
 *
 * (C) 2013, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/version.h>
#include <linux/hdreg.h>
#include "walb/logger.h"
#include "wdev_util.h"
#include "kern.h"
#include "super.h"
#include "io.h"
#include "sector_io.h"

/**
 * Check logpack of the given lsid exists.
 *
 * @lsid lsid to check.
 *
 * @return Non-zero if valid, or 0.
 */
int walb_check_lsid_valid(struct walb_dev *wdev, u64 lsid)
{
	struct sector_data *sect;
	struct walb_logpack_header *logh;
	u64 off;

	ASSERT(wdev);

	sect = sector_alloc(wdev->physical_bs, GFP_NOIO);
	if (!sect) {
		LOGe("walb_check_lsid_valid: alloc sector failed.\n");
		goto error0;
	}
	ASSERT(is_same_size_sector(sect, wdev->lsuper0));
	logh = get_logpack_header(sect);

	spin_lock(&wdev->lsuper0_lock);
	off = get_offset_of_lsid_2(get_super_sector(wdev->lsuper0), lsid);
	spin_unlock(&wdev->lsuper0_lock);
	if (!sector_io(READ, wdev->ldev, off, sect)) {
		LOGe("walb_check_lsid_valid: read sector failed.\n");
		goto error1;
	}

	/* Check valid logpack header. */
	if (!is_valid_logpack_header_with_checksum(
			logh, wdev->physical_bs, wdev->log_checksum_salt)) {
		goto error1;
	}

	/* Check lsid. */
	if (logh->logpack_lsid != lsid) {
		goto error1;
	}

	sector_free(sect);
	return 1;

error1:
	sector_free(sect);
error0:
	return 0;
}

/**
 * Get oldest lsid of a walb data device.
 *
 * @return written_lsid of the walb device.
 */
u64 get_oldest_lsid(struct walb_dev *wdev)
{
	u64 ret;

	ASSERT(wdev);

	spin_lock(&wdev->lsid_lock);
	ret = wdev->lsids.oldest;
	spin_unlock(&wdev->lsid_lock);

	return ret;
}

/**
 * Get written lsid of a walb data device.
 *
 * @return written_lsid of the walb device.
 */
u64 get_written_lsid(struct walb_dev *wdev)
{
	u64 ret;

	ASSERT(wdev);

	spin_lock(&wdev->lsid_lock);
	ret = wdev->lsids.written;
	spin_unlock(&wdev->lsid_lock);

	return ret;
}

/**
 * Get permanent_lsid of the walb device.
 *
 * @return permanent_lsid of the walb device.
 */
u64 get_permanent_lsid(struct walb_dev *wdev)
{
	u64 ret;

	ASSERT(wdev);

	spin_lock(&wdev->lsid_lock);
	ret = wdev->lsids.permanent;
	spin_unlock(&wdev->lsid_lock);

	return ret;
}

/**
 * Get completed lsid of a walb log device.
 *
 * RETURN:
 *   completed_lsid of the walb device.
 */
u64 get_completed_lsid(struct walb_dev *wdev)
{
	u64 ret;
	spin_lock(&wdev->lsid_lock);
	ret = wdev->lsids.completed;
	spin_unlock(&wdev->lsid_lock);
	return ret;
}

/**
 * Set device name.
 *
 * @wdev walb device.
 * @minor minor id. This will be used for default name.
 * @name Name to set.
 *   If null or empty string is given and
 *   the preset name is empty,
 *   default name will be set using minor id.
 *
 * @return 0 in success, or -1.
 */
int walb_set_name(struct walb_dev *wdev,
			unsigned int minor, const char *name)
{
	int name_len;
	char *dev_name;

	ASSERT(wdev);
	ASSERT(wdev->lsuper0);

	dev_name = get_super_sector(wdev->lsuper0)->name;

	if (name && *name) {
		memset(dev_name, 0, DISK_NAME_LEN);
		snprintf(dev_name, DISK_NAME_LEN, "%s", name);
	} else if (*dev_name == 0) {
		memset(dev_name, 0, DISK_NAME_LEN);
		snprintf(dev_name, DISK_NAME_LEN, "%u", minor / 2);
	}
	LOGd("minor %u dev_name: %s\n", minor, dev_name);

	name_len = strlen(dev_name);
	ASSERT(name_len < DISK_NAME_LEN);
	if (name_len > WALB_DEV_NAME_MAX_LEN) {
		LOGe("Device name is too long: %s.\n", name);
		return -1;
	}
	return 0;
}

/**
 * Decide flush support or not.
 */
void walb_decide_flush_support(struct walb_dev *wdev)
{
	struct request_queue *q;
	const struct request_queue *lq, *dq;
	bool lq_flush, dq_flush, lq_fua, dq_fua;
	ASSERT(wdev);

	/* Get queues. */
	q = wdev->queue;
	ASSERT(q);
	lq = bdev_get_queue(wdev->ldev);
	dq = bdev_get_queue(wdev->ddev);

	/* Get flush/fua flags. */
	lq_flush = lq->flush_flags & REQ_FLUSH;
	dq_flush = dq->flush_flags & REQ_FLUSH;
	lq_fua = lq->flush_flags & REQ_FUA;
	dq_fua = dq->flush_flags & REQ_FUA;
	LOGn("flush/fua flags: log_device %d/%d data_device %d/%d\n",
		lq_flush, lq_fua, dq_flush, dq_fua);

	/* Check REQ_FLUSH/REQ_FUA supports. */
	if (lq_flush && dq_flush) {
		if (lq_fua) {
			LOGn("Supports REQ_FLUSH | REQ_FUA.");
			blk_queue_flush(q, REQ_FLUSH | REQ_FUA);
		} else {
			LOGn("Supports REQ_FLUSH.");
			blk_queue_flush(q, REQ_FLUSH);
		}
		blk_queue_flush_queueable(q, true);
	} else {
		LOGw("REQ_FLUSH is not supported!\n"
			"WalB can not guarantee data consistency...\n");
	}
}

/**
 * Support discard.
 */
void walb_discard_support(struct walb_dev *wdev)
{
	struct request_queue *q = wdev->queue;

	LOGn("Supports REQ_DISCARD.\n");
	q->limits.discard_granularity = wdev->physical_bs;

	/* Should be stored in u16 variable and aligned. */
	q->limits.max_discard_sectors = 1 << 15;
	q->limits.discard_zeroes_data = 0;
	queue_flag_set_unlocked(QUEUE_FLAG_DISCARD, q);
}

/**
 * Resize disk.
 *
 * @gd disk.
 * @new_size new size [logical block].
 *
 * RETURN:
 *   true in success, or false.
 */
bool resize_disk(struct gendisk *gd, u64 new_size)
{
	struct block_device *bdev;
	u64 old_size;

	ASSERT(gd);

	old_size = get_capacity(gd);
	if (old_size == new_size) {
		return true;
	}
	set_capacity(gd, new_size);

	bdev = bdget_disk(gd, 0);
	if (!bdev) {
		LOGe("bdget_disk failed.\n");
		return false;
	}
	mutex_lock(&bdev->bd_mutex);
	if (old_size > new_size) {
		LOGn("Shrink disk should discard block cache.\n");
		check_disk_size_change(gd, bdev);
		bdev->bd_invalidated = 0; /* This is bugfix. */
	} else {
		i_size_write(bdev->bd_inode,
			(loff_t)new_size * LOGICAL_BLOCK_SIZE);
	}
	mutex_unlock(&bdev->bd_mutex);
	bdput(bdev);
	return true;
}

/**
 * Invalidate lsid inside ring buffer.
 */
bool invalidate_lsid(struct walb_dev *wdev, u64 lsid)
{
	struct sector_data *zero_sector;
	struct walb_super_sector *super;
	u64 off;
	bool ret;

	ASSERT(lsid != INVALID_LSID);

	zero_sector = sector_alloc(
		wdev->physical_bs, GFP_KERNEL | __GFP_ZERO);
	if (!zero_sector) {
		LOGe("sector allocation failed.\n");
		return false;
	}

	spin_lock(&wdev->lsuper0_lock);
	super = get_super_sector(wdev->lsuper0);
	off = get_offset_of_lsid_2(super, lsid);
	spin_unlock(&wdev->lsuper0_lock);

	ret = sector_io(WRITE, wdev->ldev, off, zero_sector);
	if (!ret) {
		LOGe("sector write failed.\n");
		iocore_set_readonly(wdev);
	}
	sector_free(zero_sector);
	return ret;
}

/**
 * Backup lsids.
 */
void backup_lsid_set(struct walb_dev *wdev, struct lsid_set *lsids)
{
	spin_lock(&wdev->lsid_lock);
	*lsids = wdev->lsids;
	spin_unlock(&wdev->lsid_lock);
}

/**
 * Restore lsids.
 */
void restore_lsid_set(struct walb_dev *wdev, const struct lsid_set *lsids)
{
	spin_lock(&wdev->lsid_lock);
	wdev->lsids = *lsids;
	spin_unlock(&wdev->lsid_lock);
}

/**
 * Melt a frozen device.
 */
void task_melt(struct work_struct *work)
{
	struct delayed_work *dwork
		= container_of(work, struct delayed_work, work);
	struct walb_dev *wdev
		= container_of(dwork, struct walb_dev, freeze_dwork);
	ASSERT(wdev);

	mutex_lock(&wdev->freeze_lock);

	switch (wdev->freeze_state) {
	case FRZ_MELTED:
		LOGn("FRZ_MELTED minor %u.\n", MINOR(wdev->devt));
		break;
	case FRZ_FREEZED:
		LOGn("FRZ_FREEZED minor %u.\n", MINOR(wdev->devt));
		break;
	case FRZ_FREEZED_WITH_TIMEOUT:
		LOGn("Melt walb device minor %u.\n", MINOR(wdev->devt));
		start_checkpointing(&wdev->cpd);
		iocore_melt(wdev);
		wdev->freeze_state = FRZ_MELTED;
		break;
	default:
		BUG();
	}

	mutex_unlock(&wdev->freeze_lock);
}

/**
 * Cancel the melt work if enqueued.
 */
void cancel_melt_work(struct walb_dev *wdev)
{
	bool should_cancel_work = false;

	/* Check existance of the melt work. */
	mutex_lock(&wdev->freeze_lock);
	if (wdev->freeze_state == FRZ_FREEZED_WITH_TIMEOUT) {
		should_cancel_work = true;
		wdev->freeze_state = FRZ_FREEZED;
	}
	mutex_unlock(&wdev->freeze_lock);

	/* Cancel the melt work if required. */
	if (should_cancel_work) {
		cancel_delayed_work_sync(&wdev->freeze_dwork);
	}
}


/**
 * Freeze if melted and enqueue a melting work if required.
 *
 * @wdev walb device.
 * @timeout_sec timeout to melt the device [sec].
 *   Specify 0 for no timeout.
 *
 * RETURN:
 *   true in success, or false (due to race condition).
 */
bool freeze_if_melted(struct walb_dev *wdev, u32 timeout_sec)
{
	unsigned int minor;
	int ret;

	ASSERT(wdev);
	minor = MINOR(wdev->devt);

	/* Freeze and enqueue a melt work if required. */
	mutex_lock(&wdev->freeze_lock);
	switch (wdev->freeze_state) {
	case FRZ_MELTED:
		/* Freeze iocore and checkpointing. */
		LOGn("Freeze walb device minor %u.\n", minor);
		iocore_freeze(wdev);
		stop_checkpointing(&wdev->cpd);
		wdev->freeze_state = FRZ_FREEZED;
		break;
	case FRZ_FREEZED:
		/* Do nothing. */
		LOGn("Already frozen minor %u.\n", minor);
		break;
	case FRZ_FREEZED_WITH_TIMEOUT:
		LOGe("Race condition occured.\n");
		mutex_unlock(&wdev->freeze_lock);
		return false;
	default:
		BUG();
	}
	ASSERT(wdev->freeze_state == FRZ_FREEZED);
	if (timeout_sec > 0) {
		LOGn("(Re)set frozen timeout to %"PRIu32" seconds.\n",
			timeout_sec);
		INIT_DELAYED_WORK(&wdev->freeze_dwork, task_melt);
		ret = queue_delayed_work(
			wq_misc_, &wdev->freeze_dwork,
			msecs_to_jiffies(timeout_sec * 1000));
		ASSERT(ret);
		wdev->freeze_state = FRZ_FREEZED_WITH_TIMEOUT;
	}
	ASSERT(wdev->freeze_state != FRZ_MELTED);
	mutex_unlock(&wdev->freeze_lock);
	return true;
}

/**
 * Melt a device if frozen.
 *
 * RETURN:
 *   true in success, or false (due to race condition).
 */
bool melt_if_frozen(
	struct walb_dev *wdev, bool restarts_checkpointing)
{
	unsigned int minor;

	ASSERT(wdev);
	minor = MINOR(wdev->devt);

	cancel_melt_work(wdev);

	/* Melt the device if required. */
	mutex_lock(&wdev->freeze_lock);
	switch (wdev->freeze_state) {
	case FRZ_MELTED:
		/* Do nothing. */
		LOGn("Already melted minor %u\n", minor);
		break;
	case FRZ_FREEZED:
		/* Melt. */
		LOGn("Melt walb device minor %u.\n", minor);
		if (restarts_checkpointing) {
			start_checkpointing(&wdev->cpd);
		}
		iocore_melt(wdev);
		wdev->freeze_state = FRZ_MELTED;
		break;
	case FRZ_FREEZED_WITH_TIMEOUT:
		/* Race condition. */
		LOGe("Race condition occurred.\n");
		mutex_unlock(&wdev->freeze_lock);
		return false;
	default:
		BUG();
	}
	ASSERT(wdev->freeze_state == FRZ_MELTED);
	mutex_unlock(&wdev->freeze_lock);
	return true;
}

/**
 * Set geometry for compatibility.
 */
void set_geometry(struct hd_geometry *geo, u64 n_sectors)
{
	geo->heads = 4;
	geo->sectors = 16;
	geo->cylinders = n_sectors >> 6;
	geo->start = 0;
}

/**
 * Get two lsid values as a range from a walb ctl buffer.
 * RETURN:
 *   true in success, or false.
 */
bool get_lsid_range_from_ctl(
	u64 *lsid0, u64 *lsid1, const struct walb_ctl *ctl)
{
	if (sizeof(u64) * 2 > ctl->u2k.buf_size) {
		LOGe("Buffer is too small for u64 * 2.\n");
		return false;
	}
	*lsid0 = ((const u64 *)ctl->u2k.kbuf)[0];
	*lsid1 = ((const u64 *)ctl->u2k.kbuf)[1];
	if (!is_lsid_range_valid(*lsid0, *lsid1)) {
		LOGe("Specify valid lsid range.\n");
		return false;
	}
	return true;
}

/**
 * Set chunk sectors.
 *
 * @chunk_sectors variable to be set.
 * @pbs physical block size.
 * @q request queue to see io_min parameter.
 */
void set_chunk_sectors(
	unsigned int *chunk_sectors, unsigned int pbs,
	const struct request_queue *q)
{
	unsigned int io_min = queue_io_min((struct request_queue *)q);
	ASSERT(io_min % LOGICAL_BLOCK_SIZE == 0);
	if (pbs < io_min) {
		*chunk_sectors = io_min / LOGICAL_BLOCK_SIZE;
	} else {
		*chunk_sectors = 0;
	}
}

/**
 * Print queue limits parameters.
 *
 * @level KERN_ERR, KERN_NOTICE, etc.
 * @msg message.
 * @limits queue limits to print.
 */
void print_queue_limits(
	const char *level, const char *msg,
	const struct queue_limits *limits)
{
	printk("%s"
		"queue limits of %s:\n"
		"    max_hw_sectors: %u\n"
		"    max_sectors: %u\n"
		"    max_segment_size: %u\n"
		"    physical_block_size: %u\n"
		"    alignment_offset: %u\n"
		"    io_min: %u\n"
		"    io_opt: %u\n"
		"    max_discard_sectors: %u\n"
		"    max_write_same_sectors: %u\n"
		"    discard_granularity: %u\n"
		"    discard_alignment: %u\n"
		"    logical_block_size: %u\n"
		"    max_segments: %u\n"
		"    max_integrity_segments: %u\n"
		, level, msg
		, limits->max_sectors
		, limits->max_hw_sectors
		, limits->max_segment_size
		, limits->physical_block_size
		, limits->alignment_offset
		, limits->io_min
		, limits->io_opt
		, limits->max_discard_sectors
		, limits->max_write_same_sectors
		, limits->discard_granularity
		, limits->discard_alignment
		, limits->logical_block_size
		, limits->max_segments
		, limits->max_integrity_segments);
}

/**
 * Get log usage.
 *
 * RETURN:
 *   Log usage [physical block].
 */
u64 walb_get_log_usage(struct walb_dev *wdev)
{
	u64 latest_lsid, oldest_lsid;

	spin_lock(&wdev->lsid_lock);
	latest_lsid = wdev->lsids.latest;
	oldest_lsid = wdev->lsids.oldest;
	spin_unlock(&wdev->lsid_lock);

	ASSERT(latest_lsid >= oldest_lsid);
	return latest_lsid - oldest_lsid;
}

/**
 * Get log capacity of a walb device.
 *
 * @return ring_buffer_size of the walb device.
 */
u64 walb_get_log_capacity(struct walb_dev *wdev)
{
	ASSERT(wdev);

	return wdev->ring_buffer_size;
}

MODULE_LICENSE("Dual BSD/GPL");
