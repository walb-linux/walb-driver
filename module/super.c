/**
 * checkpoint.c - Checkpointing functions.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"

#include <linux/module.h>
#include "sector_io.h"
#include "super.h"

/**
 * Sync down super block.
 *
 * This always fails if read-only flag is set.
 * This will set read-only flag if write/flush IOs failed.
 *
 * RETURN:
 *   true in success, or false.
 */
bool walb_sync_super_block(struct walb_dev *wdev)
{
	u64 written_lsid, oldest_lsid;
	struct sector_data *lsuper_tmp;
	struct walb_super_sector *sect;
	u64 device_size;

	ASSERT(wdev);

	/* It always fails in read only mode. */
	if (test_bit(WALB_STATE_READ_ONLY, &wdev->flags))
		return false;

	/* Allocate temporary super block. */
	lsuper_tmp = sector_alloc(wdev->physical_bs, GFP_NOIO);
	if (!lsuper_tmp)
		goto error0;

	/* Get written/oldest lsid. */
	spin_lock(&wdev->lsid_lock);
	written_lsid = wdev->lsids.written;
	oldest_lsid = wdev->lsids.oldest;
	spin_unlock(&wdev->lsid_lock);

	/* device size. */
	spin_lock(&wdev->size_lock);
	device_size = wdev->size;
	spin_unlock(&wdev->size_lock);

	/* Modify super sector and copy. */
	spin_lock(&wdev->lsuper0_lock);
	ASSERT_SECTOR_DATA(wdev->lsuper0);
	ASSERT(is_same_size_sector(wdev->lsuper0, lsuper_tmp));
	sect = get_super_sector(wdev->lsuper0);
	sect->oldest_lsid = oldest_lsid;
	sect->written_lsid = written_lsid;
	sect->device_size = device_size;
	sect->log_checksum_salt = wdev->log_checksum_salt;
	sector_copy(lsuper_tmp, wdev->lsuper0);
	spin_unlock(&wdev->lsuper0_lock);

	/* Flush the data device for written_lsid to be permanent. */
	if (supports_flush_request_bdev(wdev->ddev)) {
		if (blkdev_issue_flush(wdev->ddev, GFP_KERNEL, NULL)) {
			WLOGe(wdev, "ddev flush failed.\n");
			goto error1;
		}
	}

	/* Write and flush superblock in the log device. */
	if (!walb_write_super_sector(wdev->ldev, lsuper_tmp)) {
		WLOGe(wdev, "write and flush super block failed.\n");
		goto error1;
	}

	sector_free(lsuper_tmp);

	/* Update previously written lsid. */
	spin_lock(&wdev->lsid_lock);
	wdev->lsids.prev_written = written_lsid;
	spin_unlock(&wdev->lsid_lock);

	return true;

error1:
	set_bit(WALB_STATE_READ_ONLY, &wdev->flags);
	sector_free(lsuper_tmp);
error0:
	return false;
}

/**
 * Finalize super block.
 *
 * @wdev walb device.
 *
 * RETURN:
 *   true in success, or false.
 */
bool walb_finalize_super_block(struct walb_dev *wdev, bool is_superblock_sync)
{
	spin_lock(&wdev->lsid_lock);
	wdev->lsids.written = wdev->lsids.latest;
	spin_unlock(&wdev->lsid_lock);

	if (is_superblock_sync) {
		WLOGi(wdev, "finalize super block\n");
		return walb_sync_super_block(wdev);
	} else {
		WLOGi(wdev, "do not finalize super block\n");
		return true;
	}
}

MODULE_LICENSE("Dual BSD/GPL");
