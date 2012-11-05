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
 * RETURN:
 *   true in success, or false.
 */
bool walb_sync_super_block(struct walb_dev *wdev)
{
	u64 written_lsid, oldest_lsid;
	struct sector_data *lsuper_tmp;
	struct walb_super_sector *sect, *sect_tmp;
	struct checkpoint_data *cpd;
	u64 device_size;

	ASSERT(wdev);
	cpd = &wdev->cpd;

	/* Get written lsid. */
	spin_lock(&cpd->written_lsid_lock);
	written_lsid = cpd->written_lsid;
	spin_unlock(&cpd->written_lsid_lock);

	/* Get oldest lsid. */
	spin_lock(&wdev->lsid_lock);
	oldest_lsid = wdev->oldest_lsid;
	spin_unlock(&wdev->lsid_lock);

	/* device size. */
	spin_lock(&wdev->size_lock);
	device_size = wdev->size;
	spin_unlock(&wdev->size_lock);

	/* Allocate temporary super block. */
	lsuper_tmp = sector_alloc(wdev->physical_bs, GFP_NOIO);
	if (!lsuper_tmp) {
		goto error0;
	}
	ASSERT_SECTOR_DATA(lsuper_tmp);
	sect_tmp = get_super_sector(lsuper_tmp);

	/* Modify super sector and copy. */
	spin_lock(&wdev->lsuper0_lock);
	ASSERT_SECTOR_DATA(wdev->lsuper0);
	ASSERT(is_same_size_sector(wdev->lsuper0, lsuper_tmp));
	sect = get_super_sector(wdev->lsuper0);
	sect->oldest_lsid = oldest_lsid;
	sect->written_lsid = written_lsid;
	sect->device_size = device_size;
	sector_copy(lsuper_tmp, wdev->lsuper0);
	spin_unlock(&wdev->lsuper0_lock);
	
	if (!walb_write_super_sector(wdev->ldev, lsuper_tmp)) {
		LOGe("walb_sync_super_block: write super block failed.\n");
		goto error1;
	}

	sector_free(lsuper_tmp);

	/* Update previously written lsid. */
	spin_lock(&cpd->written_lsid_lock);
	cpd->prev_written_lsid = written_lsid;
	spin_unlock(&cpd->written_lsid_lock);
	
	return true;

error1:
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
	/* 
	 * 1. Wait for all related IO are finished.
	 * 2. Cleanup snapshot metadata and write down.
	 * 3. Generate latest super block and write down.
	 */
	
	u64 latest_lsid;
	struct checkpoint_data *cpd = &wdev->cpd;

	spin_lock(&wdev->lsid_lock);
	latest_lsid = wdev->latest_lsid;
	spin_unlock(&wdev->lsid_lock);
	
	spin_lock(&cpd->written_lsid_lock);
	cpd->written_lsid = latest_lsid;
	spin_unlock(&cpd->written_lsid_lock);

	if (is_superblock_sync) {
		LOGn("is_superblock_sync is on\n");
		if (!walb_sync_super_block(wdev)) {
			goto error0;
		}
	} else {
		LOGn("is_superblock_sync is off\n");
	}
	return true;

error0:
	return false;
}

MODULE_LICENSE("Dual BSD/GPL");
