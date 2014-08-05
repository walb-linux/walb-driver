/**
 * sector_io.c - Sector IO operations.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/slab.h>
#include <linux/bio.h>
#include <linux/blkdev.h>
#include "walb/logger.h"
#include "sector_io.h"

/**
 * Read/write sector from/to block device.
 * This is blocked operation.
 * Do not call this function in interuption handlers.
 *
 * @bi_rw should be bio->bi_rw like REQ_WRITE, REQ_READ, etc.
 * @bdev block device, which is already opened.
 * @addr address in the block device [physical block].
 * @sect sector data.
 *
 * @return true in success, or false.
 */
bool sector_io(
	ulong bi_rw, struct block_device *bdev,
	u64 addr, struct sector_data *sect)
{
	struct bio *bio;
	int pbs, error;
	struct page *page;
	uint off;
	u8 *buf;

	LOG_("walb_sector_io begin\n");

	ASSERT_SECTOR_DATA(sect);
	buf = sect->data;
	ASSERT(buf);

	pbs = bdev_physical_block_size(bdev);

	if (sect->size != pbs) {
		LOGe("Sector size is invalid %d %d.\n", sect->size, pbs);
		goto error0;
	}

	/* Alloc bio */
	bio = bio_alloc(GFP_NOIO, 1);
	if (!bio) {
		LOGe("bio_alloc failed.\n");
		goto error0;
	}
	ASSERT(virt_addr_valid(buf));
	page = virt_to_page(buf);
	off = offset_in_page(buf);

	bio->bi_rw = bi_rw;
	bio->bi_bdev = bdev;
	bio->bi_iter.bi_sector = addr_lb(pbs, addr);
	bio_add_page(bio, page, pbs, off);

	LOGd("sector %" PRIu64 " "
		"buf %p page %p offset %u sectorsize %d rw %lu\n"
		, addr_lb(pbs, addr)
		, buf, page, off, pbs, bi_rw);

	/* Submit, wait for completion,
	   check errors, and deallocate. */
	error = submit_bio_wait(bi_rw, bio);
	if (error) {
		LOGe("sector io failed with error %d\n", error);
		goto error1;
	}
	bio_put(bio);

	LOG_("walb_sector_io end\n");
	return true;

error1:
	bio_put(bio);
error0:
	return false;
}

/**
 * Print super sector for debug.
 *
 * @lsuper0 super sector.
 */
void walb_print_super_sector(struct walb_super_sector *lsuper0)
{
#ifdef WALB_DEBUG
	char uuidstr[UUID_STR_SIZE];
	sprint_uuid(uuidstr, UUID_STR_SIZE, lsuper0->uuid);

	LOGd("-----super block------\n"
		"checksum %08x\n"
		"logical_bs %u\n"
		"physical_bs %u\n"
		"metadata_size %u\n"
		"uuid: %s\n"
		"sector_type: %04x\n"
		"ring_buffer_size %llu\n"
		"oldest_lsid %llu\n"
		"written_lsid %llu\n"
		"device_size %llu\n"
		"----------\n",
		lsuper0->checksum,
		lsuper0->logical_bs,
		lsuper0->physical_bs,
		lsuper0->metadata_size,
		uuidstr,
		lsuper0->sector_type,
		lsuper0->ring_buffer_size,
		lsuper0->oldest_lsid,
		lsuper0->written_lsid,
		lsuper0->device_size);
#endif
}

/**
 * Read super sector.
 * Currently only super sector 0 will be read.
 *
 * @ldev walb log device.
 * @lsuper sector data to be overwritten by read data.
 *
 * @return true in success, or false.
 */
bool walb_read_super_sector(
	struct block_device *ldev, struct sector_data *lsuper)
{
	u64 off0;
	struct walb_super_sector *sect;
	unsigned int pbs;

	LOGd("walb_read_super_sector begin\n");

	ASSERT_SECTOR_DATA(lsuper);
	pbs = lsuper->size;
	sect = get_super_sector(lsuper);

	/* Really read. */
	off0 = get_super_sector0_offset(pbs);
	/* off1 = get_super_sector1_offset(wdev->physical_bs, 0); */
	if (!sector_io(READ, ldev, off0, lsuper)) {
		LOGe("read super sector0 failed\n");
		goto error0;
	}

	/* Validate checksum. */
	if (checksum((u8 *)sect, lsuper->size, 0) != 0) {
		LOGe("walb_read_super_sector: checksum check failed.\n");
		goto error0;
	}

	/* Validate sector type */
	if (sect->sector_type != SECTOR_TYPE_SUPER) {
		LOGe("walb_read_super_sector: sector type check failed.\n");
		goto error0;
	}

	/* Validate version number. */
	if (sect->version != WALB_LOG_VERSION) {
		LOGe("walb version mismatch: superblock: %u module %u\n",
			sect->version, WALB_LOG_VERSION);
		goto error0;
	}

	/* Validate name structure. */
	if (strnlen(sect->name, DISK_NAME_LEN) >= DISK_NAME_LEN) {
		LOGe("superblock device name is not terminated by 0.\n");
		goto error0;
	}

#ifdef WALB_DEBUG
	walb_print_super_sector(sect);
#endif

	LOGd("walb_read_super_sector end\n");
	return true;
error0:
	return false;
}

/**
 * Write super sector.
 * Currently only super sector 0 will be written. (super sector 1 is not.)
 *
 * @wdev walb device.
 * @lsuper super sector to write.
 *
 * @return true in success, or false.
 */
bool walb_write_super_sector(
	struct block_device *ldev, struct sector_data *lsuper)
{
	u64 off0;
	struct walb_super_sector *sect;
	unsigned int pbs;

	LOG_("walb_write_super_sector begin\n");

	ASSERT(ldev);
	ASSERT_SECTOR_DATA(lsuper);
	sect = get_super_sector(lsuper);
	pbs = lsuper->size;
	ASSERT_PBS(pbs);

	/* Set sector_type. */
	sect->sector_type = SECTOR_TYPE_SUPER;

	/* Generate checksum.
	   zero-clear before calculating checksum. */
	sect->checksum = 0;
	sect->checksum = checksum((u8 *)sect, pbs, 0);

	/* Really write. */
	off0 = get_super_sector0_offset(pbs);
	if (!sector_io(WRITE_FLUSH_FUA, ldev, off0, lsuper)) {
		LOGe("write super sector0 failed\n");
		return false;
	}

	LOG_("walb_write_super_sector end\n");
	return true;
}

MODULE_LICENSE("Dual BSD/GPL");
