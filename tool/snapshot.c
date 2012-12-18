/**
 * Snapshot functions for walbctl.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <stdlib.h>
#include <string.h>

#include "util.h"
#include "walb_util.h"
#include "snapshot.h"

/*******************************************************************************
 * Utility macros.
 *******************************************************************************/

#define get_n_sectors(snapd) ((int)(snapd)->super->snapshot_metadata_size)
#define get_sector_size(snapd) ((snapd)->super->physical_bs)
#define get_sector(snapd, i) (get_sector_data_in_array((snapd)->sect_ary, i))

/*******************************************************************************
 * Prototype of static functions.
 *******************************************************************************/

/*******************************************************************************
 * Static functions.
 *******************************************************************************/

/*******************************************************************************
 * Utility functions.
 *******************************************************************************/

/**
 * Print snapshot record for debug.
 */
void print_snapshot_record(const struct walb_snapshot_record* snap_rec)
{
	ASSERT(snap_rec);
	PRINT_I_SNAPSHOT_RECORD(snap_rec);
}

/**
 * print a snapshot sector for debug.
 */
void print_snapshot_sector_raw(
	const struct walb_snapshot_sector* snap_sect, u32 sector_size)
{
	printf("checksum: %u\n", snap_sect->checksum);

	printf("bitmap: ");
	print_u32bitmap(snap_sect->bitmap);
	printf("\n");

	/* Print continuous snapshot records */
	int i;
	int max = get_max_n_records_in_snapshot_sector(sector_size);
	for (i = 0; i < max; i++) {
		printf("snapshot record %d: ", i);
		print_snapshot_record(&snap_sect->record[i]);
	}
}

/**
 * print a snapshot sector for debug.
 */
void print_snapshot_sector(const struct sector_data *snap_sect)
{
	ASSERT_SECTOR_DATA(snap_sect);
	print_snapshot_sector_raw(
		get_snapshot_sector_const(snap_sect),
		snap_sect->size);
}

/**
 * Write snapshot sector.
 *
 * @fd File descriptor of log device.
 * @super_sect super sector data to refer its members.
 * @snap_sect snapshot sector data to be written.
 *	      It's allocated size must be really sector size.
 *	      Only checksum area will be overwritten.
 * @idx idx'th sector is written. (0 <= idx < snapshot_metadata_size)
 *
 * @return true in success, or false.
 */
bool write_snapshot_sector(
	int fd, const struct sector_data *super_sect,
	struct sector_data *snap_sect, u32 idx)
{
	ASSERT(fd >= 0);
	ASSERT_SECTOR_DATA(super_sect);
	ASSERT_SECTOR_DATA(snap_sect);

	const struct walb_super_sector *super
		= get_super_sector_const(super_sect);
	struct walb_snapshot_sector *snap
		= get_snapshot_sector(snap_sect);
	ASSERT(super->physical_bs == super_sect->size);
	ASSERT(super->physical_bs == snap_sect->size);

	u32 sect_sz = super->physical_bs;
	u32 meta_sz = super->snapshot_metadata_size;
	if (idx >= meta_sz) {
		LOGe("idx range over. idx: %u meta_sz: %u\n", idx, meta_sz);
		return false;
	}

	/* checksum */
	u8 *sector_buf = (u8*)snap;
	snap->checksum = 0;
	u32 csum = checksum(sector_buf, sect_sz, 0);
	snap->checksum = csum;
	ASSERT(checksum(sector_buf, sect_sz, 0) == 0);

	/* really write snapshot sector. */
	u64 off = get_metadata_offset_2(super) + idx;
	if (!sector_write(fd, off, snap_sect)) {
		return false;
	}
	return true;
}

/**
 * Read snapshot sector.
 *
 * @fd File descriptor of log device.
 * @super_sect super sector data to refer its members.
 * @snap_sect snapshot sector buffer to be read.
 *	      It's allocated size must be really sector size.
 * @idx idx'th sector is read. (0 <= idx < snapshot_metadata_size)
 *
 * @return true in success, or false.
 */
bool read_snapshot_sector(
	int fd, const struct sector_data *super_sect,
	struct sector_data *snap_sect, u32 idx)
{
	ASSERT(fd >= 0);
	ASSERT_SECTOR_DATA(super_sect);
	ASSERT_SECTOR_DATA(snap_sect);

	const struct walb_super_sector *super
		= get_super_sector_const(super_sect);
	ASSERT(super->physical_bs == super_sect->size);
	ASSERT(super->physical_bs == snap_sect->size);

	u32 sect_sz = super->physical_bs;
	u32 meta_sz = super->snapshot_metadata_size;
	if (idx >= meta_sz) {
		LOGe("idx range over. idx: %u meta_sz: %u\n", idx, meta_sz);
		return false;
	}

	/* Read sector data and confirm checksum. */
	u8 *sector_buf = (u8 *)snap_sect->data;
	u64 off = get_metadata_offset_2(super) + idx;
	if (!sector_read(fd, off, snap_sect)) {
		LOGe("sector read failed.\n");
		return false;
	}
	if (checksum(sector_buf, sect_sz, 0) != 0) {
		LOGe("checksum is invalid.\n");
		return false;
	}

	return true;
}

/* end of file */
