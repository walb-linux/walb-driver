/**
 * sector_io.h - Sector IO operations.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_SECTOR_IO_H_KERNEL
#define WALB_SECTOR_IO_H_KERNEL

#include "check_kernel.h"
#include <linux/list.h>
#include <linux/completion.h>
#include <linux/bio.h>
#include <linux/blkdev.h>
#include "linux/walb/walb.h"
#include "linux/walb/sector.h"
#include "linux/walb/super.h"
#include "linux/walb/log_record.h"
#include "linux/walb/log_device.h"
#include "linux/walb/block_size.h"

/*******************************************************************************
 * Function prototypes.
 *******************************************************************************/

/* Sector IO function. */
bool sector_io(
	uint op, uint op_flags, struct block_device *bdev,
	u64 off, struct sector_data *sect);

/* Super sector functions. */
void walb_print_super_sector(struct walb_super_sector *lsuper0);
bool walb_read_super_sector(
	struct block_device *ldev, struct sector_data *lsuper);
bool walb_write_super_sector(
	struct block_device *ldev, struct sector_data *lsuper);

#endif /* WALB_SECTOR_IO_H_KERNEL */
