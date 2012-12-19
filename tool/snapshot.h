/**
 * Snapshot functions for walbctl.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_SNAPSHOT_USER_H
#define WALB_SNAPSHOT_USER_H

#include "check_userland.h"

#include "walb/walb.h"
#include "walb/log_record.h"
#include "walb/log_device.h"
#include "walb/snapshot.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Prototypes for utility functions.
 */
void print_snapshot_record(const struct walb_snapshot_record* snap_rec);
void print_snapshot_sector_raw(const struct walb_snapshot_sector* snap_sect, u32 sector_size);
void print_snapshot_sector(const struct sector_data *snap_sect);
bool write_snapshot_sector(
	int fd, const struct sector_data *super_sect,
	struct sector_data *snap_sect, u32 idx);
bool read_snapshot_sector(
	int fd, const struct sector_data *super_sect,
	struct sector_data *snap_sect, u32 idx);

#ifdef __cplusplus
}
#endif

#endif /* WALB_SNAPSHOT_USER_H */
