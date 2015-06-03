/**
 * Utility functions for walbctl.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_WALB_UTIL_USER_H
#define WALB_WALB_UTIL_USER_H

#include "check_userland.h"

#include "linux/walb/walb.h"
#include "linux/walb/log_device.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Bitmap functions for debug. */
void print_bitmap(const u8* bitmap, size_t size);
void print_u32bitmap(const u32 bitmap);

/* Sector functions. */
bool sector_read(int fd, u64 offset, struct sector_data *sect);
bool sector_write(int fd, u64 offset, const struct sector_data *sect);
bool sector_read_lb(
	int fd,  u64 offset_lb, struct sector_data *sect,
	unsigned int idx_lb, unsigned int n_lb);
bool sector_write_lb(
	int fd,  u64 offset_lb, const struct sector_data *sect,
	unsigned int idx_lb, unsigned int n_lb);
bool sector_array_pread(
	int fd, u64 offset,
	struct sector_data_array *sect_ary,
	unsigned int start_idx, unsigned int n_sectors);
bool sector_array_pwrite(
	int fd, u64 offset,
	const struct sector_data_array *sect_ary,
	unsigned int start_idx, unsigned int n_sectors);
bool sector_array_pread_lb(
	int fd, u64 offset_lb,
	struct sector_data_array *sect_ary,
	unsigned int idx_lb, unsigned int n_lb);
bool sector_array_pwrite_lb(
	int fd, u64 offset_lb,
	const struct sector_data_array *sect_ary,
	unsigned int idx_lb, unsigned int n_lb);
bool sector_array_read(
	int fd,
	struct sector_data_array *sect_ary,
	unsigned int start_idx, unsigned int n_sectors);
bool sector_array_write(
	int fd,
	const struct sector_data_array *sect_ary,
	unsigned int start_idx, unsigned int n_sectors);

/* Raw super sector operations. */
bool init_super_sector_raw(
	struct walb_super_sector* super_sect,
	unsigned int pbs, unsigned int lbs,
	u64 ddev_lb, u64 ldev_lb,
	const char *name);
void print_super_sector_raw(const struct walb_super_sector* super_sect);
bool write_super_sector_raw(
	int fd, const struct walb_super_sector* super_sect);

/* Super sector operations. */
bool init_super_sector(
	struct sector_data *sect,
	unsigned int pbs, unsigned int lbs,
	u64 ddev_lb, u64 ldev_lb,
	const char *name);
void print_super_sector(const struct sector_data *sect);
bool read_super_sector(int fd, struct sector_data *sect);
bool write_super_sector(int fd, const struct sector_data *sect);

#ifdef __cplusplus
}
#endif

#endif /* WALB_WALB_UTIL_USER_H */
