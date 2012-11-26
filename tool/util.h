/**
 * Utility functions for walbctl.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_UTIL_USER_H
#define WALB_UTIL_USER_H

#include "check_userland.h"

#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "walb/walb.h"
#include "walb/log_device.h"

/* utility */
void print_binary_hex(const u8* data, size_t size);
bool get_datetime_str(time_t t, char* buf, size_t n);

/* for block device information */
int check_bdev(const char* path);
int get_bdev_logical_block_size(const char* devpath);
int get_bdev_physical_block_size(const char* devpath);
u64 get_bdev_size(const char* devpath);
dev_t get_bdev_devt(const char *devpath);

/* uuid functions */
void generate_uuid(u8* uuid);
void print_uuid(const u8* uuid);
void copy_uuid(u8* dst, const u8* src);

/* Bitmap functions for debug. */
void print_bitmap(const u8* bitmap, size_t size);
void print_u32bitmap(const u32 bitmap);

/* Sector functions (will be obsolute). */
bool read_sectors_raw(int fd, u8* sectors_buf, u32 sector_size, u64 offset, int n);
bool read_sector_raw(int fd, u8* sector_buf, u32 sector_size, u64 offset);
bool write_sectors_raw(int fd, const u8* sectors_buf, u32 sector_size, u64 offset, int n);
bool write_sector_raw(int fd, const u8* sector_buf, u32 sector_size, u64 offset);

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
void init_super_sector_raw(
	struct walb_super_sector* super_sect,
	unsigned int pbs, unsigned int lbs,
	u64 ddev_lb, u64 ldev_lb, int n_snapshots,
	const char *name);
void print_super_sector_raw(const struct walb_super_sector* super_sect);
bool read_super_sector_raw(int fd, struct walb_super_sector* super_sect,
			u32 sector_size, u32 n_snapshots);
bool write_super_sector_raw(int fd, const struct walb_super_sector* super_sect);

/* Super sector operations. */
void init_super_sector(
	struct sector_data *sect,
	unsigned int pbs, unsigned int lbs,
	u64 ddev_lb, u64 ldev_lb, int n_snapshots,
	const char *name);
void print_super_sector(const struct sector_data *sect);
bool read_super_sector(int fd, struct sector_data *sect);
bool write_super_sector(int fd, const struct sector_data *sect);

/* basic IO functions. */
bool read_data(int fd, u8* data, size_t size);
bool write_data(int fd, const u8* data, size_t size);

/* blocksize function. */
bool is_same_block_size(const char* devpath1, const char* devpath2);


#endif /* WALB_UTIL_USER_H */
