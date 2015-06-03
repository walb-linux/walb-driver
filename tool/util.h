/**
 * Utility functions for walbctl.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_UTIL_USER_H
#define WALB_UTIL_USER_H

#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "linux/walb/common.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Block device info.
 */
struct bdev_info
{
	u64 size; /* block device size [byte]. */
	unsigned int lbs; /* logical block size [byte]. */
	unsigned int pbs; /* physical block size [byte]. */
	struct stat sb; /* fstat result. */
	dev_t devt; /* device id. */
};

/* utility */
void print_binary_hex(const u8* data, size_t size);
bool get_datetime_str(time_t t, char* buf, size_t n);

/* Block device operations. */
bool open_bdev_and_get_info(const char *path, struct bdev_info *info_p, int *fd_p, int flags);
bool get_bdev_info(const char *path, struct bdev_info *info_p);
bool is_block_size_same(const struct bdev_info *info0, const struct bdev_info *info1);
bool is_discard_supported(int fd);
bool discard_whole_area(int fd);

/* uuid functions */
bool generate_uuid(u8* uuid);
void print_uuid(const u8* uuid);
void copy_uuid(u8* dst, const u8* src);

/* basic IO functions. */
bool read_data(int fd, u8* data, size_t size);
bool write_data(int fd, const u8* data, size_t size);

/* Sector functions (will be obsolute). */
bool read_sectors_raw(
	int fd, u8* sectors_buf, u32 sector_size, u64 offset, int n);
bool read_sector_raw(
	int fd, u8* sector_buf, u32 sector_size, u64 offset);
bool write_sectors_raw(
	int fd, const u8* sectors_buf, u32 sector_size, u64 offset, int n);
bool write_sector_raw(
	int fd, const u8* sector_buf, u32 sector_size, u64 offset);

#ifdef __cplusplus
}
#endif

#endif /* WALB_UTIL_USER_H */
