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

#include "walb/common.h"

#ifdef __cplusplus
extern "C" {
#endif

/* utility */
void print_binary_hex(const u8* data, size_t size);
bool get_datetime_str(time_t t, char* buf, size_t n);

/* Block device operations. */
bool is_valid_bdev(const char* path);
int get_bdev_logical_block_size(const char* devpath);
int get_bdev_physical_block_size(const char* devpath);
u64 get_bdev_size(const char* devpath);
dev_t get_bdev_devt(const char *devpath);
bool is_discard_supported(int fd);
bool discard_whole_area(int fd);

/* uuid functions */
void generate_uuid(u8* uuid);
void print_uuid(const u8* uuid);
void copy_uuid(u8* dst, const u8* src);

/* basic IO functions. */
bool read_data(int fd, u8* data, size_t size);
bool write_data(int fd, const u8* data, size_t size);

/* blocksize function. */
bool is_same_block_size(const char* devpath1, const char* devpath2);

/* Sector functions (will be obsolute). */
bool read_sectors_raw(int fd, u8* sectors_buf, u32 sector_size, u64 offset, int n);
bool read_sector_raw(int fd, u8* sector_buf, u32 sector_size, u64 offset);
bool write_sectors_raw(int fd, const u8* sectors_buf, u32 sector_size, u64 offset, int n);
bool write_sector_raw(int fd, const u8* sector_buf, u32 sector_size, u64 offset);

#ifdef __cplusplus
}
#endif

#endif /* WALB_UTIL_USER_H */
