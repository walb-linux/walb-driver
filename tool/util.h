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

/* sector functions
   These will be deprecated.
   Use sector_alloc() instead. */
u8* alloc_sectors(int sector_size, int n);
u8* alloc_sectors_zero(int sector_size, int n);
bool realloc_sectors(u8** ptr, int sector_size, int n);
u8* alloc_sector(int sector_size);
u8* alloc_sector_zero(int sector_size);

/* Sector functions (will be obsolute). */
bool read_sectors(int fd, u8* sectors_buf, u32 sector_size, u64 offset, int n);
bool read_sector(int fd, u8* sector_buf, u32 sector_size, u64 offset);
bool write_sectors(int fd, const u8* sectors_buf, u32 sector_size, u64 offset, int n);
bool write_sector(int fd, const u8* sector_buf, u32 sector_size, u64 offset);

/* Sector functions. */
bool sector_read(int fd, u64 offset, struct sector_data *sect);
bool sector_write(int fd, u64 offset, const struct sector_data *sect);
bool sector_array_read(int fd, u64 offset,
                       struct sector_data_array *sect_ary,
                       int start_idx, int n_sectors);
bool sector_array_write(int fd, u64 offset,
                        const struct sector_data_array *sect_ary,
                        int start_idx, int n_sectors);

/* Obsolute super sector operations. */
void __init_super_sector(walb_super_sector_t* super_sect,
                         int physical_bs, int logical_bs,
                         u64 ddev_lb, u64 ldev_lb, int n_snapshots,
                         const char *name);
void __print_super_sector(const walb_super_sector_t* super_sect);
bool __read_super_sector(int fd, walb_super_sector_t* super_sect,
                         u32 sector_size, u32 n_snapshots);
bool __write_super_sector(int fd, const walb_super_sector_t* super_sect);

/* New super sector operations. */
void init_super_sector(struct sector_data *sect,
                       int physical_bs, int logical_bs,
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
