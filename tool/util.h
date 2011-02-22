/**
 * Utility functions for walbctl.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_UTIL_H
#define _WALB_UTIL_H

#include <stdio.h>
#include <time.h>

#include "walb.h"
#include "walb_log_device.h"

#define LOG0(level, fmt, ...)                                   \
        fprintf(stderr, "%s(%s:%d:%s) " fmt, level,             \
                __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define LOG1(level, fmt, ...)                                   \
        fprintf(stderr, "%s " fmt, level, ##__VA_ARGS__)

#ifdef DEBUG
#define LOGd(fmt, ...) LOG0("DEBUG",   fmt, ##__VA_ARGS__)
#else
#define LOGd(fmt, ...)
#endif
#define LOGn(fmt, ...) LOG1("NOTICE",  fmt, ##__VA_ARGS__)
#define LOGw(fmt, ...) LOG1("WARNING", fmt, ##__VA_ARGS__)
#define LOGe(fmt, ...) LOG1("ERROR",   fmt, ##__VA_ARGS__)


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


#endif /* _WALB_UTIL_H */
