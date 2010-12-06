/**
 * Utility functions for walbctl.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_UTIL_H
#define _WALB_UTIL_H

#include <stdio.h>

#include "walb.h"
#include "walb_log_device.h"

#define LOG(fmt, ...)                                                   \
        fprintf(stderr, "DEBUG(%s:%d) " fmt, __FILE__, __LINE__, ##__VA_ARGS__)

/* utility */
void print_binary_hex(const u8* data, size_t size);

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

/* sector functions */
u8* alloc_sectors(int sector_size, int n);
u8* alloc_sectors_zero(int sector_size, int n);
bool realloc_sectors(u8** ptr, int sector_size, int n);
u8* alloc_sector(int sector_size);
u8* alloc_sector_zero(int sector_size);

bool read_sectors(int fd, u8* sectors_buf, u32 sector_size, u64 offset, int n);
bool read_sector(int fd, u8* sector_buf, u32 sector_size, u64 offset);
bool write_sectors(int fd, const u8* sectors_buf, u32 sector_size, u64 offset, int n);
bool write_sector(int fd, const u8* sector_buf, u32 sector_size, u64 offset);

void print_super_sector(const walb_super_sector_t* super_sect);
bool write_super_sector(int fd, const walb_super_sector_t* super_sect);
bool read_super_sector(int fd, walb_super_sector_t* super_sect,
                       u32 sector_size, u32 n_snapshots);

void print_snapshot_record(const walb_snapshot_record_t* snap_rec);
void print_snapshot_sector(const walb_snapshot_sector_t* snap_sect, u32 sector_size);

bool write_snapshot_sector(int fd, const walb_super_sector_t* super_sect,
                           walb_snapshot_sector_t* snap_sect, u32 idx);
bool read_snapshot_sector(int fd, const walb_super_sector_t* super_sect,
                          walb_snapshot_sector_t* snap_sect, u32 idx);

bool read_data(int fd, u8* data, size_t size);
bool write_data(int fd, const u8* data, size_t size);


#endif /* _WALB_UTIL_H */
