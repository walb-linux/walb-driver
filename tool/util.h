/**
 * Utility functions for walbctl.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_UTIL_H
#define _WALB_UTIL_H

#include "walb.h"
#include "walb_log_device.h"

#define LOG(fmt, ...)                                               \
        fprintf(stderr, "DEBUG(%s,%d): " fmt, __FILE__, __LINE__, ##__VA_ARGS__)


int check_log_dev(const char* path);
int get_bdev_sector_size(const char* devpath);
u64 get_bdev_size(const char* devpath);
dev_t get_bdev_devt(const char *devpath);

void generate_uuid(u8* uuid);


bool read_sector(int fd, u8* sector_buf, u32 sector_size, u64 offset);
bool write_sector(int fd, const u8* sector_buf, u32 sector_size, u64 offset);

bool write_super_sector(int fd, const walb_super_sector_t* super_sect);
bool read_super_sector(int fd, walb_super_sector_t* super_sect,
                       u32 sector_size, u32 n_snapshots);

bool write_snapshot_sector(int fd, const walb_super_sector_t* super_sect,
                           const walb_snapshot_sector_t* snap_sect, size_t idx);
bool read_snapshot_sector(int fd, const walb_super_sector_t* super_sect,
                          walb_snapshot_sector_t* snap_sect, size_t idx);


#endif /* _WALB_UTIL_H */
