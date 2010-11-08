/**
 * Utility functions for walbctl.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_UTIL_H
#define _WALB_UTIL_H

int check_log_dev(char* path);
int get_bdev_sector_size(char* devpath);
u64 get_bdev_size(char* devpath);


/**
 * generate uuid.
 *
 * @uuid size must be 16.
 */
void generate_uuid(u8* uuid);

#endif /* _WALB_UTIL_H */
