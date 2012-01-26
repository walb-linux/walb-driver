/**
 * memblk.h - Definition for memblk driver.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_MEMBLK_H_KERNEL
#define _WALB_MEMBLK_H_KERNEL

#include "check_kernel.h"
#include "walb/disk_name.h"

#define MEMBLK_NAME "memblk"
#define MEMBLK_DIR_NAME "memblk"
#define MEMBLK_DEV_NAME_MAX_LEN (DISK_NAME_LEN - sizeof(MEMBLK_DIR_NAME)

#define MEMBLK_SINGLE_WQ_NAME "memblk_s"
#define MEMBLK_MULTI_WQ_NAME "memblk_m"


#endif /* _WALB_MEMBLK_H_KERNEL */
