/**
 * super.h - Super block functions.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_SUPER_H_KERNEL
#define WALB_SUPER_H_KERNEL

#include "kern.h"

int walb_sync_super_block(struct walb_dev *wdev);
int walb_finalize_super_block(struct walb_dev *wdev, bool is_superblock_sync);

#endif /* WALB_SUPER_H_KERNEL */
