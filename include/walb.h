/**
 * General definitions for Walb.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_H
#define _WALB_H

#ifdef __KERNEL__
#include <linux/types.h>
#include <linux/kdev_t.h>
#define ASSERT(cond) BUG_ON(!(cond))
#else /* __KERNEL__ */
#include "userland.h"
#include <assert.h>
#define ASSERT(cond) assert(cond)
#endif /* __KERNEL__ */

static inline u32 checksum(const u8 *data, u32 size)
{
        u64 sum = 0;
        u32 n = size / sizeof(u32);
        u32 i;

        ASSERT(size % sizeof(u32) == 0);

        for (i = 0; i < n; i ++) {
                sum += *(u32 *)(data + (sizeof(u32) * i));
        }
        u32 ret = ~(u32)((sum >> 32) + (sum << 32 >> 32)) + 1;
        return (ret == (u32)(-1) ? 0 : ret);
}

#endif /* _WALB_H */
