/**
 * util.h - Utility macros and functions.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_UTIL_H_KERNEL
#define WALB_UTIL_H_KERNEL

#include "check_kernel.h"
#include <linux/random.h>
#include "walb/common.h"

/**
 * Get random integer.
 */
static inline u32 get_random_u32(void)
{
        u32 ret;
        get_random_bytes(&ret, sizeof(u32));
        return ret;
}

/**
 * Get random integer.
 * The value will be 0 <= value < max.
 */
static inline u32 get_random_u32_max(u32 max)
{
        if (max == 0) {
                return 0;
        } else {
                return get_random_u32() % max;
        }
}

/**
 * Fill a memory area randomly.
 */
static inline void fill_random(u8 *buf, size_t size)
{
        size_t i, n, m, count = 0;

        /* LOGd("fill_random start.\n"); */

        n = size / sizeof(u32);
        m = size % sizeof(u32);
        ASSERT(sizeof(u32) * n + m == size);
        
        for (i = 0; i < n; i ++) {
                ((u32 *)buf)[i] = get_random_u32();
                count += sizeof(u32);
        }
        for (i = 0; i < m; i ++) {
                buf[n * sizeof(u32) + i] = (u8)get_random_u32();
                count ++;
        }
        /* LOGe("fill_random %zu bytes filled.\n", count); */
        /* LOGd("fill_random end.\n"); */
}

#endif /* WALB_UTIL_H_KERNEL */
