/**
 * util.h - Utility macros and functions.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_UTIL_H_KERNEL
#define _WALB_UTIL_H_KERNEL

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
        return get_random_u32() % max;
}

/**
 * Fill a memory area randomly.
 */
static inline void fill_random(u8 *buf, size_t size)
{
        int i;
        size_t off = 0;
        size_t loop;
        size_t count = 0;

        LOGd("fill_random start.\n");
        
        loop = size / sizeof(u32);
        for (i = 0; i < loop; i ++) {

                off = i * sizeof(u32);
                ((u32 *)buf)[off] = get_random_u32();
                count += sizeof(u32);
        }
        loop = size % sizeof(u32);
        for (i = 0; i < loop; i ++) {

                off = (size / sizeof(u32)) * sizeof(u32) + i;
                buf[off] = (u8)get_random_u32();
                count ++;
        }
        LOGe("fill_random %zu bytes filled.\n", count);
        LOGd("fill_random end.\n");
}

#endif /* _WALB_UTIL_H_KERNEL */
