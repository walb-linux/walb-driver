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

#endif /* WALB_UTIL_H_KERNEL */
