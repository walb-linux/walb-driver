/**
 * General definitions for Walb.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_U64BITS_H
#define WALB_U64BITS_H

#include "common.h"

/**
 * Determine whether a bit is set.
 *
 * @nr bit number to test. 0 <= nr < 63.
 * @bits pointer to a u64 value as a bit array.
 *
 * @return On: non-zero, off: 0.
 */
static inline int test_u64bits(int nr, const u64 *bits)
{
        ASSERT(0 <= nr && nr < 64);
        return (((*bits) & ((u64)(1) << nr)) != 0);
}

/**
 * Set a bit of u64 bits.
 */
static inline void set_u64bits(int nr, u64 *bits)
{
        ASSERT(0 <= nr && nr < 64);
        (*bits) |= ((u64)(1) << nr);
}

/**
 * Clear a bit of u64 bits.
 */
static inline void clear_u64bits(int nr, u64 *bits)
{
        ASSERT(0 <= nr && nr < 64);
        (*bits) &= ~((u64)(1) << nr);
}

#endif /* WALB_U64BITS_H */
