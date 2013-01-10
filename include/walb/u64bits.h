/**
 * General definitions for Walb.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_U64BITS_H
#define WALB_U64BITS_H

#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Determine whether a bit is set.
 *
 * @nr bit number to test. 0 <= nr < 63.
 * @addr pointer to a u64 value as a bit array.
 *
 * @return On: non-zero, off: 0.
 */
static inline int test_bit_u64(int nr, const u64 *addr)
{
	return ((*addr) & ((u64)1 << nr)) != 0;
}

/**
 * Set a bit of u64 bits.
 */
static inline void set_bit_u64(int nr, u64 *addr)
{
	(*addr) |= (u64)1 << nr;
}

/**
 * Clear a bit of u64 bits.
 */
static inline void clear_bit_u64(int nr, u64 *addr)
{
	(*addr) &= ~((u64)1 << nr);
}

#ifdef __cplusplus
}
#endif

#endif /* WALB_U64BITS_H */
