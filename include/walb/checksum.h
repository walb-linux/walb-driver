/**
 * checksum.h - Checksum functions for WalB.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_CHECKSUM_H
#define WALB_CHECKSUM_H

#include "common.h"

/**
 * Calculate checksum incrementally.
 *
 * @sum previous checksum. specify 0 for first call.
 * @data pointer to u8 array to calculate
 * @size data size in bytes. This must be dividable by sizeof(u32).
 *
 * @return current checksum.
 */
static inline u64 checksum_partial(u64 sum, const u8 *data, u32 size)
{
        u32 n = size / sizeof(u32);
        u32 i;

        ASSERT(size % sizeof(u32) == 0);

        for (i = 0; i < n; i ++) {
                sum += *(u32 *)(data + (sizeof(u32) * i));
        }
        return sum;
}

/**
 * Finish checksum.
 *
 * You must call this at the end of checksum calculation.
 *
 * @sum previous checksum.
 *
 * @return checksum of whole data.
 */
static inline u32 checksum_finish(u64 sum)
{
        u32 ret;
        
        ret = ~(u32)((sum >> 32) + (sum << 32 >> 32)) + 1;
        return (ret == (u32)(-1) ? 0 : ret);
}

/**
 * Calclate checksum of byte array.
 *
 * @data pointer to u8 array to calculate
 * @size data size in bytes. This must be dividable by sizeof(u32).
 *
 * @return checksum of the data.
 */
static inline u32 checksum(const u8 *data, u32 size)
{
        return checksum_finish(checksum_partial(0, data, size));
}

#endif /* WALB_CHECKSUM_H */
