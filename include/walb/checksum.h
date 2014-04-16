/**
 * checksum.h - Checksum functions for WalB.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_CHECKSUM_H
#define WALB_CHECKSUM_H

#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Calculate checksum incrementally.
 *
 * @sum previous checksum. specify 0 for first call.
 * @data pointer to u8 array to calculate
 * @size data size in bytes. This must be dividable by sizeof(u32).
 *
 * @return current checksum.
 */
static inline u32 checksum_partial(u32 sum, const void *data, u32 size)
{
	u32 n = size / sizeof(u32);
	u32 i;
	const u8 *p;

	ASSERT(size % sizeof(u32) == 0);
	p = (const u8 *)data;

	for (i = 0; i < n; i++) {
		u32 buf;
		memcpy(&buf, p, sizeof(u32));
		sum += buf;
		p += sizeof(u32);
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
static inline u32 checksum_finish(u32 sum)
{
	return ~sum + 1;
}

/**
 * Calclate checksum of byte array.
 *
 * @data pointer to u8 array to calculate
 * @size data size in bytes. This must be dividable by sizeof(u32).
 *
 * @return checksum of the data.
 */
static inline u32 checksum(const void *data, u32 size, u32 salt)
{
	return checksum_finish(checksum_partial(salt, data, size));
}

#ifdef __cplusplus
}
#endif

#endif /* WALB_CHECKSUM_H */
