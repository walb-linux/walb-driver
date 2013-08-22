/**
 * Utilities for walb.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_UTIL_H
#define WALB_UTIL_H

#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Sprint byte array.
 *
 * @data pointer of the data to print.
 * @size size in bytes.
 * @buf buffer to store result.
 * @buf_size buffer size. size * 3 + 1 is required at least.
 * @return written size except the last '\0' in success, or 0.
 */
static inline int sprint_hex(char *str, int str_size, const void* data, int size)
{
	int i;

	ASSERT(data);
	ASSERT(size >= 0);
	ASSERT(str);
	ASSERT(str_size > 0);
	if (str_size < size * 3 + 1) { return 0; }

	for (i = 0; i < size; i++ ) {
		sprintf(&str[i * 3], "%02X ", ((u8 *)data)[i]);
	}
	return size * 3;
}

/**
 * Constants for UUID data.
 */
#define UUID_SIZE 16
#define UUID_STR_SIZE (16 * 3 + 1)

/**
 * Sprint uuid.
 *
 * @str string buffer to store result.
 * @str_size Its size must be UUID_STR_SIZE or more.
 * @uuid uuid ary. Its size must be UUID_SIZE.
 * RETURN:
 *   written size except the last '\0' in success, or 0.
 */
static inline int sprint_uuid(char *str, int str_size, const u8 *uuid)
{
	return sprint_hex(str, str_size, uuid, UUID_SIZE);
}

/**
 * FNVa hash function.
 */
static inline u32 fnv1a_hash(const u8 *x, unsigned int n)
{
	u32 v = 2166136261;
	unsigned int i;

	for (i = 0; i < n; i++) {
		v ^= x[i];
		v *= 16777619;
	}
	return v;
}

#ifdef __cplusplus
}
#endif

#endif /* WALB_UTIL_H */
