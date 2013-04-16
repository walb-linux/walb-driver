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
 * Check macro for is_valid_* functions.
 */
#define CHECK(label, cond, msg, level) do {   \
	if (!(cond)) {			     \
		LOG ## level("CHECK failed at %s line %d: %s", __func__, __LINE__, msg); \
		goto label;							\
	}								\
} while (0)

#define CHECKLd(label, cond) CHECK(label, cond, "", d)
#define CHECKd(cond) CHECK(error, cond, "", d)
#define CHECKe(cond) CHECK(error, cond, "", e)

/**
 * Sprint byte array.
 *
 * @data pointer of the data to print.
 * @size size in bytes.
 * @buf buffer to store result.
 * @buf_size buffer size. size * 3 + 1 is required at least.
 * @return Non-zero if buf_size is enough, or 0.
 */
static inline int sprint_hex(char *str, int str_size, const void* data, int size)
{
	int i;
	char tmp[4];

	ASSERT(data);
	ASSERT(size >= 0);
	ASSERT(str);
	ASSERT(str_size > 0);
	ASSERT(str_size >= size * 3 + 1);

	str[0] = '\0';
	for (i = 0; i < size; i++ ) {
		if (str_size < (i + 1) * 3 + 1) { return 0; }
		sprintf(tmp, "%02X ", ((u8 *)data)[i]);
		strcat(str, tmp);
	}
	return 1;
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
 */
static inline void sprint_uuid(char *str, int str_size, const u8 *uuid)
{
	sprint_hex(str, str_size, uuid, UUID_SIZE);
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
