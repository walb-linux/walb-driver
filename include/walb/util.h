/**
 * General definitions for Walb.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_UTIL_H
#define WALB_UTIL_H

#include "common.h"

/**
 * Check macro for is_valid_* functions.
 */
#define CHECK(cond) do {						\
		if (!(cond)) {						\
			LOGe("CHECK failed at line %d.\n", __LINE__);	\
			goto error;					\
		}							\
	} while (0)
#define CHECK_MSG(cond, msg) do {		\
		if (!(cond)) {			\
			LOGe("%s", msg);	\
			goto error;		\
		}				\
	} while (0)

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
	for (i = 0; i < size; i ++ ) {
		if (str_size < (i + 1) * 3 + 1) { return 0; }
		sprintf(tmp, "%02X ", ((u8 *)data)[i]);
		strcat(str, tmp);
	}
	return 1;
}

/**
 * Sprint uuid.
 *
 * @str string buffer to store result.
 * @str_size Its size must be 16 * 3 + 1.
 * @uuid uuid ary. Its size must be 16.
 */
static inline void sprint_uuid(char *str, int str_size, const u8 *uuid)
{
	sprint_hex(str, str_size, uuid, 16);
}

#endif /* WALB_UTIL_H */
