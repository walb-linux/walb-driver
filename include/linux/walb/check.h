/**
 * check and related macros.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#ifndef WALB_CHECK_H
#define WALB_CHECK_H

#ifdef __cplusplus
extern "C" {
#endif

#include "print.h"

/**
 * Check macro for is_valid_* functions.
 */
#define WALB_CHECK(label, cond, msg, level) do {			\
		if (!(cond)) {						\
			PRINT(level, "WALB_CHECK func:%s LINE:%d\n", __func__, __LINE__); \
			goto label;					\
		}							\
	} while (0)

#define CHECKd(cond) WALB_CHECK(error, cond, "", KERN_DEBUG)
#define CHECKi(cond) WALB_CHECK(error, cond, "", KERN_INFO)
#define CHECKn(cond) WALB_CHECK(error, cond, "", KERN_NOTICE)
#define CHECKw(cond) WALB_CHECK(error, cond, "", KERN_WARNING)
#define CHECKe(cond) WALB_CHECK(error, cond, "", KERN_ERROR)

#define CHECKld(label, cond) WALB_CHECK(label, cond, "", KERN_DEBUG)
#define CHECKli(label, cond) WALB_CHECK(label, cond, "", KERN_INFO)
#define CHECKln(label, cond) WALB_CHECK(label, cond, "", KERN_NOTICE)
#define CHECKlw(label, cond) WALB_CHECK(label, cond, "", KERN_WARNING)
#define CHECKle(label, cond) WALB_CHECK(label, cond, "", KERN_ERROR)

#ifdef __cplusplus
}
#endif

#endif /* WALB_CHECK_H */
