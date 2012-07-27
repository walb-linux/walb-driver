/**
 * common.h - This is common header for both kernel and userland code.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_COMMON_H
#define WALB_COMMON_H

/**
 * Assert macro, integer typedef, etc.
 */
#ifdef __KERNEL__
#include <linux/types.h>
#include <linux/kdev_t.h>
#include "inttypes_kernel.h"
#if defined(WALB_DEBUG) || defined(ASSERT_ON)
#define ASSERT(cond) BUG_ON(!(cond))
#else /* WALB_DEBUG */
#define ASSERT(cond)
#endif /* WALB_DEBUG */
#else /* __KERNEL__ */
#include "userland.h"
#include <assert.h>
#include <string.h>
#define ASSERT(cond) assert(cond)
#endif /* __KERNEL__ */


#define SRC_FILE (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

/**
 * Print macro for debug.
 */
#ifdef __KERNEL__
#include <linux/kernel.h>
#define PRINT(flag, fmt, args...) printk(flag fmt, ##args)
#define PRINT_E(fmt, args...) PRINT(KERN_ERR, fmt, ##args)
#define PRINT_W(fmt, args...) PRINT(KERN_WARNING, fmt, ##args)
#define PRINT_N(fmt, args...) PRINT(KERN_NOTICE, fmt, ##args)
#define PRINT_I(fmt, args...) PRINT(KERN_INFO, fmt, ##args)
#ifdef WALB_DEBUG
#define PRINT_D(fmt, args...) PRINT(KERN_DEBUG, fmt, ##args)
#else
#define PRINT_D(fmt, args...)
#endif
#define PRINTV_E(fmt, args...) PRINT_E("walb(%s) " fmt, __func__, ##args)
#define PRINTV_W(fmt, args...) PRINT_W("walb(%s) " fmt, __func__, ##args)
#define PRINTV_N(fmt, args...) PRINT_N("walb(%s) " fmt, __func__, ##args)
#define PRINTV_I(fmt, args...) PRINT_I("walb(%s) " fmt, __func__, ##args)
#define PRINTV_D(fmt, args...) PRINT_D(                                 \
                "walb(%s:%d:%s) " fmt, SRC_FILE, __LINE__, __func__, ##args)
#else /* __KERNEL__ */
#include <stdio.h>
#ifdef WALB_DEBUG
#define PRINT_D(fmt, args...) printf(fmt, ##args)
#else
#define PRINT_D(fmt, args...)
#endif
#define PRINT_E(fmt, args...) fprintf(stderr, fmt, ##args)
#define PRINT_W PRINT_E
#define PRINT_N PRINT_E
#define PRINT_I PRINT_E
#define PRINT(flag, fmt, args...) printf(fmt, ##args)
#define PRINTV_E(fmt, args...) PRINT_E("ERROR(%s) " fmt, __func__, ##args)
#define PRINTV_W(fmt, args...) PRINT_W("WARNING(%s) " fmt, __func__, ##args)
#define PRINTV_N(fmt, args...) PRINT_N("NOTICE(%s) " fmt, __func__, ##args)
#define PRINTV_I(fmt, args...) PRINT_I("INFO(%s) " fmt, __func__, ##args)
#define PRINTV_D(fmt, args...) PRINT_D(                                 \
                "DEBUG(%s:%d:%s) " fmt, SRC_FILE, __LINE__, __func__, ##args)
#endif /* __KERNEL__ */
#define PRINT_D_(fmt, args...)
#define PRINTV_D_(fmt, args...)

/**
 * Simple logger.
 */
#define LOGd_ PRINTV_D_
#ifdef USE_DYNAMIC_DEBUG
#define LOGd pr_debug
#else
#define LOGd PRINTV_D
#endif
#define LOGi PRINTV_I
#define LOGn PRINTV_N
#define LOGw PRINTV_W
#define LOGe PRINTV_E

/**
 * Memory allocator/deallocator.
 */
#ifdef __KERNEL__
#include <linux/kernel.h>
#include <linux/slab.h>
#define MALLOC(size, mask) kmalloc(size, mask)
#define ZALLOC(size, mask) kzalloc(size, mask)
#define REALLOC(p, size, mask) krealloc(p, size, mask)
#define FREE(p) kfree(p)
#define AMALLOC(size, align, mask) kmalloc((align > size ? align : size), mask)
#else
#include <stdlib.h>
#define MALLOC(size, mask) malloc(size)
#define ZALLOC(size, mask) calloc(1, size)
#define REALLOC(p, size, mask) realloc(p, size)
static inline void* amalloc(size_t size, size_t align)
{
        void *p; return (posix_memalign(&p, align, size) == 0 ? p : NULL);
}
#define AMALLOC(size, align, mask) amalloc(size, align)
#define FREE(p) free(p)
#endif

/**
 * Function/variable attribute macros.
 */
#define DEPRECATED __attribute__((deprecated))
#define UNUSED __attribute__((unused))
#define NOT_YET_IMPLEMENTED __attribute__((warning("NOT YET IMPLEMENTED")))

/**
 * For test.
 */
#define WALB_CHECK_LABEL(cond, label) do {			\
		if (! (cond)) {					\
			LOGe("CHECK FAILED in %s:%d:%s.\n",	\
				__FILE__, __LINE__, __func__);	\
			goto label;				\
		}						\
	} while(0)

#define WALB_CHECK(cond) WALB_CHECK_LABEL(cond, error)

#endif /* WALB_COMMON_H */
