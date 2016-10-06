/**
 * common.h - This is common header for both kernel and userland code.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_COMMON_H
#define WALB_COMMON_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Assert macro, integer typedef, etc.
 */
#ifdef __KERNEL__
#include <linux/types.h>
#include <linux/kdev_t.h>
#include "inttypes_kernel.h"
#if defined(WALB_DEBUG) || defined(ASSERT_ON)
#define ASSERT(cond) WARN_ON(!(cond))
#else /* WALB_DEBUG */
#define ASSERT(cond)
#endif /* WALB_DEBUG */
#else /* __KERNEL__ */
#include "userland.h"
#include <assert.h>
#include <string.h>
#include <stdio.h>
#define ASSERT(cond) assert(cond)
#endif /* __KERNEL__ */


#define SRC_FILE (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

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
 * min/max.
 *
 * Do not use min/max directly
 * because c++ namespace will be affected.
 */
#ifdef __KERNEL__
#define get_min_value(x, y) min(x, y)
#define get_max_value(x, y) max(x, y)
#else
#define get_min_value(x, y) ((x) < (y) ? (x) : (y))
#define get_max_value(x, y) ((x) > (y) ? (x) : (y))
#endif

#ifdef __cplusplus
}
#endif

#endif /* WALB_COMMON_H */
