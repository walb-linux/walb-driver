/**
 * sg_util.h - Utilities for scatterlist.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_SG_UTIL_H_KERNEL
#define _WALB_SG_UTIL_H_KERNEL

#include "check_kernel.h"
#include <linux/scatterlist.h>

/* Get data length of a scatterlist. */
unsigned int sg_data_length(const struct sg_table *sgt);

/* Fill zero in a scatterlist. */
bool sg_fill_zero_offset(struct sg_table *sgt, unsigned int offset, unsigned int size);
void sg_fill_zero(struct sg_table *sgt);

/* Allocate a scatterlist and its pages. */
bool sg_alloc_pages(struct sg_table *sgt, unsigned int nents, gfp_t gfp_mask);
/* Free memories allocated by sg_alloc_pages(). */
void sg_free_pages(struct sg_table *sgt);

/* Copy data of a scatterlist to another. */
bool sg_copy_to_sg_offset(
        struct sg_table *dst, unsigned int dst_offset,
        const struct sg_table *src, unsigned int src_offset,
        unsigned int size);
#define sg_copy_to_sg(dst, src, size) sg_copy_to_sg_offset(dst, 0, src, 0, size)

/* Copy from/to a buffer to/from a scatterlist. */
bool sg_copy_to_buffer_offset(
        const struct sg_table *sgt, unsigned int offset, 
        u8 *buf, unsigned int size);
bool sg_copy_from_buffer_offset(
        struct sg_table *sgt, unsigned int offset, 
        const u8 *buf, unsigned int size);

/* Test original scatterlist functionalities. */
__UNUSED
void test_scatterlist(unsigned int nents, unsigned int entsize);
/* Test utilities defined in thie header file. */
__UNUSED
void test_sg_util(void);
/* Test sg_pos and related function. */
__UNUSED
void test_sg_pos(void);

#endif /* WALB_SG_UTIL_H_KERNEL */
