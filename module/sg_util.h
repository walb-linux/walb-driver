/**
 * sg_util.h - Utilities for scatterlist.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_SG_UTIL_H_KERNEL
#define _WALB_SG_UTIL_H_KERNEL

#include "check_kernel.h"
#include <linux/scatterlist.h>


unsigned int sg_data_length(const struct sg_table *sgt);

bool sg_fill_zero_offset(struct sg_table *sgt, unsigned int offset, unsigned int size);
void sg_fill_zero_old(struct sg_table *sgt);
void sg_fill_zero(struct sg_table *sgt);

bool sg_alloc_pages(struct sg_table *sgt, unsigned int nents, gfp_t gfp_mask);
void sg_free_pages(struct sg_table *sgt);

void sg_copy_to_sg_offset(
        struct sg_table *dst, unsigned int dst_offset,
        const struct sg_table *src, unsigned int src_offset,
        unsigned int size);
void sg_copy_to_sg_offset_old(
        struct sg_table *dst, unsigned int dst_offset,
        const struct sg_table *src, unsigned int src_offset,
        unsigned int size);

#define sg_copy_to_sg(dst, src, size) sg_copy_offset(dst, 0, src, 0, size)


__UNUSED
void test_scatterlist(unsigned int nents, unsigned int entsize);
__UNUSED
void test_sg_util(void);

#endif /* WALB_SG_UTIL_H_KERNEL */
