/**
 * memblk_io.h - Definition for memblk io functions.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_MEMBLK_DATA_H_KERNEL
#define _WALB_MEMBLK_DATA_H_KERNEL

#include "check_kernel.h"

#include <linux/mm.h>
#include "walb/common.h"
#include "treemap.h"

/**
 * Memory blocks.
 * Each block can be accessed by address with O(log N) where
 * N is number of blocks.
 */
struct memblk_data
{
        map_t *index;

        u32 block_size; /* 512 to 4096 */
        u64 capacity; /* capacity [block_size] */
};

__UNUSED
static inline void mdata_assert_block_size(u32 block_size) 
{
        ASSERT(block_size >= 512);
        ASSERT(block_size <= PAGE_SIZE);
        ASSERT(PAGE_SIZE % block_size == 0);
}

__UNUSED
static inline u32 mdata_get_n_blocks_in_a_page(u32 block_size)
{
        mdata_assert_block_size(block_size);
        
        return PAGE_SIZE / block_size;
}

__UNUSED
static inline u64 mdata_get_required_n_pages(u64 capacity, u32 block_size)
{
        u64 n = (u64)mdata_get_n_blocks_in_a_page(block_size);
        return  (capacity + n - 1) / n;
}

__UNUSED
static inline u64 mdata_get_page_id(u64 addr, u32 block_size)
{
        return addr / mdata_get_n_blocks_in_a_page(block_size);
}

__UNUSED
static inline u32 mdata_get_page_offset(u64 addr, u32 block_size)
{
        return addr % mdata_get_n_blocks_in_a_page(block_size);
}

/* Create/destroy. */
struct memblk_data* mdata_create(u64 capacity, u32 block_size, gfp_t gfp_mask);
void mdata_destroy(struct memblk_data *memblk_data);

/* Get pointer to the block data. */
u8* mdata_get_block(struct memblk_data *mdev, u64 block_addr);
/* Read/write a block. */
void mdata_read_block(const struct memblk_data *mdata, u64 block_id, u8 *dst);
void mdata_write_block(struct memblk_data *mdata, u64 block_id, const u8 *src);

/* Read/write continuous blocks. */
void mdata_read_blocks(
        const struct memblk_data *mdata,
        u64 block_id, u32 n_blocks, u8 *dst);
void mdata_write_blocks(
        struct memblk_data *mdata,
        u64 block_id, u32 n_blocks, const u8 *src);

__DEPRECATED
void mdata_copy_from(
        struct memblk_data *mdata, u64 block_addr, u32 offset,
        void *buf, size_t size);
__DEPRECATED
void mdata_copy_to(
        struct memblk_data *mdata, u64 block_addr, u32 offset,
        const void *buf, size_t size);

__UNUSED
bool test_memblk_data(u64 capacity, const u32 block_size);
__UNUSED
bool test_memblk_data_simple(u64 capacity, const u32 block_size);

#endif /* _WALB_MEMBLK_DATA_H_KERNEL */
