/**
 * memblk_data.c - Memory data implementation.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"

#include <linux/mm.h>
#include "walb/common.h"
#include "treemap.h"
#include "memblk_data.h"
#include "util.h" /* for debug */
#include "walb/util.h" /* for debug */

/*******************************************************************************
 * Static variables prototype.
 *******************************************************************************/

/* for debug */
int count_ = 0;

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

/* Copy from/to a buffer. */
static void __memblk_data_copy(
        struct memblk_data *mdata, u64 block_addr, u32 offset,
        void *buf, size_t size, bool is_from);
/* Read/write a block. */
static void __memblk_data_block_io(
        struct memblk_data *mdata, u64 block_id, u8 *data, bool is_write);
/* Read/write continuous blocks. */
static void __memblk_data_blocks_io(
        struct memblk_data *mdata, u64 block_id, u32 n_blocks,
        u8 *data, bool is_write);

/* For test */
static u32 get_random_capacity(u32 block_size);
static u64 get_random_addr(u64 capacity);

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * Copy data from/to memblk_data to/from a buffer.
 *
 * @mdata memblk data.
 * @block_addr block address in the mdata [blocks].
 * @offset offset in the block [bytes].
 * @buf buffer pointer to copy data to.
 * @size copy size [bytes].
 * @is_from true when copy from a memblk_data to a buffer.
 */
static void __memblk_data_copy(
        struct memblk_data *mdata, u64 block_addr, u32 offset,
        void *buf, size_t size, bool is_from)
{
        u8 *data, *buf2;
        size_t tmp_size;
        size_t remaining;
        size_t count = 0; /* debug */
        
        ASSERT(mdata);
        ASSERT(block_addr >= 0);
        ASSERT(block_addr < mdata->capacity);
        ASSERT(offset < mdata->block_size);
        ASSERT(buf);


        ASSERT(block_addr +
               (((u64) offset + (u64) size + (u64)mdata->block_size - 1) / (u64)mdata->block_size)
               <= mdata->capacity);

        LOGd("__memblk_data_copy start.\n");
        
        /* Copy fragment of first block. */
        ASSERT(mdata->block_size >= offset);
        tmp_size = mdata->block_size - offset;
        data = memblk_data_get_block(mdata, block_addr) + offset;
        if (is_from) {
                memcpy(buf, data, tmp_size);
        } else {
                memcpy(data, buf, tmp_size);
        }
        remaining = size - tmp_size;
        count += tmp_size;
        block_addr ++;

        /* Copy remaining blocks */
        while (remaining > 0) {
                data = memblk_data_get_block(mdata, block_addr);
                tmp_size = min((size_t)mdata->block_size, remaining);
                buf2 = (u8 *)buf + (size - remaining);
                if (is_from) {
                        memcpy(buf2, data, tmp_size);
                } else {
                        memcpy(data, buf2 , tmp_size);
                }
                remaining -= tmp_size;
                count += tmp_size;
                block_addr ++;
        }
        LOGd("data copy size %zu count %zu remaining %zu.\n",
             size, count, remaining); /* debug */
        ASSERT(remaining == 0);

        printk(KERN_DEBUG "__memblk_data_copy end.\n");
        LOGd("__memblk_data_copy end.\n");
}

/**
 * Multiple-block IO.
 */
static void __memblk_data_blocks_io(struct memblk_data *mdata, u64 block_id, u32 n_blocks,
                                    u8 *data, bool is_write)
{
        u64 ui;
        unsigned int offset = 0;
        
        ASSERT(mdata);
        ASSERT(data);
        for (ui = 0; ui < n_blocks; ui ++) {
                if (block_id + ui >= mdata->capacity) {
                        LOGe("Access to outside the capacity %"PRIu64" addr %"PRIu64".",
                             block_id + ui, mdata->capacity);
                        return;
                }
                __memblk_data_block_io(mdata, block_id + ui, data + offset, is_write);
                offset += mdata->block_size;
        }
}

/**
 * Block IO.
 */
static void __memblk_data_block_io(struct memblk_data *mdata, u64 block_id, u8 *data, bool is_write)
{
        u8 *buf, *src, *dst;

        ASSERT(mdata);
        ASSERT(block_id < mdata->capacity);
        ASSERT(data);
        
        buf = memblk_data_get_block(mdata, block_id);
        ASSERT(buf);
        
        if (is_write) {
                src = data;
                dst = buf;
        } else {
                src = buf;
                dst = data;
        }
        memcpy(dst, src, mdata->block_size);
}

/*******************************************************************************
 * Global functions prototype.
 *******************************************************************************/

/**
 * Allocate memblk data.
 */
struct memblk_data* create_memblk_data(u64 capacity, u32 block_size, gfp_t gfp_mask)
{
        struct memblk_data *mdata;
        u64 ui, n_pages;
        unsigned long addr;

        assert_block_size(block_size);
        ASSERT(capacity > 0);

        /* Allocate mdata */
        mdata = ZALLOC(sizeof(struct memblk_data), gfp_mask);
        if (!mdata) {
                LOGe("Memory allocation failure.\n");
                goto error0;
        }
        mdata->block_size = block_size;
        mdata->capacity = capacity;

        /* Allocate index */
        mdata->index = map_create(gfp_mask);
        if (!mdata->index) {
                LOGe("map_create failed.\n");
                goto error1;
        }

        /* Allocate each block */
        n_pages = get_required_n_pages(capacity, block_size);
        /* LOGd("n_pages: %"PRIu64"\n", n_pages); */
        for (ui = 0; ui < n_pages; ui ++) {
                addr = __get_free_page(gfp_mask); count_ ++;
                /* LOGd("allocate a page addr %p.\n", (void *)addr); */
                if (!addr) {
                        LOGe("__get_free_page failed.\n");
                        goto error1;
                }
                if (map_add(mdata->index, ui, addr, gfp_mask)) {
                        LOGe("map_add failed.\n");
                        free_page(addr); count_ --;
                        goto error1;
                }
                /* LOGd("allocate ui %"PRIu64" addr %p.\n", ui, (void *)addr); */
        }
        return mdata;
        
error1:
        destroy_memblk_data(mdata);
error0:
        return NULL;
}

/**
 * Destroy memblk_data.
 */
void destroy_memblk_data(struct memblk_data *mdata)
{
        u64 n_pages;
        u64 page_id;
        unsigned long addr;

        if (!mdata) { return; }

        if (mdata->index) {
                n_pages = get_required_n_pages(
                        mdata->capacity, mdata->block_size);
                for (page_id = 0; page_id < n_pages; page_id ++) {
                        addr = map_del(mdata->index, page_id);
                        ASSERT(addr != TREEMAP_INVALID_VAL);
                        /* LOGd("page_id %"PRIu64" n_pages %"PRIu64" addr %p\n", */
                        /*      page_id, n_pages, (void *)addr); */
                        free_page(addr); count_ --;
                }
                map_destroy(mdata->index);
        }
        FREE(mdata);
}

/**
 * @mdata memory data.
 * @block_addr addr [mdata->block_size].
 * CONTEXT:
 * Any.
 * RETURN:
 * Pointer to the data.
 * The area of mdata->block_size is available at least.
 */
u8* memblk_data_get_block(struct memblk_data *mdata, u64 block_addr)
{
        u64 page_id;
        unsigned long addr;

        ASSERT(mdata);
        ASSERT(block_addr < mdata->capacity);
        page_id = get_page_id(block_addr, mdata->block_size);

        addr = map_lookup(mdata->index, page_id);
        ASSERT(addr != TREEMAP_INVALID_VAL);
        ASSERT(addr != 0);

        addr += mdata->block_size *
                get_page_offset(block_addr, mdata->block_size);

        return (u8 *)addr;
}

/**
 * Copy data from memblk_data to a buffer.
 *
 * @mdata memblk data.
 * @block_addr block address in the mdata [blocks].
 * @offset offset in the block [bytes].
 * @buf buffer pointer to copy data to.
 * @size copy size [bytes].
 */
void memblk_data_copy_from(
        struct memblk_data *mdata, u64 block_addr, u32 offset,
        void *buf, size_t size)
{
        LOGd("memblk_data_copy_from() begin\n"); /* debug */
        __memblk_data_copy(mdata, block_addr, offset,
                           buf, size, true);
        LOGd("memblk_data_copy_from() end\n"); /* debug */
}

/**
 * Copy data from a buffer to memblk_data.
 *
 * @mdata memblk data.
 * @block_addr block address in the mdata [blocks].
 * @offset offset in the block [bytes].
 * @buf buffer pointer to copy data from.
 * @size copy size [bytes]. Satisfy size <= block_size.
 */
void memblk_data_copy_to(
        struct memblk_data *mdata, u64 block_addr, u32 offset,
        const void *buf, size_t size)
{
        LOGd("memblk_data_copy_to() begin block_addr %"PRIu64" offset %"PRIu32" size %zu\n",
             block_addr, offset, size); /* debug */

        __memblk_data_copy(mdata, block_addr, offset,
                           (void *)buf, size, false);
        LOGd("memblk_data_copy_to() end\n"); /* debug */
}

/**
 * Read a block.
 */
void memblk_data_read_block(const struct memblk_data *mdata, u64 block_id, u8 *dst)
{
        __memblk_data_block_io((struct memblk_data *)mdata, block_id, dst, false);
}

/**
 * Write a block.
 */
void memblk_data_write_block(struct memblk_data *mdata, u64 block_id, const u8 *src)
{
        __memblk_data_block_io(mdata, block_id, (u8 *)src, true);
}

/**
 * Read blocks.
 */
void memblk_data_read_blocks(
        const struct memblk_data *mdata,
        u64 block_id, u32 n_blocks, u8 *dst)
{
        __memblk_data_blocks_io((struct memblk_data *)mdata, block_id, n_blocks, dst, false);
}

/**
 * Write blocks.
 */
void memblk_data_write_blocks(
        struct memblk_data *mdata,
        u64 block_id, u32 n_blocks, const u8 *src)
{
        __memblk_data_blocks_io(mdata, block_id, n_blocks, (u8 *)src, true);
}


/*******************************************************************************
 * For test.
 *******************************************************************************/

/**
 * For test.
 */
static u32 get_random_capacity(u32 block_size)
{
        const u32 max_capacity_in_bytes = 1048576;

        return (get_random_u32_max(max_capacity_in_bytes) / block_size);
}

static u64 get_random_addr(u64 capacity)
{
        return get_random_u32_max((u32)capacity);
}

/**
 * Allocate/deallocate memblk_data.
 */
bool test_memblk_data_simple(u64 capacity, const u32 block_size)
{
        struct memblk_data *mdata = NULL;
        u64 b_id;
        u8 *data;
        
        ASSERT(capacity > 0);
        assert_block_size(block_size);

        mdata = create_memblk_data(capacity, block_size, GFP_KERNEL);
        if (!mdata) {
                LOGe("create_memblk_data failed.\n");
                goto error0;
        }
        for (b_id = 0; b_id < mdata->capacity; b_id ++) {
                data = memblk_data_get_block(mdata, b_id);
                LOGd("b_id %"PRIu64" capacity %"PRIu64" data %p\n",
                     b_id, mdata->capacity, data);
        }
        return true;
/* error1: */
/*         destroy_memblk_data(mdata); */
error0:
        return false;
}

/**
 * Test memblk data.
 */
bool test_memblk_data(u64 capacity, const u32 block_size)
{
        u64 addr;
        struct memblk_data *mdata = NULL;
        u8 *data1 = NULL, *data2 = NULL;
        int i;
        u32 size;
        char *strbuf = NULL;

        LOGd("test_memblk_data start.\n");
        strbuf = (char *)__get_free_page(GFP_KERNEL); count_ ++;
        __CHECK(strbuf, error);

        if (capacity == 0) {
                capacity = get_random_capacity(block_size) + 4;
        }
        mdata = create_memblk_data(capacity, block_size, GFP_KERNEL);
        __CHECK(mdata, error);

        data1 = (u8 *)__get_free_page(GFP_KERNEL); count_ ++;
        __CHECK(data1, error);
        data2 = (u8 *)__get_free_page(GFP_KERNEL); count_ ++;
        __CHECK(data2, error);

        sprint_hex(strbuf, PAGE_SIZE, data1, 128);
        /* LOGd("data1: %s\n", strbuf); */
        sprint_hex(strbuf, PAGE_SIZE, data2, 128);
        /* LOGd("data2: %s\n", strbuf); */
        
        /* First block */
        addr = 0;
        fill_random(data1, PAGE_SIZE);
        memblk_data_write_block(mdata, addr, data1);
        memblk_data_read_block(mdata, addr, data2);

        sprint_hex(strbuf, PAGE_SIZE, data1, 128);
        /* LOGd("data1: %s\n", strbuf); */
        sprint_hex(strbuf, PAGE_SIZE, data2, 128);
        /* LOGd("data2: %s\n", strbuf); */
        
        __CHECK(memcmp(data1, data2, block_size) == 0, error);

        /* Last block */
        addr = capacity - 1;
        fill_random(data1, PAGE_SIZE);
        memblk_data_write_block(mdata, addr, data1);
        memblk_data_read_block(mdata, addr, data2);
        __CHECK(memcmp(data1, data2, block_size) == 0, error);

        /* First two blocks */
        if (block_size * 2 <= PAGE_SIZE) {
                addr = 0;
                fill_random(data1, PAGE_SIZE);
                memblk_data_write_blocks(mdata, 0, 2, data1);
                memblk_data_read_blocks(mdata, 0, 2, data2);
                __CHECK(memcmp(data1, data2, block_size * 2) == 0, error);
        }
        
        /* Random area */
        for (i = 0; i < 10; i ++) {
                
                addr = get_random_addr(capacity - 4);
                fill_random(data1, PAGE_SIZE);

                size = get_random_u32_max(4) + 1;
                size = min(size, (u32)(PAGE_SIZE / block_size));

                /* LOGd("iteration %d (addr %"PRIu64" size %"PRIu32")\n", i, addr, size); */

                memblk_data_write_blocks(mdata, addr, size, data1);
                memblk_data_read_blocks(mdata, addr, size, data2);
                
                __CHECK(memcmp(data1, data2, size * block_size) == 0, error);
        }

        free_page((unsigned long)strbuf); count_ --;
        free_page((unsigned long)data2); count_ --;
        free_page((unsigned long)data1); count_ --;
        destroy_memblk_data(mdata);

        LOGd("test_memblk_data succeeded.\n");
        LOGd("count_: %d\n", count_);
        return true;
error:
        LOGe("ERROR\n");
        if (strbuf) {
                free_page((unsigned long)strbuf); count_ --;
        }
        if (data2) {
                free_page((unsigned long)data2); count_ --;
        }
        if (data1) {
                free_page((unsigned long)data1); count_ --;
        }
        destroy_memblk_data(mdata);
        LOGe("test_memblk_data failed..\n");
        return false;
}


/* end of file */
