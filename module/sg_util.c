/**
 * sg_util.c - Utility for scatterlist.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"

#include <linux/scatterlist.h>
#include "walb/common.h"
#include "util.h"
#include "sg_util.h"

/*******************************************************************************
 * Static structs definition.
 *******************************************************************************/

/**
 * Position in a scatterlist.
 *
 * If the position is the end:
 *   sg == NULL;
 *   offset == 0;
 *   total_offset == data length of the sgt.
 */
struct sg_pos
{
        struct scatterlist *sg;
        unsigned int offset; /* offset in the sg entry. */
        unsigned int total_offset; /* total offset from the begennng. */
};

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

static bool sg_pos_is_end(const struct sg_pos *pos);
static void sg_pos_assert(const struct sg_pos *pos);
static bool sg_pos_get(const struct sg_table *sgt, struct sg_pos *pos, unsigned int offset);
static unsigned int sg_pos_fragment_size(const struct sg_pos *pos);
static bool sg_pos_go_forward(struct sg_pos *pos, unsigned int size);
static void *sg_pos_virt(const struct sg_pos *pos);

static bool __sg_copy_buffer_offset(
        struct sg_table *sgt, unsigned int offset, 
        u8 *buf, unsigned int size, bool is_to_buffer);

#define ASSERT_SG_POS(pos) sg_pos_assert(pos)

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * Check whether the position indicates the end of the sgt.
 * REUTRN:
 * true if the position indicates the end, or false.
 */
static bool sg_pos_is_end(const struct sg_pos *pos)
{
        ASSERT(pos);
        return (pos->sg == NULL);
}

/**
 * Assertion of sg_pos.
 */
static void sg_pos_assert(const struct sg_pos *pos)
{
        ASSERT(pos);
        if (sg_pos_is_end(pos)) {
                ASSERT(pos->sg == NULL);
                ASSERT(pos->offset == 0);
        } else {
                ASSERT(pos->offset < pos->sg->length);
                ASSERT(pos->offset <= pos->total_offset);
        }
}

/**
 * Get a position inside a scatterlist.
 * @sgt target scatterlist.
 * @pos where the result will be set.
 * @offset offset from the head of the scatterlist [bytes].
 * 0 <= @offset <= length of the sgt.
 * RETURN:
 * true in success, or false.
 */
static bool sg_pos_get(const struct sg_table *sgt, struct sg_pos *pos, unsigned int offset)
{
        struct scatterlist *sg;
        unsigned int off;
        
        ASSERT(sgt);
        ASSERT(pos);
        
        sg = sgt->sgl; ASSERT(sg);
        off = 0;
        while (off + sg->length <= offset) {
                off += sg->length;
                sg = sg_next(sg);
                if (!sg && off < offset) {
                        LOGe("offset is too large.\n");
                        return false;
                }
        }
        ASSERT(off <= offset);
        if (sg) {
                ASSERT(offset - off < sg->length);
        }

        pos->sg = sg;
        pos->offset = offset - off;
        pos->total_offset = offset;
        return true;
}

/**
 * Memory fragment size that we can access directly from the position.
 * RETURN:
 * fragment size [bytes], or 0 if the pos reaches the end of the sgt.
 */
static unsigned int sg_pos_fragment_size(const struct sg_pos *pos)
{
        ASSERT_SG_POS(pos);
        if (pos->sg) {
                ASSERT(pos->sg->length > pos->offset);
                return pos->sg->length - pos->offset;
        } else {
                return 0;
        }
}

/**
 * Go forward inside the scatterlist.
 * @size size to go forward [bytes].
 * RETURN:
 * true in success, or false.
 */
static bool sg_pos_go_forward(struct sg_pos *pos, unsigned int size)
{
        unsigned int remaining = size;
        unsigned int tmp_size;
        struct scatterlist *sg;
        unsigned int off;

        ASSERT_SG_POS(pos);
        sg = pos->sg;
        off = pos->offset;

        while (remaining > 0) {
                tmp_size = min(remaining, sg_pos_fragment_size(pos));
                off += tmp_size;
                ASSERT(off <= sg->length);
                if (off == sg->length) {
                        sg = sg_next(sg);
                        off = 0;
                }
                remaining -= tmp_size;
                pos->sg = sg;
                pos->offset = off;
                pos->total_offset += tmp_size;
                if (!sg && remaining > 0) {
                        LOGe("scatterlist reached the end during going forward.\n");
                        return false;
                }
        }
        ASSERT(remaining == 0);
        return true;
}

/**
 * Get virtual address for the position.
 */
static void *sg_pos_virt(const struct sg_pos *pos)
{
        ASSERT_SG_POS(pos);
        if (pos->sg) {
                return (void *)((u8 *)sg_virt(pos->sg) + pos->offset);
        } else {
                /* reaches end. */
                return NULL;
        }
}

/*******************************************************************************
 * Global fucntions prototype.
 *******************************************************************************/

/**
 * Get data length of a scatterlist.
 */
unsigned int sg_data_length(const struct sg_table *sgt)
{
        unsigned int ret = 0;
        struct scatterlist *sg;
        int i;
        
        ASSERT(sgt);
        for_each_sg(sgt->sgl, sg, sgt->nents, i) {
                ret += sg->length;
        }
        return ret;
}

/**
 * Copy data in a scatterlist to another with offsets.
 * @dst destination scatterlist.
 * @dst_offset offset of the dst [bytes].
 * @src source scatterlist.
 * @src_offset offset of the src [bytes].
 * @size copy size [bytes].
 * RETURN:
 * true in success, or false.
 */
bool sg_copy_to_sg_offset(
        struct sg_table *dst, unsigned int dst_offset,
        const struct sg_table *src, unsigned int src_offset,
        unsigned int size)
{
        struct sg_pos src_pos, dst_pos;
        unsigned int remaining = size, tmp_size;

        LOGd("sg_copy_to_sg_offset() begin.\n");
        LOGd("dst(off %u len %u) src(off %u len %u) size %u\n",
             dst_offset, sg_data_length(dst),
             src_offset, sg_data_length(src), size);
        
        if (!sg_pos_get(dst, &dst_pos, dst_offset)) { return false; }
        if (!sg_pos_get(src, &src_pos, src_offset)) { return false; }

        while (remaining > 0) {
                tmp_size = min(sg_pos_fragment_size(&dst_pos),
                               sg_pos_fragment_size(&src_pos));
                tmp_size = min(tmp_size, remaining);
                /* LOGd("remaining %u tmp_size %u\n", remaining, tmp_size); */
                memcpy(sg_pos_virt(&dst_pos), sg_pos_virt(&src_pos), tmp_size);
                if (!sg_pos_go_forward(&dst_pos, tmp_size)) { return false; }
                if (!sg_pos_go_forward(&src_pos, tmp_size)) { return false; }
                remaining -= tmp_size;

                if (remaining > 0 &&
                    (sg_pos_is_end(&dst_pos) || sg_pos_is_end(&src_pos))) {
                        return false;
                }
        }
        ASSERT(remaining == 0);

        LOGd("sg_copy_to_sg_offset() end.\n");
        return true;
}

/**
 * Fill zero data to a scatterlist.
 *
 * Simple implementation.
 */
__DEPRECATED __UNUSED
static void sg_fill_zero_old(struct sg_table *sgt)
{
        struct scatterlist *sg;
        int i;

        ASSERT(sgt);
        for_each_sg(sgt->sgl, sg, sgt->nents, i) {
                memset(sg_virt(sg), 0, sg->length);
        }
}

/**
 * Fill zero data to a scatterlist.
 */
void sg_fill_zero(struct sg_table *sgt)
{
        sg_fill_zero_offset(sgt, 0, -1);
}

/**
 * Fill zero data in a scatterlist.
 * @sgt target scatterlist table.
 * @offset start offset to fill [bytes].
 * @size fill size [bytes].
 * RETURN:
 * true in success, or false (reaching the end on the way).
 */
bool sg_fill_zero_offset(struct sg_table *sgt, unsigned int offset, unsigned int size)
{
        struct sg_pos pos;
        unsigned int remaining = size, tmp_size;
        
        ASSERT(sgt);

        if (!sg_pos_get(sgt, &pos, offset)) { return false; }
        while (remaining > 0) {
                tmp_size = min(sg_pos_fragment_size(&pos), remaining);
                memset(sg_pos_virt(&pos), 0, tmp_size);
                if (!sg_pos_go_forward(&pos, tmp_size)) { return false; }
                remaining -= tmp_size;
                if (sg_pos_is_end(&pos)) { break; }
        }
        if (!sg_pos_is_end(&pos)) {
                ASSERT(remaining == 0);
        }
        return true;
}

/**
 * Allocate scatterlist and pages.
 * Each scatterlist entry will have a page.
 * RETURN:
 * true in success, or false.
 */
bool sg_alloc_pages(struct sg_table *sgt, unsigned int nents, gfp_t gfp_mask)
{
        int ret, i;
        struct scatterlist *sg;
        struct page *page;

        ASSERT(sgt);
        ASSERT(nents > 0);

        ret = sg_alloc_table(sgt, nents, gfp_mask);
        if (ret) { LOGe("sg_alloc_table failed.\n"); goto error0; }

        for_each_sg(sgt->sgl, sg, sgt->nents, i) {

                page = alloc_page(gfp_mask);
                if (!page) { LOGe("alloc_page failed.\n"); goto error1; }
                sg_set_page(sg, page, PAGE_SIZE, 0);
        }

        return true;
error1:
        sg_free_pages(sgt);
error0:
        return false;
}

/**
 * Free scatterlist and pages.
 */
void sg_free_pages(struct sg_table *sgt)
{
        int i;
        struct scatterlist *sg;
        struct page *page;
        
        if (!sgt) { return; }

        for_each_sg(sgt->sgl, sg, sgt->nents, i) {
                page = sg_page(sg);
                if (page) {
                        __free_page(page);
                }
        }
        sg_free_table(sgt);
}

/**
 * Copy data from/to a scatterlist to/from a buffer with a offset and size.
 * @sgt target scatter list.
 * @offset start offset in the scatterlist [bytes].
 * @buf buffer pointer.
 * @size size to copy [bytes].
 * @is_to_buffer true for from-sg-to-buffer copy.
 */
static bool __sg_copy_buffer_offset(
        struct sg_table *sgt, unsigned int offset, 
        u8 *buf, unsigned int size, bool is_to_buffer)
{
        struct sg_pos pos;
        unsigned int tmp_size;
        unsigned int off = 0;
        unsigned int remaining = size;

        ASSERT(sgt);
        ASSERT(buf);
        
        if (!sg_pos_get(sgt, &pos, offset)) { return false; }
        while (remaining > 0) {
                tmp_size = min(remaining, sg_pos_fragment_size(&pos));
                if (is_to_buffer) {
                        memcpy(buf + off, sg_pos_virt(&pos), tmp_size);
                } else {
                        memcpy(sg_pos_virt(&pos), buf + off, tmp_size);
                }
                if (!sg_pos_go_forward(&pos, tmp_size)) { return false; }
                off += tmp_size;
                remaining -= tmp_size;
        }
        ASSERT(remaining == 0);
        return true;
}

/**
 * Copy to a buffer with a offset and size.
 *
 * RETURN:
 * true in success, or false.
 */
bool sg_copy_to_buffer_offset(
        const struct sg_table *sgt, unsigned int offset, 
        u8 *buf, unsigned int size)
{
        return __sg_copy_buffer_offset((struct sg_table *)sgt, offset, buf, size, true);
}

/**
 * Copy from a buffer with a offset and size.
 * RETURN:
 * true in success, or false.
 */
bool sg_copy_from_buffer_offset(
        struct sg_table *sgt, unsigned int offset, 
        const u8 *buf, unsigned int size)
{
        return __sg_copy_buffer_offset(sgt, offset, (u8 *)buf, size, false);
}

/**
 * Test original functionalities of scatterlist.
 *
 * @nents Number of entries.
 * @entsize Data size of each entry.
 */
void test_scatterlist(unsigned int nents, unsigned int entsize)
{
        struct sg_table sgt;
        struct scatterlist *sg;
        int i, j;
        struct page *page;
        u8 *tmp_data;
        unsigned int sg_off;

        LOGd("test_scatterlist start with %u entries\n", nents);
        LOGd("SG_MAX_SINGLE_ALLOC %lu\n", SG_MAX_SINGLE_ALLOC);

        ASSERT(entsize <= PAGE_SIZE);

        /* Prepare temporary data where all bit is 1. */
        tmp_data = (u8 *)__get_free_page(GFP_KERNEL);
        ASSERT(tmp_data);
        for (i = 0; i < PAGE_SIZE; i ++) {
                tmp_data[i] = 0xff;
        }
        
        /* Allocate sg and pages which are zero-cleared. */
        sg_alloc_table(&sgt, nents, GFP_KERNEL);
        for_each_sg(sgt.sgl, sg, sgt.nents, i) {
                page = alloc_page(GFP_KERNEL);
                ASSERT(page);
                memset(page_address(page), 0, PAGE_SIZE);
                sg_set_page(sg, page, entsize, 0);
                /* LOGd("alloc iteration %d\n", i); */
        }
        LOGd("test_scatterlist middle\n");

        /* Copy data. */
        sg_copy_from_buffer(sgt.sgl, sgt.nents, tmp_data, PAGE_SIZE);
        sg_copy_to_buffer(sgt.sgl, sgt.nents, tmp_data, PAGE_SIZE);

        /* Check data. */
        for (i = 0; i < PAGE_SIZE; i ++) {
                ASSERT(tmp_data[i] == 0xff);
        }
        sg_off = 0;
        for_each_sg(sgt.sgl, sg, sgt.nents, i) {
                /* LOGd("sg %d sg offset %u sg length %u\n", */
                /*      i, sg->offset, sg->length); */
                /* LOGd("alloc iteration %d\n", i); */

                for (j = 0; j < entsize; j ++) {
                        if (sg_off < PAGE_SIZE) {
                                ASSERT(((u8 *)page_address(sg_page(sg)))[j] == 0xff);
                        } else {
                                ASSERT(((u8 *)page_address(sg_page(sg)))[j] == 0);
                        }
                        sg_off ++;
                }
        }
        ASSERT(sg_off == sgt.nents * entsize);

        /* Free pages and sg. */
        for_each_sg(sgt.sgl, sg, sgt.nents, i) {
                page = sg_page(sg);
                ASSERT(page);
                __free_page(page);
                /* LOGd("free iteration %d done\n", i); */
        }
        sg_free_table(&sgt);

        free_page((unsigned long)tmp_data);
        LOGd("test_scatterlist end\n");
}

/*******************************************************************************
 * Test code.
 *******************************************************************************/

/**
 * This is for test.
 */
static void alloc_sg_and_pages_randomly(
        struct sg_table *sgt, unsigned int nents,
        unsigned int min_entsize, unsigned int max_entsize)
{
        __UNUSED int ret;
        int i;
        struct scatterlist *sg;
        unsigned int siz, off;
        struct page *page;
        
        ASSERT(sgt);
        ASSERT(nents > 0);
        ASSERT(min_entsize > 0);
        ASSERT(min_entsize <= max_entsize);
        ASSERT(max_entsize <= PAGE_SIZE);

        LOGd("alloc_sg_and_pages_randomly() begin.\n");
        
        ret = sg_alloc_table(sgt, nents, GFP_KERNEL);
        ASSERT(!ret);

        /* Alloc pages and asssign it to the sg entries randomly. */
        for_each_sg(sgt->sgl, sg, sgt->nents, i) {
                page = alloc_page(GFP_KERNEL);
                siz = (unsigned int)get_random_u32_max(
                        max_entsize - min_entsize) + min_entsize;
                off = get_random_u32_max(PAGE_SIZE - siz);
                sg_set_page(sg, page, siz, off);
                /* LOGd("sg_set_page(sg, %p, %u, %u)\n", page, siz, off); */
        }
        LOGd("alloc_sg_and_pages_randomly() end.\n");
}

/**
 * This is for test.
 */
static void free_sg_and_pages(struct sg_table *sgt)
{
        int i;
        struct scatterlist *sg;
        struct page *page;
        
        LOGd("free_sg_and_pages begin.\n");
        ASSERT(sgt);
        for_each_sg(sgt->sgl, sg, sgt->nents, i) {
                page = sg_page(sg);
                ASSERT(page);
                __free_page(page);
        }
        sg_free_table(sgt);
        LOGd("free_sg_and_pages end.\n");
}

/**
 * Test sg_pos and related functions.
 */
void test_sg_pos(void)
{
        struct sg_table sgt;
        struct sg_pos pos;

        unsigned int nents = 32;
        unsigned int entsize = PAGE_SIZE / nents;
        __UNUSED int ret;
        int i;
        unsigned int ui;
        u8 *tmp_data;

        /* Allocate a page and fill randomly. */
        tmp_data = (u8 *)__get_free_page(GFP_KERNEL);
        fill_random(tmp_data, PAGE_SIZE);

        /* Allocate a scatterlist with pages. */
        ASSERT(PAGE_SIZE % nents == 0);
        alloc_sg_and_pages_randomly(&sgt, nents, entsize, entsize);
        LOGd("A scatterlist has been allocated with nents %u, entsize %u\n",
             nents, entsize);
        ASSERT(sg_data_length(&sgt) == PAGE_SIZE);

        /* Copy data. */
        sg_copy_from_buffer(sgt.sgl, sgt.nents, tmp_data, PAGE_SIZE);

        /* Check functionality of sg_pos_get() and sg_pos_fragment_size(). */
        for (ui = 0; ui < PAGE_SIZE; ui ++) {
                sg_pos_get(&sgt, &pos, ui);
                ASSERT(pos.total_offset == ui);
                ASSERT(*(u8 *)sg_pos_virt(&pos) == tmp_data[ui]);
                ASSERT(sg_pos_fragment_size(&pos) == entsize - ui % entsize);
        }

        /* Check sg_pos_go_forward() functionality. */
        {
                /* Do not move */
                ret = sg_pos_get(&sgt, &pos, 0); 
                ASSERT(ret);
                sg_pos_go_forward(&pos, 0);
                ASSERT(ret);
        }
        {
                /* End */
                ret = sg_pos_get(&sgt, &pos, 0); 
                ASSERT(ret);
                sg_pos_go_forward(&pos, PAGE_SIZE);
                ASSERT(ret);
        }
        for (i = 0; i < 100; i ++) {
                /* Randomly */
                ret = sg_pos_get(&sgt, &pos, get_random_u32_max(PAGE_SIZE));
                ASSERT(ret);
                sg_pos_go_forward(&pos, get_random_u32_max(PAGE_SIZE - pos.total_offset));
                ASSERT(ret);
        }
        
        /* Free the scatterlist and the page. */
        free_sg_and_pages(&sgt);
        free_page((unsigned long)tmp_data);
}

/**
 * Test functions defined in sg_util.h.
 */
void test_sg_util(void)
{
        struct sg_table sgt0, sgt1;
        unsigned int nents = get_random_u32_max(64) + 128;
        __UNUSED int ret;
        __UNUSED bool ret_b;
        u8 *tmp_data0, *tmp_data1;
        struct scatterlist *sg;
        int i;
        struct page *page;
        unsigned int off, siz;
        unsigned int sgt0_off, sgt1_off;

        /* Alloc tmp page. */
        tmp_data0 = (u8 *)__get_free_page(GFP_KERNEL);
        ASSERT(tmp_data0);
        tmp_data1 = (u8 *)__get_free_page(GFP_KERNEL);
        ASSERT(tmp_data1);

        /* Alloc two scatterlists.  */
        LOGd("Make %u entries.\n", nents);
        ret = sg_alloc_table(&sgt0, nents, GFP_KERNEL);
        ASSERT(!ret);
        for_each_sg(sgt0.sgl, sg, sgt0.nents, i) {
                page = alloc_page(GFP_KERNEL);
                ASSERT(page);
                siz = (unsigned int)get_random_u32_max(PAGE_SIZE - 32) + 32;
                off = (unsigned int)get_random_u32_max(PAGE_SIZE - siz);
                /* LOGd("sgt0 off %u siz %u\n", off, siz); */
                sg_set_page(sg, page, siz, off);
        }
        LOGd("sg_fill_zero() start.\n");
        sg_fill_zero(&sgt0);
        LOGd("sg_fill_zero() end.\n");
        ret = sg_alloc_table(&sgt1, nents, GFP_KERNEL);
        ASSERT(!ret);
        for_each_sg(sgt1.sgl, sg, sgt1.nents, i) {
                page = alloc_page(GFP_KERNEL);
                ASSERT(page);
                siz = (unsigned int)get_random_u32_max(PAGE_SIZE - 32) + 32;
                off = (unsigned int)get_random_u32_max(PAGE_SIZE - siz);
                /* LOGd("sgt1 off %u siz %u\n", off, siz); */
                sg_set_page(sg, page, siz, off);
        }
        LOGd("sg_fill_zero() start.\n");
        sg_fill_zero(&sgt1);
        LOGd("sg_fill_zero() end.\n");

        LOGd("sgt0 data length is %u\n", sg_data_length(&sgt0));
        LOGd("sgt1 data length is %u\n", sg_data_length(&sgt1));
        ASSERT(sg_data_length(&sgt0) >= PAGE_SIZE);
        ASSERT(sg_data_length(&sgt1) >= PAGE_SIZE);
        
        /* Make random data */
        fill_random(tmp_data0, PAGE_SIZE);

        /* Copy data with offset 0. */
        sg_copy_from_buffer(sgt0.sgl, sgt0.nents, tmp_data0, PAGE_SIZE);
        ret_b = sg_copy_to_sg(&sgt1, &sgt0, PAGE_SIZE);
        ASSERT(ret_b);
        memset(tmp_data1, 0, PAGE_SIZE);
        sg_copy_to_buffer(sgt1.sgl, sgt1.nents, tmp_data1, PAGE_SIZE);
        /* Check */
        ASSERT(memcmp(tmp_data0, tmp_data1, PAGE_SIZE) == 0);
        
        /* Copy data with random offsets */
        sgt0_off = (unsigned int)get_random_u32_max(sg_data_length(&sgt0) - PAGE_SIZE);
        sgt1_off = (unsigned int)get_random_u32_max(sg_data_length(&sgt1) - PAGE_SIZE);
        sg_copy_from_buffer_offset(&sgt0, sgt0_off, tmp_data0, PAGE_SIZE);
        sg_copy_to_sg_offset(&sgt1, sgt1_off, &sgt0, sgt0_off, PAGE_SIZE);
        memset(tmp_data1, 0, PAGE_SIZE);
        sg_copy_to_buffer_offset(&sgt1, sgt1_off, tmp_data1, PAGE_SIZE);
        /* Check */
        ASSERT(memcmp(tmp_data0, tmp_data1, PAGE_SIZE) == 0);

        /* Free the scatterlists. */
        for_each_sg(sgt1.sgl, sg, sgt1.nents, i) {
                page = sg_page(sg);
                ASSERT(page);
                __free_page(page);
        }
        sg_free_table(&sgt1);
        for_each_sg(sgt0.sgl, sg, sgt0.nents, i) {
                page = sg_page(sg);
                ASSERT(page);
                __free_page(page);
        }
        sg_free_table(&sgt0);
        
        /* Free tmp page. */
        free_page((unsigned long)tmp_data1);
        free_page((unsigned long)tmp_data0);
}

/* end of file */
