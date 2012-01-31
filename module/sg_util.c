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
 */
struct sg_pos
{
        struct scatterlist *sg;
        unsigned int offset;
};


/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

static void sg_pos_assert(struct sg_pos *pos);
static bool sg_pos_get(const struct sg_table *sgt, struct sg_pos *pos, unsigned int offset);
static unsigned int sg_pos_fragment_size(struct sg_pos *pos);
static bool sg_pos_proceed(struct sg_pos *pos, unsigned int size);
static void *sg_pos_virt(const struct sg_pos *pos);

#define ASSERT_SG_POS(pos) sg_pos_assert(pos)

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * Assertion of sg_pos.
 */
static void sg_pos_assert(struct sg_pos *pos)
{
        ASSERT(pos);
        ASSERT(pos->sg);
        ASSERT(pos->offset < pos->sg->length);
}

/**
 * Get a position inside a scatterlist.
 * @sgt target scatterlist.
 * @pos where the result will be set.
 * @offset offset from the head of the scatterlist [bytes].
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
                if (!sg) {
                        LOGe("offset is too large.\n");
                        return false;
                }
        }
        ASSERT(off <= offset);
        ASSERT(offset - off < sg->length);

        pos->sg = sg;
        pos->offset = offset - off;
        return true;
}

/**
 * Memory fragment size that we can access directly from the position.
 * RETURN:
 * fragment size [bytes].
 */
static unsigned int sg_pos_fragment_size(struct sg_pos *pos)
{
        ASSERT_SG_POS(pos);
        return pos->sg->length - pos->offset;
}

/**
 * Proceed position inside the scatterlist.
 * @size size to proceed [bytes].
 * RETURN:
 * true in success, or false.
 */
static bool sg_pos_proceed(struct sg_pos *pos, unsigned int size)
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
                        if (!sg) {
                                LOGe("scatterlist reached the end during proceeding.\n");
                                return false;
                        }
                        off = 0;
                }
                remaining -= tmp_size;
        }
        ASSERT(remaining == 0);
        return true;
}

/**
 * Get virtual address for the position.
 */
static void *sg_pos_virt(const struct sg_pos *pos)
{
        return (void *)((u8 *)sg_virt(pos->sg) + pos->offset);
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
 * Copy a scatterlist to another.
 */
void sg_copy_to_sg_offset(
        struct sg_table *dst, unsigned int dst_offset,
        const struct sg_table *src, unsigned int src_offset,
        unsigned int size)
{
        struct sg_pos src_pos, dst_pos;
        unsigned int remaining = size, tmp_size;

        if (!sg_pos_get(dst, &dst_pos, dst_offset)) { return; }
        if (!sg_pos_get(src, &src_pos, src_offset)) { return; }

        while (remaining > 0) {
                tmp_size = min(sg_pos_fragment_size(&dst_pos),
                               sg_pos_fragment_size(&src_pos));
                memcpy(sg_pos_virt(&dst_pos), sg_pos_virt(&src_pos), tmp_size);
                if (!sg_pos_proceed(&dst_pos, tmp_size)) { return; }
                if (!sg_pos_proceed(&src_pos, tmp_size)) { return; }
                remaining -= tmp_size;
        }
        ASSERT(remaining == 0);
}

/**
 * Fill zero data to a scatterlist.
 */
void sg_fill_zero_old(struct sg_table *sgt)
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
 * true in success, or false;
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
                if (!sg_pos_proceed(&pos, tmp_size)) { return false; }
                remaining -= tmp_size;
        }
        ASSERT(remaining == 0);
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
 * Copy data in a scatter list to another.
 * @dst destination.
 * @dst_off start offset of destination sg [bytes].
 * @src source.
 * @src_off start offset of source sg [bytes].
 * @size copy size [bytes].
 */
void sg_copy_to_sg_offset_old(
        struct sg_table *dst, unsigned int dst_offset,
        const struct sg_table *src, unsigned int src_offset,
        unsigned int size)
{
        unsigned int remaining = size, tmp_size;
        struct scatterlist *dst_sg, *src_sg;
        unsigned int off_dst, off_src; /* Offset in a sg of dst/src. */

        ASSERT(dst);
        ASSERT(src);
        ASSERT(dst_offset < sg_data_length(dst));
        ASSERT(src_offset < sg_data_length(src));
        ASSERT(sg_data_length(dst) - dst_offset >= size);
        ASSERT(sg_data_length(src) - src_offset >= size);

        /* Initialize dst_sg and off_dst. */
        dst_sg = dst->sgl; ASSERT(dst_sg);
        off_dst = 0;
        while (off_dst + dst_sg->length <= dst_offset) {
                off_dst += dst_sg->length;
                dst_sg = sg_next(dst_sg);
                if (!dst_sg) {
                        LOGe("dst_offset is too large.\n");
                        return;
                }
        }
        ASSERT(off_dst <= dst_offset);
        ASSERT(dst_offset - off_dst < dst_sg->length);
        off_dst = dst_offset - off_dst;
        
        /* Initialize src_sg and off_src. */
        src_sg = dst->sgl; ASSERT(src_sg);
        off_src = 0;
        while (off_src + src_sg->length <= src_offset) {
                off_src += src_sg->length;
                src_sg = sg_next(src_sg);
                if (!src_sg) {
                        LOGe("src_offset is too large.\n ");
                        return;
                }
        }
        ASSERT(off_src <= src_offset);
        ASSERT(src_offset - off_src < src_sg->length);
        off_src = src_offset - off_src;
        
        /* Copy memory fragments one by one. */
        while (remaining > 0) {
                tmp_size = min(dst_sg->length - off_dst, src_sg->length - off_src);

                memcpy((u8 *)sg_virt(dst_sg) + off_dst, 
                       (u8 *)sg_virt(src_sg) + off_src, tmp_size);

                off_dst += tmp_size;
                off_src += tmp_size;
                ASSERT(off_dst <= dst_sg->length);
                ASSERT(off_src <= src_sg->length);
                
                if (off_dst == dst_sg->length) {
                        dst_sg = sg_next(dst_sg);
                        if (!dst_sg) {
                                LOGe("dst scatterlist reached the end during copy.\n");
                                return;
                        }
                        off_dst = 0;
                }
                if (off_src == src_sg->length) {
                        src_sg = sg_next(src_sg);
                        if (!src_sg) {
                                LOGe("src scatterlist reached the end during copy.\n");
                                return ;
                        }
                        off_src = 0;
                }
                remaining -= tmp_size;
        }
        ASSERT(remaining == 0);
}

/**
 * Test raw scatterlist.
 *
 * @nents Number of entries.
 * @entsize Data size of each entry.
 */
void test_scatterlist(unsigned int nents, unsigned int entsize)
{
        struct sg_table sgt;
        struct scatterlist *sg;
        int i;
        struct page *page;
        u8 *tmp_data;

        LOGd("test_scatterlist start with %u entries\n", nents);
        LOGd("SG_MAX_SINGLE_ALLOC %lu\n", SG_MAX_SINGLE_ALLOC);

        ASSERT(entsize <= PAGE_SIZE);
        
        tmp_data = (u8 *)__get_free_page(GFP_KERNEL);
        ASSERT(tmp_data);
        for (i = 0; i < PAGE_SIZE; i ++) {
                tmp_data[i] = 0xff;
        }
        
        /* Allocate sg and pages. */
        sg_alloc_table(&sgt, nents, GFP_KERNEL);
        for_each_sg(sgt.sgl, sg, sgt.nents, i) {
                page = alloc_page(GFP_KERNEL);
                ASSERT(page);
                memset(page_address(page), 0, PAGE_SIZE);
                sg_set_page(sg, page, entsize, 0);
                /* LOGd("alloc iteration %d\n", i); */
        }

        LOGd("test_scatterlist middle\n");

        /* Access data */
        sg_copy_from_buffer(sgt.sgl, sgt.nents, tmp_data, PAGE_SIZE);
        sg_copy_to_buffer(sgt.sgl, sgt.nents, tmp_data, PAGE_SIZE);
        for_each_sg(sgt.sgl, sg, sgt.nents, i) {
                LOGd("sg %d sg offset %u sg length %u\n",
                     i, sg->offset, sg->length);
                /* LOGd("alloc iteration %d\n", i); */
        }

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

/**
 * Test 
 */
void test_sg_util(void)
{
        struct sg_table sgt0, sgt1;
        unsigned int nents = get_random_u32_max(64) + 128;
        int ret;
        u8 *tmp_data;
        struct scatterlist *sg;
        int i;
        struct page *page;
        unsigned int off, siz;

        /* Alloc tmp page. */
        tmp_data = (u8 *)__get_free_page(GFP_KERNEL);
        ASSERT(tmp_data);

        /* Alloc two scatterlists.  */
        ret = sg_alloc_table(&sgt0, nents, GFP_KERNEL);
        ASSERT(!ret);
        for_each_sg(sgt0.sgl, sg, sgt0.nents, i) {
                page = alloc_page(GFP_KERNEL);
                ASSERT(page);
                siz = (unsigned int)get_random_u32_max(PAGE_SIZE - 32) + 32;
                off = (unsigned int)get_random_u32_max(PAGE_SIZE - siz);
                sg_set_page(sg, page, siz, off);
        }
        sg_fill_zero(&sgt0);
        ret = sg_alloc_table(&sgt1, nents, GFP_KERNEL);
        ASSERT(!ret);
        for_each_sg(sgt1.sgl, sg, sgt1.nents, i) {
                page = alloc_page(GFP_KERNEL);
                ASSERT(page);
                siz = (unsigned int)get_random_u32_max(PAGE_SIZE - 32) + 32;
                off = (unsigned int)get_random_u32_max(PAGE_SIZE - siz);
                sg_set_page(sg, page, siz, off);
        }
        sg_fill_zero(&sgt1);

        LOGd("sgt0 data length is %u\n", sg_data_length(&sgt0));
        LOGd("sgt1 data length is %u\n", sg_data_length(&sgt1));
        ASSERT(sg_data_length(&sgt0) >= PAGE_SIZE);
        ASSERT(sg_data_length(&sgt1) >= PAGE_SIZE);
        
        
        /* Make random data */
        fill_random(tmp_data, PAGE_SIZE);
        
        sg_copy_from_buffer(sgt0.sgl, sgt0.nents, tmp_data, PAGE_SIZE);
        
        sg_copy_to_buffer(sgt0.sgl, sgt0.nents, tmp_data, PAGE_SIZE);




        
        /* now editing */


        

        /* Free the scatterlists. */
        for_each_sg(sgt0.sgl, sg, sgt0.nents, i) {
                page = sg_page(sg);
                ASSERT(page);
                __free_page(page);
        }
        sg_free_table(&sgt0);
        for_each_sg(sgt1.sgl, sg, sgt1.nents, i) {
                page = sg_page(sg);
                ASSERT(page);
                __free_page(page);
        }
        sg_free_table(&sgt1);
        
        /* Free tmp page. */
        free_page((unsigned long)tmp_data);
}

/* end of file */
