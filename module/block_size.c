/**
 * memblk.c - Memory block device driver for performance test.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"

#include <linux/kernel.h>
#include "block_size.h"
#include "walb/common.h"

/*******************************************************************************
 * Global functions.
 *******************************************************************************/

/**
 * Allocate block_sizes.
 */
struct block_sizes* blksiz_alloc(gfp_t gfp_mask)
{
        struct block_sizes *blksiz;
        
        blksiz = MALLOC(sizeof(struct block_sizes), gfp_mask);
        if (!blksiz) {
                LOGe("Memory allocation failed.\n");
                goto error0;
        }
        return blksiz;
error0:
        return NULL;
}

/**
 * Free block_sizes.
 */
void blksiz_free(struct block_sizes *blksiz)
{
        if (blksiz) {
                FREE(blksiz);
        }
}

/**
 * Initialize block_sizes.
 */
void blksiz_init(struct block_sizes *blksiz,
                 unsigned int logical_block_size, unsigned int physical_block_size)
{
        unsigned int lbs = logical_block_size;
        unsigned int pbs = physical_block_size;
        
        ASSERT(blksiz);
        ASSERT(0 < lbs);
        ASSERT(lbs <= pbs);
        ASSERT(pbs % lbs == 0);
        
        blksiz->lbs = lbs;
        blksiz->pbs = pbs;
        blksiz->n_lb_in_pb = pbs / lbs;
        ASSERT(blksiz->n_lb_in_pb * lbs == pbs);
}

/**
 * Initialize block_sizes.
 */
void blksiz_copy(struct block_sizes *dst, const struct block_sizes *src)
{
        ASSERT(dst);
        ASSERT_BLKSIZ(src);

        *dst = *src;
}

/**
 * Assert block_sizes data.
 */
void blksiz_assert(const struct block_sizes *blksiz)
{
        ASSERT(blksiz);
        ASSERT(blksiz->lbs > 0);
        ASSERT(blksiz->lbs <= blksiz->pbs);
        ASSERT(blksiz->pbs / blksiz->lbs == blksiz->n_lb_in_pb);
        ASSERT(blksiz->pbs % blksiz->lbs == 0);
}

/**
 * Offset [logical block] in the physical block of logical address.
 */
unsigned int blksiz_off_in_p(const struct block_sizes *blksiz, u64 logical_addr)
{
        u64 tmp;
        
        ASSERT_BLKSIZ(blksiz);
        tmp = logical_addr % (u64)blksiz->n_lb_in_pb;
        ASSERT(tmp <= (u64)UINT_MAX);
        return (unsigned int)tmp;
}

/**
 * Logical address -> physical address.
 */
u64 blksiz_to_p(const struct block_sizes *blksiz, u64 logical_addr)
{
        ASSERT_BLKSIZ(blksiz);
        return logical_addr / (u64)blksiz->n_lb_in_pb;
}

/**
 * Logical capacity [logical block] -> physical capacity [physical blocks].
 */
u64 blksiz_required_n_pb(const struct block_sizes *blksiz, u64 logical_capacity)
{
        ASSERT_BLKSIZ(blksiz);
        return (logical_capacity + (u64)blksiz->n_lb_in_pb - 1) / (u64)blksiz->n_lb_in_pb;
}

/**
 * Pysical address -> logical address.
 */
u64 blksiz_to_l(const struct block_sizes *blksiz, u64 physical_addr)
{
        ASSERT_BLKSIZ(blksiz);
        return physical_addr * (u64)blksiz->n_lb_in_pb;
}

/* end of file. */
