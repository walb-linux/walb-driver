/**
 * block_size.h - Header for logical/physical block size.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_BLOCK_SIZE_H_KERNEL
#define _WALB_BLOCK_SIZE_H_KERNEL

#include "check_kernel.h"
#include "walb/common.h"

/**
 * Logical/physical block sizes.
 */
struct block_sizes
{
        unsigned int lbs; /* Logical block size [bytes] */
        unsigned int pbs; /* Physical block size [bytes] */
        unsigned int n_lb_in_pb; /* Number of logical blocks in a physical block. */
};

/* Allocate. */
struct block_sizes* blksiz_alloc(gfp_t gfp_mask);
/* Free */
void blksiz_free(struct block_sizes *blksiz);
/* Initialize */
void blksiz_init(struct block_sizes *blksiz,
                 unsigned int logical_block_size, unsigned int physical_block_size);

/* Assert */
void blksiz_assert(const struct block_sizes *blksiz);
#define ASSERT_BLKSIZ(blksiz) blksiz_assert(blksiz)

/* Offset [logical block] in the physical block of logical address. */
unsigned int blksiz_off_in_p(const struct block_sizes *blksiz, u64 logical_addr);
/* Logical address -> physical address. */
u64 blksiz_to_p(const struct block_sizes *blksiz, u64 logical_addr);
/* Logical capacity [logical block] -> physical capacity [physical blocks]. */
u64 blksiz_required_n_pb(const struct block_sizes *blksiz, u64 logical_capacity);
/* Pyysical address -> logical address. */
u64 blksiz_to_l(const struct block_sizes *blksiz, u64 physical_addr);

#endif /* _WALB_BLOCK_SIZE_H_KERNEL */
