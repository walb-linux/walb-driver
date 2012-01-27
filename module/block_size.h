/**
 * block_size.h - Header for logical/physical block size.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_BLOCK_SIZE_H
#define _WALB_BLOCK_SIZE_H

#include "check_kernel.h"
#include "walb/common.h"

/**
 * Class for block size.
 */
struct block_size_op
{
        u32 logical_bs; /* Logical block size [bytes] */
        u32 physical_bs; /* Physical block size [bytes] */
        u32 n_lb_in_pb; /* Number of logical blocks in a physical block. */
        
        u64 (*off_in_p) (struct block_size_op *this, u64 logical_addr);
        u64 (*to_p) (struct block_size_op *this, u64 logical_addr);
        u64 (*required_n_pb) (struct block_size_op *this, u64 logical_capacity);
        u64 (*to_l) (struct block_size_op *this, u64 physical_addr);
};

struct block_size_op* alloc_block_size_op(gfp_t gfp_mask);
void free_block_size_op(struct block_size_op *op);

void init_block_size_op(struct block_size_op *op, u32 logical_bs, u32 physical_bs);

struct block_size_op* create_block_size_op(u32 logical_bs, u32 physical_bs, gfp_t gfp_mask);
#define destroy_block_size_op(op) free_block_size_op(op)

#endif /* _WALB_BLOCK_SIZE_H */
