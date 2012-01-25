/**
 * memblk.c - Memory block device driver for performance test.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */

#include "block_size.h"
#include "treemap.h"

/*******************************************************************************
 * Static functions.
 *******************************************************************************/

static u64 __block_size_op__off_in_p(struct block_size_op *this, u64 logical_addr)
{
        ASSERT(this);
        return logical_addr % (u64)this->n_lb_in_pb;
}

static u64 __block_size_op__to_p(struct block_size_op *this, u64 logical_addr)
{
        ASSERT(this);
        return logical_addr / (u64)this->n_lb_in_pb;
}

static u64 __block_size_op__required_n_pb(struct block_size_op *this, u64 logical_capacity)
{
        ASSERT(this);
        return (logical_capacity + (u64)this->n_lb_in_pb - 1) / (u64)this->n_lb_in_pb;
}

static u64 __block_size_op__to_l(struct block_size_op *this, u64 physical_addr)
{
        ASSERT(this);
        return physical_addr * (u64)this->n_lb_in_pb;
}

/*******************************************************************************
 * Global functions.
 *******************************************************************************/

struct block_size_op* alloc_block_size_op(gfp_t gfp_mask)
{
        struct block_size_op *op;
        
        op = MALLOC(sizeof(struct block_size_op), gfp_mask);
        if (!op) {
                LOGe("Memory allocation failed.\n");
                goto error0;
        }
        return op;
error0:
        return NULL;
}
 
void free_block_size_op(struct block_size_op *op)
{
        FREE(op);
}

void init_block_size_op(struct block_size_op *op, u32 logical_bs, u32 physical_bs)
{
        ASSERT(op);
        ASSERT(0 < logical_bs);
        ASSERT(logical_bs <= physical_bs);
        ASSERT(physical_bs % logical_bs == 0);
        
        op->logical_bs = logical_bs;
        op->physical_bs = physical_bs;
        op->n_lb_in_pb = physical_bs / logical_bs;
        ASSERT(op->n_lb_in_pb * logical_bs == physical_bs);
        
        op->off_in_p = __block_size_op__off_in_p;
        op->to_p = __block_size_op__to_p;
        op->required_n_pb = __block_size_op__required_n_pb;
        op->to_l = __block_size_op__to_l;
}

struct block_size_op* create_block_size_op(
        u32 logical_bs, u32 physical_bs, gfp_t gfp_mask)
{
        struct block_size_op *op;
        
        op = alloc_block_size_op(gfp_mask);
        if (!op) { goto error0; }

        init_block_size_op(op, logical_bs, physical_bs);
        return op;

error0:
        return NULL;
}

/* end of file. */
