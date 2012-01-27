/**
 * memblk_io_simple.c - make_request which simply read/write blocks.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"

#include <linux/blkdev.h>
#include "walb/common.h"
#include "memblk_io.h"
#include "memblk.h"

/**
 * Make request.
 * Currently do nothing.
 */
int memblk_make_request(struct request_queue *q, struct bio *bio)
{
        struct memblk_dev *mdev = q->queue_data;
        ASSERT(mdev);

        
        /* now editing */



        
        bio_endio(bio, 0); /* do nothing */
        return 0; /* never retry */
}

/* end of file */
