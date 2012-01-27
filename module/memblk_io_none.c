/**
 * memblk_io_none.c - make_request which do nothing.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"

#include <linux/blkdev.h>
#include "memblk_io.h"

/**
 * Make request.
 * Currently do nothing.
 */
int memblk_make_request(struct request_queue *q, struct bio *bio)
{
        bio_endio(bio, 0); /* do nothing */
        return 0; /* never retry */
}

/* end of file */
