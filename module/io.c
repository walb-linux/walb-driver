/**
 * io.c - IO processing core of WalB.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include "kern.h"
#include "io.h"

/**
 * Make request.
 */
void walb_make_request(struct request_queue *q, struct bio *bio)
{
	/* now editing */
	bio_endio(bio, 0);
}

/**
 * Walblog device make request.
 *
 * 1. Completion with error if write.
 * 2. Just forward to underlying log device if read.
 *
 * @q request queue.
 * @bio bio.
 */
void walblog_make_request(struct request_queue *q, struct bio *bio)
{
        struct walb_dev *wdev = q->queuedata;
        
        if (bio->bi_rw & WRITE) {
                bio_endio(bio, -EIO);
        } else {
                bio->bi_bdev = wdev->ldev;
        }
}

MODULE_LICENSE("Dual BSD/GPL");
