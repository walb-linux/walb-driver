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
	UNUSED struct walb_dev *wdev = get_wdev_from_queue(q);

	/* Set a clock ahead. */
	spin_lock(&wdev->latest_lsid_lock);
	wdev->latest_lsid++;
	spin_unlock(&wdev->latest_lsid_lock);

#ifdef WALB_FAST_ALGORITHM
	spin_lock(&wdev->completed_lsid_lock);
	wdev->completed_lsid++;
	spin_unlock(&wdev->completed_lsid_lock);
#endif
	spin_lock(&wdev->cpd.written_lsid_lock);
	wdev->cpd.written_lsid++;
	spin_unlock(&wdev->cpd.written_lsid_lock);

	/* not yet implemented. */
	set_bit(BIO_UPTODATE, &bio->bi_flags);
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
	struct walb_dev *wdev = get_wdev_from_queue(q);
	
	if (bio->bi_rw & WRITE) {
		bio_endio(bio, -EIO);
	} else {
		bio->bi_bdev = wdev->ldev;
		generic_make_request(bio);
	}
}

MODULE_LICENSE("Dual BSD/GPL");
