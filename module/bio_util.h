/**
 * bio_util.h - Definition for bio utilities.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_BIO_UTIL_H_KERNEL
#define WALB_BIO_UTIL_H_KERNEL

#include "check_kernel.h"
#include <linux/types.h>
#include <linux/blkdev.h>

#include "walb/common.h"

/**
 * Calculate checksum.
 *
 * RETURN:
 *   checksum if bio->bi_size > 0, else 0.
 */
static inline u32 bio_calc_checksum(struct bio *bio)
{
        struct bio_vec *bvec;
	u64 sum;
	int i;
	u8 *buf;

	ASSERT(bio);

	if (bio->bi_size == 0) {
		return 0;
	}

	sum = 0;
	bio_for_each_segment(bvec, bio, i) {
		buf = (u8 *)kmap_atomic(bvec->bv_page) + bvec->bv_offset;
		sum = checksum_partial(sum, buf, bvec->bv_len);
		kunmap_atomic(buf);
	}

	return checksum_finish(sum);
}		

#endif /* WALB_BIO_UTIL_H_KERNEL */
