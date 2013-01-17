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
 * @bio target bio
 * @salt checksum salt.
 *
 * RETURN:
 *   checksum if bio->bi_size > 0, else 0.
 */
static inline u32 bio_calc_checksum(struct bio *bio, u32 salt)
{
	struct bio_vec *bvec;
	u32 sum = salt;
	int i;
	u8 *buf;

	ASSERT(bio);

	if (bio->bi_size == 0) {
		return 0;
	}

	if (bio->bi_rw & REQ_DISCARD) {
		return 0;
	}

	bio_for_each_segment(bvec, bio, i) {
		buf = (u8 *)kmap_atomic(bvec->bv_page) + bvec->bv_offset;
		sum = checksum_partial(sum, buf, bvec->bv_len);
		kunmap_atomic(buf);
	}

	return checksum_finish(sum);
}

/**
 * Print request flags for debug.
 */
static inline void print_bio_flags(const char *flag, struct bio *bio)
{
	ASSERT(bio);
	printk("%s""REQ_FLAGS: "
		"%s%s%s%s%s"
		"%s%s%s%s%s"
		"%s%s%s%s%s"
		"%s%s%s%s%s"
		"%s%s%s%s%s"
		"%s%s%s%s\n",
		flag,
		((bio->bi_rw & REQ_WRITE) ?		  "REQ_WRITE" : ""),
		((bio->bi_rw & REQ_FAILFAST_DEV) ?	  " REQ_FAILFAST_DEV" : ""),
		((bio->bi_rw & REQ_FAILFAST_TRANSPORT) ? " REQ_FAILFAST_TRANSPORT" : ""),
		((bio->bi_rw & REQ_FAILFAST_DRIVER) ?	  " REQ_FAILFAST_DRIVER" : ""),
		((bio->bi_rw & REQ_SYNC) ?		  " REQ_SYNC" : ""),
		((bio->bi_rw & REQ_META) ?		  " REQ_META" : ""),
		((bio->bi_rw & REQ_PRIO) ?		  " REQ_PRIO" : ""),
		((bio->bi_rw & REQ_DISCARD) ?		  " REQ_DISCARD" : ""),
		((bio->bi_rw & REQ_NOIDLE) ?		  " REQ_NOIDLE" : ""),
		((bio->bi_rw & REQ_RAHEAD) ?		  " REQ_RAHEAD" : ""),
		((bio->bi_rw & REQ_THROTTLED) ?	  " REQ_THROTTLED" : ""),
		((bio->bi_rw & REQ_SORTED) ?		  " REQ_SORTED" : ""),
		((bio->bi_rw & REQ_SOFTBARRIER) ?	  " REQ_SOFTBARRIER" : ""),
		((bio->bi_rw & REQ_FUA) ?		  " REQ_FUA" : ""),
		((bio->bi_rw & REQ_NOMERGE) ?		  " REQ_NOMERGE" : ""),
		((bio->bi_rw & REQ_STARTED) ?		  " REQ_STARTED" : ""),
		((bio->bi_rw & REQ_DONTPREP) ?		  " REQ_DONTPREP" : ""),
		((bio->bi_rw & REQ_QUEUED) ?		  " REQ_QUEUED" : ""),
		((bio->bi_rw & REQ_ELVPRIV) ?		  " REQ_ELVPRIV" : ""),
		((bio->bi_rw & REQ_FAILED) ?		  " REQ_FAILED" : ""),
		((bio->bi_rw & REQ_QUIET) ?		  " REQ_QUIET" : ""),
		((bio->bi_rw & REQ_PREEMPT) ?		  " REQ_PREEMPT" : ""),
		((bio->bi_rw & REQ_ALLOCED) ?		  " REQ_ALLOCED" : ""),
		((bio->bi_rw & REQ_COPY_USER) ?	  " REQ_COPY_USER" : ""),
		((bio->bi_rw & REQ_FLUSH) ?		  " REQ_FLUSH" : ""),
		((bio->bi_rw & REQ_FLUSH_SEQ) ?	  " REQ_FLUSH_SEQ" : ""),
		((bio->bi_rw & REQ_IO_STAT) ?		  " REQ_IO_STAT" : ""),
		((bio->bi_rw & REQ_MIXED_MERGE) ?	  " REQ_MIXED_MERGE" : ""),
		((bio->bi_rw & REQ_SECURE) ?		  " REQ_SECURE" : ""));
}

#endif /* WALB_BIO_UTIL_H_KERNEL */
