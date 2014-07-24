/**
 * bio_wrapper.c - bio_wrapper related functions.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"
#include <linux/module.h>
#include <linux/list.h>
#include <linux/time.h>
#include "bio_entry.h"
#include "walb/common.h"
#include "walb/logger.h"
#include "walb/util.h"
#include "walb/block_size.h"
#include "bio_entry.h"
#include "bio_wrapper.h"
#include "bio_util.h"

/*******************************************************************************
 * Static data.
 *******************************************************************************/

/* kmem cache for bio_entry. */
#define KMEM_CACHE_BIO_WRAPPER_NAME "walb_bio_wrapper_cache"
static struct kmem_cache *bio_wrapper_cache_ = NULL;

/* shared coutner of the cache. */
static atomic_t shared_cnt_ = ATOMIC_INIT(0);

/*******************************************************************************
 * Global functions definition.
 *******************************************************************************/

#ifdef WALB_PERFORMANCE_ANALYSIS
void print_bio_wrapper_performance(const char *level, struct bio_wrapper *biow)
{
	struct timespec *tsp[WALB_TIME_MAX];
	struct timespec ts[WALB_TIME_MAX - 1];

	if (!biow) { return; }

	tsp[0] = &biow->ts[WALB_TIME_BEGIN];
	tsp[1] = &biow->ts[WALB_TIME_LOG_SUBMITTED];
	tsp[2] = &biow->ts[WALB_TIME_LOG_COMPLETED];
	tsp[3] = &biow->ts[WALB_TIME_DATA_SUBMITTED];
	tsp[4] = &biow->ts[WALB_TIME_DATA_COMPLETED];
	tsp[5] = &biow->ts[WALB_TIME_END];

	ts[0] = timespec_sub(*tsp[1], *tsp[0]);
	ts[1] = timespec_sub(*tsp[2], *tsp[1]);
	ts[2] = timespec_sub(*tsp[3], *tsp[2]);
	ts[3] = timespec_sub(*tsp[4], *tsp[3]);
	ts[4] = timespec_sub(*tsp[5], *tsp[4]);

	printk("%s"
		"biow_perf lsid %" PRIu64 " pos %" PRIu64 " len %4u "
		"time %ld.%09ld %ld.%09ld %ld.%09ld %ld.%09ld %ld.%09ld\n"
		, level
		, biow->lsid, (u64)biow->pos , biow->len
		, ts[0].tv_sec, ts[0].tv_nsec
		, ts[1].tv_sec, ts[1].tv_nsec
		, ts[2].tv_sec, ts[2].tv_nsec
		, ts[3].tv_sec, ts[3].tv_nsec
		, ts[4].tv_sec, ts[4].tv_nsec);
}
#endif

/**
 * Print a req_entry.
 */
UNUSED
void print_bio_wrapper(const char *level, const struct bio_wrapper *biow)
{
	ASSERT(biow);
	printk("%s"
		"biow %p\n"
		"  bio %p\n"
		"  pos %" PRIu64 "\n"
		"  len %u\n"
		"  csum %08x\n"
		"  error %d\n"
		"  is_started %d\n"
		"  lsid %" PRIu64 "\n"
		"  private_data %p\n"
#ifdef WALB_OVERLAPPED_SERIALIZE
		"  n_overlapped %d\n"
#ifdef WALB_DEBUG
		"  ol_id %" PRIu64 "\n"
#endif
#endif
		"  is_prepared %d\n"
		"  is_submitted %d\n"
		"  is_completed %d\n"
		"  is_discard %d\n"
		"  is_overwritten %d\n"
#ifdef WALB_OVERLAPPED_SERIALIZE
		"  is_delayed %d\n"
#endif
		, level, biow, biow->bio,
		(u64)biow->pos, biow->len, biow->csum, biow->error,
		(int)biow->is_started, biow->lsid,
		biow->private_data
#ifdef WALB_OVERLAPPED_SERIALIZE
		, biow->n_overlapped
#ifdef WALB_DEBUG
		, biow->ol_id
#endif
#endif
		, bio_wrapper_state_is_prepared(biow) ? 1 : 0
		, bio_wrapper_state_is_submitted(biow) ? 1 : 0
		, bio_wrapper_state_is_completed(biow) ? 1 : 0
		, bio_wrapper_state_is_discard(biow) ? 1 : 0
		, bio_wrapper_state_is_overwritten(biow) ? 1 : 0
#ifdef WALB_OVERLAPPED_SERIALIZE
		, bio_wrapper_state_is_delayed(biow) ? 1 : 0
#endif
		);
	if (biow->cloned_bioe) {
		const struct bio_entry *bioe = biow->cloned_bioe;
		printk("%s""  cloned_bioe %p pos %" PRIu64 " len %u\n"
			, level, bioe
			, (u64)bio_entry_pos(bioe)
			, bio_entry_len(bioe));
	}
}

void init_bio_wrapper(struct bio_wrapper *biow, struct bio *bio)
{
	ASSERT(biow);

	biow->cloned_bioe = NULL;
	bio_list_init(&biow->cloned_bio_list);
	biow->error = 0;
	biow->csum = 0;
	biow->private_data = NULL;
	init_completion(&biow->done);
	biow->flags = 0;

	if (bio) {
		biow->bio = bio;
		biow->pos = bio_begin_sector(bio);
		biow->len = bio_sectors(bio);
		if (bio->bi_rw & REQ_DISCARD) {
			set_bit(BIO_WRAPPER_DISCARD, &biow->flags);
		}
	} else {
		biow->bio = NULL;
		biow->pos = 0;
		biow->len = 0;
	}
#ifdef WALB_OVERLAPPED_SERIALIZE
	biow->n_overlapped = -1;
#ifdef WALB_DEBUG
	biow->ol_id = (u64)(-1);
#endif
#endif
#ifdef WALB_DEBUG
	atomic_set(&biow->state, 0);
#endif
}

struct bio_wrapper* alloc_bio_wrapper(gfp_t gfp_mask)
{
	struct bio_wrapper *biow;

	biow = kmem_cache_alloc(bio_wrapper_cache_, gfp_mask);
	if (!biow) {
		LOGe("kmem_cache_alloc() failed.");
		return NULL;
	}
	return biow;
}

/**
 * Do not touch biow->bio if not null.
 */
void destroy_bio_wrapper(struct bio_wrapper *biow)
{
	if (!biow)
		return;

	if (biow->cloned_bioe)
		destroy_bio_entry(biow->cloned_bioe);

	kmem_cache_free(bio_wrapper_cache_, biow);
}

/**
 * Copy data from a source bio_wrapper to a destination bio_wrapper.
 * Do not call this function if they are not overlapped.
 *
 * @dst destination bio_wrapper.
 *     This function modifies dst->cloned_bio_list
 *     and may split bios in the list at overlapped borders.
 *     For all copied bio(s), the least significant bit (LSB)
 *     of bio->bi_private will be set
 *     to detect whether they are copied or not later.
 *     You must clear the bit before calling bio_endio().
 * @src source bio_wrapper.
 *     This uses src->cloned_bioe->bio and src->cloned_bioe->iter.
 *     This function does not modify them.
 * @gfp_mask for memory allocation in bio split.
 *
 * RETURN:
 *   true if copy has done successfully,
 *   or false (due to memory allocation failure).
 */
bool bio_wrapper_copy_overlapped(
	struct bio_wrapper *dst, struct bio_wrapper *src, gfp_t gfp_mask)
{
	struct bio *src_bio = src->cloned_bioe->bio;
	struct bio_list *dst_list = &dst->cloned_bio_list;
	struct bio *dst_bio, *prev_bio = NULL, *next_bio;

	ASSERT(src_bio);
	ASSERT(bio_wrapper_is_overlap(dst, src));
	ASSERT(!bio_list_empty(dst_list));

	bio_list_for_each_safe(dst_bio, next_bio, dst_list) {
		struct bvec_iter dst_iter = dst_bio->bi_iter;
		struct bvec_iter src_iter = src->cloned_bioe->iter;
		uint sectors, written;
		struct bio *split0 = NULL, *split1 = NULL;

		if (!bvec_iter_is_overlap(&dst_iter, &src_iter)) {
			prev_bio = dst_bio;
			continue;
		}

		bio_get_overlapped(
			dst_bio, &dst_iter,
			src_bio, &src_iter, &sectors);
		ASSERT(sectors > 0);
		ASSERT((dst_iter.bi_size >> 9) >= sectors);
		ASSERT((src_iter.bi_size >> 9) >= sectors);

		written = bio_copy_data_partial(
			dst_bio, dst_iter,
			src_bio, src_iter, sectors);
		ASSERT(written == sectors);

		/* Split top */
		if (dst_bio->bi_iter.bi_sector < dst_iter.bi_sector) {
			const uint split_sectors =
				dst_iter.bi_sector - dst_bio->bi_iter.bi_sector;
			split0 = bio_split(
				dst_bio, split_sectors, gfp_mask, fs_bio_set);
			if (!split0)
				return false;

			bio_chain(split0, dst_bio);
		}
		/* Split bottom */
		if (sectors < (dst_iter.bi_size >> 9)) {
			split1 = bio_split(
				dst_bio, sectors, gfp_mask, fs_bio_set);
			if (!split1)
				return false;

			bio_chain(split1, dst_bio);
		}

		if (split0 && split1) {
			/*
			 * src       |--|
			 * dst    |--------|
			 * split0 |--|
			 * split1    |--|    (copied)
			 * dst'         |--|
			 */
			bio_private_lsb_set(split1);
			bio_list_insert(dst_list, split0, prev_bio);
			bio_list_insert(dst_list, split1, split0);
		} else if (split0 && !split1) {
			/*
			 * src       |-----|
			 * dst    |-----|
			 * split0 |--|
			 * dst'      |--|    (copied)
			 */
			bio_private_lsb_set(dst_bio);
			bio_list_insert(dst_list, split0, prev_bio);
		} else if (!split0 && split1) {
			/*
			 * src    |-----|
			 * dst       |-----|
			 * split1    |--|    (copied)
			 * dst'         |--|
			 */
			bio_private_lsb_set(split1);
			bio_list_insert(dst_list, split1, prev_bio);
		} else {
			/*
			 * src |--------|
			 * dst    |--|    (copied)
			 */
			bio_private_lsb_set(dst_bio);
		}
		prev_bio = dst_bio;
	}
	return true;
}

/**
 * For all bio(s) in biow->cloned_bio_list,
 * call bio_endio() and remove from the list
 * if lsb of bio->private_data is set.
 */
void bio_wrapper_endio_copied(struct bio_wrapper *biow)
{
	struct bio_list *bio_list = &biow->cloned_bio_list;
	struct bio *bio, *prev_bio = NULL, *next_bio;

	ASSERT(bio_list);
	ASSERT(!bio_list_empty(bio_list));

	bio_list_for_each_safe(bio, next_bio, bio_list) {
		if (bio_private_lsb_get(bio)) {
			bio_private_lsb_clear(bio);
			bio_list_del(bio_list, bio, prev_bio);
			bio_endio(bio, 0);
		} else {
			prev_bio = bio;
		}
	}
}

/**
 * Initilaize bio_entry cache.
 */
bool bio_wrapper_init(void)
{
	int cnt;

	cnt = atomic_inc_return(&shared_cnt_);
	if (cnt > 1) {
		return true;
	}

	ASSERT(cnt == 1);
	bio_wrapper_cache_ = kmem_cache_create(
		KMEM_CACHE_BIO_WRAPPER_NAME,
		sizeof(struct bio_wrapper), 0, 0, NULL);
	if (!bio_wrapper_cache_) {
		LOGe("failed to create a kmem_cache (bio_wrapper).\n");
		return false;
	}
	return true;
}

/**
 * Finalize bio_entry cache.
 */
void bio_wrapper_exit(void)
{
	int cnt;

	cnt = atomic_dec_return(&shared_cnt_);

	if (cnt < 0) {
		LOGe("bio_wrapper_init() is not called yet.\n");
		atomic_inc(&shared_cnt_);
		return;
	}
	if (cnt == 0) {
		kmem_cache_destroy(bio_wrapper_cache_);
		bio_wrapper_cache_ = NULL;
	}
}

MODULE_LICENSE("Dual BSD/GPL");
