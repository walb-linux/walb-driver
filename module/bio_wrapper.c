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

/*******************************************************************************
 * Static data.
 *******************************************************************************/

/* kmem cache for bio_entry. */
#define KMEM_CACHE_BIO_WRAPPER_NAME "walb_bio_wrapper_cache"
static struct kmem_cache *bio_wrapper_cache_ = NULL;

/* shared coutner of the cache. */
static atomic_t shared_cnt_ = ATOMIC_INIT(0);


/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

#ifdef WALB_FAST_ALGORITHM
static void bio_wrapper_get_overlapped_pos_and_len(
	struct bio_wrapper *biow0,  struct bio_wrapper *biow1,
	u64 *ol_pos_p, unsigned int *ol_len_p);
#endif

/*******************************************************************************
 * Macros definition.
 *******************************************************************************/

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * Get overlapped position and length of two bio entries.
 *
 * @biow0 a bio wrapper.
 * @biow1 another bio wrapper.
 * @ol_pos_p pointer to store result position [sectors].
 * @ol_len_p pointer to store result length [sectors].
 */
#ifdef WALB_FAST_ALGORITHM
static void bio_wrapper_get_overlapped_pos_and_len(
	struct bio_wrapper *biow0,  struct bio_wrapper *biow1,
	u64 *ol_pos_p, unsigned int *ol_len_p)
{
	u64 pos, pos_end0, pos_end1;
	unsigned int len;

	/* Bigger one as the begining position. */
	if (biow0->pos < biow1->pos) {
		pos = biow1->pos;
	} else {
		pos = biow0->pos;
	}
	ASSERT(biow0->pos <= pos);
	ASSERT(biow1->pos <= pos);

	/* Smaller one as the ending position. */
	pos_end0 = biow0->pos + biow0->len;
	pos_end1 = biow1->pos + biow1->len;
	if (pos_end0 < pos_end1) {
		len = pos_end0 - pos;
	} else {
		len = pos_end1 - pos;
	}
	ASSERT(biow0->len >= len);
	ASSERT(biow1->len >= len);

	/* Set results. */
	*ol_pos_p = pos;
	*ol_len_p = len;
}
#endif

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
void print_bio_wrapper(const char *level, struct bio_wrapper *biow)
{
	struct bio_entry *bioe;
	int i;

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
#ifdef WALB_FAST_ALGORITHM
		"  is_overwritten %d\n"
#endif
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
#ifdef WALB_FAST_ALGORITHM
		, bio_wrapper_state_is_overwritten(biow) ? 1 : 0
#endif
#ifdef WALB_OVERLAPPED_SERIALIZE
		, bio_wrapper_state_is_delayed(biow) ? 1 : 0
#endif
		);
	i = 0;
	list_for_each_entry(bioe, &biow->bioe_list, list) {
		printk("%s"
			"  [%d] bioe %p pos %" PRIu64 " len %u\n"
			, level,
			i, bioe, (u64)bioe->pos, bioe->len);
		i++;
	}
	printk("%s"
		"  number of bioe %d\n", level, i);
}

void init_bio_wrapper(struct bio_wrapper *biow, struct bio *bio)
{
	ASSERT(biow);

	INIT_LIST_HEAD(&biow->bioe_list);
	biow->error = 0;
	biow->csum = 0;
	biow->private_data = NULL;
	init_completion(&biow->done);
	biow->flags = 0;
	if (bio) {
		biow->bio = bio;
		biow->pos = bio->bi_sector;
		biow->len = bio->bi_size / LOGICAL_BLOCK_SIZE;
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

void destroy_bio_wrapper(struct bio_wrapper *biow)
{
	struct bio_entry *bioe, *bioe_next;

	if (!biow) { return; }
	list_for_each_entry_safe(bioe, bioe_next, &biow->bioe_list, list) {
		list_del(&bioe->list);
		destroy_bio_entry(bioe);
	}
	kmem_cache_free(bio_wrapper_cache_, biow);
}

/**
 * Copy data from a source bio_wrapper to a destination bio_wrapper.
 *
 * @dst destination bio_wrapper. You can split inside bio(s).
 * @src source bio_wrapper. You must not modify this.
 * @gfp_mask for memory allocation in bio split.
 *
 * bio_entry(s) inside the dst->bioe_list
 * will be splitted due to overlapped border.
 *
 * RETURN:
 *   true if copy has done successfully,
 *   or false (due to memory allocation failure).
 */
#ifdef WALB_FAST_ALGORITHM
bool data_copy_bio_wrapper(
	struct bio_wrapper *dst, struct bio_wrapper *src, gfp_t gfp_mask)
{
	u64 ol_bio_pos;
	unsigned int ol_bio_len;
	unsigned int dst_off, src_off;
	unsigned int copied;
	int tmp_copied;
	struct bio_entry_cursor dst_cur, src_cur;

	ASSERT(dst);
	ASSERT(src);
	LOG_("begin dst %p src %p.\n", dst, src);

	/* Get overlapped area. */
	bio_wrapper_get_overlapped_pos_and_len(
		dst, src, &ol_bio_pos, &ol_bio_len);
	ASSERT(ol_bio_len > 0);

	LOG_("ol_bio_pos: %"PRIu64" ol_bio_len: %u\n",
		ol_bio_pos, ol_bio_len);

	/* Initialize cursors. */
	bio_entry_cursor_init(&dst_cur, &dst->bioe_list);
	bio_entry_cursor_init(&src_cur, &src->bioe_list);
	dst_off = (unsigned int)(ol_bio_pos - dst->pos);
	src_off = (unsigned int)(ol_bio_pos - src->pos);
	bio_entry_cursor_proceed(&dst_cur, dst_off);
	bio_entry_cursor_proceed(&src_cur, src_off);

	/* Copy data in the range. */
	copied = 0;
	while (copied < ol_bio_len) {
		tmp_copied = bio_entry_cursor_try_copy_and_proceed(
			&dst_cur, &src_cur, ol_bio_len - copied);
		ASSERT(tmp_copied > 0);
		copied += tmp_copied;
	}
	ASSERT(copied == ol_bio_len);

	/* Set copied flag. This may split the bio. */
	if (!bio_entry_list_mark_copied(
			&dst->bioe_list, dst_off, ol_bio_len,
			gfp_mask)) {
		LOGe("mark_copied failed.\n");
		return false;
	}

	LOG_("end dst %p src %p.\n", dst, src);
	return true;
}
#endif


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
