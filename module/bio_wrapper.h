/**
 * bio_wrapper.h - Definition for bio_wrapper.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_BIO_WRAPPER_H_KERNEL
#define WALB_BIO_WRAPPER_H_KERNEL

#include "check_kernel.h"
#include <linux/types.h>
#include <linux/blkdev.h>
#include <linux/list.h>
#include <linux/completion.h>
#include <linux/time.h>

#include "bio_entry.h"
#include "linux/walb/common.h"

/**
 * Bio wrapper.
 */
struct bio_wrapper
{
	struct list_head list; /* list entry. */
	struct list_head list2; /* another list entry. */
	struct list_head list3; /* another list entry. */
	struct list_head list4; /* another list entry. */

	struct work_struct work; /* for workqueue tasks. */

	struct bio *bio; /* original bio. */
	sector_t pos; /* position of the original bio [logical block]. */
	unsigned int len; /* length of the original bio [logical block]. */
	u32 csum; /* checksum for write IO. */
	blk_status_t status;
	struct completion done;

	unsigned long flags; /* For atomic state management. */

	/* lsid of bio wrapper.
	   This is for
	   (1) sort in pending data copy,
	   (2) comparison with permanent_lsid. */
	u64 lsid;

	/* Original bio's buffer will be updated during IO.
	   Walb requires a fixed snapshot of data during IO.
	   So submitted bio will be copied to here at first.
	   For discard IOs, this is NULL. */
	struct bio *copied_bio;

	/* for temporary use for IOs for log/data devices. */
	struct bio_entry cloned_bioe;

	/* for temporary use. must be empty after submitted. */
	struct bio_list cloned_bio_list;

	unsigned long start_time; /* for diskstats. */

	void *private_data;

#ifdef WALB_OVERLAPPED_SERIALIZE
	int n_overlapped; /* initial value is -1. */
#ifdef WALB_DEBUG
	u64 ol_id; /* in order to check FIFO property. */
#endif
#endif
#ifdef WALB_DEBUG
	atomic_t state;
#endif
#ifdef WALB_PERFORMANCE_ANALYSIS
	struct timespec ts[8];
#endif
};

#ifdef WALB_PERFORMANCE_ANALYSIS
enum
{
	WALB_TIME_W_BEGIN = 0,
	WALB_TIME_W_LOG_SUBMITTED,
	WALB_TIME_W_LOG_COMPLETED,
	WALB_TIME_W_LOG_END,
	WALB_TIME_W_DATA_SUBMITTED,
	WALB_TIME_W_DATA_COMPLETED,
	WALB_TIME_W_DATA_END,
	WALB_TIME_W_END,
	WALB_TIME_W_MAX,
};
enum
{
	WALB_TIME_R_BEGIN = 0,
	WALB_TIME_R_SUBMITTED,
	WALB_TIME_R_COMPLETED,
	WALB_TIME_R_END,
	WALB_TIME_R_MAX,
};
#endif

/**
 * bio_wrapper.flags.
 */
enum
{
	/*
	 * Started flag.
	 */
	BIO_WRAPPER_STARTED,

	/*
	 * Information bit.
	 */
	BIO_WRAPPER_DISCARD,
	/* Set if the biow data will be fully overwritten by newer IO(s). */
	BIO_WRAPPER_OVERWRITTEN,
#ifdef WALB_OVERLAPPED_SERIALIZE
	/* Set if the biow submission for data device is delayed
	   due to overlapped. */
	BIO_WRAPPER_DELAYED,
#endif

#ifdef WALB_DEBUG
	/*
	 * State bits.
	 */
	BIO_WRAPPER_PREPARED,
	BIO_WRAPPER_SUBMITTED,
	BIO_WRAPPER_COMPLETED,
#endif
#ifdef WALB_PERFORMANCE_ANALYSIS
	BIO_WRAPPER_WRITE,
#endif
};

#define bio_wrapper_state_is_started(biow) \
	test_bit(BIO_WRAPPER_STARTED, &(biow)->flags)
#define bio_wrapper_state_is_discard(biow) \
	test_bit(BIO_WRAPPER_DISCARD, &(biow)->flags)
#define bio_wrapper_state_is_overwritten(biow) \
	test_bit(BIO_WRAPPER_OVERWRITTEN, &(biow)->flags)
#ifdef WALB_OVERLAPPED_SERIALIZE
#define bio_wrapper_state_is_delayed(biow) \
	test_bit(BIO_WRAPPER_DELAYED, &(biow)->flags)
#endif
#ifdef WALB_DEBUG
#define bio_wrapper_state_is_prepared(biow) \
	test_bit(BIO_WRAPPER_PREPARED, &(biow)->flags)
#define bio_wrapper_state_is_submitted(biow) \
	test_bit(BIO_WRAPPER_SUBMITTED, &(biow)->flags)
#define bio_wrapper_state_is_completed(biow) \
	test_bit(BIO_WRAPPER_COMPLETED, &(biow)->flags)

static inline void bio_wrapper_state_set(struct bio_wrapper *biow, uint flag)
{
	if (test_and_set_bit(flag, &biow->flags))
		WARN(1, "BUG: set a bio_wrapper flag twice: %p %u\n", biow, flag);
}

#define bio_wrapper_state_set_prepared(biow) \
	bio_wrapper_state_set(biow, BIO_WRAPPER_PREPARED)
#define bio_wrapper_state_set_submitted(biow) \
	bio_wrapper_state_set(biow, BIO_WRAPPER_SUBMITTED)
#define bio_wrapper_state_set_completed(biow) \
	bio_wrapper_state_set(biow, BIO_WRAPPER_COMPLETED)
#else
#define bio_wrapper_state_is_prepared(biow)
#define bio_wrapper_state_is_submitted(biow)
#define bio_wrapper_state_is_completed(biow)
#define bio_wrapper_state_set_prepared(biow)
#define bio_wrapper_state_set_submitted(biow)
#define bio_wrapper_state_set_completed(biow)
#endif

#ifdef WALB_PERFORMANCE_ANALYSIS
#define bio_wrapper_state_is_write(biow)	\
	test_bit(BIO_WRAPPER_WRITE, &(biow)->flags)
void print_bio_wrapper_performance(const char *level, struct bio_wrapper *biow);
#endif

UNUSED void print_bio_wrapper(const char *level, const struct bio_wrapper *biow);
UNUSED void print_bio_wrapper_short(
	const char *level, const struct bio_wrapper *biow, const char *prefix);

void init_bio_wrapper(struct bio_wrapper *biow, struct bio *bio);
struct bio_wrapper* alloc_bio_wrapper(gfp_t gfp_mask);
void destroy_bio_wrapper(struct bio_wrapper *biow);

bool bio_wrapper_copy_overlapped(
	struct bio_wrapper *dst, struct bio_wrapper *src, gfp_t gfp_mask);
void bio_wrapper_endio_copied(struct bio_wrapper *biow);
void wait_for_bio_wrapper(struct bio_wrapper *biow, ulong timeo_ms);

bool bio_wrapper_init(void);
void bio_wrapper_exit(void);

#ifdef WALB_TRACK_BIO_WRAPPER
#define BIO_WRAPPER_PRINT(prefix, biow) \
	print_bio_wrapper_short(KERN_INFO, biow, prefix)
#define BIO_WRAPPER_PRINT_CSUM(prefix, biow) do {			\
		const struct walb_dev *wdev = biow->private_data;	\
		biow->csum = bio_calc_checksum(				\
			biow->bio, wdev->log_checksum_salt);		\
		print_bio_wrapper_short(KERN_INFO, biow, prefix);	\
	} while (0)
#define BIO_WRAPPER_PRINT_LS(prefix, biow, list_size) do {	\
		char buf[32];					\
		if (list_size == 0) break;			\
		snprintf(buf, 32, "%s %u", prefix, list_size);	\
		print_bio_wrapper_short(KERN_INFO, biow, buf);	\
	} while (0)
#else
#define BIO_WRAPPER_PRINT(prefix, biow)
#define BIO_WRAPPER_PRINT_CSUM(prefix, biow)
#define BIO_WRAPPER_PRINT_LS(prefix, biow, list_size)
#endif

#ifdef WALB_DEBUG
#define BIO_WRAPPER_CHANGE_STATE(biow) atomic_inc(&biow->state)
#else
#define BIO_WRAPPER_CHANGE_STATE(biow)
#endif

/**
 * Check overlapped.
 */
static inline bool bio_wrapper_is_overlap(
	const struct bio_wrapper *biow0, const struct bio_wrapper *biow1)
{
	ASSERT(biow0);
	ASSERT(biow1);
	return (biow0->pos + (biow0->len) > biow1->pos &&
		biow1->pos + (biow1->len) > biow0->pos);
}

/**
 * Check overwritten.
 *
 * RETURN:
 *   True if biow0 data is fully overwritten by biow1, or false.
 */
static inline bool bio_wrapper_is_overwritten_by(
	const struct bio_wrapper *biow0, const struct bio_wrapper *biow1)
{
	ASSERT(biow0);
	ASSERT(biow1);

	return biow1->pos <= biow0->pos &&
		biow0->pos + biow0->len <= biow1->pos + biow1->len;
}

#endif /* WALB_BIO_WRAPPER_H_KERNEL */
