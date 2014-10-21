/**
 * bio_entry.c - bio_entry related functions.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"
#include <linux/module.h>
#include <linux/list.h>
#include "bio_entry.h"
#include "bio_util.h"
#include "walb/common.h"
#include "walb/logger.h"
#include "walb/check.h"
#include "walb/util.h"
#include "walb/block_size.h"

/**
 * Bio entry normal procedure:
 *
 * (1) prepare bioe or call alloc_bio_entry().
 * (2) call init_bio_entry(bioe, bio)
 * (3) call generic_make_request(bio) or do the corresponding operations.
 * (4) call wait_for_bio_entry(bioe)
 * (5) call fin_bio_entry(bioe)
 * (6) (call free_bio_entry(bioe) if allocated before)
 */

/**
 * Deep cloning procedure.
 *
 * (1) prepare clone by calling alloc_bio_with_pages() or bio_deep_clone().
 * (2) use clone.
 * (3) bio_put_with_pages(clone)
 */

/*******************************************************************************
 * Static data.
 *******************************************************************************/

/* kmem cache for bio_entry. */
#define KMEM_CACHE_BIO_ENTRY_NAME "walb_bio_entry_cache"
static struct kmem_cache *bio_entry_cache_ = NULL;

/* shared coutner of the cache. */
static atomic_t shared_cnt_ = ATOMIC_INIT(0);

/*
 * Number of bio_entry allocated.
 */
#ifdef WALB_DEBUG
static atomic_t n_allocated_ = ATOMIC_INIT(0);
#endif

/*
 * Allocated pages by alloc_page().
 */
#ifdef WALB_DEBUG
static atomic_t n_allocated_pages_ = ATOMIC_INIT(0);
#endif

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * Page allocator with counter.
 */
static inline struct page* alloc_page_inc(gfp_t gfp_mask)
{
	struct page *p;

	p = alloc_page(gfp_mask);
#ifdef WALB_DEBUG
	if (p)
		atomic_inc(&n_allocated_pages_);
#endif
	return p;
}

/**
 * Page deallocator with counter.
 */
static inline void free_page_dec(struct page *page)
{
	ASSERT(page);
	__free_page(page);
#ifdef WALB_DEBUG
	atomic_dec(&n_allocated_pages_);
#endif
}

static void bio_entry_end_io(struct bio *bio, int error);

/*******************************************************************************
 * Global functions definition.
 *******************************************************************************/

void print_bio_entry(const char *level, struct bio_entry *bioe)
{
	char buf[512];
	ASSERT(bioe);

	if (bioe->bio)
		snprint_bio(buf, 512, bioe->bio);
	else
		buf[0] = '\0';

	printk("%s""bio %p error %d\n"
		"%s\n"
		, level
		, bioe->bio, bioe->error
		, buf);
}

#if 0
struct bio_entry* alloc_bio_entry(gfp_t gfp_mask)
{
	struct bio_entry *bioe;

	bioe = kmem_cache_alloc(bio_entry_cache_, gfp_mask);
	if (!bioe) {
		LOGd("kmem_cache_alloc() failed.");
		return NULL;
	}
#ifdef WALB_DEBUG
	atomic_inc(&n_allocated_);
#endif
	return bioe;
}
#endif

#if 0
void free_bio_entry(struct bio_entry *bioe)
{
	if (!bioe)
		return;
	ASSERT(!bioe->bio);
#ifdef WALB_DEBUG
	atomic_dec(&n_allocated_);
#endif
	kmem_cache_free(bio_entry_cache_, bioe);
}
#endif

static void bio_entry_end_io(struct bio *bio, int error)
{
	struct bio_entry *bioe = bio->bi_private;
	int uptodate = test_bit(BIO_UPTODATE, &bio->bi_flags);
	ASSERT(bioe);
	ASSERT(bio->bi_bdev);
	ASSERT(bioe->bio == bio);

	if (!uptodate) {
		UNUSED const unsigned int devt = bio->bi_bdev->bd_dev;
		LOG_("BIO_UPTODATE is false"
			" (dev %u:%u rw %lu pos %" PRIu64 " len %u).\n"
			, MAJOR(devt), MINOR(devt)
			, bio->bi_rw
			, bio_entry_pos(bioe), bio_entry_len(bioe));
	}

	bioe->error = error;
	LOG_("complete bioe %p pos %" PRIu64 " len %u\n"
		, bioe, bio_entry_pos(bioe), bio_entry_len(bioe));

	complete(&bioe->done);
}

void init_bio_entry(struct bio_entry *bioe, struct bio *bio)
{
	ASSERT(bioe);
	ASSERT(bio);

	init_completion(&bioe->done);
	bioe->error = 0;
	bioe->bio = bio;
	bioe->iter = bio->bi_iter; /* copy */
	bio->bi_private = bioe;
	bio->bi_end_io = bio_entry_end_io;
}

void fin_bio_entry(struct bio_entry *bioe)
{
	if (!bioe)
		return;
	if (bioe->bio) {
		bio_put(bioe->bio);
		bioe->bio = NULL;
	}
}

/**
 * Create a bio_entry by clone.
 *
 * @bioe bio entry (bioe->bio must be NULL)
 * @bio original bio.
 * @bdev block device to forward bio.
 */
bool init_bio_entry_by_clone(
	struct bio_entry *bioe, struct bio *bio,
	struct block_device *bdev, gfp_t gfp_mask)
{
	struct bio *clone;

	clone = bio_clone(bio, gfp_mask);
	if (!clone)
		return false;

	clone->bi_bdev = bdev;

	init_bio_entry(bioe, clone);
	return true;
}

void init_bio_entry_by_clone_never_giveup(
	struct bio_entry *bioe, struct bio *bio,
	struct block_device *bdev, gfp_t gfp_mask)
{
	while (!init_bio_entry_by_clone(bioe, bio, bdev, gfp_mask)) {
		LOGd_("clone bio failed %p.\n", bio);
		schedule();
	}
}

#if 0
void destroy_bio_entry(struct bio_entry *bioe)
{
	fin_bio_entry(bioe);
	free_bio_entry(bioe);
}
#endif

void wait_for_bio_entry(struct bio_entry *bioe, ulong timeoutMs)
{
	const ulong timeo = msecs_to_jiffies(timeoutMs);
	ulong rtimeo;
	int c = 0;

retry:
	rtimeo = wait_for_completion_io_timeout(&bioe->done, timeo);
	if (rtimeo == 0) {
		LOGn("timeout(%d): bioe %p bio %p pos %" PRIu64 " len %u\n"
			, c, bioe, bioe->bio
			, (u64)bio_entry_pos(bioe), bio_entry_len(bioe));
		c++;
		goto retry;
	}
}

#ifdef WALB_DEBUG
unsigned int bio_entry_get_n_allocated(void)
{
	return atomic_read(&n_allocated_);
}
#endif

/**
 * Allocate a bio with pages.
 *
 * @size size in bytes.
 *
 * You must set bi_bdev, bi_rw, bi_iter by yourself.
 * bi_iter.bi_size will be set to the specified size if size is not 0.
 */
struct bio* bio_alloc_with_pages(uint size, struct block_device *bdev, gfp_t gfp_mask)
{
	struct bio *bio;
	uint i, nr_pages, remaining;

	nr_pages = (size + PAGE_SIZE - 1) / PAGE_SIZE;

	bio = bio_alloc(gfp_mask, nr_pages);
	if (!bio)
		return NULL;

	bio->bi_bdev = bdev; /* required to bio_add_page(). */

	remaining = size;
	for (i = 0; i < nr_pages; i++) {
		uint len0, len1;
		struct page *page = alloc_page_inc(gfp_mask);
		if (!page)
			goto err;
		len0 = min_t(uint, PAGE_SIZE, remaining);
		len1 = bio_add_page(bio, page, len0, 0);
		ASSERT(len0 == len1);
		remaining -= len0;
	}
	ASSERT(remaining == 0);
	ASSERT(bio->bi_iter.bi_size == size);
	return bio;
err:
	bio_put_with_pages(bio);
	return NULL;
}

/**
 * Free its all pages and call bio_put().
 */
void bio_put_with_pages(struct bio *bio)
{
	struct bio_vec *bv;
	int i;
	ASSERT(bio);

	bio_for_each_segment_all(bv, bio, i) {
		if (bv->bv_page) {
			free_page_dec(bv->bv_page);
			bv->bv_page = NULL;
		}
	}
	ASSERT(atomic_read(&bio->bi_cnt) == 1);
	bio_put(bio);
}

/**
 * Create a copy of a write bio.
 */
struct bio* bio_deep_clone(struct bio *bio, gfp_t gfp_mask)
{
	uint size;
	struct bio *clone;

	ASSERT(bio);
	ASSERT(bio->bi_rw & REQ_WRITE);
	ASSERT(!bio->bi_next);

	if (bio_has_data(bio))
		size = bio->bi_iter.bi_size;
	else
		size = 0;

	clone = bio_alloc_with_pages(size, bio->bi_bdev, gfp_mask);
	if (!clone)
		return NULL;

	clone->bi_rw = bio->bi_rw;
	clone->bi_iter.bi_sector = bio->bi_iter.bi_sector;

	if (size == 0) {
		/* This is for discard IOs. */
		clone->bi_iter.bi_size = bio->bi_iter.bi_size;
	} else {
		bio_copy_data(clone, bio);
	}
	return clone;
}

#ifdef WALB_DEBUG
unsigned int bio_entry_get_n_allocated_pages(void)
{
	return atomic_read(&n_allocated_pages_);
}
#endif

/**
 * Initilaize bio_entry cache.
 */
bool bio_entry_init(void)
{
	int cnt;

	cnt = atomic_inc_return(&shared_cnt_);
	if (cnt > 1) {
		return true;
	}

	ASSERT(cnt == 1);
	bio_entry_cache_ = kmem_cache_create(
		KMEM_CACHE_BIO_ENTRY_NAME,
		sizeof(struct bio_entry), 0, 0, NULL);
	if (!bio_entry_cache_) {
		LOGe("failed to create a kmem_cache (bio_entry).\n");
		return false;
	}
	return true;
}

/**
 * Finalize bio_entry cache.
 */
void bio_entry_exit(void)
{
	int cnt;

	cnt = atomic_dec_return(&shared_cnt_);

	if (cnt < 0) {
		LOGe("bio_entry_init() is not called yet.\n");
		atomic_inc(&shared_cnt_);
		return;
	}
	if (cnt == 0) {
#ifdef WALB_DEBUG
		LOGw("n_allocated %u n_allocated_pages %u\n",
			bio_entry_get_n_allocated(),
			bio_entry_get_n_allocated_pages());
#endif
		kmem_cache_destroy(bio_entry_cache_);
		bio_entry_cache_ = NULL;
	}
}

MODULE_LICENSE("Dual BSD/GPL");
