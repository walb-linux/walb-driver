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

/*******************************************************************************
 * Global functions definition.
 *******************************************************************************/

/**
 * Print a bio_entry.
 */
void print_bio_entry(const char *level, struct bio_entry *bioe)
{
	char buf[512];
	ASSERT(bioe);

	if (bioe->bio)
		snprint_bio(buf, 512, bioe->bio);
	else
		buf[0] = '\0';

	printk("%s""bio %p error %d has_own_pages %d\n"
		"%s\n"
		, level
		, bioe->bio, bioe->error
		, (int)bioe->has_own_pages
		, buf);
}

/**
 * Initialize a bio_entry.
 */
void init_bio_entry(struct bio_entry *bioe, struct bio *bio)
{
	ASSERT(bioe);

	init_completion(&bioe->done);
	bioe->error = 0;
	bioe->has_own_pages = false;
	if (bio) {
		bioe->bio = bio;
		bioe->iter = bio->bi_iter; /* copy */
	} else {
		bioe->bio = NULL;
		memset(&bioe->iter, 0, sizeof(bioe->iter));
	}
}

/**
 * Create a bio_entry.
 * Internal bio and bi_size will be set NULL.
 */
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

/**
 * Destroy a bio_entry.
 */
void destroy_bio_entry(struct bio_entry *bioe)
{
	struct bio *bio;

	LOG_("destroy_bio_entry() begin.\n");

	if (!bioe)
		return;

	bio = bioe->bio;
	if (bio) {
		LOG_("bio_put %p\n", bio);
		if (bioe->has_own_pages) {
			copied_bio_put(bio);
		} else {
			bio_put(bio);
		}
	}
#ifdef WALB_DEBUG
	atomic_dec(&n_allocated_);
#endif
	kmem_cache_free(bio_entry_cache_, bioe);

	LOG_("destroy_bio_entry() end.\n");
}

/**
 * Allocate pages and copy its data.
 * This is for write bio.
 * CAUSION:
 *   bio->bi_next must be NULL for bio_copy_data().
 */
struct bio* bio_deep_clone(struct bio *bio, gfp_t gfp_mask)
{
	struct bio *clone;
	struct bio_vec *bv;
	int i;

	ASSERT(bio);
	ASSERT(bio->bi_rw & REQ_WRITE);
	ASSERT(!bio->bi_next); /* for bio_copy_data(). */

	clone = bio_clone(bio, gfp_mask);
	if (!clone)
		return NULL;

	clone->bi_flags &= ~(1UL << BIO_CLONED);

	if (!bio_has_data(bio))
		goto fin;

	/* Allocate its own pages. */
	bio_for_each_segment_all(bv, clone, i) {
		if (bv->bv_page)
			bv->bv_page = NULL;
		/* cloned bio has only required bvec array. */
		bv->bv_page = alloc_page_inc(gfp_mask);
		if (!bv->bv_page)
			goto error;
	}
	bio_copy_data(clone, bio);
fin:
	return clone;
error:
	copied_bio_put(clone);
	return NULL;
}

/**
 * Initialize a bio_entry with a bio with copy.
 */
void init_copied_bio_entry(
	struct bio_entry *bioe, struct bio *bio_with_copy)
{
	ASSERT(bioe);
	ASSERT(bio_with_copy);

	init_bio_entry(bioe, bio_with_copy);
	if (bio_entry_len(bioe) > 0 && !(bio_with_copy->bi_rw & REQ_DISCARD)) {
		bioe->has_own_pages = true;
	}

	/* You must call copied_bio_put() explicitly
	   or call bio_entry_destroy(). */
	bio_get(bio_with_copy);
}

/**
 * Free its all pages and call bio_put().
 */
void copied_bio_put(struct bio *bio)
{
	struct bio_vec *bvec;
	int i;
	ASSERT(bio);
	ASSERT(!(bio->bi_flags & (1 << BIO_CLONED)));
	ASSERT(!(bio->bi_rw & REQ_DISCARD));

	bio_for_each_segment_all(bvec, bio, i) {
		if (bvec->bv_page) {
			free_page_dec(bvec->bv_page);
			bvec->bv_page = NULL;
		}
	}

	ASSERT(atomic_read(&bio->bi_cnt) == 1);
	bio_put(bio);
}

#ifdef WALB_DEBUG
unsigned int bio_entry_get_n_allocated(void)
{
	return atomic_read(&n_allocated_);
}
#endif

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
