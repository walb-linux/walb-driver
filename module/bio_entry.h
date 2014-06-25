/**
 * bio_entry.h - Definition for bio_entry.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_BIO_ENTRY_H_KERNEL
#define WALB_BIO_ENTRY_H_KERNEL

#include "check_kernel.h"
#include <linux/types.h>
#include <linux/blkdev.h>
#include <linux/list.h>
#include <linux/completion.h>

#include "walb/common.h"

/********************************************************************************
 * Struct data.
 ********************************************************************************/

/**
 * bio as a list entry.
 */
struct bio_entry
{
	struct bio *bio; /* must be NULL if bio->bi_cnt is 0 (and deallocated). */

	/* bio->bi_iter will be changed after splitted and submitted.
	   So store it here before splitted or submitted. */
	struct bvec_iter iter;

	int error; /* bio error status. */
	struct completion done;
	unsigned long flags;
};

/**
 * bio_entry.flags.
 */
enum {
	/*
	 * Information.
	 */

	/* Set if the bio is discard request. */
	BIO_ENTRY_DISCARD = 0,

	/* Set when pages are managed by itself.
	   destroy_bio_entry() must free the page. */
	BIO_ENTRY_OWN_PAGES,
};

#define bio_entry_state_is_discard(bioe) \
	test_bit(BIO_ENTRY_DISCARD, &(bioe)->flags)
#define bio_entry_state_set_discard(bioe) \
	set_bit(BIO_ENTRY_DISCARD, &(bioe)->flags)
#define bio_entry_state_has_own_pages(bioe) \
	test_bit(BIO_ENTRY_OWN_PAGES, &(bioe)->flags)
#define bio_entry_state_set_own_pages(bioe) \
	set_bit(BIO_ENTRY_OWN_PAGES, &(bioe)->flags)

/********************************************************************************
 * Utility functions for bio entry.
 ********************************************************************************/

/* print for debug */
void print_bio_entry(const char *level, struct bio_entry *bioe);

void init_bio_entry(struct bio_entry *bioe, struct bio *bio);
struct bio_entry* alloc_bio_entry(gfp_t gfp_mask);
void destroy_bio_entry(struct bio_entry *bioe);

struct bio* bio_deep_clone(struct bio *bio, gfp_t gfp_mask);
void init_copied_bio_entry(
	struct bio_entry *bioe, struct bio *bio_with_copy);
void copied_bio_put(struct bio *bio);

#ifdef WALB_DEBUG
unsigned int bio_entry_get_n_allocated(void);
unsigned int bio_entry_get_n_allocated_pages(void);
#endif

/********************************************************************************
 * Init/exit.
 ********************************************************************************/

/* init/exit */
bool bio_entry_init(void);
void bio_entry_exit(void);

/********************************************************************************
 * Static utilities.
 ********************************************************************************/

static inline sector_t bio_entry_pos(const struct bio_entry *bioe)
{
	return bioe->iter.bi_sector;
}

static inline uint bio_entry_len(const struct bio_entry *bioe)
{
	return bioe->iter.bi_size >> 9;
}

#endif /* WALB_BIO_ENTRY_H_KERNEL */
