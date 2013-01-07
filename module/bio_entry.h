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
	struct list_head list; /* list entry */
	struct bio *bio; /* must be NULL if bio->bi_cnt is 0 (and deallocated). */
	sector_t pos; /* [logical block] */
	unsigned int len; /* keep (bi_size << 9) at initialization,
			     because bio->bi_size will be 0 after endio. */
	int error; /* bio error status. */
	struct completion done; /* If is_splitted is true and bio_orig is NULL,
				   this completion never be called. */
	struct bio *bio_orig; /* If non-NULL, is_splitted is always true and
				 this is the original bio
				 while bio member is the first splitted one.
				 You must finalize the bio_orig. */
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
	/* Set if the bio is splitted one. */
	BIO_ENTRY_SPLITTED,
#ifdef WALB_FAST_ALGORITHM
	/* Set if read is done by copy from pending data. */
	BIO_ENTRY_COPIED,
	/* Set when pages are managed by itself.
	   destroy_bio_entry() must free the page. */
	BIO_ENTRY_OWN_PAGES,
#endif
};

#define bio_entry_state_is_discard(bioe) \
	test_bit(BIO_ENTRY_DISCARD, &(bioe)->flags)
#define bio_entry_state_set_discard(bioe) \
	set_bit(BIO_ENTRY_DISCARD, &(bioe)->flags)
#define bio_entry_state_is_splitted(bioe) \
	test_bit(BIO_ENTRY_SPLITTED, &(bioe)->flags)
#define bio_entry_state_set_splitted(bioe) \
	set_bit(BIO_ENTRY_SPLITTED, &(bioe)->flags)
#ifdef WALB_FAST_ALGORITHM
#define bio_entry_state_is_copied(bioe) \
	test_bit(BIO_ENTRY_COPIED, &(bioe)->flags)
#define bio_entry_state_set_copied(bioe) \
	set_bit(BIO_ENTRY_COPIED, &(bioe)->flags)
#define bio_entry_state_is_own_pages(bioe) \
	test_bit(BIO_ENTRY_OWN_PAGES, &(bioe)->flags)
#define bio_entry_state_set_own_pages(bioe) \
	set_bit(BIO_ENTRY_OWN_PAGES, &(bioe)->flags)
#endif

/**
 * bio_entry cursor.
 */
struct bio_entry_cursor
{
	struct list_head *bio_ent_list; /* bio_entry list. */
	unsigned int off; /* offset [sectors] in the whole list. */

	struct bio_entry *bioe; /* current bioe */
	unsigned int off_in; /* offset [sectors] inside bioe. */
};

/********************************************************************************
 * Utility functions for bio entry.
 ********************************************************************************/

/* print for debug */
void print_bio_entry(const char *level, struct bio_entry *bioe);

void init_bio_entry(struct bio_entry *bioe, struct bio *bio);
struct bio_entry* alloc_bio_entry(gfp_t gfp_mask);
void destroy_bio_entry(struct bio_entry *bioe);

void get_bio_entry_list(struct list_head *bio_ent_list);
void put_bio_entry_list(struct list_head *bio_ent_list);
void destroy_bio_entry_list(struct list_head *bio_ent_list);

#ifdef WALB_FAST_ALGORITHM
struct bio* bio_clone_copy(struct bio *bio, gfp_t gfp_mask);
void init_copied_bio_entry(
	struct bio_entry *bioe, struct bio *bio_with_copy);
bool bio_entry_list_mark_copied(
	struct list_head *bio_ent_list,
	unsigned int off, unsigned int sectors, gfp_t gfp_mask);

unsigned int bio_entry_cursor_try_copy_and_proceed(
	struct bio_entry_cursor *dst,
	struct bio_entry_cursor *src,
	unsigned int sectors);
#endif

bool bio_entry_cursor_is_valid(struct bio_entry_cursor *cur);
void bio_entry_cursor_init(
	struct bio_entry_cursor *cur, struct list_head *bio_ent_list);
bool bio_entry_cursor_proceed(struct bio_entry_cursor *cur,
			unsigned int sectors);
bool should_split_bio_entry_list_for_chunk(
	struct list_head *bio_ent_list, unsigned int chunk_sectors);
bool split_bio_entry_list_for_chunk(
	struct list_head *bio_ent_list,
	unsigned int chunk_sectors, gfp_t gfp_mask);

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

/**
 * Check whether complete(&bioe->done) will be called, or not.
 *
 * RETURN:
 *   true if you should wait the completion.
 */
static inline bool bio_entry_should_wait_completion(struct bio_entry *bioe)
{
	ASSERT(bioe);
	return !bio_entry_state_is_splitted(bioe) || bioe->bio_orig;
}

#endif /* WALB_BIO_ENTRY_H_KERNEL */
