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

	bool has_own_pages; /* QQQ this is not necessary anymore. */
};

/********************************************************************************
 * Utility functions for bio entry.
 ********************************************************************************/

/* print for debug */
void print_bio_entry(const char *level, struct bio_entry *bioe);

void init_bio_entry(struct bio_entry *bioe, struct bio *bio);
struct bio_entry* alloc_bio_entry(gfp_t gfp_mask);
void destroy_bio_entry(struct bio_entry *bioe);

struct bio* bio_deep_clone(struct bio *bio, gfp_t gfp_mask);
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
