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

struct bio_entry
{
	struct bio *bio; /* must be NULL if bio->bi_cnt is 0 (and deallocated). */

	/* bio->bi_iter will be changed after splitted and submitted.
	   So store it here before splitted or submitted. */
	struct bvec_iter iter;

	int error; /* bio error status. */
	struct completion done;
};

/********************************************************************************
 * Utility functions for bio entry.
 ********************************************************************************/

/* for debug */
void print_bio_entry(const char *level, struct bio_entry *bioe);

void init_bio_entry(struct bio_entry *bioe, struct bio *bio);
void fin_bio_entry(struct bio_entry *bioe);

bool init_bio_entry_by_clone(
	struct bio_entry *bioe, struct bio *bio,
	struct block_device *bdev, gfp_t gfp_mask);
void init_bio_entry_by_clone_never_giveup(
	struct bio_entry *bioe, struct bio *bio,
	struct block_device *bdev, gfp_t gfp_mask);

void wait_for_bio_entry(struct bio_entry *bioe, ulong timeoutMs);

/*
 * with own pages.
 */
struct bio* bio_alloc_with_pages(
	uint sectors, struct block_device *bdev, gfp_t gfp_mask);
void bio_put_with_pages(struct bio *bio);
struct bio* bio_deep_clone(struct bio *bio, gfp_t gfp_mask);

/********************************************************************************
 * Init/exit.
 ********************************************************************************/

bool bio_entry_init(void);
void bio_entry_exit(void);

/********************************************************************************
 * Static utilities.
 ********************************************************************************/

static inline u64 bio_entry_pos(const struct bio_entry *bioe)
{
	return (u64)bioe->iter.bi_sector;
}

static inline uint bio_entry_len(const struct bio_entry *bioe)
{
	return bioe->iter.bi_size >> 9;
}

static inline void bio_entry_clear(struct bio_entry *bioe)
{
	bioe->bio = NULL;
}

static inline bool bio_entry_exists(const struct bio_entry *bioe)
{
	return bioe->bio != NULL;
}

#endif /* WALB_BIO_ENTRY_H_KERNEL */
