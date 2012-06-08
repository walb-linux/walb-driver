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

/**
 * bio as a list entry.
 */
struct bio_entry
{
	struct list_head list; /* list entry */
	struct bio *bio; /* must be NULL if bio->bi_cnt is 0 (and deallocated). */
	struct completion done;
	unsigned int bi_size; /* keep bi_size at initialization,
				 because bio->bi_size will be 0 after endio. */
	int error; /* bio error status. */
#ifdef WALB_FAST_ALGORITHM
	bool is_copied; /* true if read is done by copy from pending data. */
#endif
};

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

void print_bio_entry(const char *level, struct bio_entry *bioe);

void init_bio_entry(struct bio_entry *bioe);
struct bio_entry* create_bio_entry(gfp_t gfp_mask);
void destroy_bio_entry(struct bio_entry *bioe);

void get_bio_entry_list(struct list_head *bio_ent_list);
void put_bio_entry_list(struct list_head *bio_ent_list);
void destroy_bio_entry_list(struct list_head *bio_ent_list);



bool bio_entry_init(void);
void bio_entry_exit(void);


#endif /* WALB_BIO_ENTRY_H_KERNEL */
