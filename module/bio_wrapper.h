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

#include "walb/common.h"

/**
 * Bio wrapper.
 */
struct bio_wrapper
{
	struct list_head list; /* list entry. */
	struct list_head list2; /* another list entry. */
	struct work_struct work; /* for workqueue tasks. */

	struct bio *bio; /* original bio. */
	sector_t pos; /* position of the original bio [logical block]. */
	unsigned int len; /* length of the original bio [logical block]. */
	u32 csum; /* checksum for write IO. */
	int error;
	struct completion done;
	bool started;
	
	struct list_head bioe_list; /* list head of bio_entry */

	void *private_data;

#ifdef WALB_OVERLAPPING_SERIALIZE
	struct completion overlapping_done;
	int n_overlapping; /* initial value is -1. */
#endif
};

UNUSED void print_bio_wrapper(const char *level, struct bio_wrapper *biow);
void init_bio_wrapper(struct bio_wrapper *biow, struct bio *bio);
struct bio_wrapper* alloc_bio_wrapper(gfp_t gfp_mask);
void destroy_bio_wrapper(struct bio_wrapper *biow);

bool data_copy_bio_wrapper(
	struct bio_wrapper *dst, struct bio_wrapper *src, gfp_t gfp_mask);

bool bio_wrapper_init(void);
void bio_wrapper_exit(void);

/**
 * Check overlapping.
 */
static inline bool bio_wrapper_is_overlap(
	struct bio_wrapper *biow0, struct bio_wrapper *biow1)
{
	ASSERT(biow0);
	ASSERT(biow1);
	return (biow0->pos + (biow0->len) > biow1->pos &&
		biow1->pos + (biow1->len) > biow0->pos);
}

#endif /* WALB_BIO_WRAPPER_H_KERNEL */
