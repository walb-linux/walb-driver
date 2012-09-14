/**
 * req_entry.c - req_entry related functions.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"
#include <linux/module.h>
#include <linux/list.h>
#include "bio_entry.h"
#include "req_entry.h"
#include "walb/common.h"
#include "walb/util.h"
#include "walb/block_size.h"

/*******************************************************************************
 * Static data.
 *******************************************************************************/

/* kmem cache for dbio. */
#define KMEM_CACHE_REQ_ENTRY_NAME "req_entry_cache"
static struct kmem_cache *req_entry_cache_ = NULL;

/* shared coutner of the cache. */
static atomic_t shared_cnt_ = ATOMIC_INIT(0);

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

/*******************************************************************************
 * Macros definition.
 *******************************************************************************/

/*******************************************************************************
 * Global functions definition.
 *******************************************************************************/

/**
 * Print a req_entry.
 */
UNUSED
void print_req_entry(const char *level, struct req_entry *reqe)
{
	ASSERT(reqe);
	/* now editing */
}

/**
 * Create req_entry struct.
 */
struct req_entry* create_req_entry(
	struct request *req,
	struct wrapper_blk_dev *wdev, gfp_t gfp_mask)
{
	struct req_entry *reqe;

	reqe = kmem_cache_alloc(req_entry_cache_, gfp_mask);
	if (!reqe) {
		goto error0;
	}
	INIT_LIST_HEAD(&reqe->list);

	/* INIT_WORK(&reqe->work, NULL); */
	reqe->wdev = wdev;
	
	ASSERT(req);
	reqe->req = req;
	INIT_LIST_HEAD(&reqe->bio_ent_list);
	init_completion(&reqe->done);
        
#ifdef WALB_OVERLAPPING_SERIALIZE
	init_completion(&reqe->overlapping_done);
	reqe->n_overlapping = -1;
#endif
	reqe->req_pos = blk_rq_pos(req);
	reqe->req_sectors = blk_rq_sectors(req);
	return reqe;
error0:
	return NULL;
}

/**
 * Destroy a req_entry.
 */
void destroy_req_entry(struct req_entry *reqe)
{
	struct bio_entry *bioe, *next;

	if (reqe) {
		list_for_each_entry_safe(bioe, next, &reqe->bio_ent_list, list) {
			list_del(&bioe->list);
			destroy_bio_entry(bioe);
		}
#ifdef WALB_DEBUG
		reqe->req = NULL;
		INIT_LIST_HEAD(&reqe->list);
		INIT_LIST_HEAD(&reqe->bio_ent_list);
#endif
		kmem_cache_free(req_entry_cache_, reqe);
	}
}

/**
 * Call bio_get() for all bio(s) inside a request.
 */
UNUSED
void req_entry_get(struct req_entry *reqe)
{
	struct bio *bio;

	ASSERT(reqe);
	ASSERT(reqe->req);

	__rq_for_each_bio(bio, reqe->req) {
		bio_get(bio);
	}
}

/**
 * Call bio_put() for all bio(s) inside a request.
 */
UNUSED
void req_entry_put(struct req_entry *reqe)
{
	struct bio *bio;

	ASSERT(reqe);
	ASSERT(reqe->req);

	__rq_for_each_bio(bio, reqe->req) {
		bio_put(bio);
	}
}

/**
 * Get overlapping position and sectors of two requests.
 *
 * @reqe0 a request entry.
 * @reqe1 another request entry.
 * @ol_req_pos_p pointer to store result position [sectors].
 * @ol_req_sectors_p pointer to store result sectors [sectors].
 */
#ifdef WALB_FAST_ALGORITHM
void get_overlapping_pos_and_sectors(
	struct req_entry *reqe0,  struct req_entry *reqe1,
	u64 *ol_req_pos_p, unsigned int *ol_req_sectors_p)
{
	u64 pos, pos_end0, pos_end1;
	unsigned int sectors;

	/* Bigger one as the begin position. */
	if (reqe0->req_pos < reqe1->req_pos) {
		pos = reqe1->req_pos;
	} else {
		pos = reqe0->req_pos;
	}
	ASSERT(reqe0->req_pos <= pos);
	ASSERT(reqe1->req_pos <= pos);
	
	/* Smaller one as the end position. */
	pos_end0 = reqe0->req_pos + reqe0->req_sectors;
	pos_end1 = reqe1->req_pos + reqe1->req_sectors;
	if (pos_end0 < pos_end1) {
		sectors = pos_end0 - pos;
	} else {
		sectors = pos_end1 - pos;
	}
	ASSERT(reqe0->req_sectors >= sectors);
	ASSERT(reqe1->req_sectors >= sectors);

	/* Set results. */
	*ol_req_pos_p = pos;
	*ol_req_sectors_p = sectors;
}
#endif

/**
 * Copy data from a source req_entry to a destination req_entry.
 *
 * @dst_reqe destination. You can split inside bio(s).
 * @src_reqe source. You must not modify this.
 * @gfp_mask for memory allocation in bio split.
 *
 * bioe->is_copied will be true when it uses data of the source.
 * bio and bioe in the destination
 * will be splitted due to overlapping border.
 *
 * RETURN:
 *   true if copy has done successfully,
 *   or false (due to memory allocation failure).
 */
#ifdef WALB_FAST_ALGORITHM
bool data_copy_req_entry(
	struct req_entry *dst_reqe,  struct req_entry *src_reqe, gfp_t gfp_mask)
{
	u64 ol_req_pos;
	unsigned int ol_req_sectors;
	unsigned int dst_off, src_off;
	unsigned int copied;
	int tmp_copied;
	struct bio_entry_cursor dst_cur, src_cur;

	ASSERT(dst_reqe);
	ASSERT(src_reqe);

	LOGd_("begin dst %p src %p.\n", dst_reqe, src_reqe);
	
	/* Get overlapping area. */
	get_overlapping_pos_and_sectors(
		dst_reqe, src_reqe, &ol_req_pos, &ol_req_sectors);
	ASSERT(ol_req_sectors > 0);

	LOGd_("ol_req_pos: %"PRIu64" ol_req_sectors: %u\n",
		ol_req_pos, ol_req_sectors);

	/* Initialize cursors. */
	bio_entry_cursor_init(&dst_cur, &dst_reqe->bio_ent_list);
	bio_entry_cursor_init(&src_cur, &src_reqe->bio_ent_list);
	dst_off = (unsigned int)(ol_req_pos - dst_reqe->req_pos);
	src_off = (unsigned int)(ol_req_pos - src_reqe->req_pos);
	bio_entry_cursor_proceed(&dst_cur, dst_off);
	bio_entry_cursor_proceed(&src_cur, src_off);

	/* Copy data in the range. */
	copied = 0;
	while (copied < ol_req_sectors) {
		
		tmp_copied = bio_entry_cursor_try_copy_and_proceed(
			&dst_cur, &src_cur, ol_req_sectors - copied);
		ASSERT(tmp_copied > 0);
		copied += tmp_copied;
	}
	ASSERT(copied == ol_req_sectors);

	/* Set copied flag. */
	if (!bio_entry_list_mark_copied(
			&dst_reqe->bio_ent_list, dst_off, ol_req_sectors,
			gfp_mask)) {
		goto error;
	}
			
	LOGd_("end dst %p src %p.\n", dst_reqe, src_reqe);
	return true;
error:
	LOGe("data_copy_req_entry failed.\n");
	return false;
}
#endif

/**
 * Initialize req_entry cache.
 */
bool req_entry_init(void)
{
	int cnt;
	LOGd("req_entry_init begin\n");
	cnt = atomic_inc_return(&shared_cnt_);
	
	if (cnt > 1) {
		return true;
	}

	ASSERT(cnt == 1);
	req_entry_cache_ = kmem_cache_create(
		KMEM_CACHE_REQ_ENTRY_NAME,
		sizeof(struct req_entry), 0, 0, NULL);
	if (!req_entry_cache_) {
		LOGe("failed to create a kmem_cache (req_entry).\n");
		goto error;
	}
	LOGd("req_entry_init end\n");
	return true;
error:
	LOGd("req_entry_init failed\n");
	return false;
}

/**
 * Finalize req_entry cache.
 */
void req_entry_exit(void)
{
	int cnt;

	cnt = atomic_dec_return(&shared_cnt_);
	
	if (cnt > 0) {
		return;
	} else if (cnt < 0) {
		LOGn("req_entry_init() is not called yet.\n");
		atomic_inc(&shared_cnt_);
		return;
	} else {
		ASSERT(cnt == 0);
		kmem_cache_destroy(req_entry_cache_);
		req_entry_cache_ = NULL;
	}

}

MODULE_LICENSE("Dual BSD/GPL");
