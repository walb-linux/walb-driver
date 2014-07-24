/**
 * pending_io.c - Pending IO processing.
 *
 * (C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/ratelimit.h>
#include "pending_io.h"
#include "treemap.h"
#include "bio_wrapper.h"

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

static void insert_to_sorted_bio_wrapper_list_by_lsid(
	struct bio_wrapper *biow, struct list_head *biow_list);

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * Insert a bio wrapper to a sorted bio wrapper list.
 * using insertion sort.
 *
 * They are sorted by biow->lsid.
 * Use biow->list3 for list operations.
 *
 * @biow (struct bio_wrapper *)
 * @biow_list (struct list_head *)
 */
static void insert_to_sorted_bio_wrapper_list_by_lsid(
	struct bio_wrapper *biow, struct list_head *biow_list)
{
	struct bio_wrapper *biow_tmp, *biow_next;
	bool moved;
#ifdef WALB_DEBUG
	u64 lsid;
#endif

	ASSERT(biow);
	ASSERT(biow_list);

	if (!list_empty(biow_list)) {
		biow_tmp = list_first_entry(
			biow_list, struct bio_wrapper, list3);
		ASSERT(biow_tmp);
		if (biow->lsid < biow_tmp->lsid) {
			list_add(&biow->list3, biow_list);
			return;
		}
	}
	moved = false;
	list_for_each_entry_safe(biow_tmp, biow_next, biow_list, list3) {
		if (biow->lsid < biow_tmp->lsid) {
			list_add_tail(&biow->list3, &biow_tmp->list3);
			moved = true;
			break;
		}
	}
	if (!moved) {
		list_add_tail(&biow->list3, biow_list);
	}

#ifdef WALB_DEBUG
	lsid = 0;
	list_for_each_entry_safe(biow_tmp, biow_next, biow_list, list3) {
		ASSERT(lsid <= biow_tmp->lsid);
		lsid = biow_tmp->lsid;
	}
#endif
}

/*******************************************************************************
 * Global functions definition.
 *******************************************************************************/

/**
 * Insert a req_entry from a pending data.
 *
 * CONTEXT:
 *   pending_data lock must be held.
 */
bool pending_insert(
	struct multimap *pending_data,
	unsigned int *max_sectors_p,
	struct bio_wrapper *biow, gfp_t gfp_mask)
{
	int ret;

	ASSERT(pending_data);
	ASSERT(max_sectors_p);
	ASSERT(biow);
	ASSERT(biow->bio);
	ASSERT(biow->bio->bi_rw & REQ_WRITE);
	ASSERT(biow->len > 0);

	/* Insert the entry. */
	ret = multimap_add(pending_data, biow->pos,
			(unsigned long)biow, gfp_mask);
	ASSERT(ret != EEXIST);
	ASSERT(ret != EINVAL);
	if (ret) {
		ASSERT(ret == ENOMEM);
		LOGe("pending_insert failed.\n");
		return false;
	}
	*max_sectors_p = max(*max_sectors_p, biow->len);
	return true;
}

/**
 * Delete a req_entry from a pending data.
 *
 * CONTEXT:
 *   pending_data lock must be held.
 */
void pending_delete(
	struct multimap *pending_data,
	unsigned int *max_sectors_p,
	struct bio_wrapper *biow)
{
	struct bio_wrapper *biow_tmp;

	ASSERT(pending_data);
	ASSERT(max_sectors_p);
	ASSERT(biow);

	/* Delete the entry. */
	biow_tmp = (struct bio_wrapper *)multimap_del(
		pending_data, biow->pos, (unsigned long)biow);
	LOG_("biow_tmp %p biow %p\n", biow_tmp, biow);
	ASSERT(biow_tmp == biow);
	if (multimap_is_empty(pending_data)) {
		*max_sectors_p = 0;
	}
}

/**
 * Check overlapped writes and copy from them.
 *
 * RETURN:
 *   true in success, or false due to data copy failed.
 *
 * CONTEXT:
 *   pending_data lock must be held.
 */
bool pending_check_and_copy(
	struct multimap *pending_data, unsigned int max_sectors,
	struct bio_wrapper *biow, gfp_t gfp_mask)
{
	struct multimap_cursor cur;
	u64 max_io_size, start_pos;
	struct bio_wrapper *biow_tmp;
	struct list_head biow_list;
	unsigned int n_overlapped_bios;
#ifdef WALB_DEBUG
	u64 lsid;
#endif

	ASSERT(pending_data);
	ASSERT(biow);

	/* Decide search start position. */
	max_io_size = max_sectors;
	if (biow->pos > max_io_size) {
		start_pos = biow->pos - max_io_size;
	} else {
		start_pos = 0;
	}

	/* Search the smallest candidate. */
	multimap_cursor_init(pending_data, &cur);
	if (!multimap_cursor_search(&cur, start_pos, MAP_SEARCH_GE, 0)) {
		/* No overlapped requests. */
		return true;
	}
	/* Copy data from pending and overlapped write requests. */
	INIT_LIST_HEAD(&biow_list);
	n_overlapped_bios = 0;
	while (multimap_cursor_key(&cur) < biow->pos + biow->len) {

		ASSERT(multimap_cursor_is_valid(&cur));

		biow_tmp = (struct bio_wrapper *)multimap_cursor_val(&cur);
		ASSERT(biow_tmp);
		if (!bio_wrapper_state_is_discard(biow_tmp) &&
			bio_wrapper_is_overlap(biow, biow_tmp)) {
			n_overlapped_bios++;
			insert_to_sorted_bio_wrapper_list_by_lsid(
				biow_tmp, &biow_list);
		}
		if (!multimap_cursor_next(&cur)) {
			break;
		}
	}
	if (n_overlapped_bios > 64) {
		pr_warn_ratelimited("Too many overlapped bio(s): %u\n",
				n_overlapped_bios);
	}
	/* Copy overlapped pending bio(s) in the order of lsid. */
	list_for_each_entry(biow_tmp, &biow_list, list3) {
		if (!bio_wrapper_copy_overlapped(biow, biow_tmp, gfp_mask))
			return false;
	}
	bio_wrapper_endio_copied(biow);

#ifdef WALB_DEBUG
	LOG_("lsid begin\n");
	lsid = 0;
	list_for_each_entry(biow_tmp, &biow_list, list3) {
		LOG_("lsid %"PRIu64"\n", biow_tmp->lsid);
		ASSERT(lsid <= biow_tmp->lsid);
		lsid = biow_tmp->lsid;
	}
	LOG_("lsid end\n");
#endif
	return true;
}

/**
 * Delete fully overwritten biow(s) by a specified biow
 * from a pending data.
 *
 * The is_overwritten field of all deleted biows will be true.
 *
 * @pending_data pending data.
 * @biow bio wrapper as a target for comparison.
 */
void pending_delete_fully_overwritten(
	struct multimap *pending_data, const struct bio_wrapper *biow)
{
	struct multimap_cursor cur;
	u64 start_pos, end_pos;

	ASSERT(pending_data);
	ASSERT(biow);
	ASSERT(biow->len > 0);

	start_pos = biow->pos;
	end_pos = start_pos + biow->len;

	/* Search the smallest candidate. */
	multimap_cursor_init(pending_data, &cur);
	if (!multimap_cursor_search(&cur, start_pos, MAP_SEARCH_GE, 0)) {
		/* No overlapped requests. */
		return;
	}

	/* Search and delete overwritten biow(s). */
	while (multimap_cursor_key(&cur) < end_pos) {
		struct bio_wrapper *biow_tmp;
		int ret;
		ASSERT(multimap_cursor_is_valid(&cur));
		biow_tmp = (struct bio_wrapper *)multimap_cursor_val(&cur);
		ASSERT(biow_tmp);
		ret = biow_tmp != biow &&
			bio_wrapper_is_overwritten_by(biow_tmp, biow);
		if (ret) {
			set_bit(BIO_WRAPPER_OVERWRITTEN, &biow_tmp->flags);
			ret = multimap_cursor_del(&cur);
			ASSERT(ret);
			ret = multimap_cursor_is_data(&cur);
		} else {
			ret = multimap_cursor_next(&cur);
		}
		if (!ret) { break; }
	}
}

/**
 * Insert a biow to and
 * delete fully overwritten (not overlapped) biow(s) by the biow from
 * a pending data.
 *
 * RETURN:
 *   true in success, or false.
 */
bool pending_insert_and_delete_fully_overwritten(
	struct multimap *pending_data, unsigned int *max_sectors_p,
	struct bio_wrapper *biow, gfp_t gfp_mask)
{
	ASSERT(pending_data);
	ASSERT(max_sectors_p);
	ASSERT(biow);

	if (!pending_insert(pending_data, max_sectors_p, biow, gfp_mask))
		return false;

	pending_delete_fully_overwritten(pending_data, biow);
	return true;
}

MODULE_LICENSE("Dual BSD/GPL");
