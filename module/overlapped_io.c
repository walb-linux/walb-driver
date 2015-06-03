/**
 * overlapped_io.c - Overlapped IO processing.
 *
 * (C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include "linux/walb/logger.h"
#include "overlapped_io.h"
#include "treemap.h"
#include "bio_wrapper.h"

/**
 * Overlapped check and insert.
 *
 * CONTEXT:
 *   overlapped_data lock must be held.
 * RETURN:
 *   true in success, or false (memory allocation failure).
 */
#ifdef WALB_OVERLAPPED_SERIALIZE
bool overlapped_check_and_insert(
	struct multimap *overlapped_data,
	unsigned int *max_sectors_p,
	struct bio_wrapper *biow, gfp_t gfp_mask
#ifdef WALB_DEBUG
	, u64 *overlapped_in_id
#endif
	)
{
	struct multimap_cursor cur;
	u64 max_io_size, start_pos;
	int ret;
	struct bio_wrapper *biow_tmp;

	ASSERT(overlapped_data);
	ASSERT(max_sectors_p);
	ASSERT(biow);
	ASSERT(biow->len > 0);

	/* Decide search start position. */
	max_io_size = *max_sectors_p;
	if (biow->pos > max_io_size) {
		start_pos = biow->pos - max_io_size;
	} else {
		start_pos = 0;
	}

	multimap_cursor_init(overlapped_data, &cur);
	biow->n_overlapped = 0;

	/* Search the smallest candidate. */
	if (!multimap_cursor_search(&cur, start_pos, MAP_SEARCH_GE, 0)) {
		goto fin;
	}

	/* Count overlapped requests previously. */
	BIO_WRAPPER_PRINT("cmpr0", biow);
	while (multimap_cursor_key(&cur) < biow->pos + biow->len) {

		ASSERT(multimap_cursor_is_valid(&cur));

		biow_tmp = (struct bio_wrapper *)multimap_cursor_val(&cur);
		ASSERT(biow_tmp);
		BIO_WRAPPER_PRINT("cmpr1", biow_tmp);
		if (bio_wrapper_is_overlap(biow, biow_tmp)) {
			biow->n_overlapped++;
		}
		if (!multimap_cursor_next(&cur)) {
			break;
		}
	}

	if (biow->n_overlapped > 0) {
		LOG_("n_overlapped %u\n", biow->n_overlapped);
		ret = test_and_set_bit(BIO_WRAPPER_DELAYED, &biow->flags);
		ASSERT(!ret);
	}
fin:
	ret = multimap_add(overlapped_data, biow->pos, (unsigned long)biow, gfp_mask);
	ASSERT(ret != -EEXIST);
	ASSERT(ret != -EINVAL);
	if (ret) {
		ASSERT(ret == -ENOMEM);
		LOGe("overlapped_check_and_insert failed.\n");
		return false;
	}
	*max_sectors_p = max(*max_sectors_p, biow->len);
#ifdef WALB_DEBUG
	{
		biow->ol_id = *overlapped_in_id;
		(*overlapped_in_id)++;
	}
#endif
	return true;
}
#endif

/**
 * Delete a bio_wrapper from the overlapped data,
 * and waiting overlapped requests
 *
 * @overlapped_data overlapped data.
 * @max_sectors_p pointer to max_sectors value.
 * @should_submit_list bio wrapper(s) which n_overlapped became 0
 *     will be added.
 *     using biow->list4 for list operations.
 * @biow biow to be deleted.
 *
 * CONTEXT:
 *   overlapped_data lock must be held.
 */
#ifdef WALB_OVERLAPPED_SERIALIZE
unsigned int overlapped_delete_and_notify(
	struct multimap *overlapped_data,
	unsigned int *max_sectors_p,
	struct list_head *should_submit_list,
	struct bio_wrapper *biow
#ifdef WALB_DEBUG
	, u64 *overlapped_out_id
#endif
	)
{
	struct multimap_cursor cur;
	u64 max_io_size, start_pos;
	struct bio_wrapper *biow_tmp;
	unsigned int n_should_submit = 0;

	ASSERT(overlapped_data);
	ASSERT(max_sectors_p);
	ASSERT(biow);
	ASSERT(biow->n_overlapped == 0);

	max_io_size = *max_sectors_p;
	if (biow->pos > max_io_size) {
		start_pos = biow->pos - max_io_size;
	} else {
		start_pos = 0;
	}

	/* Delete from the overlapped data. */
	biow_tmp = (struct bio_wrapper *)multimap_del(
		overlapped_data, biow->pos, (unsigned long)biow);
	LOG_("biow_tmp %p biow %p\n", biow_tmp, biow); /* debug */
	ASSERT(biow_tmp == biow);

#ifdef WALB_DEBUG
	{
		ASSERT(biow->ol_id == *overlapped_out_id);
		(*overlapped_out_id)++;
	}
#endif
	/* Initialize max_sectors. */
	if (multimap_is_empty(overlapped_data)) {
		*max_sectors_p = 0;
	}

	/* Search the smallest candidate. */
	multimap_cursor_init(overlapped_data, &cur);
	if (!multimap_cursor_search(&cur, start_pos, MAP_SEARCH_GE, 0)) {
		return 0;
	}
	/* Decrement count of overlapped requests afterward and notify if need. */
	while (multimap_cursor_key(&cur) < biow->pos + biow->len) {

		ASSERT(multimap_cursor_is_valid(&cur));

		biow_tmp = (struct bio_wrapper *)multimap_cursor_val(&cur);
		ASSERT(biow_tmp);
		if (bio_wrapper_is_overlap(biow, biow_tmp)) {
			biow_tmp->n_overlapped--;
			if (biow_tmp->n_overlapped == 0) {
				/* There is no overlapped request before it. */
				list_add_tail(
					&biow_tmp->list4, should_submit_list);
				n_should_submit++;
			}
		}
		if (!multimap_cursor_next(&cur)) {
			break;
		}
	}
	return n_should_submit;
}
#endif

MODULE_LICENSE("Dual BSD/GPL");
