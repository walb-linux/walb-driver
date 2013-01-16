/**
 * test_hashtbl.c - Test module.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>
#include <linux/sort.h>
#include <linux/random.h>
#include <linux/time.h>
#include <linux/list.h>

#include "walb/common.h"
#include "treemap.h"

/* Module parameter. */
static unsigned int n_test_ = 1000;
module_param_named(n_test, n_test_, uint, S_IRUGO);
static unsigned int n_items_ = 256;
module_param_named(n_items, n_items_, uint, S_IRUGO);

struct a_item
{
	u64 key;
	void *ptr;
};

#define N_ITEMS_IN_PAGE (PAGE_SIZE / sizeof(struct a_item))

struct l_item
{
	u64 key;
	void *ptr;
	struct list_head list;
};

static int cmp_func(const void *a, const void *b)
{
	const struct a_item *x = a;
	const struct a_item *y = b;
	u64 u1, u2;

	ASSERT(x);
	ASSERT(y);
	u1 = x->key;
	u2 = y->key;

	if (u1 < u2) {
		return -1;
	}
	if (u1 > u2) {
		return 1;
	}
	ASSERT(u1 == u2);
	return 0;
}


static void test_hsort(unsigned int n_test)
{
	void *p;
	unsigned int i;
	struct timespec ts_bgn, ts_end, ts_time, ts_time1, ts_time2;

	p = kmalloc(PAGE_SIZE, GFP_KERNEL);
	if (!p) {
		LOGe("Memory allocation error.\n");
		goto fin;
	}

	/* warm up */
	for (i = 0; i < n_test; i++) {
		get_random_bytes(p, PAGE_SIZE);
	}

	/* baseline */
	getnstimeofday(&ts_bgn);
	for (i = 0; i < n_test; i++) {
		get_random_bytes(p, PAGE_SIZE);
	}
	getnstimeofday(&ts_end);
	ts_time = timespec_sub(ts_end, ts_bgn);
	LOGn("%ld.%09ld seconds\n", ts_time.tv_sec, ts_time.tv_nsec);
	ts_time1 = ts_time;

	/* target */
	getnstimeofday(&ts_bgn);
	for (i = 0; i < n_test; i++) {
		get_random_bytes(p, PAGE_SIZE);
		sort(p, N_ITEMS_IN_PAGE, sizeof(struct a_item), cmp_func, NULL);
	}
	getnstimeofday(&ts_end);
	ts_time = timespec_sub(ts_end, ts_bgn);
	LOGn("%ld.%09ld seconds\n", ts_time.tv_sec, ts_time.tv_nsec);
	ts_time2 = ts_time;

	ts_time = timespec_sub(ts_time2, ts_time1);
	LOGn("%ld.%09ld seconds\n", ts_time.tv_sec, ts_time.tv_nsec);

	/* for (i = 0; i < N_ITEMS_IN_PAGE; i++) { */
	/* 	LOGn("%04u %"PRIu64"\n", i, ((struct a_item *)p)[i].key); */
	/* } */

	kfree(p);
fin:
	return;
}

static bool create_item_list(unsigned int n_items, struct list_head *list0)
{
	unsigned int i;
	struct l_item *item;

	for (i = 0; i < n_items; i++) {
		item = kmalloc(sizeof(struct l_item), GFP_KERNEL);
		if (!item) {
			LOGe("allocation failure.\n");
			goto error0;
		}
		list_add_tail(&item->list, list0);
	}

	return true;

error0:
	return false;
}

static void fill_item_list_randomly(struct list_head *list0)
{
	struct l_item *item;

	list_for_each_entry(item, list0, list) {
		get_random_bytes(&item->key, sizeof(u64));
	}
}

static void move_item_list_all(struct list_head *dst, struct list_head *src)
{
	struct l_item *item, *item_next;

	list_for_each_entry_safe(item, item_next, src, list) {
		list_move_tail(&item->list, dst);
	}
}

static void destroy_item_list(struct list_head *list0)
{
	struct l_item *item, *item_next;

	list_for_each_entry_safe(item, item_next, list0, list) {
		list_del(&item->list);
		kfree(item);
	}
}

static void insertion_sort(struct list_head *dst, struct list_head *src)
{
	struct l_item *item, *item_next, *item_tmp;
	bool moved;

	/* sort. */
	list_for_each_entry_safe(item, item_next, src, list) {
		/* first. */
		if (!list_empty(dst)) {
			item_tmp = list_first_entry(dst, struct l_item, list);
			ASSERT(item_tmp);
			if (item->key < item_tmp->key) {
				list_move(&item->list, dst);
			}
			continue;
		}
		/* middle */
		moved = false;
		list_for_each_entry(item_tmp, dst, list) {
			if (item->key < item_tmp->key) {
				list_move(&item->list, &item_tmp->list);
				moved = true;
				break;
			}
		}
		/* last. */
		if (!moved) {
			list_move_tail(&item->list, dst);
		}
	}

#if 0
	/* print */
	list_for_each_entry(item, list1, list) {
		LOGn("%"PRIu64"\n", item->key);
	}
#endif
}

static void test_lsort(unsigned int n_test, unsigned int n_items)
{
	unsigned int i;
	struct list_head list0, list1;
	struct timespec ts_bgn, ts_end, ts_time, ts_time1, ts_time2;

	INIT_LIST_HEAD(&list0);
	INIT_LIST_HEAD(&list1);

	/* prepare */
	if (!create_item_list(n_items, &list0)) {
		goto fin;
	}

	/* warm up */
	for (i = 0; i < n_test; i++) {
		fill_item_list_randomly(&list0);
		move_item_list_all(&list0, &list1);
	}

	/* baseline */
	getnstimeofday(&ts_bgn);
	for (i = 0; i < n_test; i++) {
		fill_item_list_randomly(&list0);
		move_item_list_all(&list0, &list1);
	}
	getnstimeofday(&ts_end);
	ts_time = timespec_sub(ts_end, ts_bgn);
	LOGn("%ld.%09ld seconds\n", ts_time.tv_sec, ts_time.tv_nsec);
	ts_time1 = ts_time;

	/* target sort. */
	getnstimeofday(&ts_bgn);
	for (i = 0; i < n_test; i++) {
		fill_item_list_randomly(&list0);
		insertion_sort(&list1, &list0);
		move_item_list_all(&list0, &list1);
	}
	getnstimeofday(&ts_end);
	ts_time = timespec_sub(ts_end, ts_bgn);
	LOGn("%ld.%09ld seconds\n", ts_time.tv_sec, ts_time.tv_nsec);
	ts_time2 = ts_time;

	ts_time = timespec_sub(ts_time2, ts_time1);
	LOGn("%ld.%09ld seconds\n", ts_time.tv_sec, ts_time.tv_nsec);

fin:
	destroy_item_list(&list0);
	destroy_item_list(&list1);
}

/*******************************************************************************
 * With treemap.
 *******************************************************************************/

static struct treemap_memory_manager mmgr_;

static void sort_by_mmap(
	struct list_head *dst, struct list_head *src, gfp_t gfp_mask)
{
	struct multimap *mmap;
	struct l_item *item, *item_next;
	int ret;
	struct multimap_cursor cur;

	ASSERT(dst);
	ASSERT(src);
	ASSERT(list_empty(dst));

	/* Prepare a multimap. */
	mmap = multimap_create(gfp_mask, &mmgr_);
	if (!mmap) { goto error0; }

	/* Insert. */
	list_for_each_entry_safe(item, item_next, src, list) {
		list_del(&item->list);
		ret = multimap_add(mmap, item->key, (unsigned long)item, gfp_mask);
		if (ret) { goto error1; }
	}
	ASSERT(list_empty(src));

	/* Traverse and delete. */
	multimap_cursor_init(mmap, &cur);
	ret = multimap_cursor_begin(&cur);
	ASSERT(ret);
	ret = multimap_cursor_next(&cur);
	while (ret) {
		item = (struct l_item *)multimap_cursor_val(&cur);
		list_add_tail(&item->list, dst);

		ret = multimap_cursor_del(&cur);
		ASSERT(ret);
		ret = multimap_cursor_is_data(&cur);
	}

	/* Destroy the mulitmap. */
	ASSERT(multimap_is_empty(mmap));
	multimap_destroy(mmap);
	return;

error1:
	multimap_destroy(mmap);
error0:
	LOGe("Error.\n");
	return;
}

static void test_tsort(unsigned int n_test, unsigned int n_items)
{
	unsigned int i;
	struct list_head list0, list1;
	struct timespec ts_bgn, ts_end, ts_time, ts_time1, ts_time2;

	INIT_LIST_HEAD(&list0);
	INIT_LIST_HEAD(&list1);

	/* prepare */
	if (!create_item_list(n_items, &list0)) {
		goto fin;
	}

	/* warm up */
	for (i = 0; i < n_test; i++) {
		fill_item_list_randomly(&list0);
		move_item_list_all(&list0, &list1);
	}

	/* baseline */
	getnstimeofday(&ts_bgn);
	for (i = 0; i < n_test; i++) {
		fill_item_list_randomly(&list0);
		move_item_list_all(&list0, &list1);
	}
	getnstimeofday(&ts_end);
	ts_time = timespec_sub(ts_end, ts_bgn);
	LOGn("%ld.%09ld seconds\n", ts_time.tv_sec, ts_time.tv_nsec);
	ts_time1 = ts_time;

	/* target sort. */
	getnstimeofday(&ts_bgn);
	for (i = 0; i < n_test; i++) {
		fill_item_list_randomly(&list0);
		sort_by_mmap(&list1, &list0, GFP_KERNEL);
		move_item_list_all(&list0, &list1);
	}
	getnstimeofday(&ts_end);
	ts_time = timespec_sub(ts_end, ts_bgn);
	LOGn("%ld.%09ld seconds\n", ts_time.tv_sec, ts_time.tv_nsec);
	ts_time2 = ts_time;

	ts_time = timespec_sub(ts_time2, ts_time1);
	LOGn("%ld.%09ld seconds\n", ts_time.tv_sec, ts_time.tv_nsec);

fin:
	destroy_item_list(&list0);
	destroy_item_list(&list1);
}

/*******************************************************************************
 * init/exit.
 *******************************************************************************/

static int __init test_init(void)
{
	initialize_treemap_memory_manager_kmalloc(&mmgr_, 128);

	test_hsort(n_test_);
	test_lsort(n_test_, n_items_);
	test_tsort(n_test_, n_items_);

	finalize_treemap_memory_manager(&mmgr_);
	return -1;
}

static void test_exit(void)
{
}

module_init(test_init);
module_exit(test_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Test sort module");
MODULE_ALIAS("test_sort");
