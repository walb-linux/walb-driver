/**
 * test_treemap.c - test_treemap module.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>
#include <linux/random.h>

#include "linux/walb/walb.h"
#include "linux/walb/logger.h"
#include "linux/walb/check.h"
#include "treemap.h"

/**
 * Test treemap for debug.
 *
 * @return 0 in success, or -1.
 */
int map_test(void)
{
	struct map *tmap;
	int i, n, count;
	u64 key;
	unsigned long val;
	struct treemap_memory_manager mmgr;
	bool ret;

	LOGd("map_test begin\n");
	LOGd("tree_map: %zu\n"
		"tree_node: %zu\n",
		sizeof(struct map),
		sizeof(struct tree_node));

	/* Initialize memory manager. */
	ret = initialize_treemap_memory_manager_kmalloc(&mmgr, 1);
	CHECKd(ret);

	/* Create. */
	tmap = map_create(GFP_KERNEL, &mmgr);

	n = map_n_items(tmap);
	CHECKd(n == 0);
	CHECKd(map_is_empty(tmap));

	/* Search in empty tree. */
	CHECKd(map_lookup(tmap, 0) == TREEMAP_INVALID_VAL);

	/* Returns error if val is TREEMAP_INVALID_VAL. */
	CHECKd(map_add(tmap, 0, TREEMAP_INVALID_VAL, GFP_KERNEL) == -EINVAL);

	/* Insert records. */
	for (i = 0; i < 10000; i++) {
		key = (u64)i;
		/* Succeed. */
		CHECKd(map_add(tmap, key, key + i, GFP_KERNEL) == 0);
		/* Fail due to key exists. */
		CHECKd(map_add(tmap, key, key + i, GFP_KERNEL) == -EEXIST);
	}
	n = map_n_items(tmap);
	CHECKd(n == 10000);
	CHECKd(!map_is_empty(tmap));

	/* Delete records. */
	for (i = 0; i < 10000; i++) {
		key = (u64)i;

		if (i % 2 == 0) {
			val = map_del(tmap, key);
		} else {
			val = map_lookup(tmap, key);
		}
		CHECKd(val != TREEMAP_INVALID_VAL);
		CHECKd(val == key + i);
		if (i % 2 == 0) {
			val = map_lookup(tmap, key);
			CHECKd(val == TREEMAP_INVALID_VAL);
		}
	}
	n = map_n_items(tmap);
	CHECKd(n == 5000);

	/* Make tree map empty. */
	map_empty(tmap);
	n = map_n_items(tmap);
	CHECKd(n == 0);
	CHECKd(map_is_empty(tmap));

	/* 2cd empty. */
	map_empty(tmap);
	n = map_n_items(tmap);
	CHECKd(n == 0);
	CHECKd(map_is_empty(tmap));

	/* Random insert. */
	count = 0;
	for (i = 0; i < 10000; i++) {
		key = get_random_u32() % 10000;
		if(map_add(tmap, key, key + i, GFP_KERNEL) == 0) {
			count++;
		}
	}
	n = map_n_items(tmap);
	CHECKd(n == count);

	/* Empty and destroy. */
	map_destroy(tmap);
	finalize_treemap_memory_manager(&mmgr);

	LOGd("map_test end\n");
	return 0;

error:
	return -1;
}

/**
 * Test map cursor for debug.
 *
 * @return 0 in success, or -1.
 */
int map_cursor_test(void)
{
	struct map *map;
	struct map_cursor curt;
	struct map_cursor *cur;
	struct treemap_memory_manager mmgr;
	bool ret;

	LOGd("map_cursor_test begin.\n");

	/* Initialize memory manager. */
	ret = initialize_treemap_memory_manager_kmalloc(&mmgr, 1);
	CHECKd(ret);

	/* Create map. */
	LOGd("Create map.\n");
	map = map_create(GFP_KERNEL, &mmgr);
	CHECKd(map);

	/* Create and init cursor. */
	LOGd("Create and init cursor.\n");
	cur = map_cursor_create(map, GFP_KERNEL);
	map_cursor_init(map, &curt);

	/* Begin -> end. */
	LOGd("Begin -> end.\n");
	map_cursor_begin(&curt);
	CHECKd(map_cursor_is_valid(&curt));
	CHECKd(!map_cursor_next(&curt));
	CHECKd(map_cursor_is_end(&curt));
	CHECKd(map_cursor_is_valid(&curt));

	/* End -> begin. */
	LOGd("End -> begin.\n");
	map_cursor_end(&curt);
	CHECKd(map_cursor_is_valid(&curt));
	CHECKd(!map_cursor_prev(&curt));
	CHECKd(map_cursor_is_begin(&curt));
	CHECKd(map_cursor_is_valid(&curt));

	/* Prepare map data. */
	LOGd("Prepare map data.\n");
	map_add(map, 10, 10, GFP_KERNEL);
	map_add(map, 20, 20, GFP_KERNEL);
	map_add(map, 30, 30, GFP_KERNEL);
	map_add(map, 40, 40, GFP_KERNEL);

	/* Begin to end. */
	LOGd("Begin to end.\n");
	map_cursor_search(cur, 0, MAP_SEARCH_BEGIN);
	CHECKd(map_cursor_is_valid(cur));
	CHECKd(map_cursor_val(cur) == TREEMAP_INVALID_VAL);
	CHECKd(map_cursor_next(cur));
	CHECKd(map_cursor_val(cur) == 10);
	CHECKd(map_cursor_next(cur));
	CHECKd(map_cursor_val(cur) == 20);
	CHECKd(map_cursor_next(cur));
	CHECKd(map_cursor_val(cur) == 30);
	CHECKd(map_cursor_next(cur));
	CHECKd(map_cursor_val(cur) == 40);
	CHECKd(!map_cursor_next(cur));
	CHECKd(map_cursor_is_end(cur));

	/* End to begin. */
	LOGd("End to begin.\n");
	map_cursor_search(cur, 0, MAP_SEARCH_END);
	CHECKd(map_cursor_is_valid(cur));
	CHECKd(map_cursor_val(cur) == TREEMAP_INVALID_VAL);
	CHECKd(map_cursor_prev(cur));
	CHECKd(map_cursor_val(cur) == 40);
	CHECKd(map_cursor_prev(cur));
	CHECKd(map_cursor_val(cur) == 30);
	CHECKd(map_cursor_prev(cur));
	CHECKd(map_cursor_val(cur) == 20);
	CHECKd(map_cursor_prev(cur));
	CHECKd(map_cursor_val(cur) == 10);
	CHECKd(!map_cursor_prev(cur));
	CHECKd(map_cursor_is_begin(cur));

	/* EQ */
	LOGd("EQ test.\n");
	map_cursor_search(cur, 20, MAP_SEARCH_EQ);
	CHECKd(map_cursor_val(cur) == 20);
	map_cursor_search(cur, 25, MAP_SEARCH_EQ);
	CHECKd(!map_cursor_is_valid(cur));
	CHECKd(map_cursor_val(cur) == TREEMAP_INVALID_VAL);

	/* LE */
	LOGd("LE test.\n");
	map_cursor_search(cur, 20, MAP_SEARCH_LE);
	CHECKd(map_cursor_val(cur) == 20);
	map_cursor_search(cur, 25, MAP_SEARCH_LE);
	CHECKd(map_cursor_val(cur) == 20);
	map_cursor_search(cur, 10, MAP_SEARCH_LE);
	CHECKd(map_cursor_val(cur) == 10);
	map_cursor_search(cur, 5, MAP_SEARCH_LE);
	CHECKd(map_cursor_val(cur) == TREEMAP_INVALID_VAL);

	/* LT */
	LOGd("LT test.\n");
	map_cursor_search(cur, 20, MAP_SEARCH_LT);
	CHECKd(map_cursor_val(cur) == 10);
	map_cursor_search(cur, 25, MAP_SEARCH_LT);
	CHECKd(map_cursor_val(cur) == 20);
	map_cursor_search(cur, 10, MAP_SEARCH_LT);
	CHECKd(map_cursor_val(cur) == TREEMAP_INVALID_VAL);

	/* GE */
	LOGd("GE test.\n");
	map_cursor_search(cur, 20, MAP_SEARCH_GE);
	CHECKd(map_cursor_val(cur) == 20);
	map_cursor_search(cur, 25, MAP_SEARCH_GE);
	CHECKd(map_cursor_val(cur) == 30);
	map_cursor_search(cur, 40, MAP_SEARCH_GE);
	CHECKd(map_cursor_val(cur) == 40);
	map_cursor_search(cur, 45, MAP_SEARCH_GE);
	CHECKd(map_cursor_val(cur) == TREEMAP_INVALID_VAL);

	/* GT */
	LOGd("GT test.\n");
	map_cursor_search(cur, 20, MAP_SEARCH_GT);
	CHECKd(map_cursor_val(cur) == 30);
	map_cursor_search(cur, 25, MAP_SEARCH_GT);
	CHECKd(map_cursor_val(cur) == 30);
	map_cursor_search(cur, 40, MAP_SEARCH_GT);
	CHECKd(map_cursor_val(cur) == TREEMAP_INVALID_VAL);

	/* Destroy cursor. */
	LOGd("Destroy cursor.\n");
	map_cursor_destroy(cur);

	/* Destroy map. */
	LOGd("Destroy map.\n");
	map_destroy(map);

	/* Create map. */
	LOGd("Create map.\n");
	map = map_create(GFP_KERNEL, &mmgr);
	CHECKd(map);

	/* Prepare map data. */
	map_add(map, 10, 10, GFP_KERNEL);
	map_add(map, 20, 20, GFP_KERNEL);
	map_add(map, 30, 30, GFP_KERNEL);
	map_add(map, 40, 40, GFP_KERNEL);

	/* Map delete continuously. */
	map_cursor_search(&curt, 10, MAP_SEARCH_EQ);
	CHECKd(map_cursor_val(&curt) == 10);
	map_cursor_del(&curt);
	CHECKd(map_cursor_val(&curt) == 20);
	map_cursor_del(&curt);
	CHECKd(map_cursor_val(&curt) == 30);
	map_cursor_del(&curt);
	CHECKd(map_cursor_val(&curt) == 40);
	map_cursor_del(&curt);
	CHECKd(map_cursor_is_end(&curt));

	/* Prepare map data. */
	map_add(map, 10, 10, GFP_KERNEL);
	map_add(map, 20, 20, GFP_KERNEL);
	map_add(map, 30, 30, GFP_KERNEL);
	map_add(map, 40, 40, GFP_KERNEL);

	/* Delete middle and check. */
	map_cursor_search(&curt, 20, MAP_SEARCH_EQ);
	CHECKd(map_cursor_val(&curt) == 20);
	map_cursor_del(&curt);
	CHECKd(map_cursor_val(&curt) == 30);
	map_cursor_prev(&curt);
	CHECKd(map_cursor_val(&curt) == 10);

	/* Delete last and check. */
	map_cursor_search(&curt, 40, MAP_SEARCH_EQ);
	CHECKd(map_cursor_val(&curt) == 40);
	map_cursor_del(&curt);
	CHECKd(map_cursor_is_end(&curt));
	map_cursor_prev(&curt);
	CHECKd(map_cursor_val(&curt) == 30);

	/* Delete first and check. */
	map_cursor_search(&curt, 10, MAP_SEARCH_EQ);
	CHECKd(map_cursor_val(&curt) == 10);
	map_cursor_del(&curt);
	CHECKd(map_cursor_val(&curt) == 30);
	map_cursor_prev(&curt);
	CHECKd(map_cursor_is_begin(&curt));

	/* Destroy map. */
	LOGd("Destroy map.\n");
	map_destroy(map);

	/* Finalize memory manager. */
	finalize_treemap_memory_manager(&mmgr);

	LOGd("map_cursor_test end.\n");
	return 0;

error:
	return -1;
}

/**
 * Test multimap for debug.
 *
 * @return 0 in success, or -1.
 */
int multimap_test(void)
{
	struct multimap *tm;
	int i, n, count;
	u64 key;
	unsigned long val;
	struct tree_cell_head *chead;
	struct tree_cell *cell;
	struct treemap_memory_manager mmgr;
	bool ret;

	LOGd("multimap_test begin\n");
	LOGd("hlist_head: %zu "
		"unsigned long: %zu "
		"tree_cell_head: %zu "
		"tree_cell: %zu\n",
		sizeof(struct hlist_head),
		sizeof(unsigned long),
		sizeof(struct tree_cell_head),
		sizeof(struct tree_cell));

	/* Initialize memory manager. */
	ret = initialize_treemap_memory_manager_kmalloc(&mmgr, 1);
	CHECKd(ret);

	/* Create. */
	LOGd("Create.\n");
	tm = multimap_create(GFP_KERNEL, &mmgr);
	ASSERT(tm);

	n = multimap_n_items(tm);
	CHECKd(n == 0);
	CHECKd(multimap_is_empty(tm));

	/* Search in empty tree. */
	LOGd("Search in empty tree.\n");
	CHECKd(!multimap_lookup(tm, 0));

	/* Returns error if val is TREEMAP_INVALID_VAL. */
	LOGd("Invalid value insert..\n");
	CHECKd(multimap_add(tm, 0, TREEMAP_INVALID_VAL, GFP_KERNEL) == -EINVAL);

	/* Insert records. */
	LOGd("Insert records.\n");
	for (i = 0; i < 10000; i++) {
		key = (u64) i;
		/* Succeed. */
		CHECKd(multimap_add(tm, key, key + i, GFP_KERNEL) == 0);
		/* Fail due to key exists. */
		CHECKd(multimap_add(tm, key, key + i, GFP_KERNEL) == -EEXIST);
		/* Succeed due to value is different. */
		CHECKd(multimap_add(tm, key, key + i + 1, GFP_KERNEL) == 0);
	}
	n = multimap_n_items(tm);
	CHECKd(n == 20000);
	CHECKd(!multimap_is_empty(tm));

	/* Delete records. */
	LOGd("Delete records.\n");
	for (i = 0; i < 10000; i++) {
		key = (u64) i;

		n = multimap_lookup_n(tm, key);
		CHECKd(n == 2);

		if (i % 2 == 0) {
			val = multimap_del(tm, key, key + i);
			CHECKd(val != TREEMAP_INVALID_VAL);
			CHECKd(val == key + i);
		} else {
			chead = multimap_lookup(tm, key);
			ASSERT(chead);
			ASSERT(chead->key == key);
			hlist_for_each_entry(cell, &chead->head, list) {
				ASSERT_TREECELL(cell);
				val = cell->val;
				CHECKd(val == key + i || val == key + i + 1);
			}
		}
		if (i % 2 == 0) {
			val = multimap_lookup_any(tm, key);
			CHECKd(val == key + i + 1);

			chead = multimap_lookup(tm, key);
			ASSERT(chead);
			ASSERT(chead->key == key);
			hlist_for_each_entry(cell, &chead->head, list) {
				ASSERT_TREECELL(cell);
				val = cell->val;
				CHECKd(val == key + i + 1);
			}
			n = multimap_lookup_n(tm, key);
			CHECKd(n == 1);
		} else {
			val = multimap_lookup_any(tm, key);
			CHECKd(val == key + i || val == key + i + 1);
			n = multimap_lookup_n(tm, key);
			CHECKd(n == 2);
		}
	}
	n = multimap_n_items(tm);
	CHECKd(n == 15000);

	/* Delete multiple records. */
	LOGd("Delete multiple records.\n");
	for (i = 0; i < 10000; i++) {
		key = (u64) i;
		if (i % 2 != 0) {
			n = multimap_del_key(tm, key);
			CHECKd(n == 2);
		}
	}
	n = multimap_n_items(tm);
	CHECKd(n == 5000);

	/* Make tree map empty. */
	LOGd("Make tree map empty.\n");
	multimap_empty(tm);
	n = multimap_n_items(tm);
	CHECKd(n == 0);
	CHECKd(multimap_is_empty(tm));

	/* 2cd empty. */
	LOGd("2nd empty.\n");
	multimap_empty(tm);
	n = multimap_n_items(tm);
	CHECKd(n == 0);
	CHECKd(multimap_is_empty(tm));

	/* Random insert. */
	LOGd("Random insert.\n");
	count = 0;
	for (i = 0; i < 10000; i++) {
		key = get_random_u32() % 1000;
		val = get_random_u32() % 10;
		if (multimap_add(tm, key, val, GFP_KERNEL) == 0) {
			count++;
		}
	}
	n = multimap_n_items(tm);
	CHECKd(n == count);
	LOGn("count %d\n", n);

	/* Empty and destroy. */
	LOGd("Empty and destroy.\n");
	multimap_destroy(tm);

	/* Finalize memory manager. */
	finalize_treemap_memory_manager(&mmgr);

	LOGd("multimap_test end\n");
	return 0;

error:
	return -1;
}

/**
 * Test multimap cursor for debug.
 *
 * @return 0 in success, or -1.
 */
int __init multimap_cursor_test(void)
{
	struct multimap *map;
	struct multimap_cursor curt;
	u64 key, keys[10];
	unsigned long val, vals[10];
	int i;
	struct treemap_memory_manager mmgr;
	bool ret;

	LOGd("multimap_cursor_test begin.\n");

	/* Initialize memory manager. */
	ret = initialize_treemap_memory_manager_kmalloc(&mmgr, 1);
	CHECKd(ret);

	/* Create multimap. */
	LOGd("Create multimap.\n");
	map = multimap_create(GFP_KERNEL, &mmgr);
	CHECKd(map);

	/* Initialize multimap cursor. */
	multimap_cursor_init(map, &curt);

	/* Begin -> end. */
	LOGd("Begin -> end.\n");
	multimap_cursor_begin(&curt);
	CHECKd(multimap_cursor_is_valid(&curt));
	CHECKd(multimap_cursor_is_begin(&curt));
	CHECKd(!multimap_cursor_next(&curt));
	CHECKd(multimap_cursor_is_end(&curt));
	CHECKd(multimap_cursor_is_valid(&curt));

	/* End -> begin. */
	LOGd("End -> begin.\n");
	multimap_cursor_end(&curt);
	CHECKd(multimap_cursor_is_valid(&curt));
	CHECKd(multimap_cursor_is_end(&curt));
	CHECKd(!multimap_cursor_prev(&curt));
	CHECKd(multimap_cursor_is_begin(&curt));
	CHECKd(multimap_cursor_is_valid(&curt));

	/* Prepare multimap data. */
	LOGd("Prepare multimap data.\n");
	multimap_add(map, 10, 10, GFP_KERNEL);
	multimap_add(map, 10, 11, GFP_KERNEL);
	multimap_add(map, 10, 12, GFP_KERNEL);
	multimap_add(map, 10, 13, GFP_KERNEL);
	multimap_add(map, 10, 14, GFP_KERNEL);
	multimap_add(map, 20, 20, GFP_KERNEL);
	multimap_add(map, 30, 30, GFP_KERNEL);
	multimap_add(map, 30, 31, GFP_KERNEL);
	multimap_add(map, 30, 32, GFP_KERNEL);
	multimap_add(map, 30, 33, GFP_KERNEL);

	/* Begin to end. */
	LOGd("Begin to end.\n");
	multimap_cursor_search(&curt, 0, MAP_SEARCH_BEGIN, 0);
	CHECKd(multimap_cursor_is_valid(&curt));
	CHECKd(multimap_cursor_is_begin(&curt));
	CHECKd(multimap_cursor_val(&curt) == TREEMAP_INVALID_VAL);
	for (i = 0; i < 10; i++) {
		CHECKd(multimap_cursor_next(&curt));
		key = multimap_cursor_key(&curt);
		val = multimap_cursor_val(&curt);
		LOGd("key, val: %"PRIu64", %lu\n", key, val);
		keys[i] = key;
		vals[i] = val;
		CHECKd(key != (u64)(-1));
		CHECKd(val != TREEMAP_INVALID_VAL);
	}
	CHECKd(!multimap_cursor_next(&curt));
	CHECKd(multimap_cursor_is_end(&curt));
	CHECKd(multimap_cursor_val(&curt) == TREEMAP_INVALID_VAL);

	/* End to begin. */
	LOGd("End to begin.\n");
	multimap_cursor_search(&curt, 0, MAP_SEARCH_END, 0);
	CHECKd(multimap_cursor_is_valid(&curt));
	CHECKd(multimap_cursor_is_end(&curt));
	CHECKd(multimap_cursor_val(&curt) == TREEMAP_INVALID_VAL);
	for (i = 10 - 1; i >= 0; i--) {
		CHECKd(multimap_cursor_prev(&curt));
		key = multimap_cursor_key(&curt);
		val = multimap_cursor_val(&curt);
		LOGd("key, val: %"PRIu64", %lu\n", key, val);
		CHECKd(key != (u64)(-1));
		CHECKd(key == keys[i]);
		CHECKd(val != TREEMAP_INVALID_VAL);
		CHECKd(val == vals[i]);
	}
	CHECKd(!multimap_cursor_prev(&curt));
	CHECKd(multimap_cursor_is_begin(&curt));
	CHECKd(multimap_cursor_val(&curt) == TREEMAP_INVALID_VAL);

	/* Forward scan. */
	multimap_cursor_search(&curt, 30, MAP_SEARCH_EQ, 0);
	CHECKd(multimap_cursor_key(&curt) == keys[6]);
	CHECKd(multimap_cursor_val(&curt) == vals[6]);

	/* Backword scan. */
	multimap_cursor_search(&curt, 10, MAP_SEARCH_EQ, 1);
	CHECKd(multimap_cursor_key(&curt) == keys[4]);
	CHECKd(multimap_cursor_val(&curt) == vals[4]);

	/* Destroy multimap. */
	LOGd("Destroy multimap.\n");
	multimap_destroy(map);

	/* Create multimap. */
	LOGd("Create multimap.\n");
	map = multimap_create(GFP_KERNEL, &mmgr);
	CHECKd(map);

	/*
	 * Cursor deletion test.
	 */
	LOGn("multimap cursor delete test 1.\n");
	multimap_add(map, 10, 12, GFP_KERNEL);
	multimap_add(map, 10, 11, GFP_KERNEL);
	multimap_add(map, 10, 10, GFP_KERNEL);
	/* the order is (10,10), (10,11), (10,12) inside the hlist. */

	multimap_cursor_search(&curt, 10, MAP_SEARCH_EQ, 0);
	CHECKd(multimap_cursor_is_valid(&curt));
	LOGn("(%"PRIu64", %lu)\n", multimap_cursor_key(&curt), multimap_cursor_val(&curt));
	CHECKd(multimap_cursor_key(&curt) == 10);
	CHECKd(multimap_cursor_val(&curt) == 10);
	multimap_cursor_del(&curt);
	CHECKd(multimap_cursor_is_valid(&curt));
	LOGn("(%"PRIu64", %lu)\n", multimap_cursor_key(&curt), multimap_cursor_val(&curt));
	CHECKd(multimap_cursor_key(&curt) == 10);
	CHECKd(multimap_cursor_val(&curt) == 11);
	multimap_cursor_prev(&curt);
	CHECKd(multimap_cursor_is_begin(&curt));

	multimap_cursor_search(&curt, 10, MAP_SEARCH_EQ, 1);
	CHECKd(multimap_cursor_is_valid(&curt));
	LOGn("(%"PRIu64", %lu)\n", multimap_cursor_key(&curt), multimap_cursor_val(&curt));
	CHECKd(multimap_cursor_key(&curt) == 10);
	CHECKd(multimap_cursor_val(&curt) == 12);
	multimap_cursor_del(&curt);
	CHECKd(multimap_cursor_is_end(&curt));
	multimap_cursor_prev(&curt);
	CHECKd(multimap_cursor_is_valid(&curt));
	LOGn("(%"PRIu64", %lu)\n", multimap_cursor_key(&curt), multimap_cursor_val(&curt));
	CHECKd(multimap_cursor_key(&curt) == 10);
	CHECKd(multimap_cursor_val(&curt) == 11);
	multimap_cursor_del(&curt);

	CHECKd(multimap_is_empty(map));
	LOGn("multimap cursor delete test 2.\n");

	multimap_add(map,  0,  0, GFP_KERNEL);
	multimap_add(map, 10, 12, GFP_KERNEL);
	multimap_add(map, 10, 11, GFP_KERNEL);
	multimap_add(map, 10, 10, GFP_KERNEL);
	multimap_add(map, 20, 20, GFP_KERNEL);

	multimap_cursor_search(&curt, 10, MAP_SEARCH_EQ, 0);
	CHECKd(multimap_cursor_is_valid(&curt));
	LOGn("(%"PRIu64", %lu)\n", multimap_cursor_key(&curt), multimap_cursor_val(&curt));
	CHECKd(multimap_cursor_key(&curt) == 10);
	CHECKd(multimap_cursor_val(&curt) == 10);
	multimap_cursor_del(&curt);
	CHECKd(multimap_cursor_is_valid(&curt));
	LOGn("(%"PRIu64", %lu)\n", multimap_cursor_key(&curt), multimap_cursor_val(&curt));
	CHECKd(multimap_cursor_key(&curt) == 10);
	CHECKd(multimap_cursor_val(&curt) == 11);
	multimap_cursor_prev(&curt);
	CHECKd(multimap_cursor_is_valid(&curt));
	LOGn("(%"PRIu64", %lu)\n", multimap_cursor_key(&curt), multimap_cursor_val(&curt));
	CHECKd(multimap_cursor_key(&curt) == 0);
	CHECKd(multimap_cursor_val(&curt) == 0);

	multimap_cursor_search(&curt, 10, MAP_SEARCH_EQ, 1);
	CHECKd(multimap_cursor_is_valid(&curt));
	LOGn("(%"PRIu64", %lu)\n", multimap_cursor_key(&curt), multimap_cursor_val(&curt));
	CHECKd(multimap_cursor_key(&curt) == 10);
	CHECKd(multimap_cursor_val(&curt) == 12);
	multimap_cursor_del(&curt);
	CHECKd(multimap_cursor_is_valid(&curt));
	LOGn("(%"PRIu64", %lu)\n", multimap_cursor_key(&curt), multimap_cursor_val(&curt));
	CHECKd(multimap_cursor_key(&curt) == 20);
	CHECKd(multimap_cursor_val(&curt) == 20);
	multimap_cursor_prev(&curt);
	CHECKd(multimap_cursor_is_valid(&curt));
	LOGn("(%"PRIu64", %lu)\n", multimap_cursor_key(&curt), multimap_cursor_val(&curt));
	CHECKd(multimap_cursor_key(&curt) == 10);
	CHECKd(multimap_cursor_val(&curt) == 11);


	/* Destroy multimap. */
	LOGd("Destroy multimap.\n");
	multimap_destroy(map);

	/* Finalize memory manager. */
	finalize_treemap_memory_manager(&mmgr);

	LOGd("multimap_cursor_test end.\n");

	return 0;
error:
	return -1;
}

struct treemap_memory_manager mmgr_;

static bool initialize(void)
{
	bool ret;
	ret = initialize_treemap_memory_manager(
		&mmgr_, 1,
		"test_node_cache",
		"test_cell_head_cache",
		"test_cell_cache");
	return ret;
}

static void finalize(void)
{
	finalize_treemap_memory_manager(&mmgr_);
}

static int __init test_treemap_init(void)
{
	printk(KERN_INFO "test_treemap_init begin\n");

	if (!initialize()) {
		printk(KERN_ERR "initialize() failed.\n");
		goto error;
	}

	/* Treemap test for debug. */
	if (map_test()) {
		printk(KERN_ERR "map_test() failed.\n");
		goto error;
	}
	if (map_cursor_test()) {
		printk(KERN_ERR "map_cursor_test() failed.\n");
		goto error;
	}
	if (multimap_test()) {
		printk(KERN_ERR "multimap_test() failed.\n");
		goto error;
	}
	if (multimap_cursor_test()) {
		printk(KERN_ERR "multimap_cursor_test() failed.\n");
		goto error;
	}

	finalize();
	printk(KERN_INFO "test_treemap_init end\n");

error:
	return -1;
}

static void test_treemap_exit(void)
{
}


module_init(test_treemap_init);
module_exit(test_treemap_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Test treemap module");
MODULE_ALIAS("test_treemap");
