/**
 * hashtbl.c - Hash table implementation.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */

#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/slab.h>
#include <linux/list.h>
#include <linux/hash.h>

#include "linux/walb/walb.h"
#include "linux/walb/logger.h"
#include "linux/walb/check.h"
#include "linux/walb/util.h"
#include "hashtbl.h"
#include "util.h" /* for debug */


/**
 * Prototypes of static functions.
 */
static unsigned int get_n_bits(u32 val);
static struct hash_cell* hashtbl_lookup_cell(
	const struct hash_tbl *htbl,
	const u8* key, unsigned int key_size);
static u32 hashtbl_get_index(
	const struct hash_tbl *htbl,
	const u8* key, unsigned int key_size);

static struct hash_cell* alloc_hash_cell(
	unsigned int key_size, gfp_t gfp_mask);
static void free_hash_cell(struct hash_cell *cell);
static void set_hash_cell_key(struct hash_cell *cell, const u8 *key);
static void set_hash_cell_val(struct hash_cell *cell, unsigned long val);
static int get_hash_cell_key_size(const struct hash_cell *cell);
static u8* get_hash_cell_key(const struct hash_cell *cell);
static unsigned long get_hash_cell_val(const struct hash_cell *cell);

UNUSED
static int is_hashtbl_struct_valid(const struct hash_tbl *htbl);
UNUSED
static int is_hashcell_struct_valid(const struct hash_cell *hcell);
UNUSED
static int is_hashtbl_cursor_struct_valid(const hashtbl_cursor_t *cursor);

#define ASSERT_HASHTBL(htbl) ASSERT(is_hashtbl_struct_valid(htbl))
#define ASSERT_HASHCELL(hcell) ASSERT(is_hashcell_struct_valid(hcell))
#define ASSERT_HASHTBL_CURSOR(cursor)			\
	ASSERT(is_hashtbl_cursor_struct_valid(cursor))

UNUSED
static void print_hashtbl_cursor(const hashtbl_cursor_t *cursor);

/*******************************************************************************
 * Static functions.
 *******************************************************************************/

/**
 * Get number of required bits to store val.
 * @val: Value to check.
 *
 * RETURNS:
 * Number of required bits.
 *
 * EXAMPLES:
 * 00100000B needs 6 bits.
 * 00011111B needs 5 bits.
 */
static unsigned int get_n_bits(u32 val)
{
	int i;
	if (val == 0) {
		return 1;
	}

	for (i = 1; i < 32; i++) {
		if ((val >> i) == 1) {
			return i + 1;
		}
	}
	WARN_ON(1);
	return 0;
}

/**
 * Lookup cell from hashtbl.
 *
 * @htbl hash table.
 * @key pointer to key.
 * @key_size key size.
 *
 * @return hash cell if found, or NULL.
 */
static struct hash_cell* hashtbl_lookup_cell(
	const struct hash_tbl *htbl,
	const u8* key, unsigned int key_size)
{
	u32 idx;
	struct hlist_node *next;
	struct hash_cell *cell;

	ASSERT_HASHTBL(htbl);

	idx = hashtbl_get_index(htbl, key, key_size);

	hlist_for_each_entry_safe(cell, next, &htbl->bucket[idx], list) {

		ASSERT_HASHCELL(cell);
		if (cell->key_size == key_size &&
			memcmp(cell->key, key, key_size) == 0) {
			return cell;
		}
	}
	/* LOGd("hashtbl_lookup_cell end\n"); */
	return NULL;
}

/**
 * Get bucket index of the key.
 *
 * @htbl hash table.
 * @key pointer to key data.
 * @key_size key size.
 *
 * @return index in the bucket.
 */
static u32 hashtbl_get_index(
	const struct hash_tbl *htbl, const u8* key, unsigned int key_size)
{
	u32 idx, hash;

	ASSERT_HASHTBL(htbl);

	hash = fnv1a_hash(key, key_size);
	idx = hash >> (32 - htbl->n_bits);
	ASSERT(idx < htbl->bucket_size);

	/* LOGd("sum %08x idx %u\n", sum, idx); */
	return idx;
}

/**
 * Allocate hash cell.
 */
static struct hash_cell* alloc_hash_cell(
	unsigned int key_size, gfp_t gfp_mask)
{
	struct hash_cell *cell;

	if (key_size <= 0) { goto invalid_key_size; }

	cell = kmalloc(sizeof(struct hash_cell), gfp_mask);
	if (!cell) { goto nomem0; }
	INIT_HLIST_NODE(&cell->list);
	cell->key_size = key_size;

	cell->key = kmalloc(key_size, gfp_mask);
	if (!cell->key) { goto nomem1; }

	return cell;
nomem1:
	kfree(cell);
nomem0:
invalid_key_size:
	return NULL;
}

/**
 * Deallocate hash cell.
 */
static void free_hash_cell(struct hash_cell *cell)
{
	if (!cell) { return; }

	if (cell->key) {
		kfree(cell->key);
	}
	if (cell->list.pprev) {
		hlist_del(&cell->list);
	}
	kfree(cell);
}

/**
 * Set key of a hash cell.
 */
static void set_hash_cell_key(struct hash_cell *cell, const u8 *key)
{
	ASSERT(cell);
	ASSERT(cell->key);
	ASSERT(key);

	memcpy(cell->key, key, cell->key_size);
}

/**
 * Set value of a hash cell.
 */
static void set_hash_cell_val(struct hash_cell *cell, unsigned long val)
{
	ASSERT(cell);
	cell->val = val;
}

/**
 * Get key of a hash cell.
 *
 * @return key in success, or NULL.
 */
static u8* get_hash_cell_key(const struct hash_cell *cell)
{
	if (cell) {
		ASSERT_HASHCELL(cell);
		return cell->key;
	}
	return NULL;
}

/**
 * Get key size of a hash cell.
 *
 * @return key size > 0 in success, or 0.
 */
static int get_hash_cell_key_size(const struct hash_cell *cell)
{
	if (cell) {
		ASSERT_HASHCELL(cell);
		return cell->key_size;
	}
	return 0;
}

/**
 * Get value of a hash cell.
 *
 * @return value if cell is not NULL, or HASHTBL_INVALID_VAL.
 */
static unsigned long get_hash_cell_val(const struct hash_cell *cell)
{
	if (cell) {
		return cell->val;
	}
	return HASHTBL_INVALID_VAL;
}

/**
 * Check validness of struct hash_tbl data.
 *
 * @return Non-zero if valud, or 0.
 */
static int is_hashtbl_struct_valid(const struct hash_tbl *htbl)
{
	return htbl &&
		htbl->bucket &&
		htbl->bucket_size > 0 &&
		htbl->n_bits > 0;
}

/**
 * Check validness of struct hash_cell data.
 *
 * @return Non-zero if valud, or 0.
 */
static int is_hashcell_struct_valid(const struct hash_cell *hcell)
{
	return hcell &&
		hcell->key &&
		hcell->key_size > 0 &&
		hcell->val != HASHTBL_INVALID_VAL;
}

/**
 * Check validness of hashtbl_cursor_t data.
 *
 * @return Non-zero if valud, or 0.
 */
static int is_hashtbl_cursor_struct_valid(const hashtbl_cursor_t *cursor)
{
	int st, idx, max_idx;
	struct hlist_head *chead, *nhead;
	struct hlist_node *cnode, *nnode;

	CHECKd(cursor);
	st = cursor->state;
	idx = cursor->bucket_idx;
	chead = cursor->curr_head;
	cnode = cursor->curr;
	nhead = cursor->next_head;
	nnode = cursor->next;

	CHECKd(is_hashtbl_struct_valid(cursor->htbl));
	max_idx = cursor->htbl->bucket_size;

	CHECKd(0 <= idx);
	CHECKd(idx <= max_idx);

	switch (st) {
	case HASHTBL_CURSOR_BEGIN:
		CHECKd(!chead && !cnode);
		break;
	case HASHTBL_CURSOR_END:
		CHECKd(!chead && !cnode && !nhead && !nnode);
		break;
	case HASHTBL_CURSOR_DATA:
		CHECKd(chead && cnode);
		break;
	case HASHTBL_CURSOR_DELETED:
		CHECKd(!chead && !cnode);
		break;
	case HASHTBL_CURSOR_INVALID:
		break;
	default:
		CHECKd(false);
	}
	return 1;
error:
	return 0;
}

/**
 * Print hashtbl cursor for debug.
 */
static void print_hashtbl_cursor(const hashtbl_cursor_t *cursor)
{
	const char *state_str[6];
	state_str[1] = "BEGIN";
	state_str[2] = "END";
	state_str[3] = "DATA";
	state_str[4] = "DELETED";
	state_str[5] = "INVALID";

	if (!cursor) {
		LOGd("HASHTBL_CURSOR null\n");
		return;
	}
	LOGd("HASHTBL_CURSOR state %s bucket_idx %d\n"
		"curr_head %p curr %p\n"
		"next_head %p next %p\n",
		state_str[cursor->state],
		cursor->bucket_idx,
		cursor->curr_head, cursor->curr,
		cursor->next_head, cursor->next);
}

/*******************************************************************************
 * Functions for hash table manipulation.
 *******************************************************************************/

/**
 * Create hash table.
 */
struct hash_tbl* hashtbl_create(int bucket_size, gfp_t gfp_mask)
{
	int i;
	struct hash_tbl *htbl;

	LOGd("hashtbl_create begin\n");
	ASSERT(bucket_size > 0);

	htbl = kzalloc(sizeof(struct hash_tbl), gfp_mask);
	if (!htbl) { goto error0; }

	htbl->bucket_size = bucket_size;
	htbl->n_bits = get_n_bits((u32)(bucket_size - 1));
	htbl->bucket = kzalloc(sizeof(struct hlist_head) * bucket_size, gfp_mask);
	if (!htbl->bucket) { goto error1; }

	for (i = 0; i < htbl->bucket_size; i++) {
		INIT_HLIST_HEAD(&htbl->bucket[i]);
	}

	ASSERT_HASHTBL(htbl);
	LOGd("hashtbl_create end\n");
	return htbl;

error1:
	kfree(htbl);
error0:
	return NULL;
}

/**
 * Destroy hash talbe.
 */
void hashtbl_destroy(struct hash_tbl *htbl)
{
	LOGd("hashtbl_destroy begin\n");
	hashtbl_empty(htbl);
	kfree(htbl->bucket);
	kfree(htbl);
	LOGd("hashtbl_destroy end\n");
}

/**
 * Delete all cells in the hash table.
 */
void hashtbl_empty(struct hash_tbl *htbl)
{
	int i;

	LOGd("hashtbl_empty begin\n");
	ASSERT_HASHTBL(htbl);

	for (i = 0; i < htbl->bucket_size; i++) {
		struct hlist_node *next;
		struct hash_cell *cell;
		hlist_for_each_entry_safe(cell, next, &htbl->bucket[i], list) {
			ASSERT_HASHCELL(cell);
			free_hash_cell(cell);
		}
		ASSERT(hlist_empty(&htbl->bucket[i]));
	}
	LOGd("hashtbl_empty end\n");
}

/**
 * Put data to hash table.
 *
 * @htbl hash table.
 * @key pointer to key data.
 * @key_size key size > 0.
 * @val value. MUST NOT be HASHTBL_INVALID_VAL.
 * @gfp_mask GFP_*
 *
 * @return 0 in success,
 *	   -EINVAL when parameters are invalid.
 *	   -EPERM when key already exists,
 *	   -ENOMEM in memory allocation failure.
 */
int hashtbl_add(struct hash_tbl *htbl,
		const u8* key, int key_size, unsigned long val, gfp_t gfp_mask)
{
	struct hash_cell *cell;
	u32 idx;

	ASSERT_HASHTBL(htbl);

	/* Validation */
	if (!key || key_size <= 0 || val == HASHTBL_INVALID_VAL) {
		return -EINVAL;
	}

	/* Duplication check */
	if (hashtbl_lookup_cell(htbl, key, key_size)) {
		return -EPERM;
	}

	/* Allocate cell. */
	cell = alloc_hash_cell(key_size, gfp_mask);
	if (!cell) { return -ENOMEM; }

	/* Fill cell. */
	set_hash_cell_key(cell, key);
	set_hash_cell_val(cell, val);

	/* Add to hashtbl. */
	idx = hashtbl_get_index(htbl, key, key_size);
	hlist_add_head(&cell->list, &htbl->bucket[idx]);

	return 0;
}

/**
 * Get data from hash table.
 *
 * @htbl hash table.
 * @key pointer to key data.
 * @key_size key size.
 *
 * @return value if found, or HASHTBL_INVALID_VAL.
 */
unsigned long hashtbl_lookup(const struct hash_tbl *htbl, const u8* key, int key_size)
{
	struct hash_cell *cell;

	cell = hashtbl_lookup_cell(htbl, key, key_size);
	return get_hash_cell_val(cell);
}

/**
 * Delete data from hash table.
 *
 * @htbl hash table.
 * @key_size key size.
 * @key pointer to key data.
 *
 * @return value if found, or HASHTBL_INVALID_VAL.
 */
unsigned long hashtbl_del(struct hash_tbl *htbl, const u8* key, int key_size)
{
	struct hash_cell *cell;
	unsigned long val = HASHTBL_INVALID_VAL;

	cell = hashtbl_lookup_cell(htbl, key, key_size);
	val = get_hash_cell_val(cell);
	free_hash_cell(cell);

	return val;
}

/**
 * Check hash table is empty or not.
 *
 * @htbl hash table to check.
 *
 * @return 1 if the hash table is empty, or 0.
 */
int hashtbl_is_empty(const struct hash_tbl *htbl)
{
	int i;

	ASSERT_HASHTBL(htbl);

	for (i = 0; i < htbl->bucket_size; i++) {
		if (!hlist_empty(&htbl->bucket[i])) {
			return 0;
		}
	}
	return 1;
}

/**
 * Get number of cells in the hashtbl.
 * This is slow, just for test.
 *
 * @htbl hash table.
 *
 * @return number of cells in the hash table.
 */
int hashtbl_n_items(const struct hash_tbl *htbl)
{
	int i;
	int n = 0;
	int n_min = INT_MAX;
	int n_max = 0;

	ASSERT_HASHTBL(htbl);

	for (i = 0; i < htbl->bucket_size; i++) {
		int n_local = 0;
		struct hlist_node *next;
		struct hash_cell *cell;
		hlist_for_each_entry_safe(cell, next, &htbl->bucket[i], list) {
			ASSERT_HASHCELL(cell);
			n_local++;
			n++;
		}
		if (n_local < n_min) { n_min = n_local; }
		if (n_max < n_local) { n_max = n_local; }
	}

	LOGd("n_min %d n_max %d n_avg %d, n_total %d\n",
		n_min, n_max, n / htbl->bucket_size, n);
	return n;
}

/**
 * Test hashtbl for debug.
 *
 * @return 0 in success, or -1.
 */
int hashtbl_test(void)
{
	int i, n;
	struct hash_tbl *htbl;
	char buf[10];
	unsigned long val;

	LOGd("hashtbl_test begin\n");

	LOGd("list_head: %zu\n"
		"hlist_head: %zu\n"
		"hash_tbl: %zu\n"
		"hash_cell: %zu\n"
		"max bucket_size: %ld\n",
		sizeof(struct list_head),
		sizeof(struct hlist_head),
		sizeof(struct hash_tbl),
		sizeof(struct hash_cell),
		HASHTBL_MAX_BUCKET_SIZE);

	/* Create. */
	htbl = hashtbl_create(HASHTBL_MAX_BUCKET_SIZE, GFP_KERNEL);
	CHECKd(htbl);
	n = hashtbl_n_items(htbl);
	CHECKd(n == 0);
	CHECKd(hashtbl_is_empty(htbl));

	/* Insert */
	for (i = 0; i < 100000; i++) {
		snprintf(buf, 10, "abcd%05d", i);
		CHECKd(hashtbl_add(htbl, buf, 9, i, GFP_KERNEL) == 0);
	}
	n = hashtbl_n_items(htbl);
	CHECKd(n == 100000);
	CHECKd(!hashtbl_is_empty(htbl));

	/* Lookup */
	for (i = 0; i < 100000; i++) {
		snprintf(buf, 10, "abcd%05d", i);
		val = hashtbl_lookup(htbl, buf, 9);
		CHECKd(val ==  i);
	}
	n = hashtbl_n_items(htbl);
	CHECKd(n == 100000);
	CHECKd(!hashtbl_is_empty(htbl));

	/* Delete */
	for (i = 0; i < 100000; i++) {
		snprintf(buf, 10, "abcd%05d", i);
		if (i % 2 == 0) {
			val = hashtbl_del(htbl, buf, 9);
		} else {
			val = hashtbl_lookup(htbl, buf, 9);
		}
		CHECKd(val != HASHTBL_INVALID_VAL && val == i);
		if (i % 2 == 0) {
			val = hashtbl_lookup(htbl, buf, 9);
			CHECKd(val == HASHTBL_INVALID_VAL);
		}
	}
	n = hashtbl_n_items(htbl);
	CHECKd(n == 50000);
	CHECKd(!hashtbl_is_empty(htbl));

	/* Empty */
	hashtbl_empty(htbl);
	n = hashtbl_n_items(htbl);
	CHECKd(n == 0);
	CHECKd(hashtbl_is_empty(htbl));

	/* 2nd empty. */
	hashtbl_empty(htbl);
	n = hashtbl_n_items(htbl);
	CHECKd(n == 0);
	CHECKd(hashtbl_is_empty(htbl));

	/* Insert */
	for (i = 0; i < 100; i++) {
		snprintf(buf, 10, "abcd%05d", i);
		CHECKd(hashtbl_add(htbl, buf, 9, i, GFP_KERNEL) == 0);
	}
	n = hashtbl_n_items(htbl);
	CHECKd(n == 100);
	CHECKd(!hashtbl_is_empty(htbl));

	/* Empty and destroy. */
	hashtbl_destroy(htbl);

	LOGd("hashtbl_test end\n");
	return 0;

error:
	return -1;
}

/*******************************************************************************
 * Functions for hash table cursor.
 *******************************************************************************/

/**
 * Initialize hash table cursor.
 */
void hashtbl_cursor_init(struct hash_tbl *htbl, hashtbl_cursor_t *cursor)
{
	ASSERT_HASHTBL(htbl);

	cursor->htbl = htbl;
	cursor->state = HASHTBL_CURSOR_INVALID;

	cursor->bucket_idx = 0;
	cursor->curr_head = NULL;
	cursor->curr = NULL;
	cursor->next_head = NULL;
	cursor->next = NULL;

	ASSERT_HASHTBL_CURSOR(cursor);
}

/**
 * Set cursor begin.
 */
void hashtbl_cursor_begin(hashtbl_cursor_t *cursor)
{
	ASSERT_HASHTBL_CURSOR(cursor);

	cursor->state = HASHTBL_CURSOR_BEGIN;
	cursor->bucket_idx = 0;
	cursor->curr_head = NULL;
	cursor->curr = NULL;
	cursor->next_head = NULL;
	cursor->next = NULL;
}

/**
 * Search bucket index where item exists,
 * starting from a specified index.
 *
 * @htbl hash table.
 * @start_idx start index to search (0 <= start_idx <= htbl->bucket_size).
 *
 * @return found index in success, or htbl->bucket_size.
 */
static int search_next_head_index(const struct hash_tbl *htbl, int start_idx)
{
	int i;
	ASSERT_HASHTBL(htbl);
	ASSERT(0 <= start_idx && start_idx <= htbl->bucket_size);

	for (i = start_idx; i < htbl->bucket_size; i++) {
		if (!hlist_empty(&htbl->bucket[i])) {
			break;
		}
	}

	ASSERT(0 <= i);
	ASSERT(i <= htbl->bucket_size);
	return i;
}

/**
 * Make the cursor goes to next.
 *
 * @return 1 if success, or 0 (the cursor reaches END).
 */
int hashtbl_cursor_next(hashtbl_cursor_t *cursor)
{
	int idx;

	ASSERT_HASHTBL_CURSOR(cursor);

	switch(cursor->state) {

	case HASHTBL_CURSOR_END:
	case HASHTBL_CURSOR_INVALID:
		/* do nothing. */
		return 0;

	case HASHTBL_CURSOR_BEGIN:

		idx = search_next_head_index(cursor->htbl, 0);

		/* Check end. */
		if (idx == cursor->htbl->bucket_size) { goto cursor_end; }

		/* Set curr. */
		cursor->curr_head = &cursor->htbl->bucket[idx];
		ASSERT(cursor->curr_head);
		cursor->curr = cursor->curr_head->first;
		ASSERT(cursor->curr);
		cursor->bucket_idx = idx;
		break;

	case HASHTBL_CURSOR_DATA:
	case HASHTBL_CURSOR_DELETED:

		/* Check end. */
		if (!cursor->next) { goto cursor_end; }

		/* Set curr. */
		cursor->curr_head = cursor->next_head;
		cursor->curr = cursor->next;
		ASSERT(cursor->curr_head);
		ASSERT(cursor->curr);
		break;

	default:
		WARN_ON(1);
	}

	/* Get and set next. */
	cursor->next = cursor->curr->next;
	if (cursor->next) {
		cursor->next_head = cursor->curr_head;
	} else {
		/* Search next head */
		idx = search_next_head_index(cursor->htbl,
					++cursor->bucket_idx);
		cursor->bucket_idx = idx;

		/* Check end of bucket array. */
		if (idx == cursor->htbl->bucket_size) {
			cursor->next_head = NULL;
		} else {
			cursor->next_head = &cursor->htbl->bucket[idx];
			ASSERT(!hlist_empty(cursor->next_head));
			cursor->next = cursor->next_head->first;
			ASSERT(cursor->next);
		}
	}

	cursor->state = HASHTBL_CURSOR_DATA;
	ASSERT_HASHTBL_CURSOR(cursor);
	return 1;

cursor_end:
	cursor->state = HASHTBL_CURSOR_END;
	cursor->curr_head = NULL;
	cursor->curr = NULL;
	cursor->next_head = NULL;
	cursor->next = NULL;
	ASSERT_HASHTBL_CURSOR(cursor);
	return 0;
}

/**
 * Delete the focused item.
 *
 * @return value in success, or HASHTBL_INVALID_VAL.
 */
unsigned long hashtbl_cursor_del(hashtbl_cursor_t *cursor)
{
	struct hash_cell *cell;
	unsigned long val;

	ASSERT_HASHTBL_CURSOR(cursor);

	if (cursor->state != HASHTBL_CURSOR_DATA) {
		return HASHTBL_INVALID_VAL;
	}

	cursor->state = HASHTBL_CURSOR_DELETED;
	ASSERT(cursor->curr);

	cell = hlist_entry(cursor->curr, struct hash_cell, list);
	ASSERT(cell);

	val = get_hash_cell_val(cell);
	free_hash_cell(cell);

	cursor->curr = NULL;
	cursor->curr_head = NULL;

	ASSERT_HASHTBL_CURSOR(cursor);

	return val;
}

/**
 * Check whether the cursor is BEGIN.
 *
 * @return Non-zero if cursor is BEGIN, or 0.
 */
int hashtbl_cursor_is_begin(const hashtbl_cursor_t *cursor)
{
	ASSERT_HASHTBL_CURSOR(cursor);

	return cursor->state == HASHTBL_CURSOR_BEGIN;
}

/**
 * Check whether the cursor is END.
 *
 * @return Non-zero if cursor is END, or 0.
 */
int hashtbl_cursor_is_end(const hashtbl_cursor_t *cursor)
{
	ASSERT_HASHTBL_CURSOR(cursor);

	return cursor->state == HASHTBL_CURSOR_END;
}

/**
 * Check whether the cursor is valid or not.
 *
 * @return Non-zero if cursor is valid, or 0.
 */
int hashtbl_cursor_is_valid(const hashtbl_cursor_t *cursor)
{
	ASSERT_HASHTBL_CURSOR(cursor);

	return cursor &&
		(cursor->state == HASHTBL_CURSOR_BEGIN ||
			cursor->state == HASHTBL_CURSOR_END ||
			cursor->state == HASHTBL_CURSOR_DATA);
}

/**
 * Get value of the focused item.
 *
 * @return get value of the focused item if the state is DATA,
 *   or HASHTBL_INVALID_VAL.
 */
unsigned long hashtbl_cursor_val(const hashtbl_cursor_t *cursor)
{
	struct hash_cell *cell;

	if (!cursor) { return HASHTBL_INVALID_VAL; }

	ASSERT_HASHTBL_CURSOR(cursor);
	if (cursor->state != HASHTBL_CURSOR_DATA) {
		return HASHTBL_INVALID_VAL;
	}

	cell = hlist_entry(cursor->curr, struct hash_cell, list);
	ASSERT(cell);
	return get_hash_cell_val(cell);
}

/**
 * Get key size of the focused item.
 *
 * @return key size (> 0) if the cursor is valid, or 0.
 */
int hashtbl_cursor_key_size(const hashtbl_cursor_t *cursor)
{
	struct hash_cell *cell;

	if (!cursor) { return 0; }

	ASSERT_HASHTBL_CURSOR(cursor);
	if (cursor->state != HASHTBL_CURSOR_DATA) { return 0; }

	cell = hlist_entry(cursor->curr, struct hash_cell, list);
	ASSERT(cell);
	return get_hash_cell_key_size(cell);
}

/**
 * Get pointer to the key of the focused item.
 *
 * Causion. If the item is deleted from the hash table,
 * The pointer will be invalid.
 *
 * @return pointer to the key if the cursor is valid, or NULL.
 */
u8* hashtbl_cursor_key(const hashtbl_cursor_t *cursor)
{
	struct hash_cell *cell;

	if (!cursor) { return NULL; }

	ASSERT_HASHTBL_CURSOR(cursor);
	if (cursor->state != HASHTBL_CURSOR_DATA) { return NULL; }

	cell = hlist_entry(cursor->curr, struct hash_cell, list);
	ASSERT(cell);
	return get_hash_cell_key(cell);
}

/**
 * Test method for hash table cursor test.
 *
 * @return 0 in test pass, or -1.
 */
int hashtbl_cursor_test(void)
{
	struct hash_tbl *htbl;
	hashtbl_cursor_t curt;
	u8 buf[sizeof(int)];
	int i, j, key;
	unsigned long val;

	LOGd("hashtbl_cursor_test begin.\n");

	/*
	 * Test with small data set.
	 */
	LOGd("***** Test with small data set *****\n");

	/* Create hash table. */
	LOGd("Create hashtbl");
	htbl = hashtbl_create(HASHTBL_MAX_BUCKET_SIZE, GFP_KERNEL);
	ASSERT(htbl);

	/* Initialize cursor. */
	LOGd("Initialize cursor.\n");
	hashtbl_cursor_init(htbl, &curt);

	/* Begin then end. */
	LOGd("Begin then end.\n");
	hashtbl_cursor_begin(&curt);
	CHECKd(hashtbl_cursor_is_valid(&curt));
	CHECKd(hashtbl_cursor_is_begin(&curt));
	CHECKd(!hashtbl_cursor_next(&curt));
	CHECKd(hashtbl_cursor_is_end(&curt));
	CHECKd(hashtbl_cursor_is_valid(&curt));

	/* Prepare hash table data. */
	LOGd("Prepare hash table data.\n");
	for (i = 0; i < 10; i++) {
		key = i;
		val = i;
		memcpy(buf, &key, sizeof(int));
		CHECKd(hashtbl_add(htbl, buf, sizeof(int),
					val, GFP_KERNEL) == 0);
	}
	CHECKd(hashtbl_n_items(htbl) == 10);

	/* Begin to end. */
	LOGd("Begin to end.\n");
	hashtbl_cursor_begin(&curt);
	i = 0;
	while (hashtbl_cursor_next(&curt)) {
		CHECKd(hashtbl_cursor_is_valid(&curt));
		print_hashtbl_cursor(&curt); /* debug */

		CHECKd(hashtbl_cursor_key_size(&curt) == sizeof(int));
		memcpy(&key, hashtbl_cursor_key(&curt), sizeof(int));
		val = hashtbl_cursor_val(&curt);
		CHECKd(val != HASHTBL_INVALID_VAL);
		LOGd("i %d key %d val %lu\n", i, key, val);
		i++;
	}
	LOGd("i: %d\n", i);
	CHECKd(i == 10);
	CHECKd(hashtbl_cursor_is_end(&curt));

	/* Begin to end with delete */
	LOGd("Begin to end with delete.\n");
	hashtbl_cursor_begin(&curt);
	i = 0; j = 0;
	while (hashtbl_cursor_next(&curt)) {
		CHECKd(hashtbl_cursor_is_valid(&curt));
		print_hashtbl_cursor(&curt); /* debug */

		val = hashtbl_cursor_val(&curt);
		CHECKd(val != HASHTBL_INVALID_VAL);
		if (val % 2 == 0) {
			CHECKd(hashtbl_cursor_del(&curt) == val);
			j++;
			CHECKd(curt.state == HASHTBL_CURSOR_DELETED);
		}
		i++;
	}
	CHECKd(i == 10);
	CHECKd(j == 5);
	CHECKd(hashtbl_cursor_is_end(&curt));
	CHECKd(hashtbl_n_items(htbl) == 5);

	LOGd("Destroy hash table.\n");
	hashtbl_destroy(htbl);

	/*
	 * Test with large data set.
	 */
	LOGd("***** Test with larget data set *****\n");

	/* Create hash table. */
	LOGd("Create hashtbl");
	htbl = hashtbl_create(HASHTBL_MAX_BUCKET_SIZE, GFP_KERNEL);
	ASSERT(htbl);

	/* Initialize cursor. */
	LOGd("Initialize cursor.\n");
	hashtbl_cursor_init(htbl, &curt);

	/* Prepare hash table data. */
	LOGd("Prepare hash table data.\n");
	for (i = 0; i < 1000; i++) {
		key = i;
		val = i;
		memcpy(buf, &key, sizeof(int));
		CHECKd(hashtbl_add(htbl, buf, sizeof(int),
					val, GFP_KERNEL) == 0);
	}
	CHECKd(hashtbl_n_items(htbl) == 1000);

	/* Begin to end. */
	LOGd("Begin to end.\n");
	hashtbl_cursor_begin(&curt);
	i = 0;
	while (hashtbl_cursor_next(&curt)) {
		CHECKd(hashtbl_cursor_is_valid(&curt));

		CHECKd(hashtbl_cursor_key_size(&curt) == sizeof(int));
		memcpy(&key, hashtbl_cursor_key(&curt), sizeof(int));
		val = hashtbl_cursor_val(&curt);
		CHECKd(val != HASHTBL_INVALID_VAL);
		i++;
	}
	LOGd("i: %d\n", i);
	CHECKd(i == 1000);
	CHECKd(hashtbl_cursor_is_end(&curt));

	/* Begin to end with delete */
	LOGd("Begin to end with delete.\n");
	hashtbl_cursor_begin(&curt);
	i = 0; j = 0;
	while (hashtbl_cursor_next(&curt)) {
		CHECKd(hashtbl_cursor_is_valid(&curt));

		val = hashtbl_cursor_val(&curt);
		CHECKd(val != HASHTBL_INVALID_VAL);
		if (val % 2 == 0) {
			CHECKd(hashtbl_cursor_del(&curt) == val);
			j++;
			CHECKd(curt.state == HASHTBL_CURSOR_DELETED);
		}
		i++;
	}
	CHECKd(i == 1000);
	CHECKd(j == 500);
	CHECKd(hashtbl_cursor_is_end(&curt));
	CHECKd(hashtbl_n_items(htbl) == 500);

	LOGd("Destroy hash table.\n");
	hashtbl_destroy(htbl);

	LOGd("hashtbl_cursor_test end.\n");
	return 0;

error:
	return -1;
}

MODULE_LICENSE("Dual BSD/GPL");
