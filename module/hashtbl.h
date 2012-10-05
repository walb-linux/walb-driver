/**
 * hashtbl.h - Hash table operations.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef HASHTBL_H_KERNEL
#define HASHTBL_H_KERNEL

#include "check_kernel.h"
#include <linux/list.h>

/**
 * DOC: Hash table.
 *
 * key: byte array with size_t length and u8[length].
 * val: unsigned long value which can be pointer.
 */

/**
 * Currently max bucket size is limited to the following definition.
 * To get over the limitation, multi-level hashing is required.
 */
#define HASHTBL_MAX_BUCKET_SIZE (PAGE_SIZE / sizeof(struct hlist_head))

/**
 * Invalid value.
 */
#define HASHTBL_INVALID_VAL ((unsigned long)(-1))

/**
 * Hash cell.
 */
struct hash_cell {
	struct hlist_node list;
	
	int key_size;
	u8 *key;
	
	unsigned long val;
};

/**
 * Hash table.
 */
struct hash_tbl {

	int bucket_size;
	struct hlist_head *bucket; /* managed memory */
	/* buckets[i] must be hlist head of hash_cell. */

	/* n_bits to store 0 to bucket_size - 1. */
	unsigned int n_bits;
};

/**
 * Prototypes
 */
struct hash_tbl* hashtbl_create(int bucket_size, gfp_t gfp_mask);
void hashtbl_destroy(struct hash_tbl *htbl);

int hashtbl_add(struct hash_tbl *htbl,
		const u8* key, int key_size, unsigned long val, gfp_t gfp_mask);
unsigned long hashtbl_lookup(const struct hash_tbl *htbl, const u8* key, int key_size);
unsigned long hashtbl_del(struct hash_tbl *htbl, const u8* key, int key_size);
void hashtbl_empty(struct hash_tbl *htbl);

int hashtbl_is_empty(const struct hash_tbl *htbl);
int hashtbl_n_items(const struct hash_tbl *htbl);

int hashtbl_test(void); /* For test. */


/**
 * Cursor state for hash table.
 */
enum {
	HASHTBL_CURSOR_BEGIN = 1,
	HASHTBL_CURSOR_END,
	HASHTBL_CURSOR_DATA, /* curr points an item. */
	HASHTBL_CURSOR_DELETED, /* after call hashtbl_cursor_del(). */
	HASHTBL_CURSOR_INVALID,
};

/**
 * Cursor structure for hash table.
 */
typedef struct
{
	struct hash_tbl *htbl;
	int state; /* Cursor state */

	int bucket_idx; /* Head index in the bucket array. */
	struct hlist_head *curr_head; /* Head of the list
					 which current item belongs to. */
	struct hlist_node *curr; /* Current pointer. */

	struct hlist_head *next_head; /* Head of the list
					 which next item belongs to. */
	struct hlist_node *next; /* Next pointer */

} hashtbl_cursor_t;

/**
 * Prototypes of hashtbl cursor operations.
 */
void hashtbl_cursor_init(struct hash_tbl *htbl, hashtbl_cursor_t *cursor);
void hashtbl_cursor_begin(hashtbl_cursor_t *cursor);
int hashtbl_cursor_next(hashtbl_cursor_t *cursor);
unsigned long hashtbl_cursor_del(hashtbl_cursor_t *cursor);
int hashtbl_cursor_is_begin(const hashtbl_cursor_t *cursor);
int hashtbl_cursor_is_end(const hashtbl_cursor_t *cursor);
int hashtbl_cursor_is_valid(const hashtbl_cursor_t *cursor);
unsigned long hashtbl_cursor_val(const hashtbl_cursor_t *cursor);
int hashtbl_cursor_key_size(const hashtbl_cursor_t *cursor);
u8* hashtbl_cursor_key(const hashtbl_cursor_t *cursor);

int hashtbl_cursor_test(void);

#endif /* HASHTBL_H_KERNEL */
