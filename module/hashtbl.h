/**
 * hashtbl.h - Hash table header.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _HASHTBL_H
#define _HASHTBL_H

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
 * Curser state for hash table.
 */
enum {
        HASHTBL_CURSER_BEGIN = 1,
        HASHTBL_CURSER_END,
        HASHTBL_CURSER_DATA,
        HASHTBL_CURSER_INVALID,
};

/**
 * Curser structure for hash table.
 */
typedef struct
{
        struct hash_tbl *htbl;
        int state;

        int bucket_idx;
        struct hlist_head *head;
        
        struct hlist_node *curr;
        struct hlist_node *next;

} hashtbl_curser_t;

/**
 * Prototypes of hashtbl curser operations.
 */
void hashtbl_curser_init(struct hash_tbl *htbl, hashtbl_curser_t *curser);
int hashtbl_curser_begin(hashtbl_curser_t *curser);
int hashtbl_curser_next(hashtbl_curser_t *curser);
unsigned long hashtbl_curser_del(hashtbl_curser_t *curser);
int hashtbl_curser_is_begin(const hashtbl_curser_t *curser);
int hashtbl_curser_is_end(const hashtbl_curser_t *curser);
int hashtbl_curser_is_valid(const hashtbl_curser_t *curser);
unsigned long hashtbl_curser_val(const hashtbl_curser_t *curser);
int hashtbl_curser_key_size(const hashtbl_curser_t *curser);
u8* hashtbl_curser_key(const hashtbl_curser_t *curser);

int hashtbl_curser_test(void);

#endif /* _HASHTBL_H */
