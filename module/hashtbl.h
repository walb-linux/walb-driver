/**
 * hashtbl.h - Hash table header.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _HASHTBL_H
#define _HASHTBL_H

#include <linux/list.h>

/**
 * Hash cell.
 */
struct hash_cell {
        struct hlist_node list;
        
        int key_size;
        u8 *key;
        
        void *val; /* This pointer is not managed
                      by hashtbl_* functions. */
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
 * Currently max bucket size is limited to the following.
 * To get over the limitation, multi-level hashing is required.
 */
#define HASHTBL_MAX_BUCKET_SIZE (PAGE_SIZE / sizeof(struct hlist_head))

/**
 * Prototypes
 */
struct hash_tbl* hashtbl_create(int bucket_size, gfp_t gfp_mask);
void hashtbl_destroy(struct hash_tbl *htbl);

int hashtbl_add(struct hash_tbl *htbl,
                const u8* key, int key_size, const void *val, gfp_t gfp_mask);
void* hashtbl_lookup(const struct hash_tbl *htbl, const u8* key, int key_size);
void* hashtbl_del(struct hash_tbl *htbl, const u8* key, int key_size);
void hashtbl_empty(struct hash_tbl *htbl);

int hashtbl_n_items(const struct hash_tbl *htbl);

int hashtbl_test(void); /* For test. */

#endif /* _HASHTBL_H */
