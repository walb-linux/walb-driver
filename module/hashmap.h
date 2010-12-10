/**
 * hashmap.h - Hash map header.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _HASHMAP_H
#define _HASHMAP_H

#include <linux/list.h>

/**
 * Hash cell.
 */
struct hash_cell {
        struct list_head list;
        
        int key_size;
        u8 *key;
        
        void *val; /* This pointer is not managed
                      by hashmap_* functions. */
};

/**
 * Hash map.
 */
struct hash_map {

        int bucket_size;
        struct list_head *bucket; /* managed memory */
        /* buckets[i] must be list head of hash_cell. */

        /* n_bits to store 0 to bucket_size - 1. */
        unsigned int n_bits;
};

/**
 * Prototypes
 */
struct hash_map* hashmap_create(int bucket_size, gfp_t gfp_mask);
void hashmap_destroy(struct hash_map *hmap);

int hashmap_add(struct hash_map *hmap,
                const u8* key, int key_size, const void *val, gfp_t gfp_mask);
void* hashmap_lookup(const struct hash_map *hmap, const u8* key, int key_size);
void* hashmap_del(struct hash_map *hmap, const u8* key, int key_size);
void hashmap_empty(struct hash_map *hmap);

int hashmap_test(void); /* For test. */


#endif /* _HASHMAP_H */
