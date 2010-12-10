/**
 * hashmap.c - Hash map implementation.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * Written by: Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 */

#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/slab.h>
#include <linux/list.h>
#include <linux/hash.h>

#include "walb_util.h" /* for debug */
#include "hashmap.h"

#ifdef ASSERT
#undef ASSERT
#endif

#define ASSERT(cond) BUG_ON(!(cond))


#define ASSERT_HASHMAP(hmap) ASSERT((hmap) != NULL &&               \
                                    (hmap)->bucket != NULL &&       \
                                    (hmap)->bucket_size > 0 &&      \
                                    (hmap)->n_bits > 0)
#define ASSERT_HASHCELL(hcell) ASSERT((hcell) != NULL &&            \
                                      (hcell)->key != NULL &&       \
                                      (hcell)->key_size > 0 &&      \
                                      (hcell)->val != NULL)

/**
 * Prototypes of static functions.
 */
static u32 get_sum(const u8* data, int size);
static unsigned int get_n_bits(u32 val);
static struct hash_cell* hashmap_lookup_cell(const struct hash_map *hmap,
                                             const u8* key, int key_size);
static u32 hashmap_get_index(const struct hash_map *hmap,
                             const u8* key, int key_size);


/**
 * Get number of required bits to store val.
 *
 * 00100000B needs 6 bits.
 * 00011111B needs 5 bits.
 */
static unsigned int get_n_bits(u32 val)
{
        int i;
        if (val == 0) {
                return 1;
        }
        
        for (i = 1; i < 32; i ++) {
                if (val >> i == 1) {
                        return i + 1;
                }
        }
        BUG();
}

/**
 * Lookup cell from hashmap.
 *
 * @hmap hash map.
 * @key pointer to key.
 * @key_size key size.
 *
 * @return hash cell if found, or NULL.
 */
static struct hash_cell* hashmap_lookup_cell(const struct hash_map *hmap,
                                             const u8* key, int key_size)
{
        u32 idx;
        struct hlist_node *node, *next;
        struct hash_cell *cell, *ret;
        
        ASSERT_HASHMAP(hmap);

        idx = hashmap_get_index(hmap, key, key_size);

        ret = NULL;
        hlist_for_each_entry_safe(cell, node, next, &hmap->bucket[idx], list) {

                ASSERT_HASHCELL(cell);
                if (cell->key_size == key_size &&
                    memcmp(cell->key, key, key_size) == 0) {

                        ret = cell;
                        return cell;
                }                
        }
        /* printk_d("hashmap_lookup_cell end\n"); */
        return ret;
}

/**
 * Get bucket index of the key.
 *
 * @hmap hash map.
 * @key pointer to key data.
 * @key_size key size.
 *
 * @return index in the bucket.
 */
static u32 hashmap_get_index(const struct hash_map *hmap, const u8* key, int key_size)
{
        u32 idx, sum;

        ASSERT_HASHMAP(hmap);

        sum = get_sum(key, key_size);
        idx = hash_32(sum, hmap->n_bits);
        ASSERT(idx < hmap->bucket_size);
        
        /* printk_d("sum %08x idx %u\n", sum, idx); */
        return idx;
}

/**
 * Get simple checksum of byte array.
 *
 * @data pointer to data.
 * @size data size.
 *
 * @return simple checksum.
 */
static u32 get_sum(const u8* data, int size)
{
        int i;
        u32 n = size / sizeof(u32);
        u32 m = size % sizeof(u32);
        u32 buf;
        u64 sum = 0;
        u32 ret;

        ASSERT(n * sizeof(u32) + m == size);

        for (i = 0; i < n; i ++) {
                sum += *(u32 *)(data + (sizeof(u32) * i));
        }

        if (m > 0) {
                buf = 0;
                memcpy(&buf, data + (sizeof(u32) * n), m);
                sum += buf;
        }
        
        ret = ~(u32)((sum >> 32) + (sum << 32 >> 32)) + 1;
        return (ret != (u32)(-1) ? ret : 0);
}

/**
 * Create hash map.
 */
struct hash_map* hashmap_create(int bucket_size, gfp_t gfp_mask)
{
        int i;
        struct hash_map *hmap;

        printk_d("hashmap_create begin\n");
        ASSERT(bucket_size > 0);
        
        hmap = kzalloc(sizeof(struct hash_map), gfp_mask);
        if (hmap == NULL) { goto error0; }

        hmap->bucket_size = bucket_size;
        hmap->n_bits = get_n_bits((u32)(bucket_size - 1));
        hmap->bucket = kzalloc(sizeof(struct hlist_head) * bucket_size, gfp_mask);
        if (hmap->bucket == NULL) { goto error1; }

        for (i = 0; i < hmap->bucket_size; i ++) {
                INIT_HLIST_HEAD(&hmap->bucket[i]);
        }

        ASSERT_HASHMAP(hmap);
        printk_d("hashmap_create end\n");
        return hmap;
        
error1:
        kfree(hmap);
error0:
        return NULL;
}

/**
 * Destroy hash map.
 */
void hashmap_destroy(struct hash_map *hmap)
{
        printk_d("hashmap_destroy begin\n");
        hashmap_empty(hmap);
        kfree(hmap->bucket);
        kfree(hmap);
        printk_d("hashmap_destroy end\n");
}

/**
 * Delete all cells in the hash map.
 */
void hashmap_empty(struct hash_map *hmap)
{
        int i;
        struct hlist_node *node, *next;
        struct hash_cell *cell;

        printk_d("hashmap_empty begin\n");
        ASSERT_HASHMAP(hmap);
        
        for (i = 0; i < hmap->bucket_size; i ++) {

                hlist_for_each_entry_safe(cell, node, next, &hmap->bucket[i], list) {

                        ASSERT_HASHCELL(cell);
                        kfree(cell->key);
                        hlist_del(&cell->list);
                        kfree(cell);
                }
                ASSERT(hlist_empty(&hmap->bucket[i]));
        }
        printk_d("hashmap_empty end\n");
}

/**
 * Put data to hash map.
 *
 * @hmap hash map.
 * @key pointer to key data.
 * @key_size key size.
 * @val pointer to value. MUST NOT be NULL.
 * @gfp_mask GFP_*
 * 
 * @return 0 in success, or -1.
 */
int hashmap_add(struct hash_map *hmap,
                const u8* key, int key_size, const void *val, gfp_t gfp_mask)
{
        struct hash_cell *cell;
        u32 idx;

        if (val == NULL) {
                printk(KERN_ERR "add_to_hashmap: val must not be NULL.\n");
                goto error0;
        }

        ASSERT_HASHMAP(hmap);

        if (hashmap_lookup_cell(hmap, key, key_size) != NULL) {
                /* Already key exists. */
                goto error0;
        }

        /* Allocate cell. */
        cell = kmalloc(sizeof(struct hash_cell), gfp_mask);
        if (cell == NULL) { goto error0; }
        cell->key = kmalloc(key_size, gfp_mask);
        if (cell->key == NULL) { goto error1; }

        /* Fill cell. */
        cell->key_size = key_size;
        memcpy(cell->key, key, key_size);
        cell->val = (void *)val;

        /* Add to hashmap. */
        idx = hashmap_get_index(hmap, key, key_size);
        hlist_add_head(&cell->list, &hmap->bucket[idx]);
        
        /* printk_d("hashmap_add end\n"); */
        return 0;

error1:
        kfree(cell);
error0:
        return -1;
}

/**
 * Get data from hash map.
 *
 * @hmap hash map.
 * @key pointer to key data.
 * @key_size key size.
 *
 * @return pointer to value if found, or NULL.
 */
void* hashmap_lookup(const struct hash_map *hmap, const u8* key, int key_size)
{
        struct hash_cell *cell;
        
        cell = hashmap_lookup_cell(hmap, key, key_size);
        /* printk_d("hashmap_lookup end\n"); */
        return (cell == NULL ? NULL : cell->val);
}

/**
 * Delete data from hash map.
 *
 * @hmap hash map.
 * @key_size key size.
 * @key pointer to key data.
 *
 * @return pointer to value if found, or NULL.
 */
void* hashmap_del(struct hash_map *hmap, const u8* key, int key_size)
{
        struct hash_cell *cell;
        void *val = NULL;
        
        cell = hashmap_lookup_cell(hmap, key, key_size);
        if (cell != NULL) {
                val = cell->val;
                hlist_del(&cell->list);
                kfree(cell->key);
                kfree(cell);
        }
        /* printk_d("hashmap_del end\n"); */
        return val;
}


/**
 * Get number of cells in the hashmap.
 * This is slow, just for test.
 *
 * @hmap hash map.
 *
 * @return number of cells in the hash map.
 */
int hashmap_n_items(const struct hash_map *hmap)
{
        int i;
        struct hlist_node *node, *next;
        struct hash_cell *cell;
        int n = 0;
        int n_min = INT_MAX;
        int n_max = 0;
        int n_local = 0;

        ASSERT_HASHMAP(hmap);

        
        for (i = 0; i < hmap->bucket_size; i ++) {

                n_local = 0;
                hlist_for_each_entry_safe(cell, node, next, &hmap->bucket[i], list) {

                        ASSERT_HASHCELL(cell);
                        n_local ++;
                        n ++;
                }
                if (n_local < n_min) { n_min = n_local; }
                if (n_max < n_local) { n_max = n_local; }
        }

        printk_d("n_min %d n_max %d n_avg %d, n_total %d\n",
                 n_min, n_max, n / hmap->bucket_size, n);
        return n;
}

/**
 * Test hashmap for debug.
 */
int hashmap_test(void)
{
        int i;
        struct hash_map *hmap;
        char buf[10];
        void *p;

        printk_d("hashmap_test begin\n");

        printk_d("list_head: %zu\n"
                 "hlist_head: %zu\n"
                 "hash_map: %zu\n"
                 "hash_cell: %zu\n"
                 "max bucket_size: %d\n",
                 sizeof(struct list_head),
                 sizeof(struct hlist_head),
                 sizeof(struct hash_map),
                 sizeof(struct hash_cell),
                 HASHMAP_MAX_BUCKET_SIZE_IN_PAGE);
        
        hmap = hashmap_create(HASHMAP_MAX_BUCKET_SIZE_IN_PAGE, GFP_KERNEL);
        if (hmap == NULL) { return -1; }

        printk_d("n_items: %d\n", hashmap_n_items(hmap));

        for (i = 0; i < 100000; i ++) {
                snprintf(buf, 10, "abcd%05d", i);
                ASSERT(hashmap_add(hmap, buf, 9, buf + i, GFP_KERNEL) == 0);
        }

        printk_d("n_items: %d\n", hashmap_n_items(hmap));

        for (i = 0; i < 100000; i ++) {
                snprintf(buf, 10, "abcd%05d", i);
                p = hashmap_lookup(hmap, buf, 9);
                ASSERT(p ==  buf + i);
        }

        printk_d("n_items: %d\n", hashmap_n_items(hmap));

        for (i = 0; i < 100000; i ++) {
                snprintf(buf, 10, "abcd%05d", i);
                if (i % 2 == 0) {
                        p = hashmap_del(hmap, buf, 9);
                } else {
                        p = hashmap_lookup(hmap, buf, 9);
                }
                ASSERT(p != NULL && p == buf + i);
                if (i % 2 == 0) {
                        p = hashmap_lookup(hmap, buf, 9);
                        ASSERT(p == NULL);
                }
        }
        printk_d("n_items: %d\n", hashmap_n_items(hmap));
        hashmap_empty(hmap);
        printk_d("n_items: %d\n", hashmap_n_items(hmap));
        hashmap_destroy(hmap);
        printk_d("hashmap_test end\n");
        return 0;
}

MODULE_LICENSE("Dual BSD/GPL");
