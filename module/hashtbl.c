/**
 * hashtbl.c - Hash table implementation.
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
#include "hashtbl.h"

#ifdef ASSERT
#undef ASSERT
#endif

#define ASSERT(cond) BUG_ON(!(cond))


#define ASSERT_HASHTBL(htbl) ASSERT((htbl) != NULL &&               \
                                    (htbl)->bucket != NULL &&       \
                                    (htbl)->bucket_size > 0 &&      \
                                    (htbl)->n_bits > 0)
#define ASSERT_HASHCELL(hcell) ASSERT((hcell) != NULL &&            \
                                      (hcell)->key != NULL &&       \
                                      (hcell)->key_size > 0 &&      \
                                      (hcell)->val != NULL)

/**
 * Prototypes of static functions.
 */
static u32 get_sum(const u8* data, int size);
static unsigned int get_n_bits(u32 val);
static struct hash_cell* hashtbl_lookup_cell(const struct hash_tbl *htbl,
                                             const u8* key, int key_size);
static u32 hashtbl_get_index(const struct hash_tbl *htbl,
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
 * Lookup cell from hashtbl.
 *
 * @htbl hash table.
 * @key pointer to key.
 * @key_size key size.
 *
 * @return hash cell if found, or NULL.
 */
static struct hash_cell* hashtbl_lookup_cell(const struct hash_tbl *htbl,
                                             const u8* key, int key_size)
{
        u32 idx;
        struct hlist_node *node, *next;
        struct hash_cell *cell, *ret;
        
        ASSERT_HASHTBL(htbl);

        idx = hashtbl_get_index(htbl, key, key_size);

        ret = NULL;
        hlist_for_each_entry_safe(cell, node, next, &htbl->bucket[idx], list) {

                ASSERT_HASHCELL(cell);
                if (cell->key_size == key_size &&
                    memcmp(cell->key, key, key_size) == 0) {

                        ret = cell;
                        return cell;
                }                
        }
        /* printk_d("hashtbl_lookup_cell end\n"); */
        return ret;
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
static u32 hashtbl_get_index(const struct hash_tbl *htbl, const u8* key, int key_size)
{
        u32 idx, sum;

        ASSERT_HASHTBL(htbl);

        sum = get_sum(key, key_size);
        idx = hash_32(sum, htbl->n_bits);
        ASSERT(idx < htbl->bucket_size);
        
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
 * Create hash table.
 */
struct hash_tbl* hashtbl_create(int bucket_size, gfp_t gfp_mask)
{
        int i;
        struct hash_tbl *htbl;

        printk_d("hashtbl_create begin\n");
        ASSERT(bucket_size > 0);
        
        htbl = kzalloc(sizeof(struct hash_tbl), gfp_mask);
        if (htbl == NULL) { goto error0; }

        htbl->bucket_size = bucket_size;
        htbl->n_bits = get_n_bits((u32)(bucket_size - 1));
        htbl->bucket = kzalloc(sizeof(struct hlist_head) * bucket_size, gfp_mask);
        if (htbl->bucket == NULL) { goto error1; }

        for (i = 0; i < htbl->bucket_size; i ++) {
                INIT_HLIST_HEAD(&htbl->bucket[i]);
        }

        ASSERT_HASHTBL(htbl);
        printk_d("hashtbl_create end\n");
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
        printk_d("hashtbl_destroy begin\n");
        hashtbl_empty(htbl);
        kfree(htbl->bucket);
        kfree(htbl);
        printk_d("hashtbl_destroy end\n");
}

/**
 * Delete all cells in the hash table.
 */
void hashtbl_empty(struct hash_tbl *htbl)
{
        int i;
        struct hlist_node *node, *next;
        struct hash_cell *cell;

        printk_d("hashtbl_empty begin\n");
        ASSERT_HASHTBL(htbl);
        
        for (i = 0; i < htbl->bucket_size; i ++) {

                hlist_for_each_entry_safe(cell, node, next, &htbl->bucket[i], list) {

                        ASSERT_HASHCELL(cell);
                        kfree(cell->key);
                        hlist_del(&cell->list);
                        kfree(cell);
                }
                ASSERT(hlist_empty(&htbl->bucket[i]));
        }
        printk_d("hashtbl_empty end\n");
}

/**
 * Put data to hash table.
 *
 * @htbl hash table.
 * @key pointer to key data.
 * @key_size key size.
 * @val pointer to value. MUST NOT be NULL.
 * @gfp_mask GFP_*
 * 
 * @return 0 in success,
 *         -EPERM when key already exists,
 *         -ENOMEM in memory allocation failure.
 */
int hashtbl_add(struct hash_tbl *htbl,
                const u8* key, int key_size, const void *val, gfp_t gfp_mask)
{
        struct hash_cell *cell;
        u32 idx;

        ASSERT(val != NULL);
        ASSERT_HASHTBL(htbl);

        if (hashtbl_lookup_cell(htbl, key, key_size) != NULL) {
                /* Already key exists. */
                goto key_exists;
        }

        /* Allocate cell. */
        cell = kmalloc(sizeof(struct hash_cell), gfp_mask);
        if (cell == NULL) { goto nomem0; }
        cell->key = kmalloc(key_size, gfp_mask);
        if (cell->key == NULL) { goto nomem1; }

        /* Fill cell. */
        cell->key_size = key_size;
        memcpy(cell->key, key, key_size);
        cell->val = (void *)val;

        /* Add to hashtbl. */
        idx = hashtbl_get_index(htbl, key, key_size);
        hlist_add_head(&cell->list, &htbl->bucket[idx]);
        
        /* printk_d("hashtbl_add end\n"); */
        return 0;

nomem1:
        kfree(cell);
nomem0:
        return -ENOMEM;

key_exists:
        return -EPERM;
}

/**
 * Get data from hash table.
 *
 * @htbl hash table.
 * @key pointer to key data.
 * @key_size key size.
 *
 * @return pointer to value if found, or NULL.
 */
void* hashtbl_lookup(const struct hash_tbl *htbl, const u8* key, int key_size)
{
        struct hash_cell *cell;
        
        cell = hashtbl_lookup_cell(htbl, key, key_size);
        /* printk_d("hashtbl_lookup end\n"); */
        return (cell == NULL ? NULL : cell->val);
}

/**
 * Delete data from hash table.
 *
 * @htbl hash table.
 * @key_size key size.
 * @key pointer to key data.
 *
 * @return pointer to value if found, or NULL.
 */
void* hashtbl_del(struct hash_tbl *htbl, const u8* key, int key_size)
{
        struct hash_cell *cell;
        void *val = NULL;
        
        cell = hashtbl_lookup_cell(htbl, key, key_size);
        if (cell != NULL) {
                val = cell->val;
                hlist_del(&cell->list);
                kfree(cell->key);
                kfree(cell);
        }
        /* printk_d("hashtbl_del end\n"); */
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
        
        for (i = 0; i < htbl->bucket_size; i ++) {
                if (! hlist_empty(&htbl->bucket[i])) {
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
        struct hlist_node *node, *next;
        struct hash_cell *cell;
        int n = 0;
        int n_min = INT_MAX;
        int n_max = 0;
        int n_local = 0;

        ASSERT_HASHTBL(htbl);
        
        for (i = 0; i < htbl->bucket_size; i ++) {

                n_local = 0;
                hlist_for_each_entry_safe(cell, node, next, &htbl->bucket[i], list) {

                        ASSERT_HASHCELL(cell);
                        n_local ++;
                        n ++;
                }
                if (n_local < n_min) { n_min = n_local; }
                if (n_max < n_local) { n_max = n_local; }
        }

        printk_d("n_min %d n_max %d n_avg %d, n_total %d\n",
                 n_min, n_max, n / htbl->bucket_size, n);
        return n;
}

/**
 * Test hashtbl for debug.
 */
int hashtbl_test(void)
{
        int i;
        struct hash_tbl *htbl;
        char buf[10];
        void *p;

        printk_d("hashtbl_test begin\n");

        printk_d("list_head: %zu\n"
                 "hlist_head: %zu\n"
                 "hash_tbl: %zu\n"
                 "hash_cell: %zu\n"
                 "max bucket_size: %ld\n",
                 sizeof(struct list_head),
                 sizeof(struct hlist_head),
                 sizeof(struct hash_tbl),
                 sizeof(struct hash_cell),
                 HASHTBL_MAX_BUCKET_SIZE);
        
        htbl = hashtbl_create(HASHTBL_MAX_BUCKET_SIZE, GFP_KERNEL);
        if (htbl == NULL) { return -1; }

        printk_d("n_items: %d\n", hashtbl_n_items(htbl));

        for (i = 0; i < 100000; i ++) {
                snprintf(buf, 10, "abcd%05d", i);
                ASSERT(hashtbl_add(htbl, buf, 9, buf + i, GFP_KERNEL) == 0);
        }

        printk_d("n_items: %d\n", hashtbl_n_items(htbl));

        for (i = 0; i < 100000; i ++) {
                snprintf(buf, 10, "abcd%05d", i);
                p = hashtbl_lookup(htbl, buf, 9);
                ASSERT(p ==  buf + i);
        }

        printk_d("n_items: %d\n", hashtbl_n_items(htbl));

        for (i = 0; i < 100000; i ++) {
                snprintf(buf, 10, "abcd%05d", i);
                if (i % 2 == 0) {
                        p = hashtbl_del(htbl, buf, 9);
                } else {
                        p = hashtbl_lookup(htbl, buf, 9);
                }
                ASSERT(p != NULL && p == buf + i);
                if (i % 2 == 0) {
                        p = hashtbl_lookup(htbl, buf, 9);
                        ASSERT(p == NULL);
                }
        }
        printk_d("n_items: %d\n", hashtbl_n_items(htbl));
        hashtbl_empty(htbl);
        printk_d("n_items: %d\n", hashtbl_n_items(htbl));
        hashtbl_destroy(htbl);
        printk_d("hashtbl_test end\n");
        return 0;
}

MODULE_LICENSE("Dual BSD/GPL");
