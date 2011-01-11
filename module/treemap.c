/**
 * treemap.c - Tree map implementation.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * Written by: Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 */

#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/slab.h>
#include <linux/random.h> /* This is for treemap_test(). */

#include "walb_util.h" /* for debug */
#include "treemap.h"

#define ASSERT_TREEMAP(tmap) ASSERT((tmap) != NULL)

#define ASSERT_TREENODE(tnode) ASSERT((tnode) != NULL &&        \
                                      (tnode)->val != NULL)

/**
 * Prototypes of static functions.
 */
static struct tree_node* treemap_lookup_node(const struct tree_map *tmap, u64 key);
static u32 get_random_u32(void);

/**
 * Create tree map.
 */
struct tree_map* treemap_create(gfp_t gfp_mask)
{
        struct tree_map *tmap;
        tmap = kmalloc(sizeof(struct tree_map), gfp_mask);
        if (tmap == NULL) {
                printk_e("treemap_create: memory allocation failed.\n");
                goto error0;
        }

        tmap->root.rb_node = NULL;
        
        ASSERT_TREEMAP(tmap);
        return tmap;

#if 0
error1:
        kfree(tmap);
#endif
error0:
        return NULL;
}

/**
 * Destroy tree map.
 */
void treemap_destroy(struct tree_map *tmap)
{
        ASSERT_TREEMAP(tmap);
        
        treemap_empty(tmap);
        kfree(tmap);
}

/**
 * Add key-value pair to the tree map.
 *
 * @return 0 in success, or -1.
 */
int treemap_add(struct tree_map *tmap, u64 key, const void* val, gfp_t gfp_mask)
{
        struct tree_node *newnode, *t;
        struct rb_node **childp, *parent;
        
        ASSERT_TREEMAP(tmap);

        if (val == NULL) {
                printk_e("treemap_add: val must not be NULL.\n");
                goto error0;
        }

        /* Generate new node. */
        newnode = kmalloc(sizeof(struct tree_node), gfp_mask);
        if (newnode == NULL) {
                printk_e("treemap_add: kmalloc failed.\n");
                goto error0;
        }
        newnode->key = key;
        newnode->val = (void *)val;

        /* Figure out where to put new node. */
        childp = &(tmap->root.rb_node);
        parent = NULL;
        while (*childp) {
                t = container_of(*childp, struct tree_node, node);
                parent = *childp;
                if (key < t->key) {
                        childp = &(parent->rb_left);
                } else if (key > t->key) {
                        childp = &(parent->rb_right);
                } else {
                        ASSERT(key == t->key);
                        goto error1;
                }
        }

        /* Add new node and rebalance tree. */
        rb_link_node(&newnode->node, parent, childp);
        rb_insert_color(&newnode->node, &tmap->root);

        return 0;

error1:
        kfree(newnode);
error0:
        return -1;
}

/**
 * Lookup value with the key in the tree map.
 *
 * @return value if found, or NULL.
 */
void* treemap_lookup(const struct tree_map *tmap, u64 key)
{
        struct tree_node *t;

        ASSERT_TREEMAP(tmap);
        
        t = treemap_lookup_node(tmap, key);
        if (t) {
                ASSERT_TREENODE(t);
                return t->val;
        } else {
                return NULL;
        }
}

/**
 * Lookup tree_node with the key in the tree map.
 *
 * @return struct tree_node if found, or NULL.
 */
static struct tree_node* treemap_lookup_node(const struct tree_map *tmap, u64 key)
{
        struct rb_node *node = tmap->root.rb_node;
        struct tree_node *t;

        ASSERT_TREEMAP(tmap);
        
        while (node) {
                t = container_of(node, struct tree_node, node);

                if (key < t->key) {
                        node = node->rb_left;
                } else if (key > t->key) {
                        node = node->rb_right;
                } else {
                        ASSERT(key == t->key);
                        return t;
                }
        }
        return NULL;
}

/**
 * Delete key-value pair from the tree map.
 *
 * @return value if found, or NULL.
 */
void* treemap_del(struct tree_map *tmap, u64 key)
{
        struct tree_node *t;
        void *val = NULL;

        ASSERT_TREEMAP(tmap);

        t = treemap_lookup_node(tmap, key);
        if (t) {
                ASSERT_TREENODE(t);
                ASSERT(key == t->key);
                val = t->val;
                rb_erase(&t->node, &tmap->root);
                kfree(t);
        }
        
        return val;
}

/**
 * Make the tree map empty.
 */
void treemap_empty(struct tree_map *tmap)
{
        struct tree_node *t;
        struct rb_node *node, *next;

        ASSERT_TREEMAP(tmap);
        
        node = rb_first(&tmap->root);
        if (node != NULL) { next = rb_next(node); }
        
        while (node) {
                rb_erase(node, &tmap->root);
                t = container_of(node, struct tree_node, node);
                kfree(t);
                
                node = next;
                if (node != NULL) { next = rb_next(node); }
        }

        ASSERT(treemap_is_empty(tmap));
}

/**
 * Check tree map is empt or not.
 *
 * @return 1 if empty, or 0.
 */
int treemap_is_empty(const struct tree_map *tmap)
{
        ASSERT_TREEMAP(tmap);
        
        if (rb_first(&tmap->root) == NULL)
                return 1;
        else
                return 0;
}

/**
 * Count items in the tree map.
 *
 * @return number of items in the tree map.
 */
int treemap_n_items(const struct tree_map *tmap)
{
        struct rb_node *node;
        int count = 0;

        ASSERT_TREEMAP(tmap);
        
        node = rb_first(&tmap->root);
        while (node) {
                count ++;
                node = rb_next(node);
        }
        
        return count;
}

/**
 * Get random integer.
 */
static u32 get_random_u32(void)
{
        u32 ret;
        get_random_bytes(&ret, sizeof(u32));
        return ret;
}


/**
 * Test treemap for debug.
 *
 * @return 0 in success, or -1.
 */
int treemap_test(void)
{
        
        struct tree_map *tmap;
        int i, n, count;
        u64 key;
        void *p;
        
        printk_d("treemap_test begin\n");
        printk_d("tree_map: %zu\n"
                 "tree_node: %zu\n",
                 sizeof(struct tree_map),
                 sizeof(struct tree_node));

        /* Create. */
        tmap = treemap_create(GFP_KERNEL);

        n = treemap_n_items(tmap);
        WALB_CHECK(n == 0);
        WALB_CHECK(treemap_is_empty(tmap));

        /* Returns error if val is NULL. */
        WALB_CHECK(treemap_add(tmap, 0, NULL, GFP_KERNEL) != 0);

        /* Insert records. */
        for (i = 0; i < 10000; i ++) {
                key = (u64)i;
                /* Succeed. */
                WALB_CHECK(treemap_add(tmap, key, &key + i, GFP_KERNEL) == 0);
                /* Fail due to key exists. */
                WALB_CHECK(treemap_add(tmap, key, &key + i, GFP_KERNEL) != 0);
        }
        n = treemap_n_items(tmap);
        WALB_CHECK(n == 10000);
        WALB_CHECK(! treemap_is_empty(tmap));

        /* Delete records. */
        for (i = 0; i < 10000; i ++) {
                key = (u64)i;
                
                if (i % 2 == 0) {
                        p = treemap_del(tmap, key);
                } else {
                        p = treemap_lookup(tmap, key);
                }
                WALB_CHECK(p != NULL);
                WALB_CHECK(p == &key + i);
                if (i % 2 == 0) {
                        p = treemap_lookup(tmap, key);
                        WALB_CHECK(p == NULL);
                }
        }
        n = treemap_n_items(tmap);
        WALB_CHECK(n == 5000);

        /* Make tree map empty. */
        treemap_empty(tmap);
        n = treemap_n_items(tmap);
        WALB_CHECK(n == 0);
        WALB_CHECK(treemap_is_empty(tmap));

        /* 2cd empty. */
        treemap_empty(tmap);
        n = treemap_n_items(tmap);
        WALB_CHECK(n == 0);
        WALB_CHECK(treemap_is_empty(tmap));

        /* Random insert. */
        count = 0;
        for (i = 0; i < 10000; i ++) {
                key = get_random_u32() % 10000;
                if(treemap_add(tmap, key, &key + i, GFP_KERNEL) == 0) {
                        count ++;
                }
        }
        n = treemap_n_items(tmap);
        WALB_CHECK(n == count);

        /* Empty and destroy. */
        treemap_destroy(tmap);
        
        printk_d("treemap_test end\n");
        return 0;

error:       
        return -1;
}

MODULE_LICENSE("Dual BSD/GPL");
