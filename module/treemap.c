/**
 * treemap.c - Tree map implementation.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * Written by: Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 */

#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/slab.h>
#include <linux/list.h>
#include <linux/random.h> /* This is for treemap_test(). */

#include "walb_util.h" /* for debug */
#include "treemap.h"


#define ASSERT_TREEMAP(tmap) ASSERT((tmap) != NULL)

#define ASSERT_TREENODE(tnode) ASSERT((tnode) != NULL && \
                                      (tnode)->val != INVALID_VAL)

#define ASSERT_TREECELL(tcell) ASSERT((tcell) != NULL && \
                                      (tcell)->val != INVALID_VAL)

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
int treemap_add(struct tree_map *tmap, u64 key, unsigned long val, gfp_t gfp_mask)
{
        struct tree_node *newnode, *t;
        struct rb_node **childp, *parent;
        
        ASSERT_TREEMAP(tmap);

        if (val == INVALID_VAL) {
                printk_e("Val must not be INVALID_VAL.\n");
                goto error0;
        }

        /* Generate new node. */
        newnode = kmalloc(sizeof(struct tree_node), gfp_mask);
        if (newnode == NULL) {
                printk_e("kmalloc failed.\n");
                goto error0;
        }
        newnode->key = key;
        newnode->val = val;

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
 * @return value if found, or INVALID_VAL.
 */
unsigned long treemap_lookup(const struct tree_map *tmap, u64 key)
{
        struct tree_node *t;

        ASSERT_TREEMAP(tmap);
        
        t = treemap_lookup_node(tmap, key);
        if (t) {
                ASSERT_TREENODE(t);
                return t->val;
        } else {
                return INVALID_VAL;
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
 * @return value if found, or INVALID_VAL.
 */
unsigned long treemap_del(struct tree_map *tmap, u64 key)
{
        struct tree_node *t;
        unsigned long val = INVALID_VAL;

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
 * Check tree map is empty or not.
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
        unsigned long val;
        
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

        /* Returns error if val is INVALID_VAL. */
        WALB_CHECK(treemap_add(tmap, 0, INVALID_VAL, GFP_KERNEL) != 0);

        /* Insert records. */
        for (i = 0; i < 10000; i ++) {
                key = (u64)i;
                /* Succeed. */
                WALB_CHECK(treemap_add(tmap, key, key + i, GFP_KERNEL) == 0);
                /* Fail due to key exists. */
                WALB_CHECK(treemap_add(tmap, key, key + i, GFP_KERNEL) != 0);
        }
        n = treemap_n_items(tmap);
        WALB_CHECK(n == 10000);
        WALB_CHECK(! treemap_is_empty(tmap));

        /* Delete records. */
        for (i = 0; i < 10000; i ++) {
                key = (u64)i;
                
                if (i % 2 == 0) {
                        val = treemap_del(tmap, key);
                } else {
                        val = treemap_lookup(tmap, key);
                }
                WALB_CHECK(val != INVALID_VAL);
                WALB_CHECK(val == key + i);
                if (i % 2 == 0) {
                        val = treemap_lookup(tmap, key);
                        WALB_CHECK(val == INVALID_VAL);
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
                if(treemap_add(tmap, key, key + i, GFP_KERNEL) == 0) {
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


/*******************************************************************************
 * Multimap_* functions.
 *******************************************************************************/

/**
 * Create multimap.
 */
struct tree_map* multimap_create(gfp_t gfp_mask)
{
        ASSERT(sizeof(struct hlist_head) == sizeof(unsigned long));
        
        return treemap_create(gfp_mask);
}

/**
 * Destroy multimap.
 */
void multimap_destroy(struct tree_map *tmap)
{
        ASSERT_TREEMAP(tmap);
        
        multimap_empty(tmap);
        kfree(tmap);
}

/**
 * Add key-value pair to the multimap.
 *
 * Different key-value pair can be added.
 * The same key-value pair can not be added.
 *
 * @return 0 in success, or -1.
 */
int multimap_add(struct tree_map *tmap, u64 key, unsigned long val, gfp_t gfp_mask)
{
        struct tree_node *t;
        struct hlist_head headt, *head;
        struct tree_cell *newcell, *cell;
        struct hlist_node *node, *next;
        int found;

        if (val == INVALID_VAL) {
                printk_e("Val must not be INVALID_VAL.\n");
                goto error0;
        }
        
        newcell = kmalloc(sizeof(struct tree_cell), gfp_mask);
        if (newcell == NULL) {
                printk_e("kmalloc failed.\n");
                goto error0;
        }
        newcell->val = val;
        ASSERT_TREECELL(newcell);
        
        t = treemap_lookup_node(tmap, key);

        if (t == NULL) {
                /* There is no record with that key. */
                
                INIT_HLIST_HEAD(&headt);
                hlist_add_head(&newcell->list, &headt);
                
                if (treemap_add(tmap, key, *((unsigned long *)(&headt)), gfp_mask)) {
                        printk_e("treemap_add failed.\n");
                        goto error1;
                }
        } else {
                /* There is already record(s) with that key. */

                ASSERT(t->key == key);
                head = (struct hlist_head *)(&t->val);
                ASSERT(! hlist_empty(head));

                found = 0;
                hlist_for_each_entry_safe(cell, node, next, head, list) {
                        ASSERT_TREECELL(cell);
                        if (cell->val == val) { found ++; break; }
                }
                if (found == 0) {
                        hlist_add_head(&newcell->list, head);
                } else {
                        goto error1;
                }
        }
        return 0;

error1:
        kfree(newcell);
error0:
        return -1;
        
}

/**
 * Lookup values with the key in the multimap.
 *
 * @return hlist of value if found, or NULL.
 *         Do not call @multimap_del() or @multimap_del_key()
 *         before scan the list.
 */
struct hlist_head* multimap_lookup(const struct tree_map *tmap, u64 key)
{
        struct tree_node *t;

        t = treemap_lookup_node(tmap, key);
        if (t == NULL) {
                return NULL;
        } else {
                return (struct hlist_head *)(&t->val);
        }
}

/**
 * Lookup first found value with the key in the multimap.
 *
 * @return first found value, or INVALID_VAL.
 */
unsigned long multimap_lookup_any(const struct tree_map *tmap, u64 key)
{
        struct hlist_head *head;
        struct tree_cell *cell;

        head = multimap_lookup(tmap, key);

        if (head == NULL) {
                return INVALID_VAL;
        } else {
                ASSERT(! hlist_empty(head));
                cell = hlist_entry(head->first, struct tree_cell, list);
                ASSERT_TREECELL(cell);
                return cell->val;
        }
}

/**
 * Number of records with the key in the multimap.
 *
 * @return number of records.
 */
int multimap_lookup_n(const struct tree_map *tmap, u64 key)
{
        int count;
        struct hlist_head *head;
        struct hlist_node *hlnode;
        struct tree_cell *cell;

        count = 0;
        head = multimap_lookup(tmap, key);
        if (head != NULL) {
                ASSERT(! hlist_empty(head));

                hlist_for_each_entry(cell, hlnode, head, list) {
                        ASSERT_TREECELL(cell);
                        count ++;
                }
        }
        return count;
}

/**
 * Delete key-value pair from the multimap.
 *
 * @return value if found, or INVALID_VAL.
 */
unsigned long multimap_del(struct tree_map *tmap, u64 key, unsigned long val)
{
        struct tree_node *t;
        struct tree_cell *cell;
        struct hlist_head *head;
        struct hlist_node *hlnode, *hlnext;
        int found;
        unsigned long retval = INVALID_VAL, ret;
        
        t = treemap_lookup_node(tmap, key);
        if (t == NULL) { return INVALID_VAL; }
        
        ASSERT(t->key == key);
        head = (struct hlist_head *)(&t->val);
        ASSERT(! hlist_empty(head));

        found = 0;
        hlist_for_each_entry_safe(cell, hlnode, hlnext, head, list) {
                ASSERT_TREECELL(cell);
                if (cell->val == val) {
                        found ++;
                        hlist_del(&cell->list);
                        retval = cell->val;
                        kfree(cell);
                }
        }
        ASSERT(found == 0 || found == 1);

        if (hlist_empty(head)) {
                ret = treemap_del(tmap, key);
                ASSERT(ret != INVALID_VAL);
        }
        return retval;
}

/**
 * Delete all kay-value pairs with the key from the multimap.
 *
 * @return number of deleted records.
 */
int multimap_del_key(struct tree_map *tmap, u64 key)
{
        int found = 0;
        unsigned long p;
        struct tree_cell *cell;
        struct hlist_head *head;
        struct hlist_node *hlnode, *hlnext;

        p = treemap_del(tmap, key);
        if (p == 0) { return 0; }
        
        head = (struct hlist_head *)(&p);
        ASSERT(! hlist_empty(head));

        hlist_for_each_entry_safe(cell, hlnode, hlnext, head, list) {
                ASSERT_TREECELL(cell);
                found ++;
                hlist_del(&cell->list);
                kfree(cell);
        }
        ASSERT(hlist_empty(head));

        return found;
}

/**
 * Make the multimap empty.
 */
void multimap_empty(struct tree_map *tmap)
{
        struct tree_node *t;
        struct rb_node *node, *next;
        struct hlist_head *head;
        struct hlist_node *hlnode, *hlnext;
        struct tree_cell *cell;
        
        node = rb_first(&tmap->root);
        if (node != NULL) { next = rb_next(node); }

        while (node) {
                rb_erase(node, &tmap->root);
                t = container_of(node, struct tree_node, node);
                head = (struct hlist_head *)(&t->val);

                hlist_for_each_entry_safe(cell, hlnode, hlnext, head, list) {

                        ASSERT_TREECELL(cell);
                        hlist_del(&cell->list);
                        kfree(cell);
                }
                kfree(t);

                node = next;
                if (node != NULL) { next = rb_next(node); }
        }

        ASSERT(treemap_is_empty(tmap));
}

/**
 * Check multimap is empty or not.
 *
 * @return 1 if empty, or 0.
 */
int multimap_is_empty(const struct tree_map *tmap)
{
        return treemap_is_empty(tmap);
}

/**
 * Count items in the multimap.
 *
 * @return number of items in the multimap.
 */
int multimap_n_items(const struct tree_map *tmap)
{
        struct tree_node *t;
        struct rb_node *node;
        int count;
        struct hlist_head *head;
        struct hlist_node *hlnode;
        struct tree_cell *cell;

        count = 0;
        node = rb_first(&tmap->root);
        while (node) {
                t = container_of(node, struct tree_node, node);
                head = (struct hlist_head *)(&t->val);
                hlist_for_each_entry(cell, hlnode, head, list) {
                        ASSERT_TREECELL(cell);
                        count ++;
                }
                node = rb_next(node);
        }
        return count;
}

/**
 * Test multimap for debug.
 *
 * @return 0 in success, or -1.
 */
int multimap_test(void)
{
        struct tree_map *tm;
        int i, n, count;
        u64 key;
        unsigned long val;
        struct hlist_head *head;
        struct hlist_node *node;
        struct tree_cell *cell;
        
        printk_d("multimap_test begin\n");
        printk_d("hlist_head: %zu\n"
                 "unsigned long: %zu\n"
                 "tree_node: %zu\n",
                 sizeof(struct hlist_head),
                 sizeof(unsigned long),
                 sizeof(struct tree_cell));

        /* Create. */
        tm = multimap_create(GFP_KERNEL);

        n = multimap_n_items(tm);
        WALB_CHECK(n == 0);
        WALB_CHECK(multimap_is_empty(tm));

        /* Returns error if val is INVALID_VAL. */
        WALB_CHECK(multimap_add(tm, 0, INVALID_VAL, GFP_KERNEL) != 0);

        /* Insert records. */
        for (i = 0; i < 10000; i ++) {
                key = (u64) i;
                /* Succeed. */
                WALB_CHECK(multimap_add(tm, key, key + i, GFP_KERNEL) == 0);
                /* Fail due to key exists. */
                WALB_CHECK(multimap_add(tm, key, key + i, GFP_KERNEL) != 0);
                /* Succeed due to value is different. */
                WALB_CHECK(multimap_add(tm, key, key + i + 1, GFP_KERNEL) == 0);
        }
        n = multimap_n_items(tm);
        WALB_CHECK(n == 20000);
        WALB_CHECK(! multimap_is_empty(tm));

        /* Delete records. */
        for (i = 0; i < 10000; i ++) {
                key = (u64) i;

                n = multimap_lookup_n(tm, key);
                WALB_CHECK(n == 2);
                
                if (i % 2 == 0) {
                        val = multimap_del(tm, key, key + i);
                        WALB_CHECK(val != INVALID_VAL);
                        WALB_CHECK(val == key + i);
                } else {
                        head = multimap_lookup(tm, key);
                        hlist_for_each_entry(cell, node, head, list) {
                                ASSERT_TREECELL(cell);
                                val = cell->val;
                                WALB_CHECK(val == key + i || val == key + i + 1);
                        }
                }                
                if (i % 2 == 0) {
                        val = multimap_lookup_any(tm, key);
                        WALB_CHECK(val == key + i + 1);
                        
                        head = multimap_lookup(tm, key);
                        hlist_for_each_entry(cell, node, head, list) {
                                ASSERT_TREECELL(cell);
                                val = cell->val;
                                WALB_CHECK(val == key + i + 1);
                        }
                        n = multimap_lookup_n(tm, key);
                        WALB_CHECK(n == 1);
                } else {
                        val = multimap_lookup_any(tm, key);
                        WALB_CHECK(val == key + i || val == key + i + 1);
                        n = multimap_lookup_n(tm, key);
                        WALB_CHECK(n == 2);
                }
        }
        n = multimap_n_items(tm);
        WALB_CHECK(n == 15000);

        /* Delete multiple records. */
        for (i = 0; i < 10000; i ++) {
                key = (u64) i;
                if (i % 2 != 0) {
                        n = multimap_del_key(tm, key);
                        WALB_CHECK(n == 2);
                }
        }
        n = multimap_n_items(tm);
        WALB_CHECK(n == 5000);

        /* Make tree map empty. */
        multimap_empty(tm);
        n = multimap_n_items(tm);
        WALB_CHECK(n == 0);
        WALB_CHECK(multimap_is_empty(tm));

        /* 2cd empty. */
        multimap_empty(tm);
        n = multimap_n_items(tm);
        WALB_CHECK(n == 0);
        WALB_CHECK(multimap_is_empty(tm));

        /* Random insert. */
        count = 0;
        for (i = 0; i < 10000; i ++) {
                key = get_random_u32() % 1000;
                val = get_random_u32() % 10;
                if (multimap_add(tm, key, val, GFP_KERNEL) == 0) {
                        count ++;
                }
        }
        n = multimap_n_items(tm);
        WALB_CHECK(n == count);
        printk_n("count %d\n", n);

        /* Empty and destroy. */
        multimap_destroy(tm);
        
        printk_d("multimap_test end\n");
        return 0;

error:
        return -1;
}


MODULE_LICENSE("Dual BSD/GPL");
