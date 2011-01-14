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

/**
 * Assertions.
 */
#define ASSERT_TREEMAP(tmap) ASSERT((tmap) != NULL)

#define ASSERT_TREENODE(tnode) ASSERT((tnode) != NULL && \
                                      (tnode)->val != TREEMAP_INVALID_VAL)

#define ASSERT_TREECELL(tcell) ASSERT((tcell) != NULL && \
                                      (tcell)->val != TREEMAP_INVALID_VAL)

#define ASSERT_MAP_CURSER(curser)                                       \
        ASSERT((curser) != NULL &&                                      \
               (curser)->map != NULL &&                                 \
               (((curser)->state == MAP_CURSER_BEGIN &&                 \
                 (curser)->prev == NULL &&                              \
                 (curser)->curr == NULL) ||                             \
                ((curser)->state == MAP_CURSER_END &&                   \
                 (curser)->curr == NULL &&                              \
                 (curser)->next == NULL) ||                             \
                ((curser)->state == MAP_CURSER_DATA &&                  \
                 (curser)->curr != NULL) ||                             \
                ((curser)->state == MAP_CURSER_INVALID)))

/**
 * Prototypes of static functions.
 */
static struct tree_node* map_lookup_node(const map_t *tmap, u64 key);
static struct tree_node* map_lookup_node_detail(const map_t *tmap, u64 key, int search_flag);
static u32 get_random_u32(void);

static struct tree_node* map_first(const map_t *tmap);
static struct tree_node* map_last(const map_t *tmap);
static struct tree_node* map_next(const struct tree_node *t);
static struct tree_node* map_prev(const struct tree_node *t);

static void map_curser_invalid(map_curser_t *curser);

static int hlist_len(const struct hlist_head *head);

static int multimap_add_newkey(multimap_t *tmap,
                               u64 key, struct tree_cell *newcell, gfp_t gfp_mask);
static int multimap_add_oldkey(struct tree_cell_head *chead, struct tree_cell *newcell);


/*******************************************************************************
 * Static functions.
 *******************************************************************************/

/**
 * Lookup tree_node with the key in the tree map.
 *
 * @return struct tree_node if found, or NULL.
 */
static struct tree_node* map_lookup_node(const map_t *tmap, u64 key)
{
        return map_lookup_node_detail(tmap, key, MAP_SEARCH_EQ);
}

/**
 * Lookup tree_node with the key and search flag in the map.
 *
 * @tmap map data.
 * @key key
 * @search_flag search flag. (MAP_SEARCH_*).
 *
 * @return struct tree_node if found, or NULL.
 */
static struct tree_node* map_lookup_node_detail(const map_t *tmap, u64 key, int search_flag)
{
        struct rb_node *node = tmap->root.rb_node;
        struct tree_node *t = NULL;

        ASSERT_TREEMAP(tmap);

        if (search_flag == MAP_SEARCH_BEGIN ||
            search_flag == MAP_SEARCH_END) { return NULL; }

        /* Travserse tree. */
        while (node) {
                t = container_of(node, struct tree_node, node);

                if (key < t->key) {
                        node = node->rb_left;
                } else if (key > t->key) {
                        node = node->rb_right;
                } else {
                        ASSERT(key == t->key);
                        if (search_flag == MAP_SEARCH_EQ ||
                            search_flag == MAP_SEARCH_LE ||
                            search_flag == MAP_SEARCH_GE) {
                                return t;
                        } else {
                                break;
                        }
                }
        }
        if (t == NULL) { /* Empty tree. */ 
                return NULL;
        }
        
        switch (search_flag) {
        case MAP_SEARCH_EQ:
                ASSERT(t->key != key);
                return NULL;
                
        case MAP_SEARCH_LT:
        case MAP_SEARCH_LE:
                if (t->key == key) {
                        ASSERT(search_flag == MAP_SEARCH_LT);
                        t = map_prev(t);
                        ASSERT(t == NULL || t->key < key);
                        return t;
                        
                } else if (t->key < key) {
                        ASSERT(map_next(t) == NULL || (map_next(t))->key > key);
                        return t;
                } else {
                        t = map_prev(t);
                        ASSERT(t == NULL || t->key < key);
                        return t;
                }
                break;
                
        case MAP_SEARCH_GT:
        case MAP_SEARCH_GE:

                if (t->key == key) {
                        ASSERT(search_flag == MAP_SEARCH_GT);
                        t = map_next(t);
                        ASSERT(t == NULL || t->key > key);
                        return t;
                        
                } else if (t->key < key) {
                        t = map_next(t);
                        ASSERT(t == NULL || t->key > key);
                        return t;
                } else {
                        ASSERT(map_prev(t) == NULL || (map_prev(t))->key < key);
                        return t;
                }
                break;
        default:
                BUG();
        }
        return NULL;
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
 * Get first node in the map.
 */
static struct tree_node* map_first(const map_t *tmap)
{
        struct rb_node *node;

        if (tmap == NULL) { return NULL; }
        node = rb_first(&tmap->root);
        if (node == NULL) { return NULL; }
        return container_of(node, struct tree_node, node);
}

/**
 * Get last node in the map.
 */
static struct tree_node* map_last(const map_t *tmap)
{
        struct rb_node *node;

        if (tmap == NULL) { return NULL; }
        node = rb_last(&tmap->root);
        if (node == NULL) { return NULL; }
        return container_of(node, struct tree_node, node);
}

/**
 * Get next node in the map.
 */
static struct tree_node* map_next(const struct tree_node *t)
{
        struct rb_node *node;
        
        if (t == NULL) { return NULL; }
        node = rb_next(&t->node);
        if (node == NULL) { return NULL; }
        return container_of(node, struct tree_node, node);
}

/**
 * Get previous node in the map.
 */
static struct tree_node* map_prev(const struct tree_node *t)
{
        struct rb_node *node;
        
        if (t == NULL) { return NULL; }
        node = rb_prev(&t->node);
        if (node == NULL) { return NULL; }
        return container_of(node, struct tree_node, node);
}

/**
 * Make curser invalid state.
 */
static void map_curser_invalid(map_curser_t *curser)
{
        curser->state = MAP_CURSER_INVALID;
        curser->curr = NULL;
        curser->prev = NULL;
        curser->next = NULL;

        ASSERT_MAP_CURSER(curser);
}

/**
 * Get length of hlist structure.
 *
 * @return number of items in the hlist in success, or -1.
 */
static int hlist_len(const struct hlist_head *head)
{
        struct hlist_node *node;
        int count = 0;
        
        if (head == NULL) { return -1; }

        hlist_for_each(node, head) {
                count ++;
        }
        return count;
}

/*******************************************************************************
 * Treemap_* functions.
 *******************************************************************************/

/**
 * Create tree map.
 */
map_t* map_create(gfp_t gfp_mask)
{
        map_t *tmap;
        tmap = kmalloc(sizeof(map_t), gfp_mask);
        if (tmap == NULL) {
                printk_e("map_create: memory allocation failed.\n");
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
void map_destroy(map_t *tmap)
{
        ASSERT_TREEMAP(tmap);
        
        map_empty(tmap);
        kfree(tmap);
}

/**
 * Add key-value pair to the tree map.
 *
 * @return 0 in success, or -1.
 */
int map_add(map_t *tmap, u64 key, unsigned long val, gfp_t gfp_mask)
{
        struct tree_node *newnode, *t;
        struct rb_node **childp, *parent;
        
        ASSERT_TREEMAP(tmap);

        if (val == TREEMAP_INVALID_VAL) {
                printk_e("Val must not be TREEMAP_INVALID_VAL.\n");
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
 * @return value if found, or TREEMAP_INVALID_VAL.
 */
unsigned long map_lookup(const map_t *tmap, u64 key)
{
        struct tree_node *t;

        ASSERT_TREEMAP(tmap);
        
        t = map_lookup_node(tmap, key);
        if (t) {
                ASSERT_TREENODE(t);
                return t->val;
        } else {
                return TREEMAP_INVALID_VAL;
        }
}

/**
 * Delete key-value pair from the tree map.
 *
 * @return value if found, or TREEMAP_INVALID_VAL.
 */
unsigned long map_del(map_t *tmap, u64 key)
{
        struct tree_node *t;
        unsigned long val = TREEMAP_INVALID_VAL;

        ASSERT_TREEMAP(tmap);

        t = map_lookup_node(tmap, key);
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
void map_empty(map_t *tmap)
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

        ASSERT(map_is_empty(tmap));
}

/**
 * Check tree map is empty or not.
 *
 * @return 1 if empty, or 0.
 */
int map_is_empty(const map_t *tmap)
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
int map_n_items(const map_t *tmap)
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
 * Test treemap for debug.
 *
 * @return 0 in success, or -1.
 */
int map_test(void)
{
        map_t *tmap;
        int i, n, count;
        u64 key;
        unsigned long val;
        
        printk_d("map_test begin\n");
        printk_d("tree_map: %zu\n"
                 "tree_node: %zu\n",
                 sizeof(map_t),
                 sizeof(struct tree_node));

        /* Create. */
        tmap = map_create(GFP_KERNEL);

        n = map_n_items(tmap);
        WALB_CHECK(n == 0);
        WALB_CHECK(map_is_empty(tmap));

        /* Search in empty tree. */
        WALB_CHECK(map_lookup(tmap, 0) == TREEMAP_INVALID_VAL);

        /* Returns error if val is TREEMAP_INVALID_VAL. */
        WALB_CHECK(map_add(tmap, 0, TREEMAP_INVALID_VAL, GFP_KERNEL) != 0);

        /* Insert records. */
        for (i = 0; i < 10000; i ++) {
                key = (u64)i;
                /* Succeed. */
                WALB_CHECK(map_add(tmap, key, key + i, GFP_KERNEL) == 0);
                /* Fail due to key exists. */
                WALB_CHECK(map_add(tmap, key, key + i, GFP_KERNEL) != 0);
        }
        n = map_n_items(tmap);
        WALB_CHECK(n == 10000);
        WALB_CHECK(! map_is_empty(tmap));

        /* Delete records. */
        for (i = 0; i < 10000; i ++) {
                key = (u64)i;
                
                if (i % 2 == 0) {
                        val = map_del(tmap, key);
                } else {
                        val = map_lookup(tmap, key);
                }
                WALB_CHECK(val != TREEMAP_INVALID_VAL);
                WALB_CHECK(val == key + i);
                if (i % 2 == 0) {
                        val = map_lookup(tmap, key);
                        WALB_CHECK(val == TREEMAP_INVALID_VAL);
                }
        }
        n = map_n_items(tmap);
        WALB_CHECK(n == 5000);

        /* Make tree map empty. */
        map_empty(tmap);
        n = map_n_items(tmap);
        WALB_CHECK(n == 0);
        WALB_CHECK(map_is_empty(tmap));

        /* 2cd empty. */
        map_empty(tmap);
        n = map_n_items(tmap);
        WALB_CHECK(n == 0);
        WALB_CHECK(map_is_empty(tmap));

        /* Random insert. */
        count = 0;
        for (i = 0; i < 10000; i ++) {
                key = get_random_u32() % 10000;
                if(map_add(tmap, key, key + i, GFP_KERNEL) == 0) {
                        count ++;
                }
        }
        n = map_n_items(tmap);
        WALB_CHECK(n == count);

        /* Empty and destroy. */
        map_destroy(tmap);
        
        printk_d("map_test end\n");
        return 0;

error:       
        return -1;
}

/*******************************************************************************
 * Map curser functions.
 *******************************************************************************/

/**
 * Allocate curser data.
 */
map_curser_t* map_curser_create(map_t *map, gfp_t gfp_mask)
{
        map_curser_t *curser;
        
        curser = kmalloc(sizeof(map_curser_t), gfp_mask);
        if (curser == NULL) { goto error0; }

        map_curser_init(map, curser);
        return curser;
        
error0:
        return NULL;
}

/**
 * Initialize curser data.
 */
void map_curser_init(map_t *map, map_curser_t *curser)
{
        ASSERT(curser != NULL);

        curser->map = map;

        curser->state = MAP_CURSER_INVALID;
        curser->curr = NULL;
        curser->prev = NULL;
        curser->next = NULL;

        ASSERT_MAP_CURSER(curser);
}

/**
 * Search key and set curser.
 *
 * @return 1 if found, or 0.
 */
int map_curser_search(map_curser_t* curser, u64 key, int search_flag)
{
        if (curser == NULL) { return 0; }
        
        ASSERT_MAP_CURSER(curser);

        switch (search_flag) {
                
        case MAP_SEARCH_BEGIN:
                map_curser_begin(curser);
                break;
                
        case MAP_SEARCH_END:
                map_curser_end(curser);
                break;
                
        case MAP_SEARCH_EQ:
        case MAP_SEARCH_LT:
        case MAP_SEARCH_LE:
        case MAP_SEARCH_GT:
        case MAP_SEARCH_GE:
                curser->curr = map_lookup_node_detail(curser->map, key, search_flag);
                if (curser->curr == NULL) {
                        map_curser_invalid(curser);
                } else {
                        curser->state = MAP_CURSER_DATA;
                        curser->prev = map_prev(curser->curr);
                        curser->next = map_next(curser->curr);
                }
                break;
                
        default:
                BUG();
        }
        
        ASSERT_MAP_CURSER(curser);
        if (curser->state != MAP_CURSER_INVALID) {
                return 1;
        } else {
                return 0;
        }
}

/**
 * Go forward curser a step.
 *
 * @return 1 if new current is data, or 0.
 */
int map_curser_next(map_curser_t *curser)
{
        if (curser == NULL) { return 0; }
        ASSERT_MAP_CURSER(curser);
        
        switch (curser->state) {
                
        case MAP_CURSER_BEGIN:
        case MAP_CURSER_DATA:
                curser->prev = curser->curr;
                curser->curr = curser->next;
                curser->next = map_next(curser->curr);
                if (curser->curr != NULL) {
                        curser->state = MAP_CURSER_DATA;
                } else {
                        curser->state = MAP_CURSER_END;
                }
                break;
                
        case MAP_CURSER_END:
        case MAP_CURSER_INVALID:
                /* do nothing. */
                break;
                
        default:
                BUG();
        }
        ASSERT_MAP_CURSER(curser);

        return (curser->state == MAP_CURSER_DATA ? 1 : 0);
}

/**
 * Go backward curser a step.
 *
 * @return 1 if new current is data, or 0.
 */
int map_curser_prev(map_curser_t *curser)
{
        if (curser == NULL) { return 0; }
        ASSERT_MAP_CURSER(curser);
        
        switch (curser->state) {
                
        case MAP_CURSER_END:
        case MAP_CURSER_DATA:
                curser->next = curser->curr;
                curser->curr = curser->prev;
                curser->prev = map_prev(curser->curr);
                if (curser->curr != NULL) {
                        curser->state = MAP_CURSER_DATA;
                } else {
                        curser->state = MAP_CURSER_BEGIN;
                }
                break;
                
        case MAP_CURSER_BEGIN:
        case MAP_CURSER_INVALID:
                /* do nothing. */
                break;
                
        default:
                BUG();
        }
        ASSERT_MAP_CURSER(curser);

        return (curser->state == MAP_CURSER_DATA ? 1 : 0);
}

/**
 * Set curser at begin.
 *
 * @return 1 in success, or 0.
 */
int map_curser_begin(map_curser_t *curser)
{
        if (curser == NULL) { return 0; }
        
        ASSERT_MAP_CURSER(curser);
        
        curser->state = MAP_CURSER_BEGIN;
        curser->prev = NULL;
        curser->curr = NULL;
        curser->next = map_first(curser->map);

        ASSERT_MAP_CURSER(curser);
        return 1;
}

/**
 * Set curser at end.
 *
 * @return 1 in success, or 0.
 */
int map_curser_end(map_curser_t *curser)
{
        if (curser == NULL) { return 0; }

        ASSERT_MAP_CURSER(curser);
        
        curser->state = MAP_CURSER_END;
        curser->prev = map_last(curser->map);
        curser->curr = NULL;
        curser->next = NULL;

        ASSERT_MAP_CURSER(curser);
        return 1;
}

/**
 * Check curser is at begin.
 */
int map_curser_is_begin(map_curser_t *curser)
{
        if (curser != NULL && curser->state == MAP_CURSER_BEGIN) {
                return 1;
        } else {
                return 0;
        }
}

/**
 * Check curser is at end.
 */
int map_curser_is_end(map_curser_t *curser)
{
        if (curser != NULL && curser->state == MAP_CURSER_END) {
                return 1;
        } else {
                return 0;
        }
}

/**
 * Check curser is valid.
 */
int map_curser_is_valid(map_curser_t *curser)
{
        return ((curser != NULL &&
                 (curser->state == MAP_CURSER_BEGIN ||
                  curser->state == MAP_CURSER_END   ||
                  curser->state == MAP_CURSER_DATA)) ? 1 : 0);
}

/**
 * Get value of the curser.
 *
 * @return value if curser if valid, or TREEMAP_INVALID_VAL.
 */
unsigned long map_curser_get(const map_curser_t *curser)
{
        if (curser == NULL) { return TREEMAP_INVALID_VAL; }

        ASSERT_MAP_CURSER(curser);
        if (curser->state == MAP_CURSER_DATA) {
                return curser->curr->val;
        } else {
                return TREEMAP_INVALID_VAL;
        }
}

/**
 * Destroy curser.
 *
 * @curser curser created by @map_curser_create().
 */
void map_curser_destroy(map_curser_t *curser)
{
        kfree(curser);
}

/**
 * Test map curser for debug.
 *
 * @return 0 in success, or -1.
 */
int map_curser_test(void)
{
        map_t *map;
        map_curser_t curt;
        map_curser_t *cur;

        printk_d("map_curser_test begin.\n");
        
        /* Create map. */
        printk_d("Create map.\n");
        map = map_create(GFP_KERNEL);
        ASSERT(map != NULL);

        /* Create and init curser. */
        printk_d("Create and init curser.\n");
        cur = map_curser_create(map, GFP_KERNEL);
        map_curser_init(map, &curt);

        /* Begin -> end. */
        printk_d("Begin -> end.\n");
        map_curser_begin(&curt);
        WALB_CHECK(map_curser_is_valid(&curt));
        WALB_CHECK(! map_curser_next(&curt));
        WALB_CHECK(map_curser_is_end(&curt));
        WALB_CHECK(map_curser_is_valid(&curt));

        /* End -> begin. */
        printk_d("End -> begin.\n");
        map_curser_end(&curt);
        WALB_CHECK(map_curser_is_valid(&curt));
        WALB_CHECK(! map_curser_prev(&curt));
        WALB_CHECK(map_curser_is_begin(&curt));
        WALB_CHECK(map_curser_is_valid(&curt));
        
        /* Prepare map data. */
        printk_d("Prepare map data.\n");
        map_add(map, 10, 10, GFP_KERNEL);
        map_add(map, 20, 20, GFP_KERNEL);
        map_add(map, 30, 30, GFP_KERNEL);
        map_add(map, 40, 40, GFP_KERNEL);

        /* Begin to end. */
        printk_d("Begin to end.\n");
        map_curser_search(cur, 0, MAP_SEARCH_BEGIN);
        WALB_CHECK(map_curser_is_valid(cur));
        WALB_CHECK(map_curser_get(cur) == TREEMAP_INVALID_VAL);
        WALB_CHECK(map_curser_next(cur));
        WALB_CHECK(map_curser_get(cur) == 10);
        WALB_CHECK(map_curser_next(cur));
        WALB_CHECK(map_curser_get(cur) == 20);
        WALB_CHECK(map_curser_next(cur));
        WALB_CHECK(map_curser_get(cur) == 30);
        WALB_CHECK(map_curser_next(cur));
        WALB_CHECK(map_curser_get(cur) == 40);
        WALB_CHECK(! map_curser_next(cur));
        WALB_CHECK(map_curser_is_end(cur));

        /* End to begin. */
        printk_d("End to begin.\n");
        map_curser_search(cur, 0, MAP_SEARCH_END);
        WALB_CHECK(map_curser_is_valid(cur));
        WALB_CHECK(map_curser_get(cur) == TREEMAP_INVALID_VAL);
        WALB_CHECK(map_curser_prev(cur));
        WALB_CHECK(map_curser_get(cur) == 40);
        WALB_CHECK(map_curser_prev(cur));
        WALB_CHECK(map_curser_get(cur) == 30);
        WALB_CHECK(map_curser_prev(cur));
        WALB_CHECK(map_curser_get(cur) == 20);
        WALB_CHECK(map_curser_prev(cur));
        WALB_CHECK(map_curser_get(cur) == 10);
        WALB_CHECK(! map_curser_prev(cur));
        WALB_CHECK(map_curser_is_begin(cur));

        /* EQ */
        printk_d("EQ test.\n");
        map_curser_search(cur, 20, MAP_SEARCH_EQ);
        WALB_CHECK(map_curser_get(cur) == 20);
        map_curser_search(cur, 25, MAP_SEARCH_EQ);
        WALB_CHECK(! map_curser_is_valid(cur));
        WALB_CHECK(map_curser_get(cur) == TREEMAP_INVALID_VAL);

        /* LE */
        printk_d("LE test.\n");
        map_curser_search(cur, 20, MAP_SEARCH_LE);
        WALB_CHECK(map_curser_get(cur) == 20);
        map_curser_search(cur, 25, MAP_SEARCH_LE);
        WALB_CHECK(map_curser_get(cur) == 20);
        map_curser_search(cur, 10, MAP_SEARCH_LE);
        WALB_CHECK(map_curser_get(cur) == 10);
        map_curser_search(cur, 5, MAP_SEARCH_LE);
        WALB_CHECK(map_curser_get(cur) == TREEMAP_INVALID_VAL);

        /* LT */
        printk_d("LT test.\n");
        map_curser_search(cur, 20, MAP_SEARCH_LT);
        WALB_CHECK(map_curser_get(cur) == 10);
        map_curser_search(cur, 25, MAP_SEARCH_LT);
        WALB_CHECK(map_curser_get(cur) == 20);
        map_curser_search(cur, 10, MAP_SEARCH_LT);
        WALB_CHECK(map_curser_get(cur) == TREEMAP_INVALID_VAL);

        /* GE */
        printk_d("GE test.\n");
        map_curser_search(cur, 20, MAP_SEARCH_GE);
        WALB_CHECK(map_curser_get(cur) == 20);
        map_curser_search(cur, 25, MAP_SEARCH_GE);
        WALB_CHECK(map_curser_get(cur) == 30);
        map_curser_search(cur, 40, MAP_SEARCH_GE);
        WALB_CHECK(map_curser_get(cur) == 40);
        map_curser_search(cur, 45, MAP_SEARCH_GE);
        WALB_CHECK(map_curser_get(cur) == TREEMAP_INVALID_VAL);

        /* GT */
        printk_d("GT test.\n");
        map_curser_search(cur, 20, MAP_SEARCH_GT);
        WALB_CHECK(map_curser_get(cur) == 30);
        map_curser_search(cur, 25, MAP_SEARCH_GT);
        WALB_CHECK(map_curser_get(cur) == 30);
        map_curser_search(cur, 40, MAP_SEARCH_GT);
        WALB_CHECK(map_curser_get(cur) == TREEMAP_INVALID_VAL);

        /* Destroy curser. */
        printk_d("Destroy curser.\n");
        map_curser_destroy(cur);

        /* Destroy map. */
        printk_d("Destroy map.\n");
        map_destroy(map);
        
        printk_d("map_curser_test end.\n");
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
multimap_t* multimap_create(gfp_t gfp_mask)
{
        ASSERT(sizeof(struct hlist_head) == sizeof(unsigned long));
        
        return (multimap_t *)map_create(gfp_mask);
}

/**
 * Destroy multimap.
 */
void multimap_destroy(multimap_t *tmap)
{
        ASSERT_TREEMAP(tmap);
        
        multimap_empty(tmap);
        kfree(tmap);
}

/**
 * Add non-existing key into the multimap.
 *
 * @tmap multimap.
 * @key key
 * @newcell prepared inserted value.
 *
 * @return 0 in success, or -1.
 */
static int
multimap_add_newkey(multimap_t *tmap, u64 key, struct tree_cell *newcell, gfp_t gfp_mask)
{
        struct tree_cell_head *newhead;

        /* Allocate and initialize new tree cell head. */
        newhead = kmalloc(sizeof(struct tree_cell_head), gfp_mask);
        if (newhead == NULL) {
                printk_e("memory allocation failed.\n");
                goto error0;
        }
        newhead->key = key;
        INIT_HLIST_HEAD(&newhead->head);
        hlist_add_head(&newcell->list, &newhead->head);
        ASSERT(! hlist_empty(&newhead->head));
                
        /* Add to the map. */
        if (map_add((map_t *)tmap, key, (unsigned long)newhead, gfp_mask)) {
                printk_e("map_add failed.\n");
                goto error1;
        }
        return 0;

error1:
        kfree(newhead);
error0:
        return -1;
}

/**
 * Add for existing key into multimap.
 *
 * @tree_cell_head tree cell head.
 * @newcell cell to be added.
 *
 * @return 0 in success, -1 when the same key-value pair already exists.
 */
static int multimap_add_oldkey(struct tree_cell_head *chead, struct tree_cell *newcell)
{
        struct tree_cell *cell;
        struct hlist_head *hlhead;
        struct hlist_node *hlnode, *hlnext;
        int found;
        
        ASSERT(chead != NULL);
        ASSERT(newcell != NULL);
        
        hlhead = &chead->head;
        ASSERT(! hlist_empty(hlhead));

        found = 0;
        hlist_for_each_entry_safe(cell, hlnode, hlnext, hlhead, list) {
                ASSERT_TREECELL(cell);
                if (cell->val == newcell->val) { found ++; break; }
        }
        if (found == 0) {
                hlist_add_head(&newcell->list, hlhead);
                return 0;
        } else {
                return -1;
        }
}

/**
 * Add key-value pair to the multimap.
 *
 * Different key-value pair can be added.
 * The same key-value pair can not be added.
 *
 * @return 0 in success.
 *         1 whe key-value already exists.
 *         -1 in error.
 */
int multimap_add(multimap_t *tmap, u64 key, unsigned long val, gfp_t gfp_mask)
{
        struct tree_node *t;
        struct tree_cell_head *chead;
        struct tree_cell *newcell;
        int ret = 0;

        if (val == TREEMAP_INVALID_VAL) {
                printk_e("Val must not be TREEMAP_INVALID_VAL.\n");
                goto error0;
        }

        /* Prepare new cell. */
        newcell = kmalloc(sizeof(struct tree_cell), gfp_mask);
        if (newcell == NULL) {
                printk_e("kmalloc failed.\n");
                goto error0;
        }
        newcell->val = val;
        ASSERT_TREECELL(newcell);
        
        t = map_lookup_node((map_t *)tmap, key);

        if (t == NULL) {
                /* There is no record with that key. */
                if (multimap_add_newkey(tmap, key, newcell, gfp_mask) != 0) {
                        printk_e("multimap_add_newkey() failed.\n");
                        goto error1;
                }
        } else {
                /* There is already record(s) with that key. */
                ASSERT(t->key == key);
                /* tree_node's val is pointer to tree_cell_head. */
                chead = (struct tree_cell_head *)(t->val);
                ASSERT(chead->key == key);

                if (multimap_add_oldkey(chead, newcell) != 0) {
                        ret = 1;
                }
        }
        return ret;
        
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
struct tree_cell_head* multimap_lookup(const multimap_t *tmap, u64 key)
{
        struct tree_node *t;

        t = map_lookup_node((map_t *)tmap, key);
        if (t == NULL) {
                return NULL;
        } else {
                return (struct tree_cell_head *)(t->val);
        }
}

/**
 * Lookup first found value with the key in the multimap.
 *
 * @return first found value, or TREEMAP_INVALID_VAL.
 */
unsigned long multimap_lookup_any(const multimap_t *tmap, u64 key)
{
        struct tree_cell_head *chead;
        struct tree_cell *cell;

        chead = multimap_lookup(tmap, key);

        if (chead == NULL) {
                return TREEMAP_INVALID_VAL;
        } else {
                ASSERT(! hlist_empty(&chead->head));
                cell = hlist_entry(chead->head.first, struct tree_cell, list);
                ASSERT_TREECELL(cell);
                return cell->val;
        }
}

/**
 * Number of records with the key in the multimap.
 *
 * @return number of records.
 */
int multimap_lookup_n(const multimap_t *tmap, u64 key)
{
        int count;
        struct tree_cell_head *chead;

        count = 0;
        chead = multimap_lookup(tmap, key);
        if (chead != NULL) {
                ASSERT(! hlist_empty(&chead->head));
                count = hlist_len(&chead->head);
                ASSERT(count >= 0);
        }
        return count;
}

/**
 * Delete key-value pair from the multimap.
 *
 * @return value if found, or TREEMAP_INVALID_VAL.
 */
unsigned long multimap_del(multimap_t *tmap, u64 key, unsigned long val)
{
        struct tree_node *t;
        struct tree_cell *cell;
        struct tree_cell_head *chead;
        struct hlist_node *hlnode, *hlnext;
        int found;
        unsigned long retval = TREEMAP_INVALID_VAL, ret;
        
        t = map_lookup_node((map_t *)tmap, key);
        if (t == NULL) { return TREEMAP_INVALID_VAL; }
        
        ASSERT(t->key == key);
        chead = (struct tree_cell_head *)(t->val);
        ASSERT(chead->key == key);
        ASSERT(! hlist_empty(&chead->head));

        found = 0;
        hlist_for_each_entry_safe(cell, hlnode, hlnext, &chead->head, list) {
                ASSERT_TREECELL(cell);
                if (cell->val == val) {
                        found ++;
                        hlist_del(&cell->list);
                        retval = cell->val;
                        kfree(cell);
                }
        }
        ASSERT(found == 0 || found == 1);

        if (hlist_empty(&chead->head)) {
                ASSERT(found == 1);
                ret = map_del((map_t *)tmap, key);
                ASSERT(ret != TREEMAP_INVALID_VAL);
                kfree(chead);
        }
        return retval;
}

/**
 * Delete all records with the key from the multimap.
 *
 * @return number of deleted records.
 */
int multimap_del_key(multimap_t *tmap, u64 key)
{
        int found = 0;
        unsigned long p;
        struct tree_cell_head *chead;
        struct tree_cell *cell;
        struct hlist_node *hlnode, *hlnext;

        p = map_del((map_t *)tmap, key);
        if (p == TREEMAP_INVALID_VAL) { return 0; }
        
        chead = (struct tree_cell_head *)p;
        ASSERT(chead != NULL);
        ASSERT(! hlist_empty(&chead->head));

        hlist_for_each_entry_safe(cell, hlnode, hlnext, &chead->head, list) {
                ASSERT_TREECELL(cell);
                found ++;
                hlist_del(&cell->list);
                kfree(cell);
        }
        ASSERT(found > 0);
        ASSERT(hlist_empty(&chead->head));
        kfree(chead);

        return found;
}

/**
 * Make the multimap empty.
 */
void multimap_empty(multimap_t *tmap)
{
        struct tree_node *t;
        struct rb_node *node, *next;
        struct hlist_node *hlnode, *hlnext;
        struct tree_cell_head *chead;
        struct tree_cell *cell;
        
        node = rb_first(&tmap->root);
        if (node != NULL) { next = rb_next(node); }

        while (node) {
                rb_erase(node, &tmap->root);
                t = container_of(node, struct tree_node, node);
                chead = (struct tree_cell_head *)(t->val);
                ASSERT(chead != NULL);
                
                hlist_for_each_entry_safe(cell, hlnode, hlnext, &chead->head, list) {

                        ASSERT_TREECELL(cell);
                        hlist_del(&cell->list);
                        kfree(cell);
                }
                kfree(chead);
                kfree(t);

                node = next;
                if (node != NULL) { next = rb_next(node); }
        }

        ASSERT(map_is_empty((map_t *)tmap));
}

/**
 * Check multimap is empty or not.
 *
 * @return 1 if empty, or 0.
 */
int multimap_is_empty(const multimap_t *tmap)
{
        return map_is_empty((map_t *)tmap);
}

/**
 * Count items in the multimap.
 *
 * @return number of items in the multimap.
 */
int multimap_n_items(const multimap_t *tmap)
{
        struct tree_node *t;
        struct rb_node *node;
        int count, c;
        struct tree_cell_head *chead;

        count = 0;
        node = rb_first(&tmap->root);
        while (node) {
                t = container_of(node, struct tree_node, node);
                chead = (struct tree_cell_head *)(t->val);
                c = hlist_len(&chead->head);
                ASSERT(c > 0);
                count += c;
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
        multimap_t *tm;
        int i, n, count;
        u64 key;
        unsigned long val;
        struct hlist_node *node;
        struct tree_cell_head *chead;
        struct tree_cell *cell;
        
        printk_d("multimap_test begin\n");
        printk_d("hlist_head: %zu "
                 "unsigned long: %zu "
                 "tree_cell_head: %zu "
                 "tree_cell: %zu\n",
                 sizeof(struct hlist_head),
                 sizeof(unsigned long),
                 sizeof(struct tree_cell_head),
                 sizeof(struct tree_cell));
        
        /* Create. */
        printk_d("Create.\n");
        tm = multimap_create(GFP_KERNEL);

        n = multimap_n_items(tm);
        WALB_CHECK(n == 0);
        WALB_CHECK(multimap_is_empty(tm));

        /* Search in empty tree. */
        printk_d("Search in empty tree.\n");
        WALB_CHECK(multimap_lookup(tm, 0) == NULL);
        
        /* Returns error if val is TREEMAP_INVALID_VAL. */
        printk_d("Invalid value insert..\n");
        WALB_CHECK(multimap_add(tm, 0, TREEMAP_INVALID_VAL, GFP_KERNEL) != 0);

        /* Insert records. */
        printk_d("Insert records.\n");
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
        printk_d("Delete records.\n");
        for (i = 0; i < 10000; i ++) {
                key = (u64) i;

                n = multimap_lookup_n(tm, key);
                WALB_CHECK(n == 2);
                
                if (i % 2 == 0) {
                        val = multimap_del(tm, key, key + i);
                        WALB_CHECK(val != TREEMAP_INVALID_VAL);
                        WALB_CHECK(val == key + i);
                } else {
                        chead = multimap_lookup(tm, key);
                        ASSERT(chead != NULL);
                        ASSERT(chead->key == key);
                        hlist_for_each_entry(cell, node, &chead->head, list) {
                                ASSERT_TREECELL(cell);
                                val = cell->val;
                                WALB_CHECK(val == key + i || val == key + i + 1);
                        }
                }                
                if (i % 2 == 0) {
                        val = multimap_lookup_any(tm, key);
                        WALB_CHECK(val == key + i + 1);
                        
                        chead = multimap_lookup(tm, key);
                        ASSERT(chead != NULL);
                        ASSERT(chead->key == key);
                        hlist_for_each_entry(cell, node, &chead->head, list) {
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
        printk_d("Delete multiple records.\n");
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
        printk_d("Make tree map empty.\n");
        multimap_empty(tm);
        n = multimap_n_items(tm);
        WALB_CHECK(n == 0);
        WALB_CHECK(multimap_is_empty(tm));

        /* 2cd empty. */
        printk_d("2nd empty.\n");
        multimap_empty(tm);
        n = multimap_n_items(tm);
        WALB_CHECK(n == 0);
        WALB_CHECK(multimap_is_empty(tm));

        /* Random insert. */
        printk_d("Random insert.\n");
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
        printk_d("Empty and destroy.\n");
        multimap_destroy(tm);
        
        printk_d("multimap_test end\n");
        return 0;

error:
        return -1;
}

MODULE_LICENSE("Dual BSD/GPL");
