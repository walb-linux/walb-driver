/**
 * treemap.h - Tree map header.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _TREEMAP_H
#define _TREEMAP_H

#include <linux/rbtree.h>
#include <linux/list.h>

/**
 * DOC: Tree map using B+Tree.
 *
 * This is a thin wrapper of generic tree implementation
 * defined in <linux/rbtree.h>.
 *
 * key: u64 value. Key must be unique in the tree.
 * val: unsigned long value that can be a pointer.
 */

/**
 * Invalid value.
 */
#define TREEMAP_INVALID_VAL ((unsigned long)(-1))


/**
 * Tree node.
 */
struct tree_node {

        struct rb_node node;
        u64 key;
        unsigned long val;
};

/**
 * Tree map.
 */
struct tree_map {

        struct rb_root root;
};

/**
 * Prototypes of treemap operations.
 */
struct tree_map* treemap_create(gfp_t gfp_mask);
void treemap_destroy(struct tree_map *tmap);

int treemap_add(struct tree_map *tmap, u64 key, unsigned long val, gfp_t gfp_mask);
unsigned long treemap_lookup(const struct tree_map *tmap, u64 key);
unsigned long treemap_del(struct tree_map *tmap, u64 key);
void treemap_empty(struct tree_map *tmap);

int treemap_is_empty(const struct tree_map *tmap);
int treemap_n_items(const struct tree_map *tmap);

int treemap_test(void); /* For unit test. */


/**
 * Prototypes for multimap operations.
 *
 * key: u64 value.
 * val: hlist_head
 *      assumption: (sizeof(struct hlist_head) == sizeof(unsigned long)).
 */

/**
 * Tree cell to deal with multiple value.
 *
 * This data structre is created by @multimap_add()
 * and deleted by @multimap_del() or @multimap_del_key().
 * Do not allocate/deallocate by yourself.
 */
struct tree_cell {
        
        struct hlist_node list;
        unsigned long val;
};

struct tree_map* multimap_create(gfp_t gfp_mask);
void multimap_destroy(struct tree_map *tmap);

int multimap_add(struct tree_map *tmap, u64 key, unsigned long val, gfp_t gfp_mask);
struct hlist_head* multimap_lookup(const struct tree_map *tmap, u64 key);
unsigned long multimap_lookup_any(const struct tree_map *tmap, u64 key);
int multimap_lookup_n(const struct tree_map *tmap, u64 key);
unsigned long multimap_del(struct tree_map *tmap, u64 key, unsigned long val);
int multimap_del_key(struct tree_map *tmap, u64 key);
void multimap_empty(struct tree_map *tmap);

int multimap_is_empty(const struct tree_map *tmap);
int multimap_n_items(const struct tree_map *tmap);

int multimap_test(void); /* For unit test. */


#endif /* _TREEMAP_H */
