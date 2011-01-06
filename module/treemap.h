/**
 * treemap.h - Tree map header.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _TREEMAP_H
#define _TREEMAP_H

#include <linux/rbtree.h>

/**
 * DOC: Tree map using B+Tree.
 *
 * This is a thin wrapper of generic tree implementation
 * defined in <linux/rbtree.h>.
 *
 * key: u64 value. Key must be unique in the tree.
 * val: pointer. must not be NULL.
 */

/**
 * Tree node.
 */
struct tree_node {

        struct rb_node node;
        u64 key;
        void* val;
};

/**
 * Tree map.
 */
struct tree_map {

        struct rb_root root;
};

/**
 * Prototypes
 */
struct tree_map* treemap_create(gfp_t gfp_mask);
void treemap_destroy(struct tree_map *tmap);

int treemap_add(struct tree_map *tmap, u64 key, const void* val, gfp_t gfp_mask);
void* treemap_lookup(const struct tree_map *tmap, u64 key);
void* treemap_del(struct tree_map *tmap, u64 key);
void treemap_empty(struct tree_map *tmap);

int treemap_is_empty(const struct tree_map *tmap);
int treemap_n_items(const struct tree_map *tmap);

int treemap_test(void); /* For unit test. */

#endif /* _TREEMAP_H */
