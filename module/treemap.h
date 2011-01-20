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
 * DOC: map and multimap using tree structure.
 *
 * This is a thin wrapper of generic tree implementation
 * defined in <linux/rbtree.h>.
 */

/**
 * Invalid value.
 * Causion: this is not NULL.
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
 * Map and multimap data structure.
 * Data type is different but the contents are the same.
 */
typedef struct { struct rb_root root; } map_t;
typedef struct { struct rb_root root; } multimap_t;

/**
 * Tree cell head for multimap.
 */
struct tree_cell_head {

        struct hlist_head head;
        u64 key;
};

/**
 * Tree cell for multimap.
 *
 * This data structre is created by @multimap_add()
 * and deleted by @multimap_del() or @multimap_del_key().
 * Do not allocate/deallocate by yourself.
 */
struct tree_cell {
        
        struct hlist_node list;
        unsigned long val;
};

/**
 * Map curser state.
 */
enum {
        MAP_CURSER_BEGIN = 1,
        MAP_CURSER_END,
        MAP_CURSER_DATA, /* Curser indicate a key-value pair. */
        MAP_CURSER_INVALID,
};

/**
 * Map search flag.
 */
enum {
        MAP_SEARCH_BEGIN = 1, /* Begin, ignore the key. */
        MAP_SEARCH_END, /* End, ignore the key. */
        MAP_SEARCH_EQ, /* Equal to the key. */
        MAP_SEARCH_LT, /* Biggest one less than key. */
        MAP_SEARCH_LE, /* Biggest one less than or equal to the key. */
        MAP_SEARCH_GT, /* Smallest one grater than the key. */
        MAP_SEARCH_GE, /* Smallest one grater than or equal to the key. */
};

/**
 * Map curser structure.
 */
typedef struct
{
        map_t *map;
        int state;
        struct tree_node *prev;
        struct tree_node *curr;
        struct tree_node *next;
        
} map_curser_t;


/**
 * Prototypes of map operations.
 *
 * key: u64 value. Key must be unique in the tree.
 * val: unsigned long value that can be a pointer.
 *      Do not use TREEMAP_INVALID_VAL.
 */
map_t* map_create(gfp_t gfp_mask);
void map_destroy(map_t *map);

int map_add(map_t *map, u64 key, unsigned long val, gfp_t gfp_mask);
unsigned long map_lookup(const map_t *map, u64 key);
unsigned long map_del(map_t *map, u64 key);
void map_empty(map_t *map);

int map_is_empty(const map_t *map);
int map_n_items(const map_t *map);

int map_test(void); /* For unit test. */

/**
 * Prototypes for map curser operations.
 */
map_curser_t* map_curser_create(map_t *map, gfp_t gfp_mask);
void map_curser_init(map_t *map, map_curser_t *curser);
int map_curser_search(map_curser_t *curser, u64 key, int search_flag);
int map_curser_next(map_curser_t *curser);
int map_curser_prev(map_curser_t *curser);
int map_curser_begin(map_curser_t *curser);
int map_curser_end(map_curser_t *curser);
int map_curser_is_begin(map_curser_t *curser);
int map_curser_is_end(map_curser_t *curser);
int map_curser_is_valid(map_curser_t *curser);
unsigned long map_curser_get(const map_curser_t *curser);
/* int map_curser_update(map_curser_t *curser); */
/* int map_curser_delete(map_curser_t *curser); */
void map_curser_destroy(map_curser_t *curser);

int map_curser_test(void); /* For unit test. */


/**
 * Prototypes for multimap operations.
 *
 * key: u64 value.
 * val: pointer to tree_cell_head.
 */
multimap_t* multimap_create(gfp_t gfp_mask);
void multimap_destroy(multimap_t *map);

int multimap_add(multimap_t *map, u64 key, unsigned long val, gfp_t gfp_mask);
struct tree_cell_head* multimap_lookup(const multimap_t *map, u64 key);
unsigned long multimap_lookup_any(const multimap_t *map, u64 key);
int multimap_lookup_n(const multimap_t *map, u64 key);
unsigned long multimap_del(multimap_t *map, u64 key, unsigned long val);
int multimap_del_key(multimap_t *map, u64 key);
void multimap_empty(multimap_t *map);

int multimap_is_empty(const multimap_t *map);
int multimap_n_items(const multimap_t *map);

int multimap_test(void); /* For unit test. */


/**
 * Prototypes for multimap curser operations.
 */
/* Currently multimap curser operations is not supported. */

#endif /* _TREEMAP_H */
