/**
 * treemap.h - Tree map operations.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef TREEMAP_H_KERNEL
#define TREEMAP_H_KERNEL

#include "check_kernel.h"

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
struct map { struct rb_root root; };
struct multimap { struct rb_root root; };

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
 * Map cursor state.
 */
enum {
        MAP_CURSOR_BEGIN = 1,
        MAP_CURSOR_END,
        MAP_CURSOR_DATA, /* Cursor indicate a key-value pair. */
        MAP_CURSOR_INVALID,
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
 * Map cursor structure.
 */
struct map_cursor
{
        struct map *map;
        int state;
        struct tree_node *prev;
        struct tree_node *curr;
        struct tree_node *next;
        
};

/**
 * Multimap cursor structure.
 */
struct multimap_cursor
{
        struct map_cursor curt;

        struct tree_cell_head *head;
        struct tree_cell *cell;
        
};


/**
 * Prototypes of map operations.
 *
 * key: u64 value. Key must be unique in the tree.
 * val: unsigned long value that can be a pointer.
 *      Do not use TREEMAP_INVALID_VAL.
 */
struct map* map_create(gfp_t gfp_mask);
void map_init(struct map *tmap);
void map_destroy(struct map *map);

int map_add(struct map *map, u64 key, unsigned long val, gfp_t gfp_mask);
unsigned long map_lookup(const struct map *map, u64 key);
unsigned long map_del(struct map *map, u64 key);
void map_empty(struct map *map);

int map_is_empty(const struct map *map);
int map_n_items(const struct map *map);

int map_test(void); /* For unit test. */

/**
 * Prototypes for map cursor operations.
 */
struct map_cursor* map_cursor_create(struct map *map, gfp_t gfp_mask);
void map_cursor_init(struct map *map, struct map_cursor *cursor);
int map_cursor_search(struct map_cursor *cursor, u64 key, int search_flag);
int map_cursor_next(struct map_cursor *cursor);
int map_cursor_prev(struct map_cursor *cursor);
int map_cursor_begin(struct map_cursor *cursor);
int map_cursor_end(struct map_cursor *cursor);
int map_cursor_is_begin(struct map_cursor *cursor);
int map_cursor_is_end(struct map_cursor *cursor);
int map_cursor_is_valid(struct map_cursor *cursor);
unsigned long map_cursor_val(const struct map_cursor *cursor);
/* int map_cursor_update(struct map_cursor *cursor); */
/* int map_cursor_delete(struct map_cursor *cursor); */
void map_cursor_destroy(struct map_cursor *cursor);

int map_cursor_test(void); /* For unit test. */


/**
 * Prototypes for multimap operations.
 *
 * key: u64 value.
 * val: pointer to tree_cell_head.
 */
struct multimap* multimap_create(gfp_t gfp_mask);
void multimap_init(struct multimap *tmap);
void multimap_destroy(struct multimap *map);

int multimap_add(struct multimap *map, u64 key, unsigned long val, gfp_t gfp_mask);
struct tree_cell_head* multimap_lookup(const struct multimap *map, u64 key);
unsigned long multimap_lookup_any(const struct multimap *map, u64 key);
int multimap_lookup_n(const struct multimap *map, u64 key);
unsigned long multimap_del(struct multimap *map, u64 key, unsigned long val);
int multimap_del_key(struct multimap *map, u64 key);
void multimap_empty(struct multimap *map);

int multimap_is_empty(const struct multimap *map);
int multimap_n_items(const struct multimap *map);

int multimap_test(void); /* For unit test. */


/**
 * Prototypes for multimap cursor operations.
 */
void multimap_cursor_init(struct multimap *map, struct multimap_cursor *cursor);
int multimap_cursor_search(struct multimap_cursor *cursor, u64 key,
                           int search_flag, int is_end);
int multimap_cursor_next(struct multimap_cursor *cursor);
int multimap_cursor_prev(struct multimap_cursor *cursor);
int multimap_cursor_begin(struct multimap_cursor *cursor);
int multimap_cursor_end(struct multimap_cursor *cursor);
int multimap_cursor_is_begin(struct multimap_cursor *cursor);
int multimap_cursor_is_end(struct multimap_cursor *cursor);
int multimap_cursor_is_valid(struct multimap_cursor *cursor);
unsigned long multimap_cursor_val(const struct multimap_cursor *cursor);
u64 multimap_cursor_key(const struct multimap_cursor *cursor);

int multimap_cursor_test(void); /* For unit test. */

/* Init/exit functions. */
bool treemap_init(void);
void treemap_exit(void);


#endif /* TREEMAP_H_KERNEL */
