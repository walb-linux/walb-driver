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
#include <linux/mempool.h>

/**
 * DOC: map and multimap using tree structure.
 *
 * This is a thin wrapper of generic tree implementation
 * defined in <linux/rbtree.h>.
 *
 * Key type is u64.
 * Value type is unsigned long, which can store value of
 * pointer type, unsigned int, unsigned long, or u32.
 */

/**
 * Invalid key and value.
 *
 * 0 or NULL can be normal key/value.
 */
#define TREEMAP_INVALID_KEY ((u64)(-1))
#define TREEMAP_INVALID_VAL ((unsigned long)(-1))

/**
 * Tree node.
 */
struct tree_node
{
	struct rb_node node;
	u64 key;
	unsigned long val;
};

/**
 * Tree cell head for multimap.
 */
struct tree_cell_head
{
	struct hlist_head head;
	u64 key;
};

/**
 * Tree cell for multimap.
 *
 * This data structre is created by @multimap_add()
 * and deleted by @multimap_del(), @multimap_del_key(),
 * or @multimap_cursor_del().
 * Do not allocate/deallocate by yourself.
 */
struct tree_cell
{
	struct hlist_node list;
	unsigned long val;
};

/**
 * Memory manager.
 */
struct treemap_memory_manager
{
	bool is_kmem_cache;

	mempool_t* node_pool;
	mempool_t* cell_head_pool;
	mempool_t* cell_pool;

	struct kmem_cache *node_cache;
	struct kmem_cache *cell_head_cache;
	struct kmem_cache *cell_cache;
};

/**
 * Map and multimap data structure.
 * Data type is different but the contents must be the same.
 */
struct map
{
	struct rb_root root;
	struct treemap_memory_manager *mmgr;
};
struct multimap
{
	struct rb_root root;
	struct treemap_memory_manager *mmgr;
};

/**
 * Map cursor state.
 */
enum
{
	MAP_CURSOR_BEGIN = 1,
	MAP_CURSOR_END,
	MAP_CURSOR_DATA, /* Cursor indicate a key-value pair. */
	MAP_CURSOR_INVALID,
};

/**
 * Map search flag.
 */
enum
{
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
 *
 * Calling map_add() or map_del() may invalidate the cursor.
 * For deletion, use map_cursor_del() instead.
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
 *
 * Calling multimap_add() or multimap_del() may invalidate the cursor.
 * For deletion, use multimap_cursor_del() instead.
 */
struct multimap_cursor
{
	struct map_cursor curt;

	struct tree_cell_head *head;
	struct tree_cell *cell;
};

/**
 * Memroy manager helper functions.
 */
bool initialize_treemap_memory_manager(
	struct treemap_memory_manager *mmgr, int min_nr,
	const char *node_cache_name,
	const char *cell_head_cache_name,
	const char *cell_cache_name);
bool initialize_treemap_memory_manager_kmalloc(
	struct treemap_memory_manager *mmgr, int min_nr);
void finalize_treemap_memory_manager(struct treemap_memory_manager *mmgr);

/**
 * Prototypes of map operations.
 *
 * key: u64 value. Key must be unique in the tree.
 * val: unsigned long value that can be a pointer.
 *	Do not use TREEMAP_INVALID_VAL.
 */
struct map* map_create(gfp_t gfp_mask, struct treemap_memory_manager *mmgr);
void map_init(struct map *tmap, struct treemap_memory_manager *mmgr);
void map_destroy(struct map *map);

int map_add(struct map *map, u64 key, unsigned long val, gfp_t gfp_mask);
unsigned long map_lookup(const struct map *map, u64 key);
unsigned long map_del(struct map *map, u64 key);
void map_empty(struct map *map);

int map_is_empty(const struct map *map);
int map_n_items(const struct map *map);

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
int map_cursor_is_data(struct map_cursor *cursor);
int map_cursor_is_valid(struct map_cursor *cursor);
void map_cursor_copy(struct map_cursor *dst, struct map_cursor *src);
u64 map_cursor_key(const struct map_cursor *cursor);
unsigned long map_cursor_val(const struct map_cursor *cursor);
/* int map_cursor_update(struct map_cursor *cursor); */
int map_cursor_del(struct map_cursor *cursor);
void map_cursor_destroy(struct map_cursor *cursor);

/**
 * Prototypes for multimap operations.
 *
 * key: u64 value.
 * val: pointer to tree_cell_head.
 */
struct multimap* multimap_create(gfp_t gfp_mask, struct treemap_memory_manager *mmgr);
void multimap_init(struct multimap *tmap, struct treemap_memory_manager *mmgr);
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
int multimap_cursor_is_data(struct multimap_cursor *cursor);
int multimap_cursor_is_valid(struct multimap_cursor *cursor);
void multimap_cursor_copy(
	struct multimap_cursor *dst, struct multimap_cursor *src);
unsigned long multimap_cursor_val(const struct multimap_cursor *cursor);
u64 multimap_cursor_key(const struct multimap_cursor *cursor);
int multimap_cursor_del(struct multimap_cursor *cursor);

/**
 * Assertions.
 */
#define ASSERT_TREEMAP_MEMORY_MANAGER(mmgr)		\
	ASSERT(is_valid_treemap_memory_manager(mmgr))

#define ASSERT_TREEMAP(tmap)						\
	ASSERT((tmap) && is_valid_treemap_memory_manager((tmap)->mmgr))

#define ASSERT_TREENODE(tnode)				\
	ASSERT((tnode) &&				\
		(tnode)->val != TREEMAP_INVALID_VAL)

#define ASSERT_TREECELL(tcell)				\
	ASSERT((tcell) &&				\
		(tcell)->val != TREEMAP_INVALID_VAL)

#define ASSERT_MAP_CURSOR(cursor) ASSERT(is_valid_map_cursor(cursor))
#define ASSERT_MULTIMAP_CURSOR(cursor) ASSERT(is_valid_multimap_cursor(cursor))


#endif /* TREEMAP_H_KERNEL */
