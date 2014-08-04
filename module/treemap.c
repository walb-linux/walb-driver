/**
 * treemap.c - Tree map implementation.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/slab.h>
#include <linux/list.h>
#include <linux/mempool.h>

#include "walb/walb.h"
#include "walb/logger.h"
#include "walb/check.h"
#include "treemap.h"
#include "util.h" /* for debug */

/**
 * Prototypes of static functions.
 */
static struct tree_node* map_lookup_node(const struct map *tmap, u64 key);
static struct tree_node* map_lookup_node_detail(
	const struct map *tmap, u64 key, int search_flag);

static struct tree_node* map_first(const struct map *tmap);
static struct tree_node* map_last(const struct map *tmap);
static struct tree_node* map_next(const struct tree_node *t);
static struct tree_node* map_prev(const struct tree_node *t);

static void make_map_cursor_invalid(struct map_cursor *cursor);

UNUSED static int is_valid_map_cursor(const struct map_cursor *cursor);
UNUSED static int is_valid_multimap_cursor(const struct multimap_cursor *cursor);
UNUSED static bool is_valid_treemap_memory_manager(struct treemap_memory_manager *mmgr);

static int hlist_len(const struct hlist_head *head);
static struct hlist_node* hlist_prev(const struct hlist_head *head,
				const struct hlist_node *node);

static int multimap_add_newkey(
	struct multimap *tmap, u64 key, struct tree_cell *newcell, gfp_t gfp_mask);
static int multimap_add_oldkey(struct tree_cell_head *chead, struct tree_cell *newcell);

static struct tree_cell* get_tree_cell_begin(struct tree_cell_head *head);
static struct tree_cell* get_tree_cell_end(struct tree_cell_head *head);
static struct tree_cell* get_tree_cell_next(struct tree_cell *cell);
static struct tree_cell* get_tree_cell_prev(
	struct tree_cell_head *head, struct tree_cell *cell);

UNUSED static void print_map_cursor(
	const char *level, struct map_cursor *cursor);
UNUSED static void print_multimap_cursor(
	const char *level, struct multimap_cursor *cursor);

/**
 * Macros.
 */
#define alloc_node(mmgr, gfp_mask) mempool_alloc(mmgr->node_pool, gfp_mask)
#define free_node(mmgr, tnode) mempool_free(tnode, mmgr->node_pool);
#define alloc_cell_head(mmgr, gfp_mask) mempool_alloc(mmgr->cell_head_pool, gfp_mask)
#define free_cell_head(mmgr, chead) mempool_free(chead, mmgr->cell_head_pool)
#define alloc_cell(mmgr, gfp_mask) mempool_alloc(mmgr->cell_pool, gfp_mask)
#define free_cell(mmgr, cell) mempool_free(cell, mmgr->cell_pool)

/*******************************************************************************
 * Static functions.
 *******************************************************************************/

/**
 * Lookup tree_node with the key in the tree map.
 *
 * @return struct tree_node if found, or NULL.
 */
static struct tree_node* map_lookup_node(const struct map *tmap, u64 key)
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
static struct tree_node* map_lookup_node_detail(const struct map *tmap, u64 key, int search_flag)
{
	struct rb_node *node;
	struct tree_node *t = NULL;

	ASSERT_TREEMAP(tmap);
	node = tmap->root.rb_node;

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
	if (!t) { /* Empty tree. */
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
			ASSERT(!t || t->key < key);
			return t;

		} else if (t->key < key) {
			ASSERT(!map_next(t) || (map_next(t))->key > key);
			return t;
		} else {
			t = map_prev(t);
			ASSERT(!t || t->key < key);
			return t;
		}
		break;

	case MAP_SEARCH_GT:
	case MAP_SEARCH_GE:

		if (t->key == key) {
			ASSERT(search_flag == MAP_SEARCH_GT);
			t = map_next(t);
			ASSERT(!t || t->key > key);
			return t;

		} else if (t->key < key) {
			t = map_next(t);
			ASSERT(!t || t->key > key);
			return t;
		} else {
			ASSERT(!map_prev(t) || (map_prev(t))->key < key);
			return t;
		}
		break;
	default:
		BUG();
	}
	return NULL;
}

/**
 * Get first node in the map.
 */
static struct tree_node* map_first(const struct map *tmap)
{
	struct rb_node *node;

	if (!tmap) { return NULL; }
	node = rb_first(&tmap->root);
	if (!node) { return NULL; }
	return container_of(node, struct tree_node, node);
}

/**
 * Get last node in the map.
 */
static struct tree_node* map_last(const struct map *tmap)
{
	struct rb_node *node;

	if (!tmap) { return NULL; }
	node = rb_last(&tmap->root);
	if (!node) { return NULL; }
	return container_of(node, struct tree_node, node);
}

/**
 * Get next node in the map.
 */
static struct tree_node* map_next(const struct tree_node *t)
{
	struct rb_node *node;

	if (!t) { return NULL; }
	node = rb_next(&t->node);
	if (!node) { return NULL; }
	return container_of(node, struct tree_node, node);
}

/**
 * Get previous node in the map.
 */
static struct tree_node* map_prev(const struct tree_node *t)
{
	struct rb_node *node;

	if (!t) { return NULL; }
	node = rb_prev(&t->node);
	if (!node) { return NULL; }
	return container_of(node, struct tree_node, node);
}

/**
 * Make cursor invalid state.
 */
static void make_map_cursor_invalid(struct map_cursor *cursor)
{
	cursor->state = MAP_CURSOR_INVALID;
	cursor->curr = NULL;
	cursor->prev = NULL;
	cursor->next = NULL;

	ASSERT_MAP_CURSOR(cursor);
}

/**
 * Check validness of map cursor.
 *
 * @return non-zero if valid, or 0.
 */
static int is_valid_map_cursor(const struct map_cursor *cursor)
{
	return cursor && cursor->map && (
		(cursor->state == MAP_CURSOR_BEGIN &&
			!cursor->prev && !cursor->curr) ||
		(cursor->state == MAP_CURSOR_END &&
			!cursor->curr && !cursor->next) ||
		(cursor->state == MAP_CURSOR_DATA &&
			cursor->curr) ||
		(cursor->state == MAP_CURSOR_INVALID));
}

/**
 * Check validness of multimap cursor.
 *
 * @return non-zero if valid, or 0.
 */
static int is_valid_multimap_cursor(const struct multimap_cursor *cursor)
{
	const struct map_cursor *cur;

	if (!cursor) { return 0; }
	cur = &cursor->curt;

	return is_valid_map_cursor(cur) && (
		(cur->state == MAP_CURSOR_BEGIN &&
			!cursor->head && !cursor->cell) ||
		(cur->state == MAP_CURSOR_END &&
			!cursor->head && !cursor->cell) ||
		(cur->state == MAP_CURSOR_DATA &&
			cursor->head && cursor->cell) ||
		(cur->state == MAP_CURSOR_INVALID));
}

/**
 * Check validness of treemap memory manager.
 */
static bool is_valid_treemap_memory_manager(struct treemap_memory_manager *mmgr)
{
	bool ret1, ret2;

	ret1 = mmgr &&
		mmgr->node_pool &&
		mmgr->cell_head_pool &&
		mmgr->cell_pool;
	ret2 = mmgr->node_cache &&
		mmgr->cell_head_cache &&
		mmgr->cell_cache;

	if (mmgr->is_kmem_cache) {
		return ret1 && ret2;
	} else {
		return ret1;
	}
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

	if (!head) { return -1; }

	hlist_for_each(node, head) {
		count++;
	}
	return count;
}

/**
 * Get previous hlist node if exists.
 *
 * @return pointer to previous node if found, or NULL.
 */
static struct hlist_node* hlist_prev(const struct hlist_head *head,
				const struct hlist_node *node)
{
	struct hlist_head *tmp_head;

	if (!head || !node) { return NULL; }

	/* First node check. */
	tmp_head = container_of(node->pprev, struct hlist_head, first);
	if (tmp_head->first == head->first) { return NULL; }

	/* Previous node exists. */
	return container_of(node->pprev, struct hlist_node, next);
}

/**
 * Add non-existing key into the multimap.
 *
 * @tmap multimap.
 * @key key
 * @newcell prepared inserted value.
 *
 * @return 0 in success,
 *	   -ENOMEM if no memory.
 *	   -EEXIST if key already exists.
 */
static int multimap_add_newkey(
	struct multimap *tmap, u64 key,
	struct tree_cell *newcell, gfp_t gfp_mask)
{
	int ret;
	struct tree_cell_head *newhead;

	/* Allocate and initialize new tree cell head. */
	newhead = alloc_cell_head(tmap->mmgr, gfp_mask);
	if (!newhead) {
		LOGe("memory allocation failed.\n");
		return -ENOMEM;
	}
	newhead->key = key;
	INIT_HLIST_HEAD(&newhead->head);
	hlist_add_head(&newcell->list, &newhead->head);
	ASSERT(!hlist_empty(&newhead->head));

	/* Add to the map. */
	ret = map_add((struct map *)tmap, key, (unsigned long)newhead, gfp_mask);
	if (ret != 0) {
		free_cell_head(tmap->mmgr, newhead);
		LOGe("map_add failed.\n");
		ASSERT(ret != -EINVAL);
	}
	return ret;
}

/**
 * Add for existing key into multimap.
 *
 * @tree_cell_head tree cell head.
 * @newcell cell to be added.
 *
 * @return 0 in success, -EEXIST when the same key-value pair already exists.
 */
static int multimap_add_oldkey(struct tree_cell_head *chead, struct tree_cell *newcell)
{
	struct tree_cell *cell;
	struct hlist_head *hlhead;
	struct hlist_node *hlnext;
	int found;

	ASSERT(chead);
	ASSERT(newcell);

	hlhead = &chead->head;
	ASSERT(!hlist_empty(hlhead));

	found = 0;
	hlist_for_each_entry_safe(cell, hlnext, hlhead, list) {
		ASSERT_TREECELL(cell);
		if (cell->val == newcell->val) { found++; break; }
	}
	if (found == 0) {
		hlist_add_head(&newcell->list, hlhead);
		return 0;
	}
	return -EEXIST;
}

/**
 * Get first tree_cell in the list of tree_cell_head.
 *
 *
 * @return tree cell in success, or NULL.
 */
static struct tree_cell* get_tree_cell_begin(struct tree_cell_head *head)
{
	if (!head || !head->head.first) { return NULL; }
	return hlist_entry(head->head.first, struct tree_cell, list);
}

/**
 * Get last tree_cell in the list of tree_cell_head.
 *
 * @return tree cell in success, or NULL.
 */
static struct tree_cell* get_tree_cell_end(struct tree_cell_head *head)
{
	struct hlist_node *node;

	if (!head) { return NULL; }

	node = head->head.first;
	if (!node) { return NULL; }

	while (node->next) { node = node->next; }
	ASSERT(node);

	return hlist_entry(node, struct tree_cell, list);
}

/**
 * Get next tree_cell.
 *
 * @return next tree cell in success, or NULL.
 */
static struct tree_cell* get_tree_cell_next(struct tree_cell *cell)
{
	if (!cell || !cell->list.next) { return NULL; }
	return hlist_entry(cell->list.next, struct tree_cell, list);
}

/**
 * Get previous tree_cell.
 *
 * @return previous tree cell in success, or NULL.
 */
static struct tree_cell* get_tree_cell_prev(struct tree_cell_head *head, struct tree_cell *cell)
{
	struct hlist_node *prev;

	if (!head || !cell) { return NULL; }
	prev = hlist_prev(&head->head, &cell->list);
	if (!prev) { return NULL; }
	return hlist_entry(prev, struct tree_cell, list);
}

/**
 * Print multimap cursor.
 */
static void print_map_cursor(const char *level, struct map_cursor *cursor)
{
	ASSERT(cursor);
	printk("%s"
		"map %p, state %d prev %p curr %p next %p\n",
		level, cursor->map, cursor->state,
		cursor->prev, cursor->curr, cursor->next);
}

/**
 * Print multimap cursor.
 */
static void print_multimap_cursor(const char *level, struct multimap_cursor *cursor)
{
	ASSERT(cursor);
	printk("%s"
		"multimap %p, state %d prev %p curr %p next %p head %p cell %p\n",
		level, cursor->curt.map, cursor->curt.state,
		cursor->curt.prev, cursor->curt.curr, cursor->curt.next,
		cursor->head, cursor->cell);
}

/*******************************************************************************
 * Global functions.
 *******************************************************************************/

/**
 * Create a treemap memory manager (using kmem_cache).
 */
bool initialize_treemap_memory_manager(
	struct treemap_memory_manager *mmgr, int min_nr,
	const char *node_cache_name,
	const char *cell_head_cache_name,
	const char *cell_cache_name)
{
	ASSERT(mmgr);
	ASSERT(min_nr > 0);
	ASSERT(node_cache_name);
	ASSERT(cell_head_cache_name);
	ASSERT(cell_cache_name);

	memset(mmgr, 0, sizeof(struct treemap_memory_manager));
	mmgr->is_kmem_cache = true;

	mmgr->node_cache = kmem_cache_create(
		node_cache_name,
		sizeof(struct tree_node), 0, 0, NULL);
	if (!mmgr->node_cache) { goto error; }

	mmgr->cell_head_cache = kmem_cache_create(
		cell_head_cache_name,
		sizeof(struct tree_cell_head), 0, 0, NULL);
	if (!mmgr->cell_head_cache) { goto error; }

	mmgr->cell_cache = kmem_cache_create(
		cell_cache_name,
		sizeof(struct tree_cell), 0, 0, NULL);
	if (!mmgr->cell_cache) { goto error; }

	mmgr->node_pool = mempool_create_slab_pool(min_nr, mmgr->node_cache);
	if (!mmgr->node_pool) { goto error; }

	mmgr->cell_head_pool = mempool_create_slab_pool(min_nr, mmgr->cell_head_cache);
	if (!mmgr->cell_head_pool) { goto error; }

	mmgr->cell_pool = mempool_create_slab_pool(min_nr, mmgr->cell_cache);
	if (!mmgr->cell_pool) { goto error; }

	return true;

error:
	finalize_treemap_memory_manager(mmgr);
	return false;
}

/**
 * Initialize a treemap memory manager using kmalloc.
 */
bool initialize_treemap_memory_manager_kmalloc(
	struct treemap_memory_manager *mmgr, int min_nr)
{
	ASSERT(mmgr);
	ASSERT(min_nr > 0);

	memset(mmgr, 0, sizeof(struct treemap_memory_manager));
	mmgr->is_kmem_cache = false;

	mmgr->node_pool = mempool_create_kmalloc_pool(
		min_nr, sizeof(struct tree_node));
	if (!mmgr->node_pool) { goto error; }

	mmgr->cell_head_pool = mempool_create_kmalloc_pool(
		min_nr, sizeof(struct tree_cell_head));
	if (!mmgr->cell_head_pool) { goto error; }

	mmgr->cell_pool = mempool_create_kmalloc_pool(
		min_nr, sizeof(struct tree_cell));
	if (!mmgr->cell_pool) { goto error; }

	return true;

error:
	finalize_treemap_memory_manager(mmgr);
	return false;
}

/**
 * Destroy a treemap memory manager (using kmem_cache).
 */
void finalize_treemap_memory_manager(struct treemap_memory_manager *mmgr)
{
	if (!mmgr) { return; }

	if (mmgr->cell_pool) {
		mempool_destroy(mmgr->cell_pool);
		mmgr->cell_pool = NULL;
	}
	if (mmgr->cell_head_pool) {
		mempool_destroy(mmgr->cell_head_pool);
		mmgr->cell_head_pool = NULL;
	}
	if (mmgr->node_pool) {
		mempool_destroy(mmgr->node_pool);
		mmgr->node_pool = NULL;
	}

	if (mmgr->is_kmem_cache) {
		if (mmgr->cell_cache) {
			kmem_cache_destroy(mmgr->cell_cache);
			mmgr->cell_cache = NULL;
		}
		if (mmgr->cell_head_cache) {
			kmem_cache_destroy(mmgr->cell_head_cache);
			mmgr->cell_head_cache = NULL;
		}
		if (mmgr->node_cache) {
			kmem_cache_destroy(mmgr->node_cache);
			mmgr->node_cache = NULL;
		}
	}
}

/**
 * Create tree map.
 */
struct map* map_create(gfp_t gfp_mask, struct treemap_memory_manager *mmgr)
{
	struct map *tmap;

	ASSERT(mmgr);

	tmap = kmalloc(sizeof(struct map), gfp_mask);
	if (!tmap) {
		LOGe("map_create: memory allocation failed.\n");
		return NULL;
	}
	map_init(tmap, mmgr);
	return tmap;
}

/**
 * Initialize map structure.
 */
void map_init(struct map* tmap, struct treemap_memory_manager *mmgr)
{
	ASSERT(tmap);
	ASSERT(mmgr);

	tmap->root.rb_node = NULL;
	tmap->mmgr = mmgr;
	ASSERT_TREEMAP(tmap);
}

/**
 * Destroy tree map.
 */
void map_destroy(struct map *tmap)
{
	if (tmap) {
		ASSERT_TREEMAP(tmap);
		map_empty(tmap);
		kfree(tmap);
	}
}

/**
 * Add key-value pair to the tree map.
 *
 * @return 0 in success,
 *	   -ENOMEM if no memory.
 *	   -EEXIST if key already exists.
 *	   -EINVAL if value is invalid.
 */
int map_add(struct map *tmap, u64 key, unsigned long val, gfp_t gfp_mask)
{
	struct tree_node *newnode, *t;
	struct rb_node **childp, *parent;

	ASSERT_TREEMAP(tmap);

	if (val == TREEMAP_INVALID_VAL) {
		LOGe("Val must not be TREEMAP_INVALID_VAL.\n");
		return -EINVAL;
	}

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
			return -EEXIST;
		}
	}

	/* Generate new node. */
	newnode = alloc_node(tmap->mmgr, gfp_mask);
	if (!newnode) {
		LOGe("allocation failed.\n");
		return -ENOMEM;
	}
	newnode->key = key;
	newnode->val = val;

	/* Add new node and rebalance tree. */
	rb_link_node(&newnode->node, parent, childp);
	rb_insert_color(&newnode->node, &tmap->root);

	return 0;
}

/**
 * Lookup value with the key in the tree map.
 *
 * @return value if found, or TREEMAP_INVALID_VAL.
 */
unsigned long map_lookup(const struct map *tmap, u64 key)
{
	struct tree_node *t;

	ASSERT_TREEMAP(tmap);

	t = map_lookup_node(tmap, key);
	if (t) {
		ASSERT_TREENODE(t);
		return t->val;
	}
	return TREEMAP_INVALID_VAL;
}

/**
 * Delete key-value pair from the tree map.
 *
 * @return value if found, or TREEMAP_INVALID_VAL.
 */
unsigned long map_del(struct map *tmap, u64 key)
{
	struct tree_node *t;
	unsigned long val;

	ASSERT_TREEMAP(tmap);

	t = map_lookup_node(tmap, key);
	if (!t) {
		return TREEMAP_INVALID_VAL;
	}

	ASSERT_TREENODE(t);
	ASSERT(key == t->key);
	val = t->val;
	rb_erase(&t->node, &tmap->root);
	free_node(tmap->mmgr, t);
	return val;
}

/**
 * Make the tree map empty.
 */
void map_empty(struct map *tmap)
{
	struct tree_node *t;
	struct rb_node *node, *next;

	ASSERT_TREEMAP(tmap);

	node = rb_first(&tmap->root);
	if (node) { next = rb_next(node); }

	while (node) {
		rb_erase(node, &tmap->root);
		t = container_of(node, struct tree_node, node);
		free_node(tmap->mmgr, t);

		node = next;
		if (node) { next = rb_next(node); }
	}

	ASSERT(map_is_empty(tmap));
}

/**
 * Check tree map is empty or not.
 *
 * @return 1 if empty, or 0.
 */
int map_is_empty(const struct map *tmap)
{
	ASSERT_TREEMAP(tmap);
	return rb_first(&tmap->root) == NULL;
}

/**
 * Count items in the tree map.
 *
 * @return number of items in the tree map.
 */
int map_n_items(const struct map *tmap)
{
	struct rb_node *node;
	int count = 0;

	ASSERT_TREEMAP(tmap);

	node = rb_first(&tmap->root);
	while (node) {
		count++;
		node = rb_next(node);
	}

	return count;
}

/*******************************************************************************
 * Map cursor functions.
 *******************************************************************************/

/**
 * Allocate cursor data.
 */
struct map_cursor* map_cursor_create(struct map *map, gfp_t gfp_mask)
{
	struct map_cursor *cursor;

	cursor = kmalloc(sizeof(struct map_cursor), gfp_mask);
	if (!cursor) { return NULL; }

	map_cursor_init(map, cursor);
	return cursor;
}

/**
 * Initialize cursor data.
 */
void map_cursor_init(struct map *map, struct map_cursor *cursor)
{
	ASSERT(cursor);

	cursor->map = map;

	cursor->state = MAP_CURSOR_INVALID;
	cursor->curr = NULL;
	cursor->prev = NULL;
	cursor->next = NULL;

	ASSERT_MAP_CURSOR(cursor);
}

/**
 * Search key and set cursor.
 *
 * @return Non-zero if found, or 0.
 */
int map_cursor_search(struct map_cursor* cursor, u64 key, int search_flag)
{
	if (!cursor) { return 0; }

	ASSERT_MAP_CURSOR(cursor);

	switch (search_flag) {

	case MAP_SEARCH_BEGIN:
		map_cursor_begin(cursor);
		break;

	case MAP_SEARCH_END:
		map_cursor_end(cursor);
		break;

	case MAP_SEARCH_EQ:
	case MAP_SEARCH_LT:
	case MAP_SEARCH_LE:
	case MAP_SEARCH_GT:
	case MAP_SEARCH_GE:
		cursor->curr = map_lookup_node_detail(cursor->map, key, search_flag);
		if (!cursor->curr) {
			make_map_cursor_invalid(cursor);
		} else {
			cursor->state = MAP_CURSOR_DATA;
			cursor->prev = map_prev(cursor->curr);
			cursor->next = map_next(cursor->curr);
		}
		break;

	default:
		BUG();
	}

	ASSERT_MAP_CURSOR(cursor);
	return cursor->state != MAP_CURSOR_INVALID;
}

/**
 * Go forward cursor a step.
 *
 * @return Non-zero if new current is data, or 0.
 */
int map_cursor_next(struct map_cursor *cursor)
{
	if (!cursor) { return 0; }
	ASSERT_MAP_CURSOR(cursor);

	switch (cursor->state) {

	case MAP_CURSOR_BEGIN:
	case MAP_CURSOR_DATA:
		cursor->prev = cursor->curr;
		cursor->curr = cursor->next;
		cursor->next = map_next(cursor->curr);
		if (cursor->curr) {
			cursor->state = MAP_CURSOR_DATA;
		} else {
			cursor->state = MAP_CURSOR_END;
		}
		break;

	case MAP_CURSOR_END:
	case MAP_CURSOR_INVALID:
		/* do nothing. */
		break;

	default:
		BUG();
	}
	ASSERT_MAP_CURSOR(cursor);

	return cursor->state == MAP_CURSOR_DATA;
}

/**
 * Go backward cursor a step.
 *
 * @return Non-zero if new current is data, or 0.
 */
int map_cursor_prev(struct map_cursor *cursor)
{
	if (!cursor) { return 0; }
	ASSERT_MAP_CURSOR(cursor);

	switch (cursor->state) {

	case MAP_CURSOR_END:
	case MAP_CURSOR_DATA:
		cursor->next = cursor->curr;
		cursor->curr = cursor->prev;
		cursor->prev = map_prev(cursor->curr);
		if (cursor->curr) {
			cursor->state = MAP_CURSOR_DATA;
		} else {
			cursor->state = MAP_CURSOR_BEGIN;
		}
		break;

	case MAP_CURSOR_BEGIN:
	case MAP_CURSOR_INVALID:
		/* do nothing. */
		break;

	default:
		BUG();
	}
	ASSERT_MAP_CURSOR(cursor);

	return cursor->state == MAP_CURSOR_DATA;
}

/**
 * Set cursor at begin.
 *
 * @return 1 in success, or 0.
 */
int map_cursor_begin(struct map_cursor *cursor)
{
	if (!cursor) { return 0; }

	ASSERT_MAP_CURSOR(cursor);

	cursor->state = MAP_CURSOR_BEGIN;
	cursor->prev = NULL;
	cursor->curr = NULL;
	cursor->next = map_first(cursor->map);

	ASSERT_MAP_CURSOR(cursor);
	return 1;
}

/**
 * Set cursor at end.
 *
 * @return 1 in success, or 0.
 */
int map_cursor_end(struct map_cursor *cursor)
{
	if (!cursor) { return 0; }

	ASSERT_MAP_CURSOR(cursor);

	cursor->state = MAP_CURSOR_END;
	cursor->prev = map_last(cursor->map);
	cursor->curr = NULL;
	cursor->next = NULL;

	ASSERT_MAP_CURSOR(cursor);
	return 1;
}

/**
 * Check cursor is at begin.
 */
int map_cursor_is_begin(struct map_cursor *cursor)
{
	return cursor && cursor->state == MAP_CURSOR_BEGIN;
}

/**
 * Check cursor is at end.
 */
int map_cursor_is_end(struct map_cursor *cursor)
{
	return cursor && cursor->state == MAP_CURSOR_END;
}

/**
 * Check cursor is data.
 */
int map_cursor_is_data(struct map_cursor *cursor)
{
	return cursor && cursor->state == MAP_CURSOR_DATA;
}

/**
 * Check cursor is valid.
 */
int map_cursor_is_valid(struct map_cursor *cursor)
{
	return is_valid_map_cursor(cursor);
}

/**
 * Copy map cursor.
 */
void map_cursor_copy(struct map_cursor *dst, struct map_cursor *src)
{
	ASSERT(dst);
	ASSERT(map_cursor_is_valid(src));

	dst->map = src->map;
	dst->state = src->state;
	dst->prev = src->prev;
	dst->curr = src->curr;
	dst->next = src->next;
}

/**
 * Get key of the cursor
 *
 * RETURN:
 *   key if cursor points data, or TREEMAP_INVALID_KEY.
 */
u64 map_cursor_key(const struct map_cursor *cursor)
{
	if (!cursor) { return TREEMAP_INVALID_KEY; }

	ASSERT_MAP_CURSOR(cursor);
	if (cursor->state != MAP_CURSOR_DATA) {
		return TREEMAP_INVALID_KEY;
	}
	ASSERT(cursor->curr);
	return cursor->curr->key;
}

/**
 * Get value of the cursor.
 *
 * @return value if cursor points data, or TREEMAP_INVALID_VAL.
 */
unsigned long map_cursor_val(const struct map_cursor *cursor)
{
	if (!cursor) { return TREEMAP_INVALID_VAL; }

	ASSERT_MAP_CURSOR(cursor);
	if (cursor->state != MAP_CURSOR_DATA) {
		return TREEMAP_INVALID_VAL;
	}
	ASSERT(cursor->curr);
	return cursor->curr->val;
}

/**
 * Destroy cursor.
 *
 * @cursor cursor created by @map_cursor_create().
 */
void map_cursor_destroy(struct map_cursor *cursor)
{
	kfree(cursor);
}

/**
 * Delete the item at the cursor.
 * The cursor will indicate the next.
 *
 * RETURN:
 *   Non-zero if deletion succeeded, or 0.
 */
int map_cursor_del(struct map_cursor *cursor)
{
	struct tree_node *t;
	struct tree_node *prev;

	ASSERT_MAP_CURSOR(cursor);

	if (cursor->state != MAP_CURSOR_DATA) {
		return 0;
	}

	/* Backup */
	t = cursor->curr;
	ASSERT_TREENODE(t);
	prev = cursor->prev;

	/* Goto next. */
	map_cursor_next(cursor);
	ASSERT(cursor->state == MAP_CURSOR_DATA || cursor->state == MAP_CURSOR_END);

	/* Restore previous node information. */
	cursor->prev = prev;

	/* Delete the node. */
	rb_erase(&t->node, &cursor->map->root);
	free_node(cursor->map->mmgr, t);

	return 1;
}

/*******************************************************************************
 * Multimap_* functions.
 *******************************************************************************/

/**
 * Create multimap.
 */
struct multimap* multimap_create(gfp_t gfp_mask, struct treemap_memory_manager *mmgr)
{
	struct multimap *tmap;

	ASSERT(sizeof(struct hlist_head) == sizeof(unsigned long));
	ASSERT(mmgr);

	tmap = kmalloc(sizeof(struct multimap), gfp_mask);
	if (!tmap) {
		LOGe("multimap_create: memory allocation failed.\n");
		return NULL;
	}
	multimap_init(tmap, mmgr);
	return tmap;
}

/**
 * Initialize multimap structure.
 */
void multimap_init(struct multimap *tmap, struct treemap_memory_manager *mmgr)
{
	ASSERT(tmap);
	ASSERT(mmgr);

	tmap->root.rb_node = NULL;
	tmap->mmgr = mmgr;
	ASSERT_TREEMAP(tmap);
}

/**
 * Destroy multimap.
 */
void multimap_destroy(struct multimap *tmap)
{
	if (!tmap) { return; }
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
 * @return 0 in success,
 *	   -ENOMEM if no memory.
 *	   -EEXIST if key-value pair already exists.
 *	   -EINVAL if value is invalid.
 */
int multimap_add(struct multimap *tmap, u64 key, unsigned long val, gfp_t gfp_mask)
{
	struct tree_node *t;
	struct tree_cell_head *chead;
	struct tree_cell *newcell;
	int ret = 0;

	if (val == TREEMAP_INVALID_VAL) {
		LOGe("Val must not be TREEMAP_INVALID_VAL.\n");
		return -EINVAL;
	}

	/* Prepare new cell. */
	newcell = alloc_cell(tmap->mmgr, gfp_mask);
	if (!newcell) {
		LOGe("memory allocation failed.\n");
		return -ENOMEM;
	}
	newcell->val = val;
	ASSERT_TREECELL(newcell);

	t = map_lookup_node((struct map *)tmap, key);

	if (t) {
		/* There is already record(s) with that key. */
		ASSERT(t->key == key);
		/* tree_node's val is pointer to tree_cell_head. */
		chead = (struct tree_cell_head *)(t->val);
		ASSERT(chead->key == key);

		ret = multimap_add_oldkey(chead, newcell);
		if (ret) { LOGd("multimap_add_oldkey() failed.\n"); }
	} else {
		/* There is no record with that key. */
		ret = multimap_add_newkey(tmap, key, newcell, gfp_mask);
		if (ret) { LOGd("multimap_add_newkey() failed.\n"); }
	}

	if (ret) { free_cell(tmap->mmgr, newcell); }
	return ret;
}

/**
 * Lookup values with the key in the multimap.
 *
 * @return hlist of value if found, or NULL.
 *   DO NOT free returned list head and its items by yourself.
 *   DO NOT call @multimap_del() or @multimap_del_key()
 *   before scan the list.
 */
struct tree_cell_head* multimap_lookup(const struct multimap *tmap, u64 key)
{
	struct tree_node *t;

	t = map_lookup_node((struct map *)tmap, key);
	if (!t) { return NULL; }
	return (struct tree_cell_head *)(t->val);
}

/**
 * Lookup first found value with the key in the multimap.
 *
 * @return first found value, or TREEMAP_INVALID_VAL.
 */
unsigned long multimap_lookup_any(const struct multimap *tmap, u64 key)
{
	struct tree_cell_head *chead;
	struct tree_cell *cell;

	chead = multimap_lookup(tmap, key);

	if (!chead) {
		return TREEMAP_INVALID_VAL;
	}

	ASSERT(!hlist_empty(&chead->head));
	cell = hlist_entry(chead->head.first, struct tree_cell, list);
	ASSERT_TREECELL(cell);
	return cell->val;
}

/**
 * Number of records with the key in the multimap.
 *
 * @return number of records.
 */
int multimap_lookup_n(const struct multimap *tmap, u64 key)
{
	int count;
	struct tree_cell_head *chead;

	chead = multimap_lookup(tmap, key);
	if (!chead) {
		return 0;
	}
	ASSERT(!hlist_empty(&chead->head));
	count = hlist_len(&chead->head);
	ASSERT(count >= 0);
	return count;
}

/**
 * Delete key-value pair from the multimap.
 *
 * @return value if found, or TREEMAP_INVALID_VAL.
 */
unsigned long multimap_del(struct multimap *tmap, u64 key, unsigned long val)
{
	struct tree_node *t;
	struct tree_cell *cell;
	struct tree_cell_head *chead;
	struct hlist_node *hlnext;
	int found;
	unsigned long retval = TREEMAP_INVALID_VAL;
	UNUSED unsigned long ret;

	t = map_lookup_node((struct map *)tmap, key);
	if (!t) { return TREEMAP_INVALID_VAL; }

	ASSERT(t->key == key);
	chead = (struct tree_cell_head *)(t->val);
	ASSERT(chead->key == key);
	ASSERT(!hlist_empty(&chead->head));

	found = 0;
	hlist_for_each_entry_safe(cell, hlnext, &chead->head, list) {
		ASSERT_TREECELL(cell);
		if (cell->val == val) {
			found++;
			hlist_del(&cell->list);
			retval = cell->val;
			free_cell(tmap->mmgr, cell);
		}
	}
	ASSERT(found == 0 || found == 1);

	if (hlist_empty(&chead->head)) {
		ASSERT(found == 1);
		ret = map_del((struct map *)tmap, key);
		ASSERT(ret != TREEMAP_INVALID_VAL);
		free_cell_head(tmap->mmgr, chead);
	}
	return retval;
}

/**
 * Delete all records with the key from the multimap.
 *
 * @return number of deleted records.
 */
int multimap_del_key(struct multimap *tmap, u64 key)
{
	int found = 0;
	unsigned long p;
	struct tree_cell_head *chead;
	struct tree_cell *cell;
	struct hlist_node *hlnext;

	p = map_del((struct map *)tmap, key);
	if (p == TREEMAP_INVALID_VAL) { return 0; }

	chead = (struct tree_cell_head *)p;
	ASSERT(chead);
	ASSERT(!hlist_empty(&chead->head));

	hlist_for_each_entry_safe(cell, hlnext, &chead->head, list) {
		ASSERT_TREECELL(cell);
		found++;
		hlist_del(&cell->list);
		free_cell(tmap->mmgr, cell);
	}
	ASSERT(found > 0);
	ASSERT(hlist_empty(&chead->head));
	free_cell_head(tmap->mmgr, chead);

	return found;
}

/**
 * Make the multimap empty.
 */
void multimap_empty(struct multimap *tmap)
{
	struct tree_node *t;
	struct rb_node *node, *next;
	struct hlist_node *hlnext;
	struct tree_cell_head *chead;
	struct tree_cell *cell;

	node = rb_first(&tmap->root);
	if (node) { next = rb_next(node); }

	while (node) {
		rb_erase(node, &tmap->root);
		t = container_of(node, struct tree_node, node);
		chead = (struct tree_cell_head *)(t->val);
		ASSERT(chead);

		hlist_for_each_entry_safe(cell, hlnext, &chead->head, list) {
			ASSERT_TREECELL(cell);
			hlist_del(&cell->list);
			free_cell(tmap->mmgr, cell);
		}
		free_cell_head(tmap->mmgr, chead);
		free_node(tmap->mmgr, t);

		node = next;
		if (node) { next = rb_next(node); }
	}

	ASSERT(map_is_empty((struct map *)tmap));
}

/**
 * Check multimap is empty or not.
 *
 * @return 1 if empty, or 0.
 */
int multimap_is_empty(const struct multimap *tmap)
{
	return map_is_empty((struct map *)tmap);
}

/**
 * Count items in the multimap.
 *
 * @return number of items in the multimap.
 */
int multimap_n_items(const struct multimap *tmap)
{
	struct tree_node *t;
	struct rb_node *node;
	int count;
	struct tree_cell_head *chead;

	count = 0;
	node = rb_first(&tmap->root);
	while (node) {
		int c;
		t = container_of(node, struct tree_node, node);
		chead = (struct tree_cell_head *)(t->val);
		c = hlist_len(&chead->head);
		ASSERT(c > 0);
		count += c;
		node = rb_next(node);
	}
	return count;
}

/*******************************************************************************
 * Multimap cursor functions.
 *******************************************************************************/

/**
 * Initialize cursor data.
 */
void multimap_cursor_init(struct multimap *map, struct multimap_cursor *cursor)
{
	ASSERT(cursor);

	map_cursor_init((struct map *)map, &cursor->curt);

	cursor->head = NULL;
	cursor->cell = NULL;
}

/**
 * Search key and set cursor.
 *
 * @is_end if zero, cursor will be set of the first item with the key.
 *	   else, cursor will be set of the end item with the key.
 *
 * @return 1 if found, or 0.
 */
int multimap_cursor_search(struct multimap_cursor *cursor, u64 key, int search_flag, int is_end)
{
	struct map_cursor *cur = NULL;
	unsigned long val;

	if (!cursor) { goto error0; }
	cur = &cursor->curt;

	ASSERT_MULTIMAP_CURSOR(cursor);

	if (!map_cursor_search(cur, key, search_flag)) {
		goto error0;
	}

	switch (search_flag) {

	case MAP_SEARCH_BEGIN:
		ASSERT(cur->state == MAP_CURSOR_BEGIN);
		cursor->head = NULL;
		cursor->cell = NULL;
		break;

	case MAP_SEARCH_END:
		ASSERT(cur->state == MAP_CURSOR_END);
		cursor->head = NULL;
		cursor->cell = NULL;
		break;

	case MAP_SEARCH_EQ:
	case MAP_SEARCH_LT:
	case MAP_SEARCH_LE:
	case MAP_SEARCH_GT:
	case MAP_SEARCH_GE:

		ASSERT(cur->state == MAP_CURSOR_DATA);

		val = map_cursor_val(cur);
		ASSERT(val != TREEMAP_INVALID_VAL);
		cursor->head = (struct tree_cell_head *)val;
		ASSERT(cursor->head != NULL);

		if (is_end) {
			cursor->cell = get_tree_cell_end(cursor->head);
		} else {
			cursor->cell = get_tree_cell_begin(cursor->head);
		}
		break;

	default:
		BUG();
	}

	return 1;

error0:
	ASSERT(cur->state == MAP_CURSOR_INVALID);
	return 0;
}

/**
 * Go forward cursor a step.
 *
 * @return Non-zero if new curent is data, or 0.
 */
int multimap_cursor_next(struct multimap_cursor *cursor)
{
	struct map_cursor *cur = NULL;
	struct tree_cell *cell;

	ASSERT_MULTIMAP_CURSOR(cursor);
	cur = &cursor->curt;

	switch (cur->state) {

	case MAP_CURSOR_BEGIN:

		if (map_cursor_next(cur)) {
			cursor->head = (struct tree_cell_head *)
				map_cursor_val(cur);
			cursor->cell = get_tree_cell_begin(cursor->head);
		} else {
			cursor->head = NULL;
			cursor->cell = NULL;
		}
		break;

	case MAP_CURSOR_DATA:

		cell = get_tree_cell_next(cursor->cell);
		if (cell) {
			cursor->cell = cell;

		} else if (map_cursor_next(cur)) {

			cursor->head = (struct tree_cell_head *)
				map_cursor_val(cur);
			cursor->cell = get_tree_cell_begin(cursor->head);
		} else {
			cursor->head = NULL;
			cursor->cell = NULL;
		}

		break;

	case MAP_CURSOR_END:
	case MAP_CURSOR_INVALID:
		/* do nothing */
		break;

	default:
		BUG();
	}
	ASSERT_MULTIMAP_CURSOR(cursor);

	return (cur->state == MAP_CURSOR_DATA);
}

/**
 * Go backward cursor a step.
 *
 * @return Non-zero if new current is data, or 0.
 */
int multimap_cursor_prev(struct multimap_cursor *cursor)
{
	struct map_cursor *cur = NULL;
	struct tree_cell *cell;

	ASSERT_MULTIMAP_CURSOR(cursor);
	cur = &cursor->curt;


	switch (cur->state) {

	case MAP_CURSOR_END:

		if (map_cursor_prev(cur)) {
			cursor->head = (struct tree_cell_head *)
				map_cursor_val(cur);
			cursor->cell = get_tree_cell_end(cursor->head);
		} else {
			cursor->head = NULL;
			cursor->cell = NULL;
		}
		break;

	case MAP_CURSOR_DATA:

		cell = get_tree_cell_prev(cursor->head, cursor->cell);
		if (cell) {
			cursor->cell = cell;

		} else if (map_cursor_prev(cur)) {

			cursor->head = (struct tree_cell_head *)
				map_cursor_val(cur);
			cursor->cell = get_tree_cell_end(cursor->head);
		} else {
			cursor->head = NULL;
			cursor->cell = NULL;
		}
		break;

	case MAP_CURSOR_BEGIN:
	case MAP_CURSOR_INVALID:
		/* do nothing */
		break;

	default:
		BUG();
	}
	ASSERT_MULTIMAP_CURSOR(cursor);

	return cur->state == MAP_CURSOR_DATA;
}

/**
 * Set cursor at begin.
 *
 * @return 1 in success, or 0.
 */
int multimap_cursor_begin(struct multimap_cursor *cursor)
{
	if (!cursor) { return 0; }

	ASSERT_MULTIMAP_CURSOR(cursor);

	map_cursor_begin(&cursor->curt);
	cursor->head = NULL;
	cursor->cell = NULL;

	ASSERT_MULTIMAP_CURSOR(cursor);
	return 1;
}

/**
 * Set cursor at end.
 *
 * @return 1 in success, or 0.
 */
int multimap_cursor_end(struct multimap_cursor *cursor)
{
	if (!cursor) { return 0; }

	ASSERT_MULTIMAP_CURSOR(cursor);

	map_cursor_end(&cursor->curt);
	cursor->head = NULL;
	cursor->cell = NULL;

	ASSERT_MULTIMAP_CURSOR(cursor);
	return 1;
}

/**
 * Check cursor indicates begin.
 *
 * @return Non-zero if begin, or 0.
 */
int multimap_cursor_is_begin(struct multimap_cursor *cursor)
{
	return cursor && map_cursor_is_begin(&cursor->curt);
}

/**
 * Check cursor indicates end.
 *
 * @return Non-zero if end, or 0.
 */
int multimap_cursor_is_end(struct multimap_cursor *cursor)
{
	return cursor && map_cursor_is_end(&cursor->curt);
}

/**
 * Check cursor indicates data.
 *
 * RETURN:
 *   Non-zero if data, or 0.
 */
int multimap_cursor_is_data(struct multimap_cursor *cursor)
{
	return cursor && map_cursor_is_data(&cursor->curt);
}

/**
 * Check cursor is valid.
 *
 * @return Non-zero if valid, or 0.
 */
int multimap_cursor_is_valid(struct multimap_cursor *cursor)
{
	return is_valid_multimap_cursor(cursor);
}

/**
 * Copy multimap cursor.
 */
void multimap_cursor_copy(
	struct multimap_cursor *dst, struct multimap_cursor *src)
{
	ASSERT(dst);
	ASSERT(multimap_cursor_is_valid(src));

	map_cursor_copy(&dst->curt, &src->curt);
	dst->head = src->head;
	dst->cell = src->cell;
}

/**
 * Get valud of the cursor.
 *
 * @return value if cursor points data, or TREEMAP_INVALID_VAL.
 */
unsigned long multimap_cursor_val(const struct multimap_cursor *cursor)
{
	if (!cursor) {
		return TREEMAP_INVALID_VAL;
	}

	ASSERT_MULTIMAP_CURSOR(cursor);

	if (cursor->curt.state != MAP_CURSOR_DATA) {
		return TREEMAP_INVALID_VAL;
	}
	ASSERT(cursor->cell);
	return cursor->cell->val;
}

/**
 * Get key of the cursor.
 *
 * @return key if cursor points data, or TREEMAP_INVALID_KEY;
 */
u64 multimap_cursor_key(const struct multimap_cursor *cursor)
{
	if (!cursor) {
		return TREEMAP_INVALID_KEY;
	}

	ASSERT_MULTIMAP_CURSOR(cursor);

	if (cursor->curt.state != MAP_CURSOR_DATA) {
		return TREEMAP_INVALID_KEY;
	}
	ASSERT(cursor->head);
	return cursor->head->key;
}

/**
 * Delete a key-value pair from the multimap.
 * cursor will indicate the next item of the deleted item.
 *
 * RETURN:
 *   Non-zero when the deletion succeeded, or 0.
 */
int multimap_cursor_del(struct multimap_cursor *cursor)
{
	struct multimap *mmap;
	struct multimap_cursor cur;
	int len;

	ASSERT_MULTIMAP_CURSOR(cursor);

	if (cursor->curt.state != MAP_CURSOR_DATA) {
		return 0;
	}

	mmap = (struct multimap *)cursor->curt.map;
	ASSERT(mmap);

	len = hlist_len(&cursor->head->head);
#if 0
	print_multimap_cursor(KERN_NOTICE, cursor);
	LOGn("len: %d\n", len);
#endif
	if (len == 1) {
		multimap_cursor_copy(&cur, cursor);
		multimap_cursor_next(cursor);
		map_cursor_del(&cur.curt); /* &cur.curt will indicate the next. */
		free_cell(mmap->mmgr, cur.cell);
		free_cell_head(mmap->mmgr, cur.head);
		map_cursor_copy(&cursor->curt, &cur.curt);
	} else {
		ASSERT(len > 1);
		multimap_cursor_copy(&cur, cursor);
		multimap_cursor_next(cursor);
		hlist_del(&cur.cell->list);
		free_cell(mmap->mmgr, cur.cell);
	}
#if 0
	print_multimap_cursor(KERN_NOTICE, cursor);
#endif
	ASSERT(is_valid_multimap_cursor(cursor));
	return 1;
}

MODULE_LICENSE("Dual BSD/GPL");
