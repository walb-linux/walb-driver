/**
 * bio_entry.c - bio_entry related functions.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"
#include <linux/module.h>
#include <linux/list.h>
#include "bio_entry.h"
#include "walb/common.h"
#include "walb/util.h"
#include "walb/block_size.h"

/*******************************************************************************
 * Static data.
 *******************************************************************************/

/* kmem cache for bio_entry. */
#define KMEM_CACHE_BIO_ENTRY_NAME "bio_entry_cache"
static struct kmem_cache *bio_entry_cache_ = NULL;

/* shared coutner of the cache. */
static unsigned int shared_cnt_ = 0;


/**
 * Bio cursor data.
 *
 * See the definition of bio_cursor_is_valid()
 * to know the specification.
 */
struct bio_cursor
{
	struct bio *bio; /* target bio. */
	unsigned int idx; /* bio io_vec index. */
	unsigned int off; /* offset inside bio [bytes]. */
	unsigned int off_in; /* offset inside io_vec [bytes]. */
};

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/


static inline void list_insert_after(struct list_head *list, struct list_head *new);

/* For bio_cursor */
#ifdef WALB_FAST_ALGORITHM
static bool bio_cursor_is_valid(struct bio_cursor *cur);
static void bio_cursor_init(struct bio_cursor *cur, struct bio *bio);
static void bio_cursor_proceed(struct bio_cursor *cur, unsigned int len);
static bool bio_cursor_is_end(struct bio_cursor *cur);
UNUSED static bool bio_cursor_is_boundary(struct bio_cursor *cur);
static unsigned int bio_cursor_size_to_boundary(struct bio_cursor *cur);
static void bio_cursor_proceed_to_boundary(struct bio_cursor *cur);
static char* bio_cursor_map(struct bio_cursor *cur, unsigned long *flags);
static unsigned int bio_cursor_try_copy_and_proceed(
	struct bio_cursor *dst, struct bio_cursor *src, unsigned int len);

/* For bio_entry_cursor */
static struct bio_entry* bio_entry_split(
	struct bio_entry *bioe1, unsigned int first_sectors);

static bool bio_entry_cursor_is_end(struct bio_entry_cursor *cur);
static bool bio_entry_cursor_is_boundary(struct bio_entry_cursor *cur);
static unsigned int bio_entry_cursor_size_to_boundary(
	struct bio_entry_cursor *cur);
static void bio_entry_cursor_proceed_to_boundary(
	struct bio_entry_cursor *cur);
static bool bio_entry_cursor_split(struct bio_entry_cursor *cur);

static void bio_data_copy(
	struct bio *dst, unsigned int dst_off,
	struct bio *src, unsigned int src_off,
	unsigned int sectors);
#endif /* WALB_FAST_ALGORITHM */


/*******************************************************************************
 * Macros definition.
 *******************************************************************************/

/**
 * Unmapping a bio_cursor.
 * You must call this after calling bio_cursor_map().
 */
#define bio_cursor_unmap(buffer, flags) bvec_kunmap_irq(buffer, flags)

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * Insert a node just after a node.
 *
 * @pos node just after which the new node will be inserted.
 * @new node to be inserted.
 *
 * Before: ..., pos, ...
 * After:  ..., pos, new, ...
 */
static inline void list_insert_after(struct list_head *pos, struct list_head *new)
{
	new->prev = pos;
	new->next = pos->next;
	pos->next = new;
}

/**
 * Check whether bio_cursor data is valid or not.
 *
 * RETURN:
 *   true if valid, or false.
 */
#ifdef WALB_FAST_ALGORITHM
static bool bio_cursor_is_valid(struct bio_cursor *cur)
{
	struct bio_vec *bvec;
	int i;
	unsigned int off;
	
	CHECK(cur);
	CHECK(cur->bio);

	if (cur->bio->bi_size == 0) {
		/* zero bio. */
		CHECK(cur->idx == 0);
		CHECK(cur->off == 0);
		CHECK(cur->off_in == 0);
		goto fin;
	}

	CHECK(cur->idx >= cur->bio->bi_idx);
	CHECK(cur->idx <= cur->bio->bi_vcnt);

	if (cur->idx == cur->bio->bi_vcnt) {
		/* cursur indicates the end. */
		CHECK(cur->off == cur->bio->bi_size);
		CHECK(cur->off_in == 0);
		goto fin;
	}

	bvec = NULL;
	off = 0;
	bio_for_each_segment(bvec, cur->bio, i) {
		if (i == cur->idx) { break; }
		off += bvec->bv_len;
	}
	CHECK(off + cur->off_in == cur->off);

fin:	
	return true;
error:
	return false;
}
#endif

/**
 * Initialize bio_cursor data.
 */
#ifdef WALB_FAST_ALGORITHM
static void bio_cursor_init(struct bio_cursor *cur, struct bio *bio)
{
	ASSERT(cur);
	ASSERT(bio);

	cur->bio = bio;
	if (bio->bi_size == 0) {
		/* zero bio */
		cur->idx = 0;
	} else {
		cur->idx = bio->bi_idx;
	}
	cur->off = 0;
	cur->off_in = 0;
	
	ASSERT(bio_cursor_is_valid(cur));
}
#endif

/**
 * Proceed a bio_cursor.
 *
 * If the cursor reaches the end before proceeding the specified length,
 * it will indicate the end.
 *
 * @cur cursor.
 * @len proceeding size [bytes].
 */
#ifdef WALB_FAST_ALGORITHM
static void bio_cursor_proceed(struct bio_cursor *cur, unsigned int len)
{
	unsigned int tmp;
	ASSERT(bio_cursor_is_valid(cur));

	while (len != 0 && !bio_cursor_is_end(cur)) {

		tmp = bio_cursor_size_to_boundary(cur);
		if (len < tmp) {
			cur->off += len;
			cur->off_in += len;
			len = 0;
			ASSERT(cur->off_in < bio_iovec_idx(cur->bio, cur->idx)->bv_len);
		} else {
			bio_cursor_proceed_to_boundary(cur);
			len -= tmp;
		}
	}
	
	ASSERT(bio_cursor_is_valid(cur));
}
#endif

/**
 * Check a bio_cursor indicates the end.
 */
#ifdef WALB_FAST_ALGORITHM
static bool bio_cursor_is_end(struct bio_cursor *cur)
{
	ASSERT(bio_cursor_is_valid(cur));
	
	if (cur->bio->bi_size == 0) {
		/* zero bio always indicates the end. */
		return true;
	} else {
		return cur->off == cur->bio->bi_size;
	}
}
#endif

/**
 * Check a cursor is now io_vec boundary.
 */
#ifdef WALB_FAST_ALGORITHM
UNUSED static bool bio_cursor_is_boundary(struct bio_cursor *cur)
{
	ASSERT(bio_cursor_is_valid(cur));
	return cur->off_in == 0;
}
#endif

/**
 * Get size to the end of the current io_vec.
 */
#ifdef WALB_FAST_ALGORITHM
static unsigned int bio_cursor_size_to_boundary(struct bio_cursor *cur)
{
	unsigned int bv_len;
	ASSERT(bio_cursor_is_valid(cur));

	if (bio_cursor_is_end(cur)) {
		return 0;
	} else {
		bv_len = bio_iovec_idx(cur->bio, cur->idx)->bv_len;
		ASSERT(bv_len > cur->off_in);
		return bv_len - cur->off_in;
	}
}
#endif

/**
 * Proceed to the next io_vec boundary.
 */
#ifdef WALB_FAST_ALGORITHM
static void bio_cursor_proceed_to_boundary(struct bio_cursor *cur)
{
	ASSERT(bio_cursor_is_valid(cur));
	
	if (!bio_cursor_is_end(cur)) {
		cur->off += bio_cursor_size_to_boundary(cur);
		cur->off_in = 0;
		cur->idx ++;
	}

	ASSERT(bio_cursor_is_boundary(cur));
}
#endif

/**
 * Get pointer to the buffer of the current io_vec of a bio_cursor.
 *
 * You must call bio_cursor_put_buf() after operations done.
 */
#ifdef WALB_FAST_ALGORITHM
static char* bio_cursor_map(struct bio_cursor *cur, unsigned long *flags)
{
	unsigned long addr;
	ASSERT(bio_cursor_is_valid(cur));
	ASSERT(!bio_cursor_is_end(cur));
	ASSERT(flags);

	addr = (unsigned long)bvec_kmap_irq(
		bio_iovec_idx(cur->bio, cur->idx), flags);
	ASSERT(addr != 0);
	addr += cur->off_in;
	return (char *)addr;
}
#endif

/**
 * Copy data from a bio_cursor to a bio_cursor.
 *
 * @dst Destination cursor.
 * @src Source cursor.
 * @len Copy size [bytes].
 *
 * RETURN:
 *   Actually copied size [bytes].
 *   This may be less than @len.
 */
#ifdef WALB_FAST_ALGORITHM
static unsigned int bio_cursor_try_copy_and_proceed(
	struct bio_cursor *dst, struct bio_cursor *src, unsigned int len)
{
	unsigned int copied;
	unsigned long dst_flags, src_flags;
	char *dst_buf, *src_buf;
	unsigned int dst_size, src_size;

	ASSERT(bio_cursor_is_valid(dst));
	ASSERT(bio_cursor_is_valid(src));
	ASSERT(len > 0);

	if (bio_cursor_is_end(dst) || bio_cursor_is_end(src)) {
		return 0;
	}

	dst_size = bio_cursor_size_to_boundary(dst);
	src_size = bio_cursor_size_to_boundary(src);
	ASSERT(dst_size > 0);
	ASSERT(src_size > 0);
	copied = min(min(dst_size, src_size), len);
	ASSERT(copied > 0);

	dst_buf = bio_cursor_map(dst, &dst_flags);
	src_buf = bio_cursor_map(src, &src_flags);
	ASSERT(dst_buf);
	ASSERT(src_buf);

	LOGd_("copied %u\n", copied);
	memcpy(dst_buf, src_buf, copied);

	bio_cursor_unmap(src_buf, &src_flags);
	bio_cursor_unmap(dst_buf, &dst_flags);

	bio_cursor_proceed(dst, copied);
	bio_cursor_proceed(src, copied);
	
	return copied;
}
#endif

/**
 * Split a bio_entry data.
 *
 * @bioe1 target bio entry.
 * @first_sectors number of sectors of first splitted bio [sectors].
 *
 * RETURN:
 *   splitted bio entry in success,
 *   or NULL due to memory allocation failure.
 */
#ifdef WALB_FAST_ALGORITHM
static struct bio_entry* bio_entry_split(
	struct bio_entry *bioe1, unsigned int first_sectors)
{
	struct bio_entry *bioe2;
	struct bio_pair *bp;

	ASSERT(bioe1);
	ASSERT(bioe1->bio);
	ASSERT(first_sectors > 0);
	
	bioe2 = alloc_bio_entry(GFP_NOIO);
	if (!bioe2) { goto error0; }
	
	bp = bio_split(bioe1->bio, first_sectors);
	if (!bp) { goto error1; }

	init_bio_entry(bioe1, &bp->bio1);
	init_bio_entry(bioe2, &bp->bio2);
	bio_pair_release(bp);

	return bioe2;
error1:
	destroy_bio_entry(bioe2);
error0:
	return NULL;
}
#endif

/**
 * Check a cursor indicates the end.
 */
#ifdef WALB_FAST_ALGORITHM
static bool bio_entry_cursor_is_end(struct bio_entry_cursor *cur)
{
	ASSERT(bio_entry_cursor_is_valid(cur));

	if (list_empty(cur->bio_ent_list)) {
		/* zero bio always indicates the end. */
		return true;
	} else {
		return !cur->bioe;
	}
}
#endif

/**
 * Check a cursor is now bio boundary.
 */
#ifdef WALB_FAST_ALGORITHM
static bool bio_entry_cursor_is_boundary(struct bio_entry_cursor *cur)
{
	ASSERT(bio_entry_cursor_is_valid(cur));
	return cur->off_in == 0;
}
#endif

/**
 * Get copiable size at once of the bio_entry cursor.
 *
 * RETURN:
 *   copiable size at once [logical blocks].
 *   0 means that the cursor has reached the end.
 */
#ifdef WALB_FAST_ALGORITHM
static unsigned int bio_entry_cursor_size_to_boundary(
	struct bio_entry_cursor *cur)
{
	ASSERT(bio_entry_cursor_is_valid(cur));
	return cur->bioe->bio->bi_size / LOGICAL_BLOCK_SIZE - cur->off_in;
}
#endif

/**
 * Proceed to the next bio boundary.
 */
#ifdef WALB_FAST_ALGORITHM
static void bio_entry_cursor_proceed_to_boundary(
	struct bio_entry_cursor *cur)
{
	ASSERT(bio_entry_cursor_is_valid(cur));

	cur->off += bio_entry_cursor_size_to_boundary(cur);
	cur->off_in = 0;
	if (list_is_last(&cur->bioe->list, cur->bio_ent_list)) {
		/* reached the end. */
		cur->bioe = NULL;
	} else {
		cur->bioe = list_entry(cur->bioe->list.next,
				struct bio_entry, list);
	}
}
#endif

/**
 * Try to bio split at a cursor position.
 *
 * @cur target bio_entry cursor.
 *
 * RETURN:
 *   true when split is successfully done or no need to split.
 *   false due to memory allocation failure. 
 */
#ifdef WALB_FAST_ALGORITHM
static bool bio_entry_cursor_split(struct bio_entry_cursor *cur)
{
	struct bio_entry *bioe1, *bioe2;
	
	ASSERT(bio_entry_cursor_is_valid(cur));

	if (bio_entry_cursor_is_boundary(cur)) {
		/* no need to split */
		return true;
	}

	ASSERT(cur->off_in > 0);
	bioe1 = cur->bioe;
	bioe2 = bio_entry_split(bioe1, cur->off);
	if (!bioe2) { goto error; }

	LOGn("bio split occurred.\n"); /* debug */
	
	list_insert_after(&bioe1->list, &bioe2->list);
	cur->bioe = bioe2;
	cur->off_in = 0;
	return true;

error:
	return false;	
}
#endif

/**
 * Copy data of a bio to another.
 *
 * @dst destionation.
 * @dst_off destination offset [sectors].
 * @src source.
 * @src_off source offset [sectors].
 * @sectors copy size [sectors].
 */
#ifdef WALB_FAST_ALGORITHM
static void bio_data_copy(
	struct bio *dst, unsigned int dst_off,
	struct bio *src, unsigned int src_off,
	unsigned int sectors)
{
	struct bio_cursor dst_cur, src_cur;
	unsigned int size, copied;
	
	ASSERT(dst);
	ASSERT(dst_off + sectors <= dst->bi_size);
	ASSERT(src);
	ASSERT(src_off + sectors <= src->bi_size);

	ASSERT(dst->bi_size > 0);
	ASSERT(src->bi_size > 0);
	ASSERT(dst->bi_size % LOGICAL_BLOCK_SIZE == 0);
	ASSERT(src->bi_size % LOGICAL_BLOCK_SIZE == 0);

	ASSERT(sectors > 0);
	size = sectors * LOGICAL_BLOCK_SIZE;

	bio_cursor_init(&dst_cur, dst);
	bio_cursor_init(&src_cur, src);
	bio_cursor_proceed(&dst_cur, dst_off * LOGICAL_BLOCK_SIZE);
	bio_cursor_proceed(&src_cur, src_off * LOGICAL_BLOCK_SIZE);

	while (size > 0) {
		copied = bio_cursor_try_copy_and_proceed(&dst_cur, &src_cur, size);
		ASSERT(copied > 0);
		size -= copied;
	}
}
#endif

/*******************************************************************************
 * Global functions definition.
 *******************************************************************************/

/**
 * Print a bio_entry.
 */
void print_bio_entry(const char *level, struct bio_entry *bioe)
{
	ASSERT(bioe);
	/* now editing */
}

/**
 * Initialize a bio_entry.
 */
void init_bio_entry(struct bio_entry *bioe, struct bio *bio)
{
	ASSERT(bioe);
	
	init_completion(&bioe->done);
	bioe->error = 0;
	if (bio) {
		bioe->bio = bio;
		bioe->bi_size = bio->bi_size;
	} else {
		bioe->bio = NULL;
		bioe->bi_size = 0;
	}

#ifdef WALB_FAST_ALGORITHM
	bioe->is_copied = false;
#endif
}

/**
 * Create a bio_entry.
 * Internal bio and bi_size will be set NULL.
 */
struct bio_entry* alloc_bio_entry(gfp_t gfp_mask)
{
	struct bio_entry *bioe;

	bioe = kmem_cache_alloc(bio_entry_cache_, gfp_mask);
	if (!bioe) {
		LOGd("kmem_cache_alloc() failed.");
		goto error0;
	}
	return bioe;

error0:
	LOGe("create_bio_entry() end with error.\n");
	return NULL;
}

/**
 * Destroy a bio_entry.
 */
void destroy_bio_entry(struct bio_entry *bioe)
{
	LOGd_("destroy_bio_entry() begin.\n");
        
	if (!bioe) {
		return;
	}

	if (bioe->bio) {
		LOGd("bio_put %p\n", bioe->bio);
		bio_put(bioe->bio);
		bioe->bio = NULL;
	}
	kmem_cache_free(bio_entry_cache_, bioe);

	LOGd_("destroy_bio_entry() end.\n");
}

/**
 * Call bio_get() for all bio in a bio_entry list.
 */
void get_bio_entry_list(struct list_head *bio_ent_list)
{
	struct bio_entry *bioe;
	
	ASSERT(bio_ent_list);
	list_for_each_entry(bioe, bio_ent_list, list) {
		ASSERT(bioe->bio);
		bio_get(bioe->bio);
	}
}

/**
 * Call bio_put() for all bio in a bio_entry list.
 */
void put_bio_entry_list(struct list_head *bio_ent_list)
{
	struct bio_entry *bioe;
	int bi_cnt;
	
	ASSERT(bio_ent_list);
	list_for_each_entry(bioe, bio_ent_list, list) {
		ASSERT(bioe->bio);
		bi_cnt = atomic_read(&bioe->bio->bi_cnt);
		bio_put(bioe->bio);
		if (bi_cnt == 1) { bioe->bio = NULL; }
	}
}

/**
 * Destroy all bio_entry in a list.
 */
void destroy_bio_entry_list(struct list_head *bio_ent_list)
{
	struct bio_entry *bioe, *next;
	
	ASSERT(bio_ent_list);
	list_for_each_entry_safe(bioe, next, bio_ent_list, list) {
		list_del(&bioe->list);
		destroy_bio_entry(bioe);
	}
}

/**
 * Mark copied bio_entry list at a range.
 *
 * @bio_ent_list bio_entry list.
 * @off offset from the begin [sectors].
 * @sectors number of sectors [sectors].
 *
 * RETURN:
 *   true in success.
 *   false in failure due to memory allocation failure for splitting.
 */
#ifdef WALB_FAST_ALGORITHM
bool bio_entry_list_mark_copied(
	struct list_head *bio_ent_list, unsigned int off, unsigned int sectors)
{
	struct bio_entry_cursor curt, *cur;

	cur = &curt;
	
	ASSERT(bio_ent_list);
	ASSERT(!list_empty(bio_ent_list));
	ASSERT(sectors > 0);

	/* Split if required. */
	bio_entry_cursor_init(cur, bio_ent_list);
	bio_entry_cursor_proceed(cur, off);
	ASSERT(cur->off == off);
	if (!bio_entry_cursor_split(cur)) {  /* left edge. */
		goto error;
	}
	bio_entry_cursor_proceed(cur, sectors);
	if (!bio_entry_cursor_split(cur)) { /* right edge. */
		goto error;
	} 
	
	/* Mark copied. */
	bio_entry_cursor_init(cur, bio_ent_list);
	bio_entry_cursor_proceed(cur, off);
	while (cur->off < off + sectors) {

		ASSERT(bio_entry_cursor_is_boundary(cur));
		ASSERT(cur->bioe);
		cur->bioe->is_copied = true;
		bio_entry_cursor_proceed_to_boundary(cur);
	}
	ASSERT(cur->off == off + sectors);
	
	return true;
error:
	return false;
}
#endif

/**
 * Check whether bio_entry_cursor data is valid or not.
 *
 * RETURN:
 *   true if valid, or false.
 */
#ifdef WALB_FAST_ALGORITHM
bool bio_entry_cursor_is_valid(struct bio_entry_cursor *cur)
{
	unsigned int off;
	struct bio_entry *bioe;
	
	CHECK(cur);
	CHECK(cur->bio_ent_list);
	
	if (list_empty(cur->bio_ent_list)) {
		/* Empty */
		CHECK(!cur->bioe);
		CHECK(cur->off == 0);
		CHECK(cur->off_in == 0);
		goto fin;
	}

	if (!cur->bioe) {
		/* End */
		CHECK(cur->off_in == 0);
		CHECK(cur->off > 0);
		goto fin;
	}

	ASSERT(cur->bioe);
	CHECK(cur->off_in < cur->bioe->bi_size);
	bioe = NULL;
	off = 0;
	list_for_each_entry(bioe, cur->bio_ent_list, list) {
		CHECK(bioe->bio);
		/* Currently zero-size bio is not supported. */
		CHECK(bioe->bi_size > 0);
		if (bioe == cur->bioe) { break; }
		off += bioe->bi_size;
	}
	CHECK(off % LOGICAL_BLOCK_SIZE == 0);
	CHECK(off / LOGICAL_BLOCK_SIZE + cur->off_in == cur->off);
	CHECK(cur->bioe == bioe);
	
fin:
	return true;
error:
	return false;
}
#endif

/**
 * Initialize bio_entry_cursor data.
 *
 * @cur bio_entry cursor.
 * @bio_ent_list target bio_entry list.
 */
#ifdef WALB_FAST_ALGORITHM
void bio_entry_cursor_init(
	struct bio_entry_cursor *cur, struct list_head *bio_ent_list)
	
{
	ASSERT(cur);
	ASSERT(bio_ent_list);

	cur->bio_ent_list = bio_ent_list;
	if (list_empty(bio_ent_list)) {
		cur->bioe = NULL;
		cur->off = 0;
		cur->off_in = 0;
	} else {
		cur->bioe = list_first_entry(bio_ent_list, struct bio_entry, list);
		cur->off = 0;
		cur->off_in = 0;
	}
	ASSERT(bio_entry_cursor_is_valid(cur));
}
#endif

/**
 * Proceed a bio_entry cursor.
 *
 * @cur bio_entry cursor.
 * @sectors proceeding size [logical blocks].
 *
 * RETURN:
 *   true in success, or false due to overrun.
 */
#ifdef WALB_FAST_ALGORITHM
bool bio_entry_cursor_proceed(struct bio_entry_cursor *cur,
			unsigned int sectors)
{
	unsigned int proceed;
	unsigned int size;
	unsigned int proceed_in;
	
	ASSERT(bio_entry_cursor_is_valid(cur));

	proceed = 0;
	while (proceed < sectors && !bio_entry_cursor_is_end(cur)) {
		size = bio_entry_cursor_size_to_boundary(cur);
		if (proceed + size <= sectors) {
			proceed += size;
			bio_entry_cursor_proceed_to_boundary(cur);
		} else {
			proceed_in = sectors - proceed;
			cur->off_in += proceed_in;
			cur->off += proceed_in;
			proceed += proceed_in;
			ASSERT(proceed == sectors);
		}
	}
	return proceed == sectors;
}
#endif

/**
 * Try to copy data from a bio_entry cursor to a bio_entry cursor.
 * Both cursors will proceed as copied size.
 *
 * Do not call bio_entry_cursor_split() inside this.
 *
 * @dst destination cursor.
 * @src source cursor.
 * @sectors number of sectors to try to copy [sectors].
 *
 * RETURN:
 *   number of copied sectors [sectors].
 */
#ifdef WALB_FAST_ALGORITHM
unsigned int bio_entry_cursor_try_copy_and_proceed(
	struct bio_entry_cursor *dst,
	struct bio_entry_cursor *src,
	unsigned int sectors)
{
	unsigned int copied_sectors, tmp1, tmp2;

	/* Decide size to copy. */
	tmp1 = bio_entry_cursor_size_to_boundary(dst);
	tmp2 = bio_entry_cursor_size_to_boundary(src);
	copied_sectors = min(min(sectors, tmp1), tmp2);
	ASSERT(dst->bioe);
	ASSERT(src->bioe);
	ASSERT(copied_sectors > 0);

	/* Copy data. */
	bio_data_copy(
		dst->bioe->bio, dst->off_in,
		src->bioe->bio, src->off_in,
		copied_sectors);

	/* Proceed both cursors. */
	bio_entry_cursor_proceed(dst, copied_sectors);
	bio_entry_cursor_proceed(src, copied_sectors);

	return copied_sectors;
}
#endif

bool bio_entry_init(void)
{
	LOGd("bio_entry_init begin\n");
	if (shared_cnt_) {
		shared_cnt_ ++;
		return true;
	}

	bio_entry_cache_ = kmem_cache_create(
		KMEM_CACHE_BIO_ENTRY_NAME,
		sizeof(struct bio_entry), 0, 0, NULL);
	if (!bio_entry_cache_) {
		LOGe("failed to create a kmem_cache (bio_entry).\n");
		goto error;
	}
	shared_cnt_ ++;
	LOGd("bio_entry_init end\n");
	return true;
error:
	LOGd("bio_entry_init failed\n");
	return false;
}

void bio_entry_exit(void)
{
	if (shared_cnt_) {
		shared_cnt_ --;
	} else {
		LOGn("bio_entry_init() is not called yet.\n");
		return;
	}

	if (!shared_cnt_) {
		kmem_cache_destroy(bio_entry_cache_);
		bio_entry_cache_ = NULL;
	}
}

MODULE_LICENSE("Dual BSD/GPL");
