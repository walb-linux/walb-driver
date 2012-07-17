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

/*******************************************************************************
 * Struct data.
 *******************************************************************************/

/**
 * Bio cursor data.
 *
 * See the definition of bio_cursor_is_valid()
 * to know the specification.
 */
struct bio_cursor
{
	struct bio_entry *bioe; /* Target bio entry.
				   You must use bioe->bi_size
				   instead of bioe->bio->bi_size,
				   because it will be 0 in the end_request(). */
	unsigned int idx; /* bio io_vec index. */
	unsigned int off; /* offset inside bio [bytes]. */
	unsigned int off_in; /* offset inside io_vec [bytes]. */
};

/**
 * This will be used by bio_split2().
 */
struct bio_pair2
{
	struct bio *bio_orig, *bio1, *bio2;
	/* bio1 and bio2 must be cloned bios.
	   bio1->bi_private and bio2->bi_private must be this object. */
	
	atomic_t cnt;
	int error;
};

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/


static inline void list_insert_after(struct list_head *list, struct list_head *new);

/* For bio_cursor */
#ifdef WALB_FAST_ALGORITHM
UNUSED static void bio_cursor_print(const char* level, struct bio_cursor *cur);
UNUSED static bool bio_cursor_is_valid(struct bio_cursor *cur);
static void bio_cursor_init(struct bio_cursor *cur, struct bio_entry *bioe);
static void bio_cursor_proceed(struct bio_cursor *cur, unsigned int len);
static bool bio_cursor_is_end(struct bio_cursor *cur);
UNUSED static bool bio_cursor_is_boundary(struct bio_cursor *cur);
static unsigned int bio_cursor_size_to_boundary(struct bio_cursor *cur);
static void bio_cursor_proceed_to_boundary(struct bio_cursor *cur);
static char* bio_cursor_map(struct bio_cursor *cur);
static void bio_cursor_unmap(char *buffer);
static unsigned int bio_cursor_try_copy_and_proceed(
	struct bio_cursor *dst, struct bio_cursor *src, unsigned int len);
#endif

/* For bio_entry_cursor */
static void get_bio_split_position(struct bio *bio, unsigned int first_sectors,
				unsigned int *mid_idx_p, unsigned int *mid_off_p);
static void bio_pair2_release(struct bio_pair2 *bp);
static void bio_pair2_end(struct bio *bio, int err);
static struct bio_pair2* bio_split2(
	struct bio *bio, unsigned int first_sectors, gfp_t gfp_mask);
static struct bio_entry* bio_entry_split(
	struct bio_entry *bioe1, unsigned int first_sectors);
static struct bio_entry* bio_entry_next(
	struct bio_entry *bioe,
	struct list_head *bio_ent_list);

UNUSED static void bio_entry_cursor_print(
	const char *level, struct bio_entry_cursor *cur);
static bool bio_entry_cursor_is_end(struct bio_entry_cursor *cur);
static bool bio_entry_cursor_is_boundary(struct bio_entry_cursor *cur);
static unsigned int bio_entry_cursor_size_to_boundary(
	struct bio_entry_cursor *cur);
static void bio_entry_cursor_proceed_to_boundary(
	struct bio_entry_cursor *cur);
static bool bio_entry_cursor_split(struct bio_entry_cursor *cur);

#ifdef WALB_FAST_ALGORITHM
static void copied_bio_put(struct bio *bio);
static void bio_data_copy(
	struct bio_entry *dst, unsigned int dst_off,
	struct bio_entry *src, unsigned int src_off,
	unsigned int sectors);
#endif /* WALB_FAST_ALGORITHM */

static struct bio_entry* bio_entry_list_get_first_nonzero(
	struct list_head *bio_ent_list);


/*******************************************************************************
 * Macros definition.
 *******************************************************************************/

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

#ifdef WALB_FAST_ALGORITHM
UNUSED static void bio_cursor_print(
	const char *level, struct bio_cursor *cur)
{
	u64 addr = 0;
	unsigned int bi_size = 0;
	ASSERT(cur);
	ASSERT(bio_cursor_is_valid(cur));

	ASSERT(cur->bioe);
	addr = cur->bioe->bio->bi_sector;
	bi_size = cur->bioe->bi_size;
	
	printk("%s"
		"bio_cursor: "
		"bioe %p bio %p (%"PRIu64", %u) idx %u off %u off_in %u\n",
		level,
		cur->bioe, cur->bioe->bio,
		addr, bi_size,
		cur->idx, cur->off, cur->off_in);
}
#endif

/**
 * Check whether bio_cursor data is valid or not.
 *
 * RETURN:
 *   true if valid, or false.
 */
#ifdef WALB_FAST_ALGORITHM
UNUSED static bool bio_cursor_is_valid(struct bio_cursor *cur)
{
	struct bio_vec *bvec;
	int i;
	unsigned int off;
	
	CHECK(cur);
	CHECK(cur->bioe);

	if (cur->bioe->bi_size == 0) {
		/* zero bio. */
		CHECK(cur->idx == 0);
		CHECK(cur->off == 0);
		CHECK(cur->off_in == 0);
		goto fin;
	}

	CHECK(cur->bioe->bio);
	CHECK(cur->idx >= cur->bioe->bio->bi_idx);
	CHECK(cur->idx <= cur->bioe->bio->bi_vcnt);

	if (cur->idx == cur->bioe->bio->bi_vcnt) {
		/* cursur indicates the end. */
		CHECK(cur->off == cur->bioe->bi_size);
		CHECK(cur->off_in == 0);
		goto fin;
	}

	bvec = NULL;
	off = 0;
	bio_for_each_segment(bvec, cur->bioe->bio, i) {
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
static void bio_cursor_init(struct bio_cursor *cur, struct bio_entry *bioe)
{
	ASSERT(cur);
	ASSERT(bioe);

	cur->bioe = bioe;
	if (bioe->bi_size == 0) {
		/* zero bio */
		cur->idx = 0;
	} else {
		cur->idx = bioe->bio->bi_idx;
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
			ASSERT(cur->off_in < bio_iovec_idx(cur->bioe->bio, cur->idx)->bv_len);
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
	
	if (cur->bioe->bi_size == 0) {
		/* zero bio always indicates the end. */
		return true;
	} else {
		return cur->off == cur->bioe->bi_size;
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
		bv_len = bio_iovec_idx(cur->bioe->bio, cur->idx)->bv_len;
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
static char* bio_cursor_map(struct bio_cursor *cur)
{
	struct bio_vec *bvec;
	unsigned long addr;
	ASSERT(bio_cursor_is_valid(cur));
	ASSERT(!bio_cursor_is_end(cur));

	ASSERT(cur->bioe->bio);
	bvec = bio_iovec_idx(cur->bioe->bio, cur->idx);
	addr = (unsigned long)kmap_atomic(bvec->bv_page);
	ASSERT(addr != 0);
	addr += bvec->bv_offset + cur->off_in;
	ASSERT(addr != 0);
	return (char *)addr;
}
#endif

/**
 * Unmapping a bio_cursor.
 * You must call this after calling bio_cursor_map().
 */
#ifdef WALB_FAST_ALGORITHM
static void bio_cursor_unmap(char *buffer)
{
	kunmap_atomic(buffer);
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

	dst_buf = bio_cursor_map(dst);
	src_buf = bio_cursor_map(src);
	ASSERT(dst_buf);
	ASSERT(src_buf);

	LOGd_("copied %u\n", copied);
	memcpy(dst_buf, src_buf, copied);

	bio_cursor_unmap(src_buf);
	bio_cursor_unmap(dst_buf);

	bio_cursor_proceed(dst, copied);
	bio_cursor_proceed(src, copied);
	
	return copied;
}
#endif

/**
 * Get split position of a bio.
 *
 * @bio target bio.
 * @first_sectors split position [logical block].
 * @mid_idx_p pointer to the index of the first io_vec index of the bottom half.
 * @mid_off_p pointer to the offset in the splitted io_vec [logical block].
 */
static void get_bio_split_position(struct bio *bio, unsigned int first_sectors,
				unsigned int *mid_idx_p, unsigned int *mid_off_p)
{
	struct bio_vec *bvec;
	unsigned int sectors, bvec_sectors;
	unsigned int mid_idx = 0;
	unsigned int mid_off = 0;

	ASSERT(bio);
	ASSERT(bio->bi_size > 0);
	
	sectors = 0;
	bio_for_each_segment(bvec, bio, mid_idx) {

		ASSERT(bvec->bv_len % LOGICAL_BLOCK_SIZE == 0);
		bvec_sectors = bvec->bv_len / LOGICAL_BLOCK_SIZE;

		if (sectors + bvec_sectors <= first_sectors) {
			sectors += bvec_sectors;
		} else {
			mid_off = first_sectors - sectors;
			break;
		}
	}
	*mid_off_p = mid_off;
	*mid_idx_p = mid_idx;
}

static void bio_pair2_release(struct bio_pair2 *bp)
{
	LOGd_("bio_pair2 %p %u\n", bp, atomic_read(&bp->cnt));
	if (atomic_dec_and_test(&bp->cnt)) {
		LOGd_("release bio_pair2 for %p\n", bp->bio_orig);
		bio_endio(bp->bio_orig, bp->error);
		kfree(bp);
	}
}

static void bio_pair2_end(struct bio *bio, int err)
{
	struct bio_pair2 *bp = bio->bi_private;
	LOGd_("bio_pair %p err %d\n", bp, err);
	if (err) {
		bp->error = err;
	}
	bio_put(bio);
	bio_pair2_release(bp);
}

/**
 * Split a bio with multiple io_vec(s).
 */
static struct bio_pair2* bio_split2(
	struct bio *bio, unsigned int first_sectors, gfp_t gfp_mask)
{
	int i;
	struct bio_pair2 *bp;
	struct bio *bio1, *bio2;
	unsigned int mid_idx, mid_off;
	struct bio_vec *bvec, *bvec2;
#ifdef WALB_DEBUG
	unsigned int idx;
	unsigned int size;
#endif
	LOGd_("bio size %u\n", bio->bi_size);

	bp = kmalloc(sizeof(struct bio_pair2), gfp_mask);
	if (!bp) { goto error0; }
	bp->bio_orig = bio;
	bp->error = 0;
	atomic_set(&bp->cnt, 3);

	bio1 = bio_clone(bio, gfp_mask);
	if (!bio1) { goto error1; }
	
	bio2 = bio_clone(bio, gfp_mask);
	if (!bio2) { goto error2; }
	
	bp->bio1 = bio1;
	bp->bio2 = bio2;

	bio1->bi_end_io = bio_pair2_end;
	bio2->bi_end_io = bio_pair2_end;
	
	bio1->bi_private = bp;
	bio2->bi_private = bp;
	
	ASSERT(bio1->bi_vcnt - bio1->bi_idx > 0);
	ASSERT(bio2->bi_vcnt - bio2->bi_idx > 0);

	get_bio_split_position(bio, first_sectors, &mid_idx, &mid_off);
	
	bio2->bi_idx = mid_idx;
	if (mid_off == 0) {
		bio1->bi_vcnt = mid_idx;
	} else {
		bio1->bi_vcnt = mid_idx + 1;

		/* Last bvec of the top half. */
		bvec = bio_iovec_idx(bio1, mid_idx);
		bvec->bv_len = mid_off * LOGICAL_BLOCK_SIZE;

		/* First bvec of the bottom half. */
		bvec2 = bio_iovec_idx(bio2, mid_idx);
		bvec2->bv_offset += mid_off * LOGICAL_BLOCK_SIZE;
		bvec2->bv_len -= mid_off * LOGICAL_BLOCK_SIZE;
	}
	bio1->bi_size = first_sectors * LOGICAL_BLOCK_SIZE;
	bio2->bi_sector += first_sectors;
	bio2->bi_size -= first_sectors * LOGICAL_BLOCK_SIZE;

	for (i = bio1->bi_vcnt; i < bio->bi_vcnt; i++) {
		bvec = bio_iovec_idx(bio1, i);
		bvec->bv_page = NULL;
		bvec->bv_len = 0;
		bvec->bv_offset = 0;
	}
	for (i = bio->bi_idx; i < bio2->bi_idx; i++) {
		bvec = bio_iovec_idx(bio2, i);
		bvec->bv_page = NULL;
		bvec->bv_len = 0;
		bvec->bv_offset = 0;
	}
	
#ifdef WALB_DEBUG
	size = 0;
	bio_for_each_segment(bvec, bio1, idx) {
		size += bvec->bv_len;
	}
	ASSERT(size == bio1->bi_size);

	size = 0;
	bio_for_each_segment(bvec, bio2, idx) {
		size += bvec->bv_len;
	}
	ASSERT(size == bio2->bi_size);
#endif
	return bp;

#if 0
error3:
	bio_put(bio2);
#endif
error2:
	bio_put(bio1);
error1:
	kfree(bp);
error0:
	return NULL;
}

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
static struct bio_entry* bio_entry_split(
	struct bio_entry *bioe1, unsigned int first_sectors)
{
	struct bio_entry *bioe2;
	struct bio_pair2 *bp;

	ASSERT(bioe1);
	ASSERT(bioe1->bio);
	ASSERT(first_sectors > 0);

	bioe2 = alloc_bio_entry(GFP_NOIO);
	if (!bioe2) { goto error0; }
	
	bp = bio_split2(bioe1->bio, first_sectors, GFP_NOIO);
	if (!bp) { goto error1; }
	
	bioe1->bio = bp->bio1;
	bioe1->bi_size = bp->bio1->bi_size;
	if (!bioe1->is_splitted) {
		ASSERT(!bioe1->bio_orig);
		bioe1->bio_orig = bp->bio_orig;
		LOGd_("bioe1->bio_orig->bi_cnt %d\n",
			atomic_read(&bioe1->bio_orig->bi_cnt));
	}
	init_bio_entry(bioe2, bp->bio2);
	bioe2->is_splitted = true;
	LOGd_("is_splitted: %d\n"
		"bio_orig addr %"PRIu64" size %u\n"
		"bioe1 %p addr %"PRIu64" size %u\n"
		"bioe2 %p addr %"PRIu64" size %u\n",
		bioe1->is_splitted,
		(u64)bp->bio_orig->bi_sector, bp->bio_orig->bi_size,
		bioe1, (u64)bioe1->bio->bi_sector, bioe1->bio->bi_size,
		bioe2, (u64)bioe2->bio->bi_sector, bioe2->bio->bi_size);
	bio_pair2_release(bp);
	
	return bioe2;
error1:
	destroy_bio_entry(bioe2);
error0:
	return NULL;
}

/**
 * Get next bio_entry.
 *
 * RETURN:
 *   pointer to the next bio_entry if found, or NULL (reached the end).
 */
static struct bio_entry* bio_entry_next(
	struct bio_entry *bioe,
	struct list_head *bio_ent_list)
{
	ASSERT(bioe);
	ASSERT(bio_ent_list);
	if (list_is_last(&bioe->list, bio_ent_list)) {
		/* reached the end. */
		return NULL;
	}
	return list_entry(bioe->list.next, struct bio_entry, list);
}

/**
 * Print bio_entry_cursor for debug.
 *
 * @level printk level.
 * @cur pointer to target bio_entry_cursor.
 */
UNUSED
static void bio_entry_cursor_print(
	const char *level, struct bio_entry_cursor *cur)
{
	u64 addr = 0;
	unsigned int sectors = 0;
	ASSERT(cur);

	if (cur->bioe) {
		addr = cur->bioe->bio->bi_sector;
		sectors = cur->bioe->bi_size / LOGICAL_BLOCK_SIZE;
	}
	printk("%s"
		"bio_entry_cursor: "
		"bio_ent_list %p off %u bioe %p (%"PRIu64", %u) off_in %u\n",
		level,
		cur->bio_ent_list, cur->off, cur->bioe,
		addr, sectors, cur->off_in);
}
	
/**
 * Check a cursor indicates the end.
 */
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

/**
 * Check a cursor is now bio boundary.
 */
static bool bio_entry_cursor_is_boundary(struct bio_entry_cursor *cur)
{
	ASSERT(bio_entry_cursor_is_valid(cur));
	return cur->off_in == 0;
}

/**
 * Get copiable size at once of the bio_entry cursor.
 *
 * RETURN:
 *   copiable size at once [logical blocks].
 *   0 means that the cursor has reached the end.
 */
static unsigned int bio_entry_cursor_size_to_boundary(
	struct bio_entry_cursor *cur)
{
	ASSERT(bio_entry_cursor_is_valid(cur));
	return cur->bioe->bi_size / LOGICAL_BLOCK_SIZE - cur->off_in;
}

/**
 * Proceed to the next bio boundary.
 */
static void bio_entry_cursor_proceed_to_boundary(
	struct bio_entry_cursor *cur)
{
	ASSERT(bio_entry_cursor_is_valid(cur));

	cur->off += bio_entry_cursor_size_to_boundary(cur);
	cur->off_in = 0;
	do {
		cur->bioe = bio_entry_next(cur->bioe, cur->bio_ent_list);
	} while (cur->bioe != NULL && cur->bioe->bi_size == 0);
}

/**
 * Try to bio split at a cursor position.
 *
 * @cur target bio_entry cursor.
 *
 * RETURN:
 *   true when split is successfully done or no need to split.
 *   false due to memory allocation failure. 
 */
static bool bio_entry_cursor_split(struct bio_entry_cursor *cur)
{
	struct bio_entry *bioe1, *bioe2;
	
	ASSERT(bio_entry_cursor_is_valid(cur));

	if (bio_entry_cursor_is_boundary(cur)) {
		/* no need to split */
		return true;
	}

#if 0
	print_bio_entry(KERN_DEBUG, cur->bioe);
	LOGd("split offset %u\n", cur->off);
#endif

	ASSERT(cur->off_in > 0);
	bioe1 = cur->bioe;
	bioe2 = bio_entry_split(bioe1, cur->off_in);
	if (!bioe2) { goto error; }

#if 0
	print_bio_entry(KERN_DEBUG, bioe1);
	print_bio_entry(KERN_DEBUG, bioe2);
	LOGd("bio split occurred.\n");
#endif
	
	list_insert_after(&bioe1->list, &bioe2->list);
	cur->bioe = bioe2;
	cur->off_in = 0;
	return true;

error:
	return false;	
}

/**
 * Free its all pages and call bio_put().
 */
#ifdef WALB_FAST_ALGORITHM
static void copied_bio_put(struct bio *bio)
{
	struct bio_vec *bvec;
	int i;
	ASSERT(bio);
	ASSERT(!(bio->bi_flags & (1 << BIO_CLONED)));

	bio_for_each_segment(bvec, bio, i) {
		__free_page(bvec->bv_page);
		bvec->bv_page = NULL;
	}
	ASSERT(atomic_read(&bio->bi_cnt) == 1);
	bio_put(bio);
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
	struct bio_entry *dst, unsigned int dst_off,
	struct bio_entry *src, unsigned int src_off,
	unsigned int sectors)
{
	struct bio_cursor dst_cur, src_cur;
	unsigned int size, copied;
	
	ASSERT(dst);
	ASSERT(dst_off + sectors <= dst->bi_size / LOGICAL_BLOCK_SIZE);
	ASSERT(src);
	ASSERT(src_off + sectors <= src->bi_size / LOGICAL_BLOCK_SIZE);

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

/**
 * Get the first non-zero bio_entry in a bio_entry list.
 *
 * RETURN:
 *   non-zero bio_entry if found, or NULL.
 */
static struct bio_entry* bio_entry_list_get_first_nonzero(struct list_head *bio_ent_list)
{
	struct bio_entry* bioe;

	if (!bio_ent_list || list_empty(bio_ent_list)) {
		return NULL;
	}

	bioe = list_first_entry(bio_ent_list, struct bio_entry, list);
	while (bioe != NULL && bioe->bi_size == 0) {
		bioe = bio_entry_next(bioe, bio_ent_list);
	}
	ASSERT(bioe == NULL || bioe->bi_size > 0);
	return bioe;
}

/*******************************************************************************
 * Global functions definition.
 *******************************************************************************/

/**
 * Print a bio_entry.
 */
void print_bio_entry(const char *level, struct bio_entry *bioe)
{
	struct bio_vec *bvec;
	int i;
	
	ASSERT(bioe);
	if (bioe->bio) {
		printk("%s""bioe pos %"PRIu64" size %u\n",
			level, (u64)bioe->bio->bi_sector, bioe->bi_size);
		bio_for_each_segment(bvec, bioe->bio, i) {
			printk("%s""segment %d off %u len %u\n",
				level, i, bvec->bv_offset, bvec->bv_len);
		}
	} else {
		printk("%s""bioe size %u\n",
			level, bioe->bi_size);
	}
}

/**
 * Initialize a bio_entry.
 */
void init_bio_entry(struct bio_entry *bioe, struct bio *bio)
{
	ASSERT(bioe);
	
	init_completion(&bioe->done);
	bioe->error = 0;
	bioe->is_splitted = false;
	bioe->bio_orig = NULL;
	if (bio) {
		bioe->bio = bio;
		bioe->bi_size = bio->bi_size;
	} else {
		bioe->bio = NULL;
		bioe->bi_size = 0;
	}

#ifdef WALB_FAST_ALGORITHM
	bioe->is_copied = false;
	bioe->is_own_pages = false;
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
	struct bio *bio = NULL;
	
	LOGd_("destroy_bio_entry() begin.\n");
        
	if (!bioe) {
		return;
	}
	if (bioe->bio_orig) {
		ASSERT(!bioe->is_splitted);
		LOGd_("bioe->bio_orig->bi_cnt %d\n",
			atomic_read(&bioe->bio_orig->bi_cnt));
		bio = bioe->bio_orig;
#ifdef WALB_FAST_ALGORITHM
		ASSERT(bioe->is_own_pages);
		ASSERT(bioe->bio); /* already put but not cleaned. */
#else
		ASSERT(!bioe->bio); 
#endif
	} else if (bioe->is_splitted) {
		/* already put but not cleaned. */
		ASSERT(bioe->bio);
		bio = NULL;
	} else {
		bio = bioe->bio;
		ASSERT(!bioe->bio_orig);
	}
	
	if (bio) {
		LOGd_("bio_put %p\n", bio);
#ifdef WALB_FAST_ALGORITHM
		if (bioe->is_own_pages) {
			copied_bio_put(bio);
		} else {
			bio_put(bio);
		}
#else
		bio_put(bio);
#endif
	}
	kmem_cache_free(bio_entry_cache_, bioe);

	LOGd_("destroy_bio_entry() end.\n");
}

/**
 * Call bio_get() for all bio and get_page()
 * for all their bio_vec's pages in a bio_entry list.
 */
void get_bio_entry_list(struct list_head *bio_ent_list)
{
	struct bio_entry *bioe;
	struct bio_vec *bvec;
	int i;
	
	ASSERT(bio_ent_list);
	list_for_each_entry(bioe, bio_ent_list, list) {
		ASSERT(bioe->bio);
		bio_get(bioe->bio);
		bio_for_each_segment(bvec, bioe->bio, i) {
			get_page(bvec->bv_page);
		}
	}
}

/**
 * Call bio_put() for all bio and put_page()
 * for all their bio_vec's pages in a bio_entry list.
 */
void put_bio_entry_list(struct list_head *bio_ent_list)
{
	struct bio_entry *bioe;
	int bi_cnt;
	struct bio_vec *bvec;
	int i;
	
	ASSERT(bio_ent_list);
	list_for_each_entry(bioe, bio_ent_list, list) {
		ASSERT(bioe->bio);
		bi_cnt = atomic_read(&bioe->bio->bi_cnt);
		bio_for_each_segment(bvec, bioe->bio, i) {
			put_page(bvec->bv_page);
		}
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
 * Clone bio with data copy.
 */
struct bio* bio_clone_copy(struct bio *bio, gfp_t gfp_mask)
{
	struct bio *clone;
	struct bio_vec *bvec, *bvec_orig;
	int i;
	char *dst_buf, *src_buf;
	
	ASSERT(bio);
	ASSERT(bio_rw(bio) == WRITE);

	/* We can use bio_alloc and copy all related data instead. */
	clone = bio_clone(bio, gfp_mask);
	if (!clone) {
		goto error0;
	}
	clone->bi_flags &= ~(1 << BIO_CLONED);
	bio_for_each_segment(bvec, clone, i) {
		if (bvec->bv_page) {
			bvec->bv_page = NULL;
		}
	}
	
	if (bio->bi_size == 0) {
		goto fin;
	}
	/* Allocate pages and copy original data. */
	bio_for_each_segment(bvec, clone, i) {
		ASSERT(!bvec->bv_page);
		bvec->bv_page = alloc_page(gfp_mask);
		if (!bvec->bv_page) {
			goto error1;
		}

		bvec_orig = bio_iovec_idx(bio, i);
		ASSERT(bvec_orig->bv_len == bvec->bv_len);
		ASSERT(bvec_orig->bv_offset == bvec->bv_offset);
		ASSERT(bvec_orig->bv_page != bvec->bv_page);

		/* copy IO data. */
		dst_buf = (char *)kmap_atomic(bvec->bv_page)
			+ bvec->bv_offset;
		src_buf = (char *)kmap_atomic(bvec_orig->bv_page)
			+ bvec_orig->bv_offset;
		memcpy(dst_buf, src_buf, bvec->bv_len);
		kunmap_atomic(src_buf);
		kunmap_atomic(dst_buf);
	}
fin:	
	return clone;
error1:
	bio_for_each_segment(bvec, clone, i) {
		if (bvec->bv_page) {
			__free_page(bvec->bv_page);
			bvec->bv_page = NULL;
		}
	}
	bio_put(clone);
error0:
	return NULL;
}

/**
 * Initialize a bio_entry with a bio with copy.
 */
#ifdef WALB_FAST_ALGORITHM
void init_copied_bio_entry(
	struct bio_entry *bioe, struct bio *bio_with_copy)
{
	ASSERT(bioe);
	ASSERT(bio_with_copy);
	
	init_bio_entry(bioe, bio_with_copy);
	bioe->is_own_pages = true;
	bio_get(bio_with_copy);
}
#endif

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
	ASSERT(cur->off == off);
	bio_entry_cursor_proceed(cur, sectors);
	ASSERT(cur->off == off + sectors);
	if (!bio_entry_cursor_split(cur)) { /* right edge. */
		goto error;
	}
	ASSERT(cur->off == off + sectors);
	
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
bool bio_entry_cursor_is_valid(struct bio_entry_cursor *cur)
{
	unsigned int off_bytes;
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
		goto fin;
	}

	ASSERT(cur->bioe);
	CHECK(cur->bioe->bi_size > 0);
	CHECK(cur->off_in < cur->bioe->bi_size / LOGICAL_BLOCK_SIZE);
	bioe = NULL;
	off_bytes = 0;
	list_for_each_entry(bioe, cur->bio_ent_list, list) {
		if (bioe == cur->bioe) { break; }
		off_bytes += bioe->bi_size;
	}
	CHECK(off_bytes % LOGICAL_BLOCK_SIZE == 0);
	CHECK(off_bytes / LOGICAL_BLOCK_SIZE + cur->off_in == cur->off);
	CHECK(cur->bioe == bioe);
	
fin:
	return true;
error:
	return false;
}

/**
 * Initialize bio_entry_cursor data.
 *
 * @cur bio_entry cursor.
 * @bio_ent_list target bio_entry list.
 */
void bio_entry_cursor_init(
	struct bio_entry_cursor *cur, struct list_head *bio_ent_list)
	
{
	ASSERT(cur);
	ASSERT(bio_ent_list);

	cur->bio_ent_list = bio_ent_list;
	cur->bioe = bio_entry_list_get_first_nonzero(bio_ent_list);
	cur->off = 0;
	cur->off_in = 0;
	
	ASSERT(bio_entry_cursor_is_valid(cur));
}

/**
 * Proceed a bio_entry cursor.
 *
 * @cur bio_entry cursor.
 * @sectors proceeding size [logical blocks].
 *
 * RETURN:
 *   true in success, or false due to overrun.
 */
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
#if 0
	struct bio_vec *bvec;
	int i;
#endif

	/* Decide size to copy. */
	tmp1 = bio_entry_cursor_size_to_boundary(dst);
	tmp2 = bio_entry_cursor_size_to_boundary(src);
	copied_sectors = min(min(sectors, tmp1), tmp2);
	ASSERT(dst->bioe);
	ASSERT(src->bioe);

#if 0
	/* Debug */
	if (copied_sectors == 0) {
		LOGn("copied_sectors 0 sectors %u tmp1 %u tmp2 %u\n",
			sectors, tmp1, tmp2);
		print_bio_entry(KERN_DEBUG, dst->bioe);
		print_bio_entry(KERN_DEBUG, src->bioe);
	}
#endif
	ASSERT(copied_sectors > 0);

	/* Debug */
	LOGd_("bio_data_copy: dst %u %u %u %u src %u %u %u %u copied_sectors %u\n",
		dst->bioe->bio->bi_size,
		dst->bioe->bi_size,
		dst->off_in,
		atomic_read(&dst->bioe->bio->bi_cnt),
		src->bioe->bio->bi_size,
		src->bioe->bi_size,
		src->off_in,
		atomic_read(&src->bioe->bio->bi_cnt),
		copied_sectors);

	ASSERT(dst->bioe->bio);
	ASSERT(src->bioe->bio);
	ASSERT(dst->bioe->bio->bi_size == dst->bioe->bi_size);
#if 0
	/* Debug */
	if (src->bioe->bio->bi_size != src->bioe->bi_size) {
		LOGn("src bio invalid: bio %u bioe %u vcnt %u\n",
			src->bioe->bio->bi_size,
			src->bioe->bi_size,
			atomic_read(&src->bioe->bio->bi_cnt));

		LOGn("bi_idx %u bi_vcnt %u bi_io_vec %p\n",
			src->bioe->bio->bi_idx,
			src->bioe->bio->bi_vcnt,
			src->bioe->bio->bi_io_vec);

		bio_for_each_segment(bvec, src->bioe->bio, i) {
			LOGn("bvec %d off %u len %u\n",
				i, bvec->bv_offset, bvec->bv_len);
		}
	}
#endif
	
	/* Copy data. */
	bio_data_copy(
		dst->bioe, dst->off_in,
		src->bioe, src->off_in,
		copied_sectors);

	/* Proceed both cursors. */
	bio_entry_cursor_proceed(dst, copied_sectors);
	bio_entry_cursor_proceed(src, copied_sectors);

	return copied_sectors;
}
#endif

/**
 * Split bio(s) if chunk_sectors.
 */
bool split_bio_entry_list_for_chunk(
	struct list_head *bio_ent_list, unsigned int chunk_sectors)
{
	struct bio_entry_cursor cur;
	u64 addr;
	unsigned int sectors;
	bool ret;
	
	if (chunk_sectors == 0) {
		/* no need to split. */
		return true;
	}
	ASSERT(bio_ent_list);

	bio_entry_cursor_init(&cur, bio_ent_list);
#if 0
	bio_entry_cursor_print(KERN_DEBUG, &cur);
#endif
	while (!bio_entry_cursor_is_end(&cur)) {
		ASSERT(bio_entry_cursor_is_boundary(&cur));
		ASSERT(cur.bioe);
#if 0
		bio_entry_cursor_print(KERN_DEBUG, &cur);
#endif
		addr = cur.bioe->bio->bi_sector;
		sectors = cur.bioe->bi_size / LOGICAL_BLOCK_SIZE;
		ASSERT(sectors > 0);
		if (addr / chunk_sectors == (addr + sectors - 1) / chunk_sectors) {
			/* no need to split for the bio. */
			ret = bio_entry_cursor_proceed(&cur, sectors);
			LOGd_("no need to split %"PRIu64" %u\n", addr, sectors);
			ASSERT(ret);
			continue;
		}
		ASSERT(chunk_sectors > addr % chunk_sectors);
		ret = bio_entry_cursor_proceed(&cur, chunk_sectors - addr % chunk_sectors);
		ASSERT(ret);
		ret = bio_entry_cursor_split(&cur);
		LOGd_("need to split %"PRIu64" %u\n", addr, sectors);
		if (!ret) {
			LOGe("bio split failed.\n");
			goto error0;
		}
	}
	return true;
error0:
	return false;
}

/**
 * Initilaize bio_entry cache.
 */
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

/**
 * Finalize bio_entry cache.
 */
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
