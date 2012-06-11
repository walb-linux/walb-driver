/**
 * bio_entry.c - bio_entry related functions.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"
#include <linux/module.h>
#include "bio_entry.h"
#include "walb/common.h"

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
 */
struct bio_cursor
{
	struct bio *bio; /* target bio */
	struct bio_vec *io_vec; /* current io_vec. */
	unsigned int off; /* offset inside bio. */
	unsigned int off_in; /* offset inside io_vec. */
};

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

static void bio_cursor_init(struct bio_cursor *cur);
static void bio_cursor_proceed(struct bio_cursor *cur, unsigned int len);
static void bio_cursor_copy_data(
	struct bio_cursor *dst, struct bio_cursor *src, unsigned int len);

/*******************************************************************************
 * Macros definition.
 *******************************************************************************/

#define ASSERT_BIO_ENTRY_CURSOR(cur) ASSERT(bio_entry_cursor_is_valid(cur))

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * Check whether bio_cursor data is valid or not.
 *
 * RETURN:
 *   true if valid, or false.
 */
static bool bio_cursor_is_valid(struct bio_sursor *cur)
{
	unsigned int size_tmp;
	struct bio_entry *bioe, *bioe_tmp;
	
	CHECK(cur);
	CHECK(cur->bio);

	if (cur->bio->bi_size == 0) {
		CHECK(cur->io_vec == NULL);
		CHECK(cur->off == 0);
		CHECK(cur->off_in == 0);
		goto fin;
	}
	
	
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
	bioe_tmp = NULL;
	size_tmp = 0;
	list_for_each_entry(bioe, cur->bio_ent_list, list) {
		CHECK(bioe->bio);
		if (bioe == cur->bioe) {
			bioe_tmp = bioe;
		}
		/* Currently zero-size bio is not supported. */
		CHECK(bioe->bi_size > 0);
		size_tmp += bioe->bi_size;
	}
	CHECK(size_tmp > 0);
	CHECK(size_tmp % LOGICAL_BLOCK_SIZE == 0);
	CHECK(size_tmp / LOGICAL_BLOCK_SIZE + cur->off_in == cur->off);
	CHECK(cur->bioe == bioe_tmp);
	
fin:
	return true;
error:
	return false;


	
}

/**
 * 
 */
static void bio_cursor_init(struct bio_cursor *cur)
{
	ASSERT(bio_cursor_is_valid(cur));


	
	
	/* now editing */
}

/**
 * 
 */
static void bio_cursor_proceed(struct bio_cursor *cur, unsigned int len)
{
	/* now editing */
}

/**
 * 
 */
static void bio_cursor_copy_data(
	struct bio_cursor *dst, struct bio_cursor *src, unsigned int len)
{
	/* now editing */
}

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

#if 0
/* now editing from here. */

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
	struct bio_pair *bp;

	ASSERT(bioe);
	ASSERT(bioe->bio);
	ASSERT(first_sectors > 0);
	
	bioe2 = alloc_bio_entry(GFP_NOIO);
	if (!bioe2) { goto error0; }
	
	bp = bio_split(bioe->bio, first_sectors);
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

/**
 * Initialize bio_entry_cursor data.
 *
 * @cur bio_entry cursor.
 * @bio_ent_list target bio_entry list.
 */
static void bio_entry_cursor_init(
	struct bio_entry_cursor *cur, struct list_head *bio_ent_list)
	
{
	ASSERT(cur);
	ASSERT(bio_ent_list);

	cur->bio_ent_list = bio_ent_list;
	if (list_empty(bio_ent_list)) {
		cur->bioe = NULL;
		ASSERT(off == 0);
		cur->off = 0;
		cur->off_in = 0;
	} else {
		cur->bioe = list_first_entry(bio_ent_list, struct bio_entry, list);
		cur->off = 0;
		cur->off_in = 0;
	}
	ASSERT(bio_entry_cursor_is_valid(cur));
}

/**
 * Check whether bio_entry_cursor data is valid or not.
 *
 * RETURN:
 *   true if valid, or false.
 */
static bool bio_entry_cursor_is_valid(struct bio_entry_cursor *cur)
{
	unsigned int size_tmp;
	struct bio_entry *bioe, *bioe_tmp;
	
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
	bioe_tmp = NULL;
	size_tmp = 0;
	list_for_each_entry(bioe, cur->bio_ent_list, list) {
		CHECK(bioe->bio);
		if (bioe == cur->bioe) {
			bioe_tmp = bioe;
		}
		/* Currently zero-size bio is not supported. */
		CHECK(bioe->bi_size > 0);
		size_tmp += bioe->bi_size;
	}
	CHECK(size_tmp > 0);
	CHECK(size_tmp % LOGICAL_BLOCK_SIZE == 0);
	CHECK(size_tmp / LOGICAL_BLOCK_SIZE + cur->off_in == cur->off);
	CHECK(cur->bioe == bioe_tmp);
	
fin:
	return true;
error:
	return false;
}

/**
 * Check the cursor is now bio boundary.
 */
static bool bio_entry_cursor_is_boundary(struct bio_entry_cursor *cur)
{
	ASSERT(bio_entry_cursor_is_valid(cur));
	return cur->off_in == 0;
}

/**
 * Check the cursor indicates the begin.
 */
static bool bio_entry_cursor_is_begin(struct bio_entry_cursor *cur)
{
	ASSERT(bio_entry_cursor_is_valid(cur));
	if (list_empty(cur->bio_ent_list)) {
		return true;
	} else {
		return cur->off == 0;
	}
}

/**
 * Check the cursor indicates the end.
 */
static bool bio_entry_cursor_is_end(struct bio_entry_cursor *cur)
{
	ASSERT(bio_entry_cursor_is_valid(cur));

	if (list_empty(cur->bio_ent_list)) {
		return true;
	} else {
		return cur->bioe == NULL;
	}
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
static bool bio_entry_cursor_proceed(struct bio_entry_cursor *cur,
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
	return bioe->bio->bi_size / LOGICAL_BLOCK_SIZE - cur->off_in;
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
	if (list_is_last(&cur->bioe->list, cur->bio_ent_list)) {
		/* reached the end. */
		cur->bioe = NULL;
	} else {
		cur->bioe = list_entry(cur->bioe->list.next,
				struct bio_entry, list);
	}
}

/**
 * Try to copy data from a bio_entry cursor to a bio_entry cursor.
 * Both cursors will proceed as copied size.
 *
 * Do not call bio_entry_cursor_split() inside this.
 *
 * @dst_cur destination cursor.
 * @src_cur source cursor.
 * @sectors number of sectors to try to copy [sectors].
 *
 * RETURN:
 *   number of copied sectors [sectors].
 */
static unsigned int bio_entry_cursor_try_copy_and_proceed(
	struct bio_entry_cursor *dst_cur,
	struct bio_entry_cursor *src_cur,
	unsigned int sectors)
{
	unsigned int copied_sectors, tmp1, tmp2;

	/* Decide size to copy. */
	tmp1 = bio_entry_cursor_size_to_boundary(dst_cur);
	tmp2 = bio_entry_cursor_size_to_boundary(src_cur);
	copied_sectors = min(min(sectors, tmp1), tmp2);
	ASSERT(dst_cur->bioe);
	ASSERT(src_cur->bioe);

	/* Copy data. */
	data_copy_bio(
		dst_cur->bioe->bio, dst_cur->off_in,
		src_cur->bioe->bio, src_cur->off_in,
		copied_sectors);

	/* Proceed both cursors. */
	bio_entry_cursor_proceed(dst_cur, copied_sectors);
	bio_entry_cursor_proceed(src_cur, copied_sectors);
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
bool bio_entry_list_mark_copied(
	struct list_head *bio_ent_list, unsigned int off, unsigned int sectors)
{
	struct bio_entry_cursor curt, *cur;
	unsigned int tmp_size;

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
	struct bio_pair *bp;
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

	list_insert(&bioe1->list, &bioe2->list);
	cur->bioe = bioe2;
	cur->off_in = 0;
	return true;

error:
	return false;	
}

/**
 * Copy data of a bio to another.
 *
 * @dst_bio destionation.
 * @dst_off destination offset [sectors].
 * @src_bio source.
 * @src_off source offset [sectors].
 * @sectors copy size [sectors].
 */
static void data_copy_bio(
	struct bio *dst_bio, unsigned int dst_off,
	struct bio *src_bio, unsigned int src_off,
	unsigned int sectors)
{
	unsigned long flag;
	char *data;
	
	ASSERT(dst_bio);
	ASSERT(dst_off + sectors <= dst_bio->bi_size);
	ASSERT(src_bio);
	ASSERT(src_off + sectors <= src_bio->bi_size);

	ASSERT(dst_bio->bi_size > 0);
	ASSERT(src_bio->bi_size > 0);
	ASSERT(dst_bio->bi_size % LOGICAL_BLOCK_SIZE == 0);
	ASSERT(src_bio->bi_size % LOGICAL_BLOCK_SIZE == 0);

	ASSERT(sectors > 0);


	bio_for_each_segment

	
	

	
	
	/* now editing */
}


/* now editing to here. */
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
