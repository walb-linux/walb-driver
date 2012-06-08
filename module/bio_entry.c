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


/* kmem cache for bio_entry. */
#define KMEM_CACHE_BIO_ENTRY_NAME "bio_entry_cache"
static struct kmem_cache *bio_entry_cache_ = NULL;

/* shared coutner of the cache. */
static unsigned int shared_cnt_ = 0;


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
void init_bio_entry(struct bio_entry *bioe)
{
	ASSERT(bioe);
	
	init_completion(&bioe->done);
	bioe->error = 0;
	bioe->bio = NULL;
	bioe->bi_size = 0;

#ifdef WALB_FAST_ALGORITHM
	bioe->is_copied = false;
#endif
}

/**
 * Create a bio_entry.
 * Internal bio and bi_size will be set NULL.
 */
struct bio_entry* create_bio_entry(gfp_t gfp_mask)
{
	struct bio_entry *bioe;

	bioe = kmem_cache_alloc(bio_entry_cache_, gfp_mask);
	if (!bioe) {
		LOGd("kmem_cache_alloc() failed.");
		goto error0;
	}
	init_bio_entry(bioe);
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
		cur->offset = 0;
	}

	/* now editing */
}

#define ASSERT_BIO_ENTRY_CURSOR(cur) ASSERT(is_valid_bio_entry_cursor(cur))

static bool is_valid_bio_entry_cursor(struct bio_entry_cursor *cur)
{
	unsigned int size = 0;
	struct bio_entry *bioe, *bioe_tmp;
	
	CHECK(cur);
	CHECK(cur->bio_ent_list);
	
	if (list_empty(cur->bio_ent_list)) {
		/* Empty */
		CHECK(!cur->bioe);
		CHECK(cur->offset == 0);
		goto fin;
	}

	if (!cur->bioe) {
		/* End */
		CHECK(cur->offset == 0);
		goto fin;
	}

	bioe_tmp = NULL;
	list_for_each_entry(bioe, cur->bio_ent_list, list) {
		CHECK(bioe->bio);
		if (bioe == cur->bioe) {
			bioe_tmp = bioe;
		}
		/* Currently zero-size bio is not supported. */
		CHECK(bioe->bio_bi_size > 0);
		
		size += bioe->bio->bi_size;
	}
	CHECK(size > 0);
	CHECK(size % LOGICAL_BLOCK_SIZE == 0);
	if (cur->bioe) {

	} else {
		/* Last */
		
		/* now editing */
	}
	
	CHECK(bioe_tmp);
	CHECK(cur->bioe == bioe_tmp);
	
fin:
	return true;
error:
	return false;
}


static void bio_entry_cursor_is_begin(struct bio_entry_cursor *cur)
{
	ASSERT(cur);
	
	/* now editing */

}

static void bio_entry_cursor_is_end(struct bio_entry_cursor *cur)
{
	ASSERT(cur);
	
	
	/* now editing */
	
}

/**
 * Proceed a bio_entry cursor.
 *
 * @cur bio_entry cursor.
 * @sectors proceeding size [sectors].
 *
 * RETURN:
 *   true in success, or false due to overrun.
 */
static bool bio_entry_cursor_proceed(struct bio_entry_cursor *cur,
				unsigned int sectors)
{
	ASSERT_BIO_ENTRY_CURSOR(cur);
	/* now editing */
	
}

static bool bio_entry_cursor_split(struct bio_entry_cursor *cur)
{

	/* now editing */
}

/**
 * Copy data of a bio to another.
 *
 * @dst_bio destionation.
 * @dst_off destination offset [sectors].
 * @src_bio source.
 * @src_off source offset [sectors].
 * @len copy length [sectors].
 */
static void data_copy_bio(
	struct bio *dst_bio, unsigned int dst_off,
	struct bio *src_bio, unsigned int src_off,
	unsigne dint len)
{
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
