/**
 * memblk_data.c - Memory data implementation.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"

#include <linux/mm.h>
#include <linux/module.h>
#include "walb/common.h"
#include "walb/logger.h"
#include "walb/check.h"
#include "treemap.h"
#include "memblk_data.h"
#include "util.h" /* for debug */
#include "walb/util.h" /* for debug */

/*
 * You must call mdata_init() at first,
 * and mdata_exit() before exit.
 */

/*******************************************************************************
 * Static variables prototype.
 *******************************************************************************/

/* for debug */
#ifdef WALB_DEBUG
static atomic_t count_ = ATOMIC_INIT(0);
#define CNT_INC() atomic_inc(&count_)
#define CNT_DEC() atomic_dec(&count_)
#define CNT() atomic_read(&count_)
#else
#define CNT_INC()
#define CNT_DEC()
#define CNT()
#endif

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

/* Read/write a block. */
static void __memblk_data_block_io(
	struct memblk_data *mdata, u64 block_id, u8 *data, bool is_write);
/* Read/write continuous blocks. */
static void __memblk_data_blocks_io(
	struct memblk_data *mdata, u64 block_id, u32 n_blocks,
	u8 *data, bool is_write);

/* For test */
static u32 get_random_capacity(u32 block_size);
static u64 get_random_addr(u64 capacity);

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * Multiple-block IO.
 */
static void __memblk_data_blocks_io(struct memblk_data *mdata, u64 block_id, u32 n_blocks,
				u8 *data, bool is_write)
{
	u64 ui;
	unsigned int offset = 0;

	ASSERT(mdata);
	ASSERT(data);
	for (ui = 0; ui < n_blocks; ui++) {
		if (block_id + ui >= mdata->capacity) {
			LOGe("Access to outside the capacity %"PRIu64" addr %"PRIu64".",
				block_id + ui, mdata->capacity);
			return;
		}
		__memblk_data_block_io(mdata, block_id + ui, data + offset, is_write);
		offset += mdata->block_size;
	}
}

/**
 * Block IO.
 */
static void __memblk_data_block_io(struct memblk_data *mdata, u64 block_id, u8 *data, bool is_write)
{
	u8 *buf, *src, *dst;

	ASSERT(mdata);
	ASSERT(block_id < mdata->capacity);
	ASSERT(data);

	buf = mdata_get_block(mdata, block_id);
	ASSERT(buf);

	if (is_write) {
		src = data;
		dst = buf;
	} else {
		src = buf;
		dst = data;
	}
	memcpy(dst, src, mdata->block_size);
}

/*******************************************************************************
 * Global functions prototype.
 *******************************************************************************/

/**
 * Allocate memblk data.
 */
struct memblk_data* mdata_create(u64 capacity, u32 block_size, gfp_t gfp_mask,
				struct treemap_memory_manager *mgr)
{
	struct memblk_data *mdata;
	u64 ui, n_pages;
	unsigned long addr;

	mdata_assert_block_size(block_size);
	ASSERT(capacity > 0);
	ASSERT(mgr);

	/* Allocate mdata */
	mdata = ZALLOC(sizeof(struct memblk_data), gfp_mask);
	if (!mdata) {
		LOGe("Memory allocation failure.\n");
		goto error0;
	}
	mdata->block_size = block_size;
	mdata->capacity = capacity;

	/* Allocate index */
	mdata->index = map_create(gfp_mask, mgr);
	if (!mdata->index) {
		LOGe("map_create failed.\n");
		goto error1;
	}

	/* Allocate each block */
	n_pages = mdata_get_required_n_pages(capacity, block_size);
	/* LOGd("n_pages: %"PRIu64"\n", n_pages); */
	for (ui = 0; ui < n_pages; ui++) {
		addr = __get_free_page(gfp_mask); CNT_INC();
		/* LOGd("allocate a page addr %p.\n", (void *)addr); */
		if (!addr) {
			LOGe("__get_free_page failed.\n");
			goto error1;
		}
		if (map_add(mdata->index, ui, addr, gfp_mask)) {
			LOGe("map_add failed.\n");
			free_page(addr); CNT_DEC();
			goto error1;
		}
		/* LOGd("allocate ui %"PRIu64" addr %p.\n", ui, (void *)addr); */
	}
	return mdata;

error1:
	mdata_destroy(mdata);
error0:
	return NULL;
}

/**
 * Destroy memblk_data.
 */
void mdata_destroy(struct memblk_data *mdata)
{
	u64 n_pages;
	u64 page_id;
	unsigned long addr;

	if (!mdata) { return; }

	if (mdata->index) {
		n_pages = mdata_get_required_n_pages(
			mdata->capacity, mdata->block_size);
		for (page_id = 0; page_id < n_pages; page_id++) {
			addr = map_del(mdata->index, page_id);
			ASSERT(addr != TREEMAP_INVALID_VAL);
			/* LOGd("page_id %"PRIu64" n_pages %"PRIu64" addr %p\n", */
			/*	page_id, n_pages, (void *)addr); */
			free_page(addr); CNT_DEC();
		}
		map_destroy(mdata->index);
	}
	FREE(mdata);
}

/**
 * @mdata memory data.
 * @block_addr addr [mdata->block_size].
 * CONTEXT:
 * Any.
 * RETURN:
 * Pointer to the data.
 * The area of mdata->block_size is available at least.
 */
u8* mdata_get_block(struct memblk_data *mdata, u64 block_addr)
{
	u64 page_id;
	unsigned long addr;

	ASSERT(mdata);
	ASSERT(block_addr < mdata->capacity);
	page_id = mdata_get_page_id(block_addr, mdata->block_size);

	addr = map_lookup(mdata->index, page_id);
	ASSERT(addr != TREEMAP_INVALID_VAL);
	ASSERT(addr != 0);

	addr += mdata->block_size *
		mdata_get_page_offset(block_addr, mdata->block_size);

	return (u8 *)addr;
}

/**
 * Read a block.
 */
void mdata_read_block(const struct memblk_data *mdata, u64 block_id, u8 *dst)
{
	__memblk_data_block_io((struct memblk_data *)mdata, block_id, dst, false);
}

/**
 * Write a block.
 */
void mdata_write_block(struct memblk_data *mdata, u64 block_id, const u8 *src)
{
	__memblk_data_block_io(mdata, block_id, (u8 *)src, true);
}

/**
 * Read blocks.
 */
void mdata_read_blocks(
	const struct memblk_data *mdata,
	u64 block_id, u32 n_blocks, u8 *dst)
{
	__memblk_data_blocks_io((struct memblk_data *)mdata, block_id, n_blocks, dst, false);
}

/**
 * Write blocks.
 */
void mdata_write_blocks(
	struct memblk_data *mdata,
	u64 block_id, u32 n_blocks, const u8 *src)
{
	__memblk_data_blocks_io(mdata, block_id, n_blocks, (u8 *)src, true);
}


/*******************************************************************************
 * For test.
 *******************************************************************************/

/**
 * For test.
 */
static u32 get_random_capacity(u32 block_size)
{
	const u32 max_capacity_in_bytes = 1048576;

	return (get_random_u32_max(max_capacity_in_bytes) / block_size);
}

static u64 get_random_addr(u64 capacity)
{
	return get_random_u32_max((u32)capacity);
}

/**
 * Allocate/deallocate memblk_data.
 */
bool test_memblk_data_simple(u64 capacity, const u32 block_size)
{
	struct memblk_data *mdata = NULL;
	u64 b_id;
	UNUSED u8 *data;
	struct treemap_memory_manager mmgr;

	ASSERT(capacity > 0);
	mdata_assert_block_size(block_size);

	if (!initialize_treemap_memory_manager_kmalloc(&mmgr, 1)) {
		goto error0;
	}

	mdata = mdata_create(capacity, block_size, GFP_KERNEL, &mmgr);
	if (!mdata) {
		LOGe("create_memblk_data failed.\n");
		goto error1;
	}
	for (b_id = 0; b_id < mdata->capacity; b_id++) {
		data = mdata_get_block(mdata, b_id);
		LOGd("b_id %"PRIu64" capacity %"PRIu64" data %p\n",
			b_id, mdata->capacity, data);
	}
	return true;
#if 0
error2:
	destroy_memblk_data(mdata);
#endif
error1:
	finalize_treemap_memory_manager(&mmgr);
error0:
	return false;
}

/**
 * Test memblk data.
 */
bool test_memblk_data(u64 capacity, const u32 block_size)
{
	u64 addr;
	struct memblk_data *mdata = NULL;
	u8 *data1 = NULL, *data2 = NULL;
	int i;
	u32 size;
	char *strbuf = NULL;
	struct treemap_memory_manager mmgr;
	bool ret;

	LOGd("test_memblk_data start.\n");
	strbuf = (char *)__get_free_page(GFP_KERNEL); CNT_INC();
	CHECKd(strbuf);

	if (capacity == 0) {
		capacity = get_random_capacity(block_size) + 4;
	}

	ret = initialize_treemap_memory_manager_kmalloc(&mmgr, 1);
	ASSERT(ret);

	mdata = mdata_create(capacity, block_size, GFP_KERNEL, &mmgr);
	CHECKd(mdata);

	data1 = (u8 *)__get_free_page(GFP_KERNEL); CNT_INC();
	CHECKd(data1);
	data2 = (u8 *)__get_free_page(GFP_KERNEL); CNT_INC();
	CHECKd(data2);

	sprint_hex(strbuf, PAGE_SIZE, data1, 128);
	/* LOGd("data1: %s\n", strbuf); */
	sprint_hex(strbuf, PAGE_SIZE, data2, 128);
	/* LOGd("data2: %s\n", strbuf); */

	/* First block */
	addr = 0;
	get_random_bytes(data1, PAGE_SIZE);
	mdata_write_block(mdata, addr, data1);
	mdata_read_block(mdata, addr, data2);

	sprint_hex(strbuf, PAGE_SIZE, data1, 128);
	/* LOGd("data1: %s\n", strbuf); */
	sprint_hex(strbuf, PAGE_SIZE, data2, 128);
	/* LOGd("data2: %s\n", strbuf); */

	CHECKd(memcmp(data1, data2, block_size) == 0);

	/* Last block */
	addr = capacity - 1;
	get_random_bytes(data1, PAGE_SIZE);
	mdata_write_block(mdata, addr, data1);
	mdata_read_block(mdata, addr, data2);
	CHECKd(memcmp(data1, data2, block_size) == 0);

	/* First two blocks */
	if (block_size * 2 <= PAGE_SIZE) {
		addr = 0;
		get_random_bytes(data1, PAGE_SIZE);
		mdata_write_blocks(mdata, 0, 2, data1);
		mdata_read_blocks(mdata, 0, 2, data2);
		CHECKd(memcmp(data1, data2, block_size * 2) == 0);
	}

	/* Random area */
	for (i = 0; i < 10; i++) {

		addr = get_random_addr(capacity - 4);
		get_random_bytes(data1, PAGE_SIZE);

		size = get_random_u32_max(4) + 1;
		size = min(size, (u32)(PAGE_SIZE / block_size));

		/* LOGd("iteration %d (addr %"PRIu64" size %"PRIu32")\n", i, addr, size); */

		mdata_write_blocks(mdata, addr, size, data1);
		mdata_read_blocks(mdata, addr, size, data2);

		CHECKd(memcmp(data1, data2, size * block_size) == 0);
	}

	free_page((unsigned long)strbuf); CNT_DEC();
	free_page((unsigned long)data2); CNT_DEC();
	free_page((unsigned long)data1); CNT_DEC();
	mdata_destroy(mdata);
	finalize_treemap_memory_manager(&mmgr);

	LOGd("test_memblk_data succeeded.\n");
	LOGd("count_: %d\n", CNT());
	return true;
error:
	LOGe("ERROR\n");
	if (strbuf) {
		free_page((unsigned long)strbuf); CNT_DEC();
	}
	if (data2) {
		free_page((unsigned long)data2); CNT_DEC();
	}
	if (data1) {
		free_page((unsigned long)data1); CNT_DEC();
	}
	mdata_destroy(mdata);
	finalize_treemap_memory_manager(&mmgr);
	LOGe("test_memblk_data failed..\n");
	return false;
}

MODULE_LICENSE("Dual BSD/GPL");
