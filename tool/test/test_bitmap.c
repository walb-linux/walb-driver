/**
 * test_bitmap.c - Test code for bitmap.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <stdio.h>

#include "walb/bitmap.h"

void test(int size)
{
	int i;
	struct walb_bitmap *bmp;

	printf("test with size %d...\n", size);

	/* Create a bitmap. */
	bmp = walb_bitmap_create(size);

	for (i = 0; i < size; i++) {
		ASSERT(! walb_bitmap_get(bmp, i));
	}
	ASSERT(walb_bitmap_is_all_off(bmp));

	/* Make some bits on. */
	walb_bitmap_on(bmp, 0);
	walb_bitmap_on(bmp, 1);
	walb_bitmap_on(bmp, 2);
	walb_bitmap_on(bmp, 7);
	walb_bitmap_on(bmp, 8);
	walb_bitmap_on(bmp, 9);
	walb_bitmap_on(bmp, size - 1);
	walb_bitmap_on(bmp, size - 2);
	walb_bitmap_on(bmp, size - 3);

	walb_bitmap_print(bmp);
	ASSERT(walb_bitmap_get(bmp, 0));
	ASSERT(walb_bitmap_get(bmp, 1));
	ASSERT(walb_bitmap_get(bmp, 2));
	ASSERT(walb_bitmap_get(bmp, 7));
	ASSERT(walb_bitmap_get(bmp, 8));
	ASSERT(walb_bitmap_get(bmp, 9));
	ASSERT(walb_bitmap_get(bmp, size - 1));
	ASSERT(walb_bitmap_get(bmp, size - 2));
	ASSERT(walb_bitmap_get(bmp, size - 3));

	/* Make all bits on. */
	for (i = 0; i < size; i++) {
		walb_bitmap_on(bmp, i);
	}
	walb_bitmap_print(bmp);
	ASSERT(walb_bitmap_is_all_on(bmp));

	walb_bitmap_off(bmp, 0);
	walb_bitmap_off(bmp, 1);
	walb_bitmap_off(bmp, 2);
	walb_bitmap_off(bmp, 7);
	walb_bitmap_off(bmp, 8);
	walb_bitmap_off(bmp, 9);
	walb_bitmap_off(bmp, size - 1);
	walb_bitmap_off(bmp, size - 2);
	walb_bitmap_off(bmp, size - 3);

	walb_bitmap_print(bmp);
	ASSERT(! walb_bitmap_get(bmp, 0));
	ASSERT(! walb_bitmap_get(bmp, 1));
	ASSERT(! walb_bitmap_get(bmp, 2));
	ASSERT(! walb_bitmap_get(bmp, 7));
	ASSERT(! walb_bitmap_get(bmp, 8));
	ASSERT(! walb_bitmap_get(bmp, 9));
	ASSERT(! walb_bitmap_get(bmp, size - 1));
	ASSERT(! walb_bitmap_get(bmp, size - 2));
	ASSERT(! walb_bitmap_get(bmp, size - 3));

	/* Clear the bitmap. */
	walb_bitmap_clear(bmp);
	walb_bitmap_print(bmp);
	ASSERT(walb_bitmap_is_all_off(bmp));

	/* Free the bitmap. */
	walb_bitmap_free(bmp);
}

int main()
{
	test(64);
	test(64 - 1);
	test(64 + 1);

	return 0;
}
