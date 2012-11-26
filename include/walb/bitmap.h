/**
 * bitmap.h - Bitmap functions.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_BITMAP_H
#define WALB_BITMAP_H

#include "common.h"

/**
 * Bitmap structure for walb.
 */
struct walb_bitmap
{
	u8 *ary;
	size_t size;
#ifdef __KERNEL__
	spinlock_t lock; /* User can use this lock. */
#endif
};


/**
 * Create a bitmap with the specified size.
 *
 * @size Number of bits to store.
 * @flags GFP_* flags. (if defined(__KERNEL__) only)
 *
 * @return Pointer to created bitmap, or NULL.
 */
#ifdef __KERNEL__
static inline struct walb_bitmap* walb_bitmap_create(size_t size, gfp_t flags)
#else
	static inline struct walb_bitmap* walb_bitmap_create(size_t size)
#endif
{
	struct walb_bitmap *bmp;

	bmp = MALLOC(sizeof(struct walb_bitmap), flags);
	if (bmp != NULL) {
#ifdef __KERNEL__
		spin_lock_init(&bmp->lock);
#endif
		bmp->size = size;
		bmp->ary = ZALLOC((size + 7) / 8, flags);
		if (bmp->ary == NULL) {
			FREE(bmp);
			bmp = NULL;
		}
	}
	return bmp;
}

/**
 * Free bitmap data.
 */
static inline void walb_bitmap_free(struct walb_bitmap *bmp)
{
	FREE(bmp->ary);
	FREE(bmp);
}

/**
 * Clear all bits.
 */
static inline void walb_bitmap_clear(struct walb_bitmap *bmp)
{
	size_t i;
	size_t ary_size;
	ary_size = (bmp->size + 7) / 8;
	for (i = 0; i < ary_size; i++) {
		bmp->ary[i] = 0;
	}
}

/**
 * Make a bit on.
 */
static inline void walb_bitmap_on(struct walb_bitmap *bmp, size_t idx)
{
	size_t ary_idx = idx / 8;
	size_t off = idx % 8;

	bmp->ary[ary_idx] |= (u8)1 << off;
}

/**
 * Make a bit off.
 */
static inline void walb_bitmap_off(struct walb_bitmap *bmp, size_t idx)
{
	size_t ary_idx = idx / 8;
	size_t off = idx % 8;

	bmp->ary[ary_idx] &= ~((u8)1 << off);
}

/**
 * Test bit.
 *
 * @return Non-zero: on,
 *	   0:	     off.
 */
static inline int walb_bitmap_get(struct walb_bitmap *bmp, size_t idx)
{
	size_t ary_idx = idx / 8;
	size_t off = idx % 8;

	return (bmp->ary[ary_idx] & ((u8)1 << off));
}

/**
 * Test all bits are on.
 *
 * @return Non-zero: all bits are on,
 *	   0: otherwise.
 */
static inline int walb_bitmap_is_all_on(struct walb_bitmap *bmp)
{
	size_t ary_size = (bmp->size + 7) / 8;
	size_t n_bit_in_last_byte = bmp->size % 8;
	size_t i;

	if (n_bit_in_last_byte == 0)
		n_bit_in_last_byte = 8;

	for (i = 0; i < ary_size - 1; i++)
		if (bmp->ary[i] != 0xff)
			return 0;

	for (i = 0; i < n_bit_in_last_byte; i++) {
		if (! (bmp->ary[ary_size - 1] & (1 << i))) {
			return 0;
		}
	}
	return 1;
}

/**
 * Test all bits are off.
 *
 * @return Non-zero: all bits are off,
 *	   0: otherwise.
 */
static inline int walb_bitmap_is_all_off(struct walb_bitmap *bmp)
{
	size_t ary_size = (bmp->size + 7) / 8;
	size_t n_bit_in_last_byte = bmp->size % 8;
	size_t i;

	if (n_bit_in_last_byte == 0)
		n_bit_in_last_byte = 8;

	for (i = 0; i < ary_size - 1; i++)
		if (bmp->ary[i] != 0)
			return 0;

	for (i = 0; i < n_bit_in_last_byte; i++) {
		if ((bmp->ary[ary_size - 1] & (1 << i))) {
			return 0;
		}
	}
	return 1;
}

/**
 * Test any bits are on.
 *
 * @return Non-zero: some bits are on,
 *	   0:	     all bits are off.
 */
static inline int walb_bitmap_is_any_on(struct walb_bitmap *bmp)
{
	return (! walb_bitmap_is_all_off(bmp));
}

/**
 * Test any bits are off.
 *
 * @return Non-zero: some bits are off,
 *	   0:	     all bits are on.
 */
static inline int walb_bitmap_is_any_off(struct walb_bitmap *bmp)
{
	return (! walb_bitmap_is_all_on(bmp));
}

/**
 * Print bitmap for test or debug.
 */
static inline void walb_bitmap_print(struct walb_bitmap *bmp)
{
	size_t i;
	for (i = 0; i < bmp->size; i++) {
		int bit = (walb_bitmap_get(bmp, i) == 0 ? 0 : 1);
		PRINT(KERN_INFO, "%d", bit);
		if (i % 64 == 63) PRINT(KERN_INFO, "\n");
	}
	PRINT(KERN_INFO, "\n");
}

#endif /* WALB_BITMAP_H */
