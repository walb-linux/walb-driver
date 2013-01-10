/**
 * bitmap.h - Bitmap functions.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_BITMAP_H
#define WALB_BITMAP_H

#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

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
static inline struct walb_bitmap* walb_bitmap_create(
	size_t size
#ifdef __KERNEL__
	, gfp_t flags)
#else
	)
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
	memset(bmp->ary, 0, (bmp->size + 7) / 8);
}

/**
 * Make a bit on.
 */
static inline void walb_bitmap_on(struct walb_bitmap *bmp, size_t idx)
{
	const size_t ary_idx = idx / 8;
	const size_t off = idx % 8;
	const u8 mask = 1 << off;

	bmp->ary[ary_idx] |= mask;
}

/**
 * Make a bit off.
 */
static inline void walb_bitmap_off(struct walb_bitmap *bmp, size_t idx)
{
	const size_t ary_idx = idx / 8;
	const size_t off = idx % 8;
	const u8 mask = 1 << off;

	bmp->ary[ary_idx] &= ~mask;
}

/**
 * Test bit.
 *
 * @return Non-zero: on,
 *	   0:	     off.
 */
static inline int walb_bitmap_get(const struct walb_bitmap *bmp, size_t idx)
{
	const size_t ary_idx = idx / 8;
	const size_t off = idx % 8;
	const u8 mask = 1 << off;

	return (bmp->ary[ary_idx] & mask) != 0;
}

/**
 * Test all bits are on.
 *
 * @return Non-zero: all bits are on,
 *	   0: otherwise.
 */
static inline int walb_bitmap_is_all_on(const struct walb_bitmap *bmp)
{
	const size_t q = bmp->size / 8;
	const size_t r = bmp->size % 8;
	const u8 mask = (1 << r) - 1;
	size_t i;

	for (i = 0; i < q; i++) {
		if (bmp->ary[i] != 0xff) {
			return 0;
		}
	}
	return (bmp->ary[q] & mask) == mask;
}

/**
 * Test all bits are off.
 *
 * @return Non-zero: all bits are off,
 *	   0: otherwise.
 */
static inline int walb_bitmap_is_all_off(const struct walb_bitmap *bmp)
{
	const size_t q = bmp->size / 8;
	const size_t r = bmp->size % 8;
	const u8 mask = (1 << r) - 1;
	size_t i;

	for (i = 0; i < q; i++) {
		if (bmp->ary[i] != 0) {
			return 0;
		}
	}
	return (bmp->ary[q] & mask) == 0;
}

/**
 * Test any bits are on.
 *
 * @return Non-zero: some bits are on,
 *	   0:	     all bits are off.
 */
static inline int walb_bitmap_is_any_on(const struct walb_bitmap *bmp)
{
	return (!walb_bitmap_is_all_off(bmp));
}

/**
 * Test any bits are off.
 *
 * @return Non-zero: some bits are off,
 *	   0:	     all bits are on.
 */
static inline int walb_bitmap_is_any_off(const struct walb_bitmap *bmp)
{
	return (!walb_bitmap_is_all_on(bmp));
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

#ifdef __cplusplus
}
#endif

#endif /* WALB_BITMAP_H */
