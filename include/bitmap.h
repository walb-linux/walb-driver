/**
 * Bitmap functions.
 * This support both kernel code and userland code.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_BITMAP_H
#define _WALB_BITMAP_H

#include "walb.h"

#ifdef __KERNEL__
#define FREE(p) kfree(p)
#define PRINT(fmt, args...) printk(KERN_INFO fmt, ##args)
#else
#include <stdio.h>
#include <stdlib.h>
#define FREE(p) free(p)
#define spin_lock_init(lock)
#define PRINT(fmt, args...) printf(fmt, ##args)
#endif

/**
 * Bitmap structure for walb.
 */
typedef struct walb_bitmap
{
        u8 *ary;
        size_t size;
#ifdef __KERNEL__
        spinlock_t lock; /* User can use this lock. */
#endif
} walb_bitmap_t;


/**
 * Create bitmap with size.
 *
 * @size Number of bits to store.
 * @flags GFP_* flags. (if defined(__KERNEL__) only)
 *
 * @return Pointer to created bitmap, or NULL.
 */
#ifdef __KERNEL__
static inline walb_bitmap_t* walb_bitmap_create(size_t size, gfp_t flags)
#else
static inline walb_bitmap_t* walb_bitmap_create(size_t size)
#endif
{
        walb_bitmap_t *bmp;

#ifdef __KERNEL__
        bmp = kmalloc(sizeof(walb_bitmap_t), flags);
#else
        bmp = malloc(sizeof(walb_bitmap_t));
#endif
        if (bmp != NULL) {
                spin_lock_init(&bmp->lock);
                bmp->size = size;
#ifdef __KERNEL__
                bmp->ary = kzalloc((size + 7) / 8, flags);
#else
                bmp->ary = malloc((size + 7) / 8);
#endif
                if (bmp->ary == NULL) {
                        FREE(bmp);
                        bmp = NULL;
                }
        }
        return bmp;
}

static inline void walb_bitmap_free(walb_bitmap_t *bmp)
{
        FREE(bmp->ary);
        FREE(bmp);
}

static inline void walb_bitmap_clear(walb_bitmap_t *bmp)
{
        size_t i;
        size_t ary_size;
        ary_size = (bmp->size + 7) / 8;
        for (i = 0; i < ary_size; i ++) {
                bmp->ary[i] = 0;
        }
}

static inline void walb_bitmap_on(walb_bitmap_t *bmp, size_t idx)
{
        size_t ary_idx = idx / 8;
        size_t off = idx % 8;

        bmp->ary[ary_idx] |= (u8)1 << off;
}

static inline void walb_bitmap_off(walb_bitmap_t *bmp, size_t idx)
{
        size_t ary_idx = idx / 8;
        size_t off = idx % 8;

        bmp->ary[ary_idx] &= ~((u8)1 << off);
}

static inline int walb_bitmap_get(walb_bitmap_t *bmp, size_t idx)
{
        size_t ary_idx = idx / 8;
        size_t off = idx % 8;

        return (bmp->ary[ary_idx] & ((u8)1 << off)) ? 1 : 0;
}                

static inline int walb_bitmap_is_all_on(walb_bitmap_t *bmp)
{
        size_t ary_size = (bmp->size + 7) / 8;
        size_t n_bit_in_last_byte = bmp->size % 8;
        size_t i;

        if (n_bit_in_last_byte == 0)
                n_bit_in_last_byte = 8;
        
        for (i = 0; i < ary_size - 1; i ++)
                if (bmp->ary[i] != 0xff)
                        return 0;
        
        for (i = 0; i < n_bit_in_last_byte; i ++) {
                if (! (bmp->ary[ary_size - 1] & (1 << i))) {
                        return 0;
                }
        }
        return 1;
}

static inline int walb_bitmap_is_all_off(walb_bitmap_t *bmp)
{
        size_t ary_size = (bmp->size + 7) / 8;
        size_t n_bit_in_last_byte = bmp->size % 8;
        size_t i;

        if (n_bit_in_last_byte == 0)
                n_bit_in_last_byte = 8;
        
        for (i = 0; i < ary_size - 1; i ++)
                if (bmp->ary[i] != 0)
                        return 0;
        
        for (i = 0; i < n_bit_in_last_byte; i ++) {
                if ((bmp->ary[ary_size - 1] & (1 << i))) {
                        return 0;
                }
        }
        return 1;
}

static inline int walb_bitmap_is_any_on(walb_bitmap_t *bmp)
{
        return (! walb_bitmap_is_all_off(bmp));
}

static inline int walb_bitmap_is_any_off(walb_bitmap_t *bmp)
{
        return (! walb_bitmap_is_all_on(bmp));
}

static inline void walb_bitmap_print(walb_bitmap_t *bmp)
{
        size_t i;
        for (i = 0; i < bmp->size; i ++) {
                PRINT("%d", walb_bitmap_get(bmp, i));
                if (i % 64 == 63) PRINT("\n");
        }
        PRINT("\n");
}

#endif /* _WALB_BITMAP_H */
