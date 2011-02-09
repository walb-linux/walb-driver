/**
 * walb_sector.h - Sector operations.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_SECTOR_H
#define _WALB_SECTOR_H

#include "walb.h"

#ifdef __KERNEL__
#include <linux/kernel.h>
#include <linux/slab.h>
#else
#include <stdlib.h>
#endif

/**
 * Sector data in the memory.
 */
struct sector_data
{
        int size; /* sector size. */
        void *data; /* pointer to buffer. */
};

/**
 * Assersion for valid sector data.
 */
#define ASSERT_SECTOR_DATA(sect) ASSERT((sect) != NULL &&       \
                                        (sect)->size > 0 &&     \
                                        (sect)->data != NULL)

/**
 * Memory allocator/deallocator.
 */
#ifdef __KERNEL__
#define MALLOC(size, mask) kmalloc(size, mask)
#define FREE(p) kfree(p)
#else
#define MALLOC(size, mask) malloc(size)
#define FREE(p) free(p)
#endif

/**
 * Allocate sector.
 *
 * @sector_size sector size.
 * @flag GFP flag. This is for kernel code only.
 *
 * @return pointer to allocated sector data in success, or NULL.
 */
#ifdef __KERNEL__
static inline struct sector_data* sector_alloc(int sector_size, gfp_t gfp_mask)
#else
static inline struct sector_data* sector_alloc(int sector_size)
#endif
{
        struct sector_data *sect;

        if (sector_size <= 0) {
#ifdef __KERNEL__
                printk(KERN_ERR "sector_size is 0 or negative %d.\n", sector_size);
#else
                fprintf(stderr, "sector_size is 0 or negative %d.\n", sector_size);
#endif
                goto error0;
        }
        sect = MALLOC(sizeof(struct sector_data), gfp_mask);
        if (sect == NULL) { goto error0; }
        
        sect->size = sector_size;

        sect->data = MALLOC(sector_size, gfp_mask);
        if (sect->data == NULL) { goto error1; }

        ASSERT_SECTOR_DATA(sect);
        return sect;

error1:
        FREE(sect);
error0:
        return NULL;
}

/**
 * Deallocate sector.
 *
 * This must be used for memory allocated with @sector_alloc().
 */
static inline void sector_free(struct sector_data *sect)
{
        if (sect != NULL) {
                if (sect->data != NULL) {
                        FREE(sect->data);
                }
                FREE(sect);
        }
}

/**
 * Copy sector image.
 *
 * @dst destination sector.
 * @src source sector.
 *      dst->size >= src->size must be satisfied.
 */
static inline void sector_copy(struct sector_data *dst, const struct sector_data *src)
{
        ASSERT_SECTOR_DATA(dst);
        ASSERT_SECTOR_DATA(src);
        ASSERT(dst->size >= src->size);
        
        memcpy(dst->data, src->data, src->size);
}

/**
 * Check size of both sectors are same or not.
 *
 * @return 1 if same, or 0.
 */
static inline int is_same_size_sector(const struct sector_data *sect0,
                        const struct sector_data *sect1)
{
        ASSERT_SECTOR_DATA(sect0);
        ASSERT_SECTOR_DATA(sect1);

        return (sect0->size == sect1->size ? 1 : 0);
}

/**
 * Compare sector image.
 *
 * @sect0 1st sector.
 * @sect1 2nd sector.
 *
 * @return 0 when their size and their image is completely same.
 */
static inline int sector_compare(const struct sector_data *sect0,
                   const struct sector_data *sect1)
{
        ASSERT_SECTOR_DATA(sect0);
        ASSERT_SECTOR_DATA(sect1);

        if (is_same_size_sector(sect0, sect1)) {
                return memcmp(sect0->data, sect1->data, sect1->size);
        } else {
                return sect0->size - sect1->size;
        }
}

#endif /* _WALB_SECTOR_H */
