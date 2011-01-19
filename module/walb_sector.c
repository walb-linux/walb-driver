/**
 * walb_sector.c - Sector operations.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/slab.h>

#include "walb_util.h"
#include "walb_sector.h"

/*******************************************************************************
 * Global functions
 *******************************************************************************/

/**
 * Allocate sector.
 *
 * @sector_size sector size.
 * @flag GFP flag.
 *
 * @return pointer to allocated sector data.
 */
struct sector_data* sector_alloc(int sector_size, gfp_t gfp_mask)
{
        struct sector_data *sect;

        if (sector_size <= 0) {
                printk_e("sector_size is negative %d.\n", sector_size);
                goto error0;
        }
        
        sect = kmalloc(sizeof(struct sector_data), gfp_mask);
        if (sect == NULL) { goto error0; }
        
        sect->size = sector_size;
        sect->data = kmalloc(sector_size, gfp_mask);
        if (sect->data == NULL) { goto error1; }

        ASSERT_SECTOR_DATA(sect);
        return sect;

error1:
        kfree(sect);
error0:
        return NULL;
}

/**
 * Deallocate sector.
 *
 * This must be used for memory allocated with @sector_alloc().
 */
void sector_free(struct sector_data *sect)
{
        if (sect != NULL) {
                if (sect->data != NULL) {
                        kfree(sect->data);
                }
                kfree(sect);
        }
}

/**
 * Copy sector image.
 *
 * @dst destination sector.
 * @src source sector.
 *      dst->size >= src->size must be satisfied.
 */
void sector_copy(struct sector_data *dst, const struct sector_data *src)
{
        ASSERT_SECTOR_DATA(dst);
        ASSERT_SECTOR_DATA(src);
        ASSERT(dst->size >= src->size);
        
        memcpy(dst->data, src->data, src->size);
}

/**
 * Compare sector image.
 *
 * @sect0 1st sector.
 * @sect1 2nd sector.
 *
 * @return 0 when their size and their image is completely same.
 */
int sector_compare(const struct sector_data *sect0,
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

/**
 * Check size of both sectors are same or not.
 *
 * @return 1 if same, or 0.
 */
int is_same_size_sector(const struct sector_data *sect0,
                        const struct sector_data *sect1)
{
        ASSERT_SECTOR_DATA(sect0);
        ASSERT_SECTOR_DATA(sect1);

        return (sect0->size == sect1->size ? 1 : 0);
}

MODULE_LICENSE("Dual BSD/GPL");
