/**
 * walb_sector.h - Sector operations.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_SECTOR_H
#define _WALB_SECTOR_H

#include "walb_util.h"

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
 * Prototypes.
 */
struct sector_data* sector_alloc(int sector_size, gfp_t gfp_mask);
void sector_free(struct sector_data *sect);
void sector_copy(struct sector_data *dst, const struct sector_data *src);
int sector_compare(const struct sector_data *sect0,
                   const struct sector_data *sect1);
int is_same_size_sector(const struct sector_data *sect0,
                        const struct sector_data *sect1);



#endif /* _WALB_SECTOR_H */
