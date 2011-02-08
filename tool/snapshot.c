/**
 * Snapshot functions for walbctl.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <stdlib.h>
#include <string.h>

#include "util.h"
#include "snapshot.h"

/*******************************************************************************
 * Prototype of static functions.
 *******************************************************************************/

walb_snapshot_sector_t* get_snapshot_sector(struct snapshot_data_u *snapd, int idx);

/*******************************************************************************
 * Static functions.
 *******************************************************************************/

/**
 * Get pointer to the i'th snapshot sector image in a snapshot_data_u.
 *
 * @snapd snapshot data.
 * @idx sector index (0 <= idx < get_n_sectors(snapd)).
 *
 * @return pointer to snapshot sector image.
 */
walb_snapshot_sector_t* get_snapshot_sector(struct snapshot_data_u *snapd, int idx)
{
        ASSERT(snapd != NULL);
        ASSERT(0 <= idx && 0 < get_n_sectors(snapd));

        return (walb_snapshot_sector_t *)
                (snapd->sector + (get_sector_size(snapd) * get_n_sectors(snapd)));
}

/**
 * Read snapshot sector of the specified index from the log device.
 *
 * @snapd snapshot data.
 * @idx sector index (0 <= idx < get_n_sectors(snapd)).
 *
 * @return true in success, or false.
 */
bool read_sector_on_snapshot_data_u(struct snapshot_data_u *snapd, int idx)
{
        ASSERT(snapd != NULL);
        ASSERT(0 <= idx && idx < get_n_sectors(snapd));
        
        return read_snapshot_sector(snapd->fd, snapd->super,
                                    get_snapshot_sector(snapd, idx), idx);
}

/**
 * Write snapshot sector of the specified index to the log device.
 *
 * @snapd snapshot data.
 * @idx sector index (0 <= idx < get_n_sectors(snapd)).
 *
 * @return true in success, or false.
 */
bool write_sector_on_snapshot_data_u(struct snapshot_data_u *snapd, int idx)
{
        ASSERT(snapd != NULL);
        ASSERT(0 <= idx && idx < get_n_sectors(snapd));
        
        return write_snapshot_sector(snapd->fd, snapd->super,
                                     get_snapshot_sector(snapd, idx), idx);
}

/*******************************************************************************
 * Prototypes for struct snapshot_data_u.
 *******************************************************************************/

/**
 * Allocate snapshot data.
 *
 * @fd file descriptor of log device.
 * @super_sectp super sector.
 *              You must keep super sector memory image
 *              during the period when snapshot data alives.
 *
 * @return snapshot data in success, or NULL.
 */
struct snapshot_data_u* alloc_snapshot_data_u(
        int fd, const walb_super_sector_t* super_sectp)
{
        ASSERT(fd > 0);
        ASSERT(super_sectp != NULL);
        
        /* Allocate memory */
        struct snapshot_data_u *snapd;
        snapd = (struct snapshot_data_u *)
                malloc(sizeof(struct snapshot_data_u));
        if (snapd == NULL) { goto nomem0; }

        snapd->fd = fd;
        snapd->super = super_sectp;
        snapd->next_snapshot_id = 0;
        snapd->n_sectors = (int)super_sectp->snapshot_metadata_size;
        snapd->sector = (u8 *)malloc(get_sector_size(snapd) * get_n_sectors(snapd));
        if (snapd->sector == NULL) { goto nomem1; }

        return snapd;
nomem1:
        free_snapshot_data_u(snapd);
nomem0:
        return NULL;
}

/**
 * Free struct snapshot_data_u data.
 */
void free_snapshot_data_u(struct snapshot_data_u* snapd)
{
        if (snapd) {
                free(snapd->sector);
        }
        free(snapd);
}

/**
 * Initialize struct snapshot_data_u data.
 *
 * @return true in success, or false.
 */
bool initialize_snapshot_data_u(struct snapshot_data_u *snapd)
{
        int n_sectors = (int)get_n_sectors(snapd);

        /*
         * 1. Load image of each snapshot sector.
         * 2. Renumbering of snapshot_id of each snapshot record.
         * 3. Set next_snapshot_id.
         */
        int i;
        for (i = 0; i < n_sectors; i ++) {

                if (! read_sector_on_snapshot_data_u(snapd, i)) {
                        LOGe("read %d'th snapshot sector failed.\n", i);
                        goto error;
                }
                
                walb_snapshot_sector_t *snap_sect =
                        get_snapshot_sector(snapd, i);
                walb_snapshot_record_t *rec;
                int rec_idx;

                /* now editing */
                /* for_each_snapshot_record(rec_idx, rec, snap_sect) { */
                        
                /*         if (! is_alloc_snapshot_record(rec_idx, snap_sect)) { */
                /*                 continue; */
                /*         } */

                /*         if (! is_valid_snapshot_record(rec)) { */
                /*                 memset(rec, 0, sizeof(struct walb_snapshot_record)); */
                /*                 clear_alloc_snapshot_record(rec_idx, snap_sect); */
                /*                 continue; */
                /*         } */
                /*         rec->snapshot_id = snapd->next_snapshot_id ++; */
                /* } */
        }
        return true;
error:
        return false;
}

/**
 * Finalize struct snapshot_data_u data.
 * This writes down all snapshot data.
 *
 * @return true in success, or false.
 */
bool finalize_snapshot_data_u(struct snapshot_data_u* snapd)
{
        if (snapd == NULL) { return false; }
        
        int i;
        int n_sectors = get_n_sectors(snapd);
        for (i = 0; i < n_sectors; i ++) {

                if (! write_sector_on_snapshot_data_u(snapd, i)) {
                        LOGe("write %d'th snapshot sector failed.\n", i);
                        goto error;
                }
        }
        return true;
error:
        return false;
}

/**
 *
 */
bool is_valid_snaphsot_data_u(struct snapshot_data_u* snapd)
{
        /* return (snapd != NULL && */
        /*         snapd->fd > 0); */

        /* now editing */

/* not yet implemented. */
        return false;
}

/*******************************************************************************
 * Functions snapshot manipulation.
 *******************************************************************************/

/**
 *
 */
bool snapshot_add(struct snapshot_data_u *snapd,
                  const char *name, u64 lsid, u64 timestamp)
{
        /* not yet implemented. */
        return false;
}

/**
 *
 */
bool snapshot_del(struct snapshot_data_u *snapd, const char *name)
{
        /* not yet implemented. */
        return false;
}

/**
 *
 */
bool snapshot_del_range(struct snapshot_data_u *snapd, u64 lsid0, u64 lsid1)
{
        /* not yet implemented. */
        return false;
}

/**
 *
 */
struct walb_snapshot_record* snapshot_get(
        struct snapshot_data_u *snapd, const char *name)
{
        /* not yet implemented */
        return NULL;
}

/**
 *
 */
int snapshot_n_records_range(const struct snapshot_data_u *snapd,
                             u64 lsid0, u64 lsid1)
{
        /* not yet implemented */
        return -1;
}
        
/**
 *
 */
int snapshot_n_records(const struct snapshot_data_u *snapd)
{
        /* not yet implemented */
        return -1;
}

/**
 *
 */
int snapshot_list_range(const struct snapshot_data_u *snapd,
                        struct walb_snapshot_record **rec_ary_p, size_t ary_size,
                        u64 lsid0, u64 lsid1)
{
        /* not yet implemented */
        return -1;
}

/**
 *
 */
int snapshot_list(const struct snapshot_data_u *snapd,
                  struct walb_snapshot_record **rec_ary_p, size_t ary_size)
{
        /* not yet implemented */
        return -1;
}

/* end of file */
