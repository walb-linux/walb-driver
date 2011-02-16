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
 * Utility macros.
 *******************************************************************************/

#define get_n_sectors(snapd) ((int)(snapd)->super->snapshot_metadata_size)
#define get_sector_size(snapd) ((snapd)->super->physical_bs)
#define get_sector(snapd, i) (get_sector_data_in_array((snapd)->sect_ary, i))

/*******************************************************************************
 * Prototype of static functions.
 *******************************************************************************/

static struct sector_data* get_sector_snapshot_data_u(
        struct snapshot_data_u *snapd, int idx);

/*******************************************************************************
 * Static functions.
 *******************************************************************************/

/**
 * Get pointer to the i'th snapshot sector image in a snapshot_data_u.
 *
 * @snapd snapshot data.
 * @idx sector index (0 <= idx < get_n_sectors(snapd)).
 *
 * @return pointer to sector_data.
 */
static struct sector_data* get_sector_snapshot_data_u(
        struct snapshot_data_u *snapd, int idx)
{
        ASSERT(snapd != NULL);
        ASSERT(0 <= idx && 0 < get_n_sectors(snapd));

        return get_sector(snapd,idx);
}

/*******************************************************************************
 * Functions for struct snapshot_data_u.
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
        
        /* For itself. */
        struct snapshot_data_u *snapd;
        snapd = (struct snapshot_data_u *)
                malloc(sizeof(struct snapshot_data_u));
        if (snapd == NULL) { goto nomem0; }

        snapd->fd = fd;
        snapd->super = super_sectp;
        snapd->next_snapshot_id = 0;

        snapd->sect_ary = sector_data_array_alloc
                (get_n_sectors(snapd), get_sector_size(snapd));
        if (! snapd->sect_ary) { goto nomem1; }
        
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
                sector_data_array_free(snapd->sect_ary);
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
        int i;
        struct sector_data *sect;

        /*
         * 1. Load image of each snapshot sector.
         * 2. Renumbering of snapshot_id of each snapshot record.
         * 3. Set next_snapshot_id.
         */
        for_each_snapshot_sector(i, sect, snapd) {
                
                if (! read_sector_snapshot_data_u(snapd, i)) {
                        LOGe("read %d'th snapshot sector failed.\n", i);
                        goto error;
                }

                ASSERT_SNAPSHOT_SECTOR(sect);
                
                walb_snapshot_record_t *rec;
                int rec_i;

                /* Check and initialize each record. */
                for_each_snapshot_record(rec_i, rec, sect) {

                        if (! is_alloc_snapshot_record(rec_i, sect)) {
                                continue;
                        }
                        if (! is_valid_snapshot_record(rec)) {
                                snapshot_record_init(rec);
                                clear_alloc_snapshot_record(rec_i, sect);
                                continue;
                        }
                        rec->snapshot_id = snapd->next_snapshot_id ++;
                }
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
        if (! snapd) { return false; }
        
        int i;
        int n_sectors = get_n_sectors(snapd);
        for (i = 0; i < n_sectors; i ++) {

                if (! write_sector_snapshot_data_u(snapd, i)) {
                        LOGe("write %d'th snapshot sector failed.\n", i);
                        goto error;
                }
        }
        return true;
error:
        return false;
}

/**
 * Initialize struct snapshot_data_u data.
 *
 * @return true in success, or false.
 */
bool clear_snapshot_data_u(struct snapshot_data_u *snapd)
{
        if (! snapd) { return false; }

        int rec_i, sect_i;
        walb_snapshot_record_t *rec;
        struct sector_data *sect;

        for_each_snapshot_record_in_snapd(rec_i, rec, sect_i, sect, snapd) {
                
                snapshot_record_init(rec);
                clear_alloc_snapshot_record(rec_i, sect);
        }
        snapd->next_snapshot_id = 0;
        
        return true;
}

/**
 * Check validness of snapshot_data_u.
 */
bool is_valid_snaphsot_data_u(struct snapshot_data_u* snapd)
{
        int i;
        struct sector_data *sect;

        if (! (snapd &&
               snapd->fd > 0 &&
               is_valid_super_sector(snapd->super, get_sector_size(snapd)))) {

                return false;
        }

        for_each_snapshot_sector(i, sect, snapd) {

                if (! is_valid_snapshot_sector(sect)) { return false; }
        }
        
        return true;
}

/*******************************************************************************
 * Functions for snapshot sector manipulation.
 *******************************************************************************/

/**
 * Read snapshot sector of the specified index from the log device.
 *
 * @snapd snapshot data.
 * @idx sector index (0 <= idx < get_n_sectors(snapd)).
 *
 * @return true in success, or false.
 */
bool read_sector_snapshot_data_u(struct snapshot_data_u *snapd, int idx)
{
        ASSERT(snapd != NULL);
        ASSERT(0 <= idx && idx < (int)get_n_sectors(snapd));
        
        return read_snapshot_sector
                (snapd->fd, snapd->super,
                 get_snapshot_sector(get_sector_snapshot_data_u(snapd, idx)),
                 idx);
}

/**
 * Read all snapshot sectors from the log device.
 *
 * @snapd pointer to snapshot_data_u.
 *        You must call alloc_snapshot_data_u() before.
 *
 * @return true in success, or false.
 */
bool read_all_sectors_snapshot_data_u(struct snapshot_data_u *snapd)
{
        if (! snapd) { return false; }
        
        int i;
        struct sector_data *sect;

        for_each_snapshot_sector(i, sect, snapd) {
                read_sector_snapshot_data_u(snapd, i);
        }
        return true;
}

/**
 * Write snapshot sector of the specified index to the log device.
 *
 * @snapd snapshot data.
 * @idx sector index (0 <= idx < get_n_sectors(snapd)).
 *
 * @return true in success, or false.
 */
bool write_sector_snapshot_data_u(struct snapshot_data_u *snapd, int idx)
{
        ASSERT(snapd != NULL);
        ASSERT(0 <= idx && idx < (int)get_n_sectors(snapd));
        
        return write_snapshot_sector
                (snapd->fd, snapd->super,
                 get_snapshot_sector(get_sector_snapshot_data_u(snapd, idx)),
                 idx);
}

/**
 * Write all snapshot sectors to the log device.
 *
 * @snapd pointer to snapshot_data_u.
 *
 * @return true in success, or false.
 */
bool write_all_sectors_snapshot_data_u(struct snapshot_data_u *snapd)
{
        if (! snapd) { return false; }
        
        int i;
        struct sector_data *sect;

        for_each_snapshot_sector(i, sect, snapd) {
                write_sector_snapshot_data_u(snapd, i);
        }
        return true;
}

/*******************************************************************************
 * Functions for snapshot manipulation.
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
