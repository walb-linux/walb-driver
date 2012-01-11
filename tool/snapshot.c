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
 * Utility functions.
 *******************************************************************************/

/**
 * Print snapshot record for debug.
 */
void print_snapshot_record(const walb_snapshot_record_t* snap_rec)
{
        ASSERT(snap_rec != NULL);
        PRINT_D_SNAPSHOT_RECORD(snap_rec);
}

/**
 * print snapshot sector for debug.
 */
void print_snapshot_sector(const walb_snapshot_sector_t* snap_sect, u32 sector_size)
{
        printf("checksum: %u\n", snap_sect->checksum);

        printf("bitmap: ");
        print_u32bitmap(snap_sect->bitmap);
        printf("\n");

        /* Print continuous snapshot records */
        int i;
        int max = get_max_n_records_in_snapshot_sector(sector_size);
        for (i = 0; i < max; i ++) {
                printf("snapshot record %d: ", i);
                print_snapshot_record(&snap_sect->record[i]);
        }
}

/**
 * Write snapshot sector.
 *
 * @fd File descriptor of log device.
 * @super_sect super sector data to refer its members.
 * @snap_sect snapshot sector data to be written.
 *            It's allocated size must be really sector size.
 *            Only checksum area will be overwritten.
 * @idx idx'th sector is written. (0 <= idx < snapshot_metadata_size)
 *
 * @return true in success, or false.
 */
bool write_snapshot_sector(int fd, const walb_super_sector_t* super_sect,
                           walb_snapshot_sector_t* snap_sect, u32 idx)
{
        ASSERT(fd >= 0);
        ASSERT(super_sect != NULL);
        ASSERT(snap_sect != NULL);
        
        u32 sect_sz = super_sect->physical_bs;
        u32 meta_sz = super_sect->snapshot_metadata_size;
        if (idx >= meta_sz) {
                LOGe("idx range over. idx: %u meta_sz: %u\n", idx, meta_sz);
                return false;
        }

        /* checksum */
        u8 *sector_buf = (u8*)snap_sect;
        snap_sect->checksum = 0;
        u32 csum = checksum(sector_buf, sect_sz);
        snap_sect->checksum = csum;
        ASSERT(checksum(sector_buf, sect_sz) == 0);

        /* really write sector data. */
        u64 off = get_metadata_offset_2(super_sect) + idx;
        if (! write_sector(fd, sector_buf, sect_sz, off)) {
                return false;
        }
        return true;
}

/**
 * Read snapshot sector.
 *
 * @fd File descriptor of log device.
 * @super_sect super sector data to refer its members.
 * @snap_sect snapshot sector buffer to be read.
 *            It's allocated size must be really sector size.
 * @idx idx'th sector is read. (0 <= idx < snapshot_metadata_size)
 *
 * @return true in success, or false.
 */
bool read_snapshot_sector(int fd, const walb_super_sector_t* super_sect,
                          walb_snapshot_sector_t* snap_sect, u32 idx)
{
        ASSERT(fd >= 0);
        ASSERT(super_sect != NULL);
        ASSERT(snap_sect != NULL);
        
        u32 sect_sz = super_sect->physical_bs;
        u32 meta_sz = super_sect->snapshot_metadata_size;
        if (idx >= meta_sz) {
                LOGe("idx range over. idx: %u meta_sz: %u\n", idx, meta_sz);
                return false;
        }
        
        /* Read sector data.
           Confirm checksum. */
        u8 *sector_buf = (u8 *)snap_sect;
        u64 off = get_metadata_offset_2(super_sect) + idx;
        if (! read_sector(fd, sector_buf, sect_sz, off) ||
            checksum(sector_buf, sect_sz) != 0) {
                return false;
        }
        return true;
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

        snapd->sect_ary = sector_array_alloc
                (get_sector_size(snapd), get_n_sectors(snapd));
        if (! snapd->sect_ary) { goto nomem1; }

        clear_snapshot_data_u(snapd);
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
                sector_array_free(snapd->sect_ary);
        }
        free(snapd);
}

/**
 * Initialize struct snapshot_data_u data.
 *
 * 1. Validate each allocated record.
 * 2. Re-assign unique snapshot_id.
 *
 * @snapd pointer to snapshot_data_u.
 *
 * @return true in success, or false.
 */
void initialize_snapshot_data_u(struct snapshot_data_u *snapd)
{
        ASSERT(snapd);
        
        int sect_i, rec_i;
        struct sector_data *sect;
        walb_snapshot_record_t *rec;

        for_each_snapshot_record_in_snapd(rec_i, rec, sect_i, sect, snapd) {

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
        ASSERT_SNAPSHOT_SECTOR_DATA_U(snapd);
}

/**
 * Initialize struct snapshot_data_u data.
 *
 * @return true in success, or false.
 */
void clear_snapshot_data_u(struct snapshot_data_u *snapd)
{
        ASSERT(snapd);

        int rec_i, sect_i;
        walb_snapshot_record_t *rec;
        struct sector_data *sect;

        for_each_snapshot_sector(sect_i, sect, snapd) {
                init_snapshot_sector(sect);
                ASSERT_SNAPSHOT_SECTOR(sect);
        }
        snapd->next_snapshot_id = 0;
        ASSERT_SNAPSHOT_SECTOR_DATA_U(snapd);
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
               __is_valid_super_sector(snapd->super, get_sector_size(snapd)))) {
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
        bool ret = true;

        for_each_snapshot_sector(i, sect, snapd) {
                ret = read_sector_snapshot_data_u(snapd, i);
                if (! ret) { LOGe("read snapshot sector %d failed.\n", i); }
        }
        return ret;
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
        bool ret = true;

        for_each_snapshot_sector(i, sect, snapd) {
                ret = write_sector_snapshot_data_u(snapd, i);
                if (! ret) { LOGe("write snapshot sector %d failed.\n", i); }
        }
        return ret;
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
