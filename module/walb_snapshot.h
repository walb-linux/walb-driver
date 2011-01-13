/**
 * walb_snapshot.h - Snapshot management of walb.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_SNAPSHOT_H
#define _WALB_SNAPSHOT_H

#include <linux/blkdev.h>
#include <linux/rwsem.h>

#include "../include/walb_log_device.h"
#include "treemap.h"
#include "hashtbl.h"

/**
 * DOC: Snapshot operations.
 *
 * Snapshot operations must not be called by interrupted context.
 */

/**
 * Data structure to manage raw image of each snapshot sector.
 */
struct snapshot_sector_control
{
        /* Offset in the log device [sector_size]. */
        u64 offset;

        /* Sync down is required if 1, or 0.
           This is meaningful when snap_sect_p is not NULL. */
        /* atomic_t is_dirty; */

        /* Number of free records.
           After initialized, must be -1.
           After loaded, 0 or more. */
        int n_free_records;
        
        /* Raw image of snapshot sector.
           There is no memory image if NULL. */
        walb_snapshot_sector_t *image;
};

/**
 * Records and indexes of all snapshots for a walb device
 * can be accessible from this data.
 */
struct snapshot_data
{
        /* Lock to access all data in the struct. */
        struct rw_semaphore lock;
        
        /* All sectors exist in start_offset <= offset < end_offset. */
        u64 start_offset;
        u64 end_offset;

        /* Block device of the log device. */
        struct block_device *bdev;

        /* Sector size (physical block size). */
        u32 sector_size;

        /* Image of sectors.
           We want to use a big array but could not
           due to PAGE_SIZE limitation.
           Index: offset -> (struct snapshot_sector_control *).
           Each struct snapshot_sector_control data is managed by this map. */
        map_t *sectors;
        
        /* Index: snapshot_id -> (struct snapshot_sector_control *)
           Value should be offset but value type is unsigned long, not u64. */
        map_t *id_idx;
        
        /* Index: name -> snapshot_id. */
        struct hash_tbl *name_idx;

        /* Index: lsid -> snapshot_id. */
        multimap_t *lsid_idx;
};

/**
 * Prototypes.
 */

/* Create/destroy snapshots structure. */
struct snapshot_data* snapshot_data_create(
        struct block_device *bdev,
        u64 start_offset, u64 end_offset, gfp_t gfp_mask);
void snapshot_data_destroy(struct snapshot_data *snapd);

/* Snapshot operations. */
int snapshot_add(struct snapshot_data *snapd, const char *name, u64 lsid, u64 timestamp);
void snapshot_del(struct snapshot_data *snapd, const char *name);
void snapshot_del_before_lsid(struct snapshot_data *snapd, u64 lsid);
int snapshot_get(struct snapshot_data *snapd, const char *name);
int snapshot_n_records(struct snapshot_data *snapd);
int snapshot_list(struct snapshot_data *snapd, u8 *buf, size_t buf_size);

/* Lock operations. We use a big lock. */
void snapshot_read_lock(struct snapshot_data *snapd);
void snapshot_read_unlock(struct snapshot_data *snapd);
void snapshot_write_lock(struct snapshot_data *snapd);
void snapshot_write_unlock(struct snapshot_data *snapd);

#endif /* _WALB_SNAPSHOT_H */
