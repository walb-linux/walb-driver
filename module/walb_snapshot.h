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
 * Records and indexes of all snapshots for a walb device
 * can be accessible from this data.
 */
struct snapshots
{
        /* Lock to access all data in the struct. */
        struct rw_semaphore lock;
        
        /* All sectors exist in min_offset <= offset < max_offset. */
        u64 min_offset;
        u64 max_offset;

        /* Block device of the log device. */
        struct block_device *bdev;

        /* Sector size (physical block size). */
        u32 sector_size;
        
        /* Index: offset -> (struct snapshot_sector_mem *) */
        struct tree_map *offset_idx;
        
        /* Index: name -> (struct snapshot_sector_mem *) */
        struct hash_tbl *name_idx;
};

/**
 * Data structure to manage raw image of each snapshot sector.
 */
struct snapshot_sector_mem
{
        /* Offset in the log device [sector_size]. */
        u64 offset;

        /* Sync down is required if 1, or 0.
           This is meaningful when snap_sect_p is not NULL. */
        atomic_t is_dirty;
        
        /* Raw image of snapshot sector.
           There is no memory image if NULL. */
        walb_snapshot_sector_t *snap_sect_p;
};

/**
 * Prototypes.
 */

/* Create/destroy snapshots structure. */
struct snapshots* snapshots_create(u64 min_offset, u64 max_offset, u32 sector_size);
void snapshots_destroy(struct snapshots *snaps);

/* Snapshot operations. */
int snapshots_add(struct snapshots *snaps, const char *name, u64 lsid, u64 timestamp);
void snapshots_del(struct snapshots *snaps, const char *name);
void snapshots_del_before_lsid(struct snapshots *snaps, u64 lsid);
void snapshots_del_before_timestamp(struct snapshots *snaps, u64 timestamp);
int snapshots_get(struct snapshots *snaps, const char *name);
int snapshots_n_records(struct snapshots *snaps);
int snapshots_list(struct snapshots *snaps, size_t buf_size, u8 __user *buf);

/* Lock operations. We use a big lock. */
void snapshots_read_lock(struct snapshots *snaps);
void snapshots_read_unlock(struct snapshots *snaps);
void snapshots_write_lock(struct snapshots *snaps);
void snapshots_write_unlock(struct snapshots *snaps);

#endif /* _WALB_SNAPSHOT_H */
