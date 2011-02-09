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
#include "../include/walb_sector.h"
#include "treemap.h"
#include "hashtbl.h"
#include "walb_util.h"

/**
 * DOC: Snapshot operations.
 *
 * Snapshot operations must not be called by interrupted context.
 */

/**
 * Printk macro for debug.
 */
#if defined(WALB_DEBUG)
#define PRINT_SNAPSHOT_RECORD(flag, rec) printk(                        \
                flag                                                    \
                "snapshot_record: id %u name "                          \
                "%."SNAPSHOT_NAME_MAX_LEN_S"s "                         \
                "lsid %"PRIu64" ts %"PRIu64"\n",                        \
                rec->snapshot_id,                                       \
                rec->name, rec->lsid, rec->timestamp)
#else
#define PRINT_SNAPSHOT_RECORD(flag, rec)
#endif

/**
 * Snapshot sector control state.
 */
enum {
        SNAPSHOT_SECTOR_CONTROL_FREE = 1, /* not allocated. */
        SNAPSHOT_SECTOR_CONTROL_ALLOC,    /* allocated but not loaded. */
        SNAPSHOT_SECTOR_CONTROL_CLEAN,    /* loaded and clean. */
        SNAPSHOT_SECTOR_CONTROL_DIRTY,    /* loaded and dirty. */
};

/**
 * Data structure to manage raw image of each snapshot sector.
 */
struct snapshot_sector_control
{
        /* Offset in the log device [sector_size]. */
        u64 offset;

        /* Number of free records.
           After created, must be -1.
           After initialized, 0 or more. */
        int n_free_records;
        
        /* SNAPSHOT_SECTOR_CONTROL_XXX */
        int state;

        /* Raw image of snapshot sector.
           If state is SNAPSHOT_SECTOR_CONTROL_FREE,
           this must be NULL, else this must be not NULL. */
        struct sector_data *sector;
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

        /* Next snapshot id for record allocation. */
        u32 next_snapshot_id;
        
        /* Image of sectors.
           We want to use a big array but could not
           due to PAGE_SIZE limitation.
           Index: offset -> (struct snapshot_sector_control *).
           Each struct snapshot_sector_control data is managed by this map. */
        map_t *sectors;
        
        /* Primary Index: snapshot_id -> (struct snapshot_sector_control *)
           Value should be offset but value type is unsigned long, not u64. */
        map_t *id_idx;
        
        /* Index: name -> snapshot_id.
           name is unique key. */
        struct hash_tbl *name_idx;

        /* Index: lsid -> snapshot_id.
           lsid is not unique key. */
        multimap_t *lsid_idx;
};

/**
 * Iterative over snapshot sectors.
 *
 * @off snapshot sector offset.
 * @ctl snapshot sector control.
 * @snapd snapshot data.
 */
#define for_each_snapshot_sector(off, ctl, snapd)                       \
        for (off = snapd->start_offset;                                 \
             off < snapd->end_offset &&                                 \
                     ({ ctl = get_sector_control_with_offset            \
                                     (snapd, off); 1; });               \
             off ++)


/**
 * Prototypes.
 */
/* Create/destroy snapshot data structure. */
struct snapshot_data* snapshot_data_create(
        struct block_device *bdev,
        u64 start_offset, u64 end_offset, gfp_t gfp_mask);
void snapshot_data_destroy(struct snapshot_data *snapd);

/* Initialize/finalize snapshot data structure. */
int snapshot_data_initialize(struct snapshot_data *snapd);
int snapshot_data_finalize(struct snapshot_data *snapd);

/* Snapshot operations. */
int snapshot_add_nolock(struct snapshot_data *snapd,
                        const char *name, u64 lsid, u64 timestamp);
int snapshot_add(struct snapshot_data *snapd,
                 const char *name, u64 lsid, u64 timestamp);

int snapshot_del_nolock(struct snapshot_data *snapd, const char *name);
int snapshot_del(struct snapshot_data *snapd, const char *name);
int snapshot_del_range_nolock(struct snapshot_data *snapd,
                              u64 lsid0, u64 lsid1);
int snapshot_del_range(struct snapshot_data *snapd, u64 lsid0, u64 lsid1);

int snapshot_get_nolock(struct snapshot_data *snapd, const char *name,
                        struct walb_snapshot_record *rec);
int snapshot_get(struct snapshot_data *snapd, const char *name,
                 struct walb_snapshot_record *rec);

int snapshot_n_records_range_nolock(struct snapshot_data *snapd,
                                    u64 lsid0, u64 lsid1);
int snapshot_n_records_range(struct snapshot_data *snapd,
                             u64 lsid0, u64 lsid1);
int snapshot_n_records(struct snapshot_data *snapd);

int snapshot_list_range_nolock(struct snapshot_data *snapd,
                               u8 *buf, size_t buf_size,
                               u64 lsid0, u64 lsid1);
int snapshot_list_range(struct snapshot_data *snapd,
                        u8 *buf, size_t buf_size,
                        u64 lsid0, u64 lsid1);
int snapshot_list(struct snapshot_data *snapd, u8 *buf, size_t buf_size);

/* Lock operations. We use a big lock. */
void snapshot_read_lock(struct snapshot_data *snapd);
void snapshot_read_unlock(struct snapshot_data *snapd);
void snapshot_write_lock(struct snapshot_data *snapd);
void snapshot_write_unlock(struct snapshot_data *snapd);

#endif /* _WALB_SNAPSHOT_H */
