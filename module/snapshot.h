/**
 * snapshot.h - Snapshot management.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_SNAPSHOT_H_KERNEL
#define WALB_SNAPSHOT_H_KERNEL

#include "check_kernel.h"

#include <linux/blkdev.h>
#include <linux/rwsem.h>

#include "walb/log_device.h"
#include "walb/sector.h"
#include "treemap.h"
#include "hashtbl.h"
#include "util.h"

/**
 * DOC: Snapshot operations.
 *
 * Snapshot operations must not be called by atomic context.
 */

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
           If the state is SNAPSHOT_SECTOR_CONTROL_FREE,
           this must be NULL, else this must be not NULL. */
        struct sector_data *sector;
};

/**
 * Records and indexes of all snapshots for a walb device
 * can be accessed from this data.
 */
struct snapshot_data
{
        /* Lock to access all data in the struct. */
        struct rw_semaphore lock;
        
        /* All sectors exist in start_offset <= offset < end_offset.
	   [physical block]. */
        u64 start_offset;
        u64 end_offset;

        /* Block device of the log device. */
        struct block_device *bdev;

        /* Sector size (physical block size). */
        u32 sector_size;

        /* Next snapshot id for record allocation.
	   This will be simply incremented per allocation. */
        u32 next_snapshot_id;
        
        /* Image of sectors.
           We want to use a big array but could not
           due to PAGE_SIZE limitation.
           Index: offset -> (struct snapshot_sector_control *).
           Each struct snapshot_sector_control data is managed by this map. */
        struct map *sectors;
        
        /* Primary Index: snapshot_id -> (struct snapshot_sector_control *)
           Value should be offset but value type is unsigned long, not u64. */
        struct map *id_idx;
        
        /* Index: name -> snapshot_id.
           name is unique key. */
        struct hash_tbl *name_idx;

        /* Index: lsid -> snapshot_id.
           lsid is not unique key. */
        struct multimap *lsid_idx;
};

/**
 * Iterative over snapshot sectors.
 *
 * @off snapshot sector offset (u64).
 * @ctl pointer to snapshot sector control
 *   (struct snapshot_sector_control *).
 * @snapd pointer to snapshot data.
 */
#define for_each_snapshot_sector(off, ctl, snapd)		\
        for (off = snapd->start_offset;				\
             off < snapd->end_offset &&				\
                     ({ ctl = get_control_by_offset		\
                                     (snapd, off); 1; });	\
             off++)

/**
 * Prototypes.
 */
/* Create/destroy snapshot data structure. */
struct snapshot_data* snapshot_data_create(
        struct block_device *bdev, u64 start_offset, u64 end_offset);
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
                        struct walb_snapshot_record **rec);
int snapshot_get(struct snapshot_data *snapd, const char *name,
                 struct walb_snapshot_record **rec);

int snapshot_n_records_range_nolock(struct snapshot_data *snapd,
                                    u64 lsid0, u64 lsid1);
int snapshot_n_records_range(struct snapshot_data *snapd,
                             u64 lsid0, u64 lsid1);
int snapshot_n_records(struct snapshot_data *snapd);

int snapshot_list_range_nolock(struct snapshot_data *snapd,
			struct walb_snapshot_record *buf, size_t buf_size,
			u64 lsid0, u64 lsid1);
int snapshot_list_range(struct snapshot_data *snapd,
			struct walb_snapshot_record *buf, size_t buf_size,
                        u64 lsid0, u64 lsid1);
int snapshot_list(struct snapshot_data *snapd,
		struct walb_snapshot_record *buf, size_t buf_size);

/*******************************************************************************
 * Lock operations. We use a big lock.
 *******************************************************************************/

/**
 * Big read lock of snapshot data.
 */
static inline void snapshot_read_lock(struct snapshot_data *snapd)
{
	ASSERT(snapd);
	might_sleep();
	down_read(&snapd->lock);
}

/**
 * Big read unlock of snapshot data.
 */
static inline void snapshot_read_unlock(struct snapshot_data *snapd)
{
	ASSERT(snapd);
	might_sleep();
	up_read(&snapd->lock);
}

/**
 * Big write lock of snapshot data.
 */
static inline void snapshot_write_lock(struct snapshot_data *snapd)
{
	ASSERT(snapd);
	might_sleep();
	down_write(&snapd->lock);
}

/**
 * Big write unlock of snapshot data.
 */
static inline void snapshot_write_unlock(struct snapshot_data *snapd)
{
	ASSERT(snapd);
	might_sleep();
	up_write(&snapd->lock);
}


#endif /* WALB_SNAPSHOT_H_KERNEL */
