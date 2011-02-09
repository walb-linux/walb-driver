/**
 * Definitions for Walb snapshot data.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_SNAPSHOT_H
#define _WALB_SNAPSHOT_H

#include "walb.h"
#include "walb_sector.h"

/**
 * Invalid snapshot id.
 */
#define INVALID_SNAPSHOT_ID ((u32)(-1))

/**
 * Print macro for snapshot record.
 */
#define PRINT_SNAPSHOT_RECORD(flag, rec) PRINT(         \
                flag,                                   \
                "snapshot_record: id %u name "          \
                "%."SNAPSHOT_NAME_MAX_LEN_S"s "         \
                "lsid %"PRIu64" ts %"PRIu64"\n",        \
                rec->snapshot_id,                       \
                rec->name, rec->lsid, rec->timestamp)

#define PRINT_E_SNAPSHOT_RECORD(rec) PRINT_SNAPSHOT_RECORD(KERN_ERR, rec)
#define PRINT_I_SNAPSHOT_RECORD(rec) PRINT_SNAPSHOT_RECORD(KERN_INFO, rec)
#ifdef WALB_DEBUG
#define PRINT_D_SNAPSHOT_RECORD(rec) PRINT_SNAPSHOT_RECORD(KERN_DEBUG, rec)
#else
#define PRINT_D_SNAPSHOT_RECORD(rec)
#endif

/**
 * Each snapshot information.
 */
typedef struct walb_snapshot_record {

        /* 8 + 8 + 4 + 64 = 84 bytes */
        
        u64 lsid;
        u64 timestamp; /* in seconds (the same as 'time' system call output). */
        u32 snapshot_id; /* Identifier of the snapshot.
                            INVALID_SNAPSHOT_ID means invalid.
                            This is not persistent. */

        /* Each character must be [-_0-9a-zA-Z].
           Terminated by '\0'.
           So the length of name must be from 1 to SNAPSHOT_NAME_MAX_LEN - 1. */
        char name[SNAPSHOT_NAME_MAX_LEN];
        
} __attribute__((packed)) walb_snapshot_record_t;

/**
 * Snapshot data inside sector.
 *
 * sizeof(walb_snapshot_sector_t) <= walb_super_sector.sector_size
 */
typedef struct walb_snapshot_sector {

        /* Checksum of snapshot sector */
        u32 checksum;

        u16 sector_type; /* must be SECTOR_TYPE_SNAPSHOT. */
        u16 reserved1;
        
        /* Allocation bitmap of the continuous records
           stored in the sector.
           i'th record exists when (bitmap & (1 << i)) != 0.
        */
        u64 bitmap;

        walb_snapshot_record_t record[0];
        /* The continuous data have records.
           The number of records is up to 64 or sector size */
        
} __attribute__((packed)) walb_snapshot_sector_t;

/**
 * Number of snapshots in a sector.
 */
static inline int max_n_snapshots_in_sector(int sector_size)
{
        int size;

#if 0 && !defined(__KERNEL__)
        printf("walb_snapshot_sector_t size: %zu\n",
               sizeof(walb_snapshot_sector_t));
        printf("walb_snapshot_record_t size: %zu\n",
               sizeof(walb_snapshot_record_t));
#endif
        
        size = (sector_size - sizeof(walb_snapshot_sector_t))
                / sizeof(walb_snapshot_record_t);
#if defined(__KERNEL__) && defined(WALB_DEBUG)
        printk(KERN_DEBUG "walb: sector size %d max num of records %d\n",
               sector_size, size);
#endif
        
        /* It depends on bitmap length. */
        return (size < 64 ? size : 64);
}

/**
 * Initialize snapshot record.
 */
static inline void snapshot_record_init(
        struct walb_snapshot_record *rec)
{
        ASSERT(rec != NULL);

        rec->snapshot_id = INVALID_SNAPSHOT_ID;
        rec->lsid = INVALID_LSID;
        rec->timestamp = 0;
        memset(rec->name, 0, SNAPSHOT_NAME_MAX_LEN);
}

/**
 * Assign snapshot record.
 */
static inline void snapshot_record_assign(
        struct walb_snapshot_record *rec,
        const char *name, u64 lsid, u64 timestamp)
{
        ASSERT(rec != NULL);

        ASSERT(rec->snapshot_id != INVALID_SNAPSHOT_ID);
        rec->lsid = lsid;
        rec->timestamp = timestamp;
        memcpy(rec->name, name, SNAPSHOT_NAME_MAX_LEN);
}

/**
 * Check snapshot record.
 *
 * @rec snapshot record to check.
 *
 * @return non-zero if valid, or 0.
 */
static inline int is_valid_snapshot_record(
        const struct walb_snapshot_record *rec)
{
        return (rec != NULL &&
                rec->snapshot_id != INVALID_SNAPSHOT_ID &&
                rec->lsid != INVALID_LSID &&
                is_valid_snapshot_name(rec->name));
}

/**
 * Assersion of (struct sector_data *).
 */
#define ASSERT_SNAPSHOT_SECTOR(sect) ASSERT(                            \
                (sect) != NULL &&                                       \
                (sect)->size > 0 && (sect)->data != NULL &&             \
                ((struct walb_snapshot_sector *)                        \
                 (sect)->data)->sector_type == SECTOR_TYPE_SNAPSHOT)

/**
 * Get snapshot sector
 */
static inline struct walb_snapshot_sector*
get_snapshot_sector(struct sector_data *sect)
{
        ASSERT_SECTOR_DATA(sect);
        return (struct walb_snapshot_sector *)(sect->data);
}

/**
 * Get snapshot sector for const pointer.
 */
static inline const struct walb_snapshot_sector*
get_snapshot_sector_const(const struct sector_data *sect)
{
        ASSERT_SECTOR_DATA(sect);
        return (const struct walb_snapshot_sector *)(sect->data);
}

/**
 * Get snapshot record by record index inside snapshot sector.
 */
static inline struct walb_snapshot_record* get_snapshot_record_by_idx(
        struct sector_data *sect, int idx)
{
        ASSERT_SECTOR_DATA(sect);
        return &get_snapshot_sector(sect)->record[idx];
}

/**
 * Iterative over snapshot record array.
 *
 * @i int record index.
 * @rec pointer to record.
 * @sect pointer to walb_snapshot_sector
 */
#define for_each_snapshot_record(i, rec, sect)                          \
        for (i = 0;                                                     \
             i < max_n_snapshots_in_sector((sect)->size) &&             \
                     ({ rec = &get_snapshot_sector                      \
                                     (sect)->record[i]; 1; });          \
             i ++)

#define for_each_snapshot_record_const(i, rec, sect)                    \
        for (i = 0;                                                     \
             i < max_n_snapshots_in_sector((sect)->size) &&             \
                     ({ rec = &get_snapshot_sector_const                \
                                     (sect)->record[i]; 1; });          \
             i ++)

#endif /* _WALB_SNAPSHOT_H */
