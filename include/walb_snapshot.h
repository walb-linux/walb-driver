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

/*******************************************************************************
 * Utility macros.
 *******************************************************************************/

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


/*******************************************************************************
 * Struct definition.
 *******************************************************************************/

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

#if 0 && !defined(__KERNEL__) && defined(WALB_DEBUG)
        printf("walb_snapshot_sector_t size: %zu\n",
               sizeof(walb_snapshot_sector_t));
        printf("walb_snapshot_record_t size: %zu\n",
               sizeof(walb_snapshot_record_t));
#endif
        
        size = (sector_size - sizeof(walb_snapshot_sector_t))
                / sizeof(walb_snapshot_record_t);
#if 0 && defined(__KERNEL__) && defined(WALB_DEBUG)
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


/*******************************************************************************
 * Functions for snapshot sector.
 *******************************************************************************/

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
 * Check whether nr'th record is allocated in a snapshot sector.
 *
 * @return non-zero if exist, or 0.
 */
static inline int is_alloc_snapshot_record(
        int nr, const struct sector_data *sect)
{
        ASSERT_SNAPSHOT_SECTOR(sect);
        ASSERT(0 <= nr && nr < 64);
        
        return test_u64bits(nr, &get_snapshot_sector_const(sect)->bitmap);
}

/**
 * Set allocation bit of a snapshot sector.
 */
static inline void set_alloc_snapshot_record(
        int nr, struct sector_data *sect)
{
        ASSERT_SNAPSHOT_SECTOR(sect);
        ASSERT(0 <= nr && nr < 64);
        
        set_u64bits(nr, &get_snapshot_sector(sect)->bitmap);
}

/**
 * Clear exist bit of a snapshot sector.
 */
static inline void clear_alloc_snapshot_record(
        int nr, struct sector_data *sect)
{
        ASSERT_SNAPSHOT_SECTOR(sect);
        ASSERT(0 <= nr && nr < 64);

        clear_u64bits(nr, &get_snapshot_sector(sect)->bitmap);
}

/**
 * Get index of snapshot record by a snapshot_id.
 *
 * @sect snapshot sector.
 * @snapshot_id snapshot id to find.
 *   DO NOT specify INVALID_SNAPSHOT_ID.
 *
 * @return record index in the snapshot sector if found, or -1.
 */
static inline int get_idx_in_snapshot_sector(
        const struct sector_data *sect, u32 snapshot_id)
{
        int i;
        const struct walb_snapshot_record *rec;
        
        ASSERT_SNAPSHOT_SECTOR(sect);
        ASSERT(snapshot_id != INVALID_SNAPSHOT_ID);

        for_each_snapshot_record_const(i, rec, sect) {

                if (rec->snapshot_id == snapshot_id) {
                        return i;
                }
        }
        return -1;
}

/**
 * Get snapshot record with a snapshot_id.
 */
static inline struct walb_snapshot_record*
get_record_in_snapshot_sector(struct sector_data *sect, u32 snapshot_id)
{
        int idx;

        idx = get_idx_in_snapshot_sector(sect, snapshot_id);
        if (idx >= 0) {
                return get_snapshot_record_by_idx(sect, idx);
        } else {
                return NULL;
        }
}

/**
 * Get number of snapshots in the sector.
 */
static inline int get_n_records_in_snapshot_sector_detail(
        struct walb_snapshot_sector *snap_sect, int max_n)
{
        int i, n;

        ASSERT(snap_sect != NULL);
        ASSERT(snap_sect->sector_type == SECTOR_TYPE_SNAPSHOT);
        
        n = 0;
        for (i = 0; i < max_n; i ++) {
                if (test_u64bits(i, &snap_sect->bitmap)) {
                        n ++;
                }
        }
        ASSERT(0 <= n && n <= 64);
        
        return n;
}

/**
 * Get number of active records in a snapshot sector.
 */
static inline int get_n_records_in_snapshot_sector(struct sector_data *sect)
{
        int max_n = max_n_snapshots_in_sector(sect->size);
        ASSERT_SNAPSHOT_SECTOR(sect);
        
        return get_n_records_in_snapshot_sector_detail(
                get_snapshot_sector(sect), max_n);
}

/**
 * Get number of free records in a snapshot sector.
 */
static inline int get_n_free_records_in_snapshot_sector(struct sector_data *sect)
{
        int max_n = max_n_snapshots_in_sector(sect->size);
        ASSERT_SNAPSHOT_SECTOR(sect);

        return max_n - get_n_records_in_snapshot_sector_detail(
                get_snapshot_sector(sect), max_n);
}

/**
 * Check whether snapshot sector is valid.
 *
 * @sect sector data which must be snapshot sector.
 *
 * @return non-zero if valid, or 0.
 */
static inline int is_valid_snapshot_sector(const struct sector_data *sect)
{
        int count = 0;
        int i;
        const struct walb_snapshot_record *rec;

        ASSERT_SNAPSHOT_SECTOR(sect);
        
        for_each_snapshot_record_const(i, rec, sect) {

                if (is_alloc_snapshot_record(i, sect)) {
                        if (! is_valid_snapshot_record(rec)) {
                                count ++;
                        }
                } else {
                        if (rec->snapshot_id != INVALID_SNAPSHOT_ID) {
                                count ++;
                        }
                }
        }
        PRINT_D("snapshot sector invalid record: %d\n", count);
        return (count == 0);
}


#endif /* _WALB_SNAPSHOT_H */
