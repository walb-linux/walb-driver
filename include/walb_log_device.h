/**
 * Definitions for Walb log device.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_LOG_DEVICE_H
#define _WALB_LOG_DEVICE_H

/* #include <linux/list.h> */
#include "walb_log_record.h"
#include "walb_sector.h"

/*
 * FORMAT: Log pack.
 *
 * ASSUMPTION1: type must be the fixed-size data like C structure.
 * ASSUMPTION2: sizeof(type) is the data size of the type.
 * ASSUMPTION3: sizeof(name) is the total data size of items defined as the name.
 * ASSUMPTION4: size must be finite value. [] as infinite value is allowd only in the last definition.
 *
 * DEFINITION1: DATA type name[size] (description)
 * DEFINITION2: a sequence of DATA definition.
 * DEFINITION3: name { DATA DEFINITION }[size] (name or size=1 can be omitted.)
 * DEFINITION4: for i in [items or range]; { DATA DEFINITION using i }
 * DEFINITION5: if (predicate) { DATA DEFINITION }
 *
 * log_pack {
 *   log_header {
 *     DATA walb_log_record header[N_LOG_RECORD_IN_SECTOR]
 *     DATA u8 padding[SECTOR_SIZE - sizeof(header)]
 *   }
 *   for i in [0...N_LOG_RECORD_IN_SECTOR] {
 *     if (header[i].is_exist) {
 *       DATA u8 io_data[header[i].size * SECTOR_SIZE]
 *     }
 *   }
 * }
 *
 * PROPERTY1: sizeof(log_pack) % SECTOR_SIZE is 0.
 * PROPERTY2: sizeof(log_header) is SECTOR_SIZE.
 * PROPERTY3: offset of i'th io_data is walb_lsid_to_offset(header[i].lsid).
 * PROPERTY4: offset of log_pack is
 *            walb_lsid_to_offset(header[i].lsid - header[i].lsid_local) for all i.
 * PROPERTY5: sizeof(log_pack)
 *            log_pack_size = 1 + sum(header[i].size for all i).
 * PROPERTY6: next lsid.
 *            next_lsid = lsid + log_pack_size + 1.
 */

/*
 * FORMAT: Meta data of the log device.
 * 
 * log_device_meta_data {
 *   DATA u8 reserved[PAGE_SIZE]
 *   DATA walb_super_sector super0
 *   DATA u8 padding[PAGE_SIZE - SECTOR_SIZE]
 *   snapshot_meta_data {
 *     DATA walb_snapshot_sector snapshot_sector[super0.snapshot_metadata_size]
 *   }
 *   DATA walb_super_sector super1
 *   DATA u8 padding[PAGE_SIZE - SECTOR_SIZE]
 * }
 *
 *
 * PROPERTY1: Offset of super0
 *            n_sector_in_page = PAGE_SIZE / SECTOR_SIZE.
 *            offset_super0 = n_sector_in_page.
 * PROPERTY2: Offset of super1
 *            offset_super1 = offset_super0 + n_sector_in_page + super0.snapshot_metadata_size.
 * PROPERTY3: sizeof(log_device_meta_data)
 *            offset_super1 + n_sector_in_page.
 */

/*
 * FORMAT: Log device.
 *
 * log_device {
 *   log_device_meta_data
 *   ring_buffer {
 *     DATA u8[super0.ring_buffer_size * SECTOR_SIZE]
 *   }
 * }
 *
 * PROPERTY1: Offset of ring_buffer
 *            offset_ring_buffer = sizeof(log_device_meta_data).
 * PROPERTY2: Offset of of a given lsid is walb_lsid_to_offset(lsid).
 *
 *   u64 walb_lsid_to_offset(u64 lsid) {
 *       return offset_ring_buffer + (lsid % super0.ring_buffer_size);
 *   }
 *
 * PROPERTY3: Offset of log_pack of a given lsid and lsid_local.
 *            offset_log_pack = walb_lsid_to_offset(lsid) - lsid_local.
 */

/**
 * Super block data of the log device.
 *
 * sizeof(walb_super_sector_t) must be <= physical block size.
 */
typedef struct walb_super_sector {

        /* (4 * 4) + (2 * 4) + 16 + 64 + (8 * 5) = 144 bytes */

        /*
         * Constant value inside the kernel.
         * Lock is not required to read these variables.
         *
         * Constant value inside kernel.
         *   logical_bs, physical_bs
         *   snapshot_metadata_size
         *   uuid
         *   ring_buffer_size
         *   sector_type
         *
         * Variable inside kernel (set only in sync down)
         *   checksum
         *   oldest_lsid
         *   written_lsid
         */
        
        /* Check sum of the super block */
        u32 checksum;

        /* Both log and data device have
           the same logical block size and physical block size.
           Each IO (offset and size) is aligned by logical block size.
           Each log (offset and size) on log device is aligned. */
        u32 logical_bs;
        u32 physical_bs;
        
        /* Number of physical blocks for snapshot metadata. */
        u32 snapshot_metadata_size;

        /* UUID of the wal device. */
        u8 uuid[16];

        /* Name of the walb device.
         * terminated by '\0'. */
        char name[DISK_NAME_LEN];

        /* sector type */
        u16 sector_type; /* must be SECTOR_TYPE_SUPER. */
        u16 reserved1;
        u16 reserved2;
        u16 reserved3;

        /* Offset of the oldest log record inside ring buffer.
           [physical block] */
        /* u64 start_offset; */

        /* Ring buffer size [physical block] */
        u64 ring_buffer_size;
        
        /* Log sequence id of the oldest log record in the ring buffer.
           [physical block] */
        u64 oldest_lsid;
        
        /* Log sequence id of next of latest log record written
         * to the data device also.
         * 
         * This is used for checkpointing.
         * When walb device is assembled redo must be
         * from written_lsid to the latest lsid stored in the log device.
         *
         * The logpack of written_lsid may not be written.
         * Just previous log is guaranteed written to data device.
         */
        u64 written_lsid;

        /* Size of wrapper block device [logical block] */
        u64 device_size;
        
} __attribute__((packed)) walb_super_sector_t;

/**
 * THIS MUST BE MERGED from tool/util.c
 *
 * Check super sector.
 * This does not test checksum value.
 *
 * @sect pointer to super sector image.
 *
 * @return non-zero if valid, or 0.
 */
static inline int NOT_TESTED_is_valid_super_sector(const struct walb_super_sector *sect)
{
#ifdef CHECK
#undef CHECK
#endif
#define CHECK(condition) if (! (condition)) goto invalid
        
        CHECK(sect->logical_bs > 0);
        CHECK(sect->physical_bs > 0);
        CHECK(sect->physical_bs % sect->logical_bs == 0);
        CHECK(sect->snapshot_metadata_size > 0);
        CHECK(sect->sector_type == SECTOR_TYPE_SUPER);
        CHECK(sect->ring_buffer_size > 0);
        CHECK(sect->oldest_lsid != INVALID_LSID);
        CHECK(sect->written_lsid != INVALID_LSID);
        CHECK(sect->oldest_lsid <= sect->written_lsid);
        CHECK(sect->device_size > 0);

#undef CHECK
        return 1;
invalid:
        return 0;
}

/**
 * Invalid snapshot id.
 */
#define INVALID_SNAPSHOT_ID ((u32)(-1))

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

/**
 * Get metadata size
 *
 * @sector_size sector size.
 * @n_snapshots number snapshot to keep.
 * 
 * @return required metadata size by the sector.
 */
static inline int get_metadata_size(int sector_size, int n_snapshots)
{
        int n_sectors;
        int t;
        
        ASSERT(PAGE_SIZE % sector_size == 0 &&
               PAGE_SIZE >= sector_size);
        
        t = max_n_snapshots_in_sector(sector_size);
        n_sectors = (n_snapshots + t - 1) / t;
        return n_sectors;
}

/**
 * Get offset of primary super sector.
 *
 * @sector_size sector size in bytes.
 * @return offset in sectors.
 */
static inline u64 get_super_sector0_offset(int sector_size)
{
#ifdef __KERNEL__
        if (PAGE_SIZE % sector_size != 0) {
                printk(KERN_ERR "PAGE_SIZE: %lu sector_size %d\n",
                       PAGE_SIZE, sector_size);
        }
#endif
        ASSERT(PAGE_SIZE % sector_size == 0);
        return PAGE_SIZE/sector_size; /* skip reserved page */
}

/**
 * Get offset of first metadata sector.
 *
 * @sector_size sector size in bytes.
 * @return offset in sectors.
 */
static inline u64 get_metadata_offset(int sector_size)
{
        return get_super_sector0_offset(sector_size) + 1;
}

/**
 * Get offset of secondary super sector.
 *
 * @sector_size sector size in bytes.
 * @n_snapshots number of snapshot to keep.
 * @return offset in sectors.
 */
static inline u64 get_super_sector1_offset(int sector_size, int n_snapshots)
{
        return  get_metadata_offset(sector_size) +
                get_metadata_size(sector_size, n_snapshots);
}

/**
 * Get ring buffer offset.
 *
 * @sector_size sector size.
 * @n_snapshots number of snapshot to keep.
 *
 * @return ring buffer offset by the sector.
 */
static inline u64 get_ring_buffer_offset(int sector_size, int n_snapshots)
{
        return  get_super_sector1_offset(sector_size, n_snapshots) + 1;
}


/**
 * Get offset of primary super sector.
 */
static inline u64 get_super_sector0_offset_2(const walb_super_sector_t* super_sect)
{
        ASSERT(super_sect != NULL);
        return get_super_sector0_offset(super_sect->physical_bs);
}

/**
 * Get offset of first metadata sector.
 */
static inline u64 get_metadata_offset_2(const walb_super_sector_t* super_sect)
{
        ASSERT(super_sect != NULL);
        return get_metadata_offset(super_sect->physical_bs);
}

/**
 * Get offset of secondary super sector.
 */
static inline u64 get_super_sector1_offset_2(const walb_super_sector_t* super_sect)
{
        ASSERT(super_sect != NULL);
        return  get_metadata_offset(super_sect->physical_bs) +
                super_sect->snapshot_metadata_size;
}

/**
 * Get ring buffer offset.
 *
 * @return offset in log device [physical sector].
 */
static inline u64 get_ring_buffer_offset_2(const walb_super_sector_t* super_sect)
{
        ASSERT(super_sect != NULL);
        return  get_super_sector1_offset_2(super_sect) + 1;
}


/**
 * Get offset inside log device of the specified lsid.
 *
 * @return offset in log device [physical sector].
 */
static inline u64 get_offset_of_lsid_2
(const walb_super_sector_t* super_sect, u64 lsid)
{
        return  get_ring_buffer_offset_2(super_sect) +
                (lsid % super_sect->ring_buffer_size);
}

#endif /* _WALB_LOG_DEVICE_H */
