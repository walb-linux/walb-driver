/**
 * Definitions for walb super sector.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_SUPER_H
#define _WALB_SUPER_H

#include "walb.h"
#include "walb_sector.h"

/**
 * Assert macro.
 */
#define ASSERT_SUPER_SECTOR(sect) ASSERT(is_valid_super_sector(sect))

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
 * Check super sector.
 * Do not use this directly. Use is_valid_super_sector() instead.
 * This function does not evaluate checksum.
 *
 * @sect pointer to super sector image.
 *
 * @return non-zero if valid, or 0.
 */
static inline int __is_valid_super_sector(const walb_super_sector_t* sect, int physical_bs)
{
#define CHECK(condition) if (! (condition)) {                   \
                PRINTV_D("super sector is not valid.\n");       \
                goto invalid;                                   \
        }

        /* physical_bs */
        CHECK(physical_bs > 0);
        
        /* sector type */
        CHECK(sect->sector_type == SECTOR_TYPE_SUPER);
        /* block size */
        CHECK(sect->physical_bs == (u32)physical_bs);
        CHECK(sect->physical_bs >= sect->logical_bs);
        CHECK(sect->physical_bs % sect->logical_bs == 0);
        /* lsid consistency. */
        CHECK(sect->oldest_lsid != INVALID_LSID);
        CHECK(sect->written_lsid != INVALID_LSID);
        CHECK(sect->oldest_lsid <= sect->written_lsid);
        CHECK(sect->written_lsid - sect->oldest_lsid <= sect->ring_buffer_size);
        
#undef CHECK
        return 1;
invalid:
        return 0;
}
        
/**
 * Check super sector.
 *
 * @return Non-zero if valid, or 0.
 */
static inline int is_valid_super_sector(const struct sector_data* sect)
{
        if (! is_valid_sector_data(sect)) { return 0; }
        return __is_valid_super_sector(sect->data, sect->size);
}

/**
 * Set super sector name.
 *
 * @super_sect super sector.
 * @name name or NULL.
 *
 * @return pointer to result name.
 */
static inline char* set_super_sector_name(walb_super_sector_t* super_sect, const char *name)
{
        if (name) {
                strncpy(super_sect->name, name, DISK_NAME_LEN);
        } else {
                super_sect->name[0] = '\0';
        }
        return super_sect->name;
}

#endif /* _WALB_SUPER_H */
