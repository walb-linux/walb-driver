/**
 * Definitions for Walb log record.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_LOG_RECORD_H
#define WALB_LOG_RECORD_H

#include <linux/list.h>

#ifdef __KERNEL__
#include <linux/types.h>
#else /* __KERNEL__ */
#include <stdint.h>
typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
#endif /* __KERNEL__ */

/**
 * Sector size is fixed value.
 */
#define SECTOR_SIZE 512

/**
 * Log record.
 */
typedef struct walb_log_record {

        /* 256 bit size */

        u64 lsid; /* Log sequence id */

        u16 lsid_local; /* local sequence id as the data offset in the log record. */
        u16 reserved1;
        u16 size; /* IO size in sector. */
        u16 is_exist; /* 0 if this record is exist. */
        
        u64 offset; /* IO offset in sector. */

        /* Just sum of the array assuming data contents as an array of u64 integer. */
        u64 checksum;

        /*
         * Data offset in the ring buffer.
         *   offset = lsid_to_offset(lsid)
         * Offset of the log record header.
         *   offset = lsid_to_offset(lsid) - lsid_local
         */
        
} __attribute__((packed)) walb_log_record_t;

#define N_LOG_RECORD_IN_SECTOR (SECTOR_SIZE / sizeof(walb_log_record_t))
#define LOG_RECORD_SIZE_IN_SECTOR (N_LOG_RECORD_IN_SECTOR * sizeof(walb_log_record_t))

/**
 * Log record header data inside sector.
 */
typedef struct walb_record_header {

        walb_log_record_t record[N_LOG_RECORD_IN_SECTOR];
        
} __attribute__((packed)) walb_record_header_t;

/**
 * IO data inside kernel memory.
 */
typedef struct walb_io_data {

        u16 size; /* in sector. */
        u8 *data; /* pointer to buffer with size is (size * SECTOR_SIZE). */
        
} __attribute__((packed)) walb_io_data_t;

/**
 * Log pack structure inside kernel memory.
 */
typedef struct walb_log_pack {

        walb_record_header_t header;
        walb_io_data_t io_data[N_LOG_RECORD_IN_SECTOR];

} __attribute__((packed)) walb_pack_t;

#endif /* WALB_LOG_RECORD_H */
