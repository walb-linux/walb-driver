/**
 * Definitions for Walb log record.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_LOG_RECORD_H
#define _WALB_LOG_RECORD_H

#include "walb.h"

/**
 * Log record.
 */
typedef struct walb_log_record {

        /* 8 + 2 * 4 + 8 * 2 = 32 bytes */

        /* Just sum of the array assuming data contents
           as an array of u32 integer. */
        u32 checksum;
        u32 reserved1;
        
        u64 lsid; /* Log sequence id */

        /* Local sequence id as the data offset in the log record. */
        u16 lsid_local; 
        u16 reserved2;
        u16 io_size; /* IO size [logical sector]. */
        u16 is_exist; /* Non-zero if this record is exist. */
        
        u64 offset; /* IO offset [logical sector]. */

        /*
         * Data offset in the ring buffer.
         *   offset = lsid_to_offset(lsid)
         * Offset of the log record header.
         *   offset = lsid_to_offset(lsid) - lsid_local
         */
        
} __attribute__((packed)) walb_log_record_t;

/**
 * Logpack header data inside sector.
 *
 * sizeof(walb_logpack_header_t) <= walb_super_sector.sector_size.
 */
typedef struct walb_logpack_header {

        u32 checksum; /* checksum of whole log pack. */
        u16 n_records; /* Number of log records in the log pack. */
        u16 total_io_size; /* Total io size in the log pack
                              [physical sector].
                              (Log pack size is total_io_size + 1.) */
        
        walb_log_record_t record[0];
        /* continuous records */
        
} __attribute__((packed)) walb_logpack_header_t;


/**
 * Get number of log records that a log pack can store.
 */
static inline int max_n_log_record_in_sector(int sector_size)
{
        return (sector_size - sizeof(walb_logpack_header_t)) /
                sizeof(walb_log_record_t);
}

/**
 * IO data inside kernel memory.
 */
/* typedef struct walb_io_data { */

/*         u16 size; /\* in sector. *\/ */
/*         u8 *data; /\* pointer to buffer with size. *\/ */
        
/* } __attribute__((packed)) walb_io_data_t; */

/**
 * Log pack structure inside kernel memory.
 */
/* typedef struct walb_log_pack { */

/*         walb_logpack_header_t header; */
/*         walb_io_data_t io_data[0]; */
/*         /\* Contiuous io_data *\/ */

/* } __attribute__((packed)) walb_pack_t; */

#endif /* _WALB_LOG_RECORD_H */
