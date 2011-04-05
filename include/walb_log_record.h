/**
 * Definitions for Walb log record.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_LOG_RECORD_H
#define _WALB_LOG_RECORD_H

#include "walb.h"

/*******************************************************************************
 * Definition of structures.
 *******************************************************************************/

/**
 * Log record.
 */
typedef struct walb_log_record {

        /* 8 + 2 * 4 + 8 * 2 = 32 bytes */

        /* Just sum of the array assuming data contents
           as an array of u32 integer.
           If is_paddingi non-zero, checksum is not calcurated. */
        u32 checksum;
        u32 reserved1;
        
        u64 lsid; /* Log sequence id */

        /* Local sequence id as the data offset in the log record. */
        u16 lsid_local; 
        u16 is_padding; /* Non-zero if this is padding log */
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
        u16 sector_type; /* type identifier */
        u16 total_io_size; /* Total io size in the log pack
                              [physical sector].
                              (Log pack size is total_io_size + 1.) */
        u64 logpack_lsid; /* logpack lsid */

        u16 n_records; /* Number of log records in the log pack.
                          This includes padding records also. */
        u16 n_padding; /* Number of padding record. 0 or 1. */
        u32 reserved1;
        
        walb_log_record_t record[0];
        /* continuous records */
        
} __attribute__((packed)) walb_logpack_header_t;

/*******************************************************************************
 * Macros.
 *******************************************************************************/

/**
 * Assertion of logical/physical block size.
 */
#define ASSERT_LBS_PBS(logical_bs, physical_bs) ASSERT( \
        logical_bs > 0 &&                               \
        physical_bs >= logical_bs &&                    \
        physical_bs % logical_bs == 0)

/**
 * NOT TESTED YET.
 *
 * for each macro for records in a logpack.
 *
 * int i;
 * walb_log_record_t *lrec;
 * walb_logpack_header_t *lhead;
 */
#define for_each_logpack_record(i, lrec, lhead)  \
    for (i = 0; i < lhead->n_records && {lrec = &lhead->record[i]; 1;}; i ++)

/*******************************************************************************
 * Prototype of static inline functions. 
 *******************************************************************************/

static inline int max_n_log_record_in_sector(int sector_size);
static inline int lb_in_pb(int logical_bs, int physical_bs);


/*******************************************************************************
 * Definition of static inline functions. 
 *******************************************************************************/
/**
 * Get number of log records that a log pack can store.
 */
static inline int max_n_log_record_in_sector(int sector_size)
{
        return (sector_size - sizeof(walb_logpack_header_t)) /
                sizeof(walb_log_record_t);
}

/**
 * Get number of logical blocks in a physical block.
 */
static inline int lb_in_pb(int logical_bs, int physical_bs)
{
    ASSERT_LBS_PBS(logical_bs, physical_bs);
    int ret = physical_bs / logical_bs;
    ASSERT(ret > 0);
    return ret;
}

/**
 * [Logical block] -> [Physial block]
 *
 * @logical_bs logical block size in bytes.
 * @physical_bs physical block size in bytes.
 * @n_lb number of losical blocks.
 *
 * @return number of physical blocks.
 */
static inline int lb_to_pb(int logical_bs, int physical_bs, int n_lb)
{
    ASSERT_LBS_PBS(logical_bs, physial_bs);
    ASSERT(n_lb >= 0);

    int lp = lb_in_pb(logical_bs, physical_bs);
    
    return ((n_lb + lp - 1) / lp);
}

/**
 * [Physial block] -> [Logical block]
 *
 * @logical_bs logical block size in bytes.
 * @physical_bs physical block size in bytes.
 * @n_pb number of physical blocks.
 *
 * @return number of logical blocks.
 */
static inline int pb_to_lb(int logical_bs, int physical_bs, int n_pb)
{
    ASSERT_LBS_PBS(logical_bs, physial_bs);
    ASSERT(n_pb >= 0);

    return (n_pb * lb_in_pb(logical_bs, physical_bs));
}

#endif /* _WALB_LOG_RECORD_H */
