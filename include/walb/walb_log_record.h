/**
 * Definitions for Walb log record.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_LOG_RECORD_H
#define _WALB_LOG_RECORD_H

#include "./walb.h"
#include "./util.h"
#include "./checksum.h"

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
           If is_padding non-zero, checksum is not calcurated. */
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
 * This is for validation of log record.
 * 
 * @return Non-zero if valid, or 0.
 */
static inline int is_valid_log_record(walb_log_record_t* rec)
{
        CHECK(rec);
        CHECK(rec->is_exist);
        
        CHECK(rec->io_size > 0);
        CHECK(rec->lsid_local > 0);
        CHECK(rec->lsid <= MAX_LSID);
        
        return 1; /* valid */
error:
        return 0; /* invalid */
}

#define ASSERT_LOG_RECORD(rec) ASSERT(is_valid_log_record(rec))

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


/**
 * Check validness of a logpack header.
 *
 * @logpack logpack to be checked.
 *
 * @return Non-zero in success, or 0.
 */
static inline int is_valid_logpack_header(const walb_logpack_header_t* lhead) {

        CHECK(lhead);
        CHECK(lhead->n_records > 0);
        CHECK(lhead->total_io_size > 0);
        CHECK(lhead->sector_type == SECTOR_TYPE_LOGPACK);
        return 1;
error:
        LOGe("log pack header is invalid "
             "(n_records: %u total_io_size %u sector_type %u).\n",
             lhead->n_records, lhead->total_io_size,
             lhead->sector_type);
        return 0;
}

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
#define for_each_logpack_record(i, lrec, lhead)                         \
        for (i = 0; i < lhead->n_records && ({lrec = &lhead->record[i]; 1;}); i ++)

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
 * @n_lb number of logical blocks.
 *
 * @return number of physical blocks.
 */
static inline int lb_to_pb(int logical_bs, int physical_bs, int n_lb)
{
    ASSERT_LBS_PBS(logical_bs, physical_bs);
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
    ASSERT_LBS_PBS(logical_bs, physical_bs);
    ASSERT(n_pb >= 0);

    return (n_pb * lb_in_pb(logical_bs, physical_bs));
}

/**
 * Initialize a log record.
 */
static inline void log_record_init(walb_log_record_t* rec)
{
        ASSERT(rec);
        
        rec->checksum = 0;
        rec->lsid = 0;
        
        rec->lsid_local = 0;
        rec->is_padding = 0;
        rec->io_size = 0;
        rec->is_exist = 0;
        
        rec->offset = 0;
}

#endif /* _WALB_LOG_RECORD_H */
