/**
 * Definitions for Walb log file.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_LOG_FILE_H
#define WALB_LOG_FILE_H

#include "walb_log_record.h"

/**
 * sizeof(walb_file_header_t) must be <= SECTOR_SIZE.
 */
typedef struct walb_file_header {
        
        u64 lsid_begin;
        u64 lsid_end;
        u64 device_uuid;

} __attribute__((packed)) walb_file_header_t;

typedef walb_log_record_t walb_file_record_t;
/* Log record is the same format as in log device. */

#endif /* WALB_LOG_FILE_H */
