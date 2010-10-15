/**
 * Definitions for Walb log device.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_LOG_DEVICE_H
#define WALB_LOG_DEVICE_H

#include <linux/list.h>
#include "walb_log_record.h"

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
 * sizeof(walb_super_sector_t) must be <= SECTOR_SIZE.
 */
typedef struct walb_super_sector {

        /* UUID of the wal device. */
        u8 uuid[16]; /* 128 bits */

        /* Offset of the oldest log record inside ring buffer.
           [sector] */
        u64 start_offset;

        /* Number of sectors for snapshot metadata. */
        u32 snapshot_metadata_size;
        u32 reserved4;

        /* Ring buffer size [sector] */
        u64 ring_buffer_size;
        
        /* Log sequence id of the oldest log record in the ring buffer. */
        u64 oldest_lsid;
        
        /* Log sequence id of the latest log record written to the data device also.
           This is used for checkpointing.
           When walb device is assembled redo must be
           from written_lsid to the latest lsid stored in the log device.
        */
        u64 written_lsid;

} __attribute__((packed)) walb_super_sector_t;

/**
 * Each snapshot information.
 */
typedef struct walb_snapshot_record {

        u64 lsid;
        u64 timestamp; /* in seconds (the same as 'time' system call output). */
        u8 name[64];
        
} __attribute__((packed)) walb_snapshot_record_t;

#define N_SNAPSHOT_RECORD_IN_SECTOR (SECTOR_SIZE / sizeof(walb_snapshot_record_t))

/**
 * Snapshot data inside sector.
 *
 * sizeof(walb_snapshot_sector_t) <= SECTOR_SIZE
 */
typedef struct walb_snapshot_sector {

        walb_snapshot_record_t record[N_SNAPSHOT_ENTRY_IN_SECTOR];

} __attribute__((packed)) walb_snapshot_sector_t;

#endif /* WALB_LOG_DEVICE_H */
