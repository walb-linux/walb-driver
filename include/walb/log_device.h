/**
 * log_device.h - Definitions for Walb log device.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_LOG_DEVICE_H
#define WALB_LOG_DEVICE_H

#include "log_record.h"
#include "super.h"

#ifdef __cplusplus
extern "C" {
#endif

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
 *	 DATA u8 io_data[header[i].size * SECTOR_SIZE]
 *     }
 *   }
 * }
 *
 * PROPERTY1: sizeof(log_pack) % SECTOR_SIZE is 0.
 * PROPERTY2: sizeof(log_header) is SECTOR_SIZE.
 * PROPERTY3: offset of i'th io_data is walb_lsid_to_offset(header[i].lsid).
 * PROPERTY4: offset of log_pack is
 *	      walb_lsid_to_offset(header[i].lsid - header[i].lsid_local) for all i.
 * PROPERTY5: sizeof(log_pack)
 *	      log_pack_size = 1 + sum(header[i].size for all i).
 * PROPERTY6: next lsid.
 *	      next_lsid = lsid + log_pack_size + 1.
 */

/*
 * FORMAT: Meta data of the log device.
 *
 * log_device_meta_data {
 *   DATA u8 reserved[PAGE_SIZE]
 *   DATA walb_super_sector super0
 *   DATA u8 padding[PAGE_SIZE - SECTOR_SIZE]
 *   DATA walb_super_sector super1
 *   DATA u8 padding[PAGE_SIZE - SECTOR_SIZE]
 * }
 *
 *
 * PROPERTY1: Offset of super0
 *	      n_sector_in_page = PAGE_SIZE / SECTOR_SIZE.
 *	      offset_super0 = n_sector_in_page.
 * PROPERTY2: Offset of super1
 *	      offset_super1 = offset_super0 + n_sector_in_page + super0.snapshot_metadata_size.
 * PROPERTY3: sizeof(log_device_meta_data)
 *	      offset_super1 + n_sector_in_page.
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
 *	      offset_ring_buffer = sizeof(log_device_meta_data).
 * PROPERTY2: Offset of of a given lsid is walb_lsid_to_offset(lsid).
 *
 *   u64 walb_lsid_to_offset(u64 lsid) {
 *	 return offset_ring_buffer + (lsid % super0.ring_buffer_size);
 *   }
 *
 * PROPERTY3: Offset of log_pack of a given lsid and lsid_local.
 *	      offset_log_pack = walb_lsid_to_offset(lsid) - lsid_local.
 */

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
	return PAGE_SIZE / sector_size; /* skip reserved page */
}

/**
 * Get offset of secondary super sector.
 *
 * @sector_size sector size in bytes.
 * @n
 * @return offset in sectors.
 */
static inline u64 get_super_sector1_offset(int sector_size)
{
	return get_super_sector0_offset(sector_size) + 1;
}

/**
 * Get ring buffer offset.
 *
 * @sector_size sector size.
 * @n_snapshots number of snapshot to keep.
 *
 * @return ring buffer offset by the sector.
 */
static inline u64 get_ring_buffer_offset(int sector_size)
{
	return	get_super_sector1_offset(sector_size) + 1;
}


/**
 * Get offset of primary super sector.
 */
static inline u64 get_super_sector0_offset_2(const struct walb_super_sector* super_sect)
{
	ASSERT(super_sect != NULL);
	return get_super_sector0_offset(super_sect->physical_bs);
}

/**
 * Get offset of secondary super sector.
 */
static inline u64 get_super_sector1_offset_2(const struct walb_super_sector* super_sect)
{
	ASSERT(super_sect != NULL);
	return	get_super_sector0_offset(super_sect->physical_bs) + 1 +
		super_sect->metadata_size;
}

/**
 * Get ring buffer offset.
 *
 * @return offset in log device [physical sector].
 */
static inline u64 get_ring_buffer_offset_2(const struct walb_super_sector* super_sect)
{
	ASSERT(super_sect != NULL);
	return	get_super_sector1_offset_2(super_sect) + 1;
}


/**
 * Get offset inside log device of the specified lsid.
 *
 * @return offset in log device [physical sector].
 */
static inline u64 get_offset_of_lsid_2
(const struct walb_super_sector* super_sect, u64 lsid)
{
	return	get_ring_buffer_offset_2(super_sect) +
		(lsid % super_sect->ring_buffer_size);
}

/*******************************************************************************
 * Static inline functions.
 *******************************************************************************/

/**
 * Get logpack header pointer.
 */
static inline struct walb_logpack_header*
get_logpack_header(struct sector_data *sect)
{
	ASSERT_SECTOR_DATA(sect);
	return (struct walb_logpack_header *)(sect->data);
}

/**
 * Get logpack head pointer (const).
 */
static inline const struct walb_logpack_header*
get_logpack_header_const(const struct sector_data *sect)
{
	ASSERT_SECTOR_DATA(sect);
	return (const struct walb_logpack_header *)(sect->data);
}

#ifdef __cplusplus
}
#endif

#endif /* WALB_LOG_DEVICE_H */
