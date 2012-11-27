/**
 * Definitions for Walb log record.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_LOG_RECORD_H
#define WALB_LOG_RECORD_H

#include "walb.h"
#include "util.h"
#include "checksum.h"

/*******************************************************************************
 * Definition of structures.
 *******************************************************************************/

/**
 * Log record.
 */
struct walb_log_record {

	/* 8 + 2 * 4 + 8 * 2 = 32 bytes */

	/* Just sum of the array assuming data contents
	   as an array of u32 integer.
	   If is_padding non-zero, checksum is not calcurated. */
	u32 checksum;
	u32 reserved1;

	u64 lsid; /* Log sequence id of the record. */

	u16 lsid_local; /* Local sequence id as the data offset in the log record.
			   lsid - lsid_local is logpack lsid. */
	u16 is_padding; /* Non-zero if this is padding log */
	u16 io_size; /* IO size [logical sector].
			512B * (65K - 1) = (32M-512)B is the maximum request size. */
	u16 is_exist; /* Non-zero if this record is exist. */

	u64 offset; /* IO offset [logical sector]. */

	/*
	 * Data offset in the ring buffer.
	 *   offset = lsid_to_offset(lsid)
	 * Offset of the log record header.
	 *   offset = lsid_to_offset(lsid) - lsid_local
	 */

} __attribute__((packed));

/**
 * Logpack header data inside sector.
 *
 * sizeof(struct walb_logpack_header) <= walb_super_sector.sector_size.
 */
struct walb_logpack_header {

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

	struct walb_log_record record[0];
	/* continuous records */

} __attribute__((packed));


/*******************************************************************************
 * Macros.
 *******************************************************************************/

#define MAX_TOTAL_IO_SIZE_IN_LOGPACK_HEADER (((unsigned int)(1) << 16) - 1)

#define ASSERT_LOG_RECORD(rec) ASSERT(is_valid_log_record(rec))

/**
 * NOT TESTED YET.
 *
 * for each macro for records in a logpack.
 *
 * int i;
 * struct walb_log_record *lrec;
 * struct walb_logpack_header *lhead;
 */
#define for_each_logpack_record(i, lrec, lhead)				\
	for (i = 0; i < lhead->n_records && ({lrec = &lhead->record[i]; 1;}); i++)

/*******************************************************************************
 * Prototype of static inline functions.
 *******************************************************************************/

static inline unsigned int max_n_log_record_in_sector(unsigned int pbs);
static inline void log_record_init(struct walb_log_record *rec);
static inline int is_valid_log_record(struct walb_log_record *rec);
static inline int is_valid_logpack_header(const struct walb_logpack_header *lhead);
static inline u64 get_next_lsid(const struct walb_logpack_header *lhead);

/*******************************************************************************
 * Definition of static inline functions.
 *******************************************************************************/

/**
 * Get number of log records that a log pack can store.
 * @pbs physical block size.
 */
static inline unsigned int max_n_log_record_in_sector(unsigned int pbs)
{
	ASSERT(pbs > sizeof(struct walb_logpack_header));
	return (pbs - sizeof(struct walb_logpack_header)) /
		sizeof(struct walb_log_record);
}

/**
 * Initialize a log record.
 */
static inline void log_record_init(struct walb_log_record *rec)
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

/**
 * This is for validation of log record.
 *
 * @return Non-zero if valid, or 0.
 */
static inline int is_valid_log_record(struct walb_log_record *rec)
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

/**
 * Check validness of a logpack header.
 * This does not validate checksum.
 *
 * @logpack logpack to be checked.
 *
 * @return Non-zero in success, or 0.
 */
static inline int is_valid_logpack_header(
	const struct walb_logpack_header *lhead)
{

	CHECK(lhead);
	CHECK(lhead->sector_type == SECTOR_TYPE_LOGPACK);
	if (lhead->n_records == 0) {
		CHECK(lhead->total_io_size == 0);
		CHECK(lhead->n_padding == 0);
	} else {
		CHECK(lhead->total_io_size > 0);
		CHECK(lhead->n_padding < lhead->n_records);
	}
	return 1;
error:
	LOGe("log pack header is invalid "
		"(n_records: %u total_io_size %u sector_type %u).\n",
		lhead->n_records, lhead->total_io_size,
		lhead->sector_type);
	return 0;
}

/**
 * Get next lsid of a logpack header.
 * This does not validate the logpack header.
 */
static inline u64 get_next_lsid_unsafe(const struct walb_logpack_header *lhead)
{
	if (lhead->total_io_size == 0) {
		return lhead->logpack_lsid;
	} else {
		return lhead->logpack_lsid + 1 + lhead->total_io_size;
	}
}

/**
 * Get next lsid of a logpack header.
 */
static inline u64 get_next_lsid(const struct walb_logpack_header *lhead)
{
	ASSERT(is_valid_logpack_header(lhead));
	return get_next_lsid_unsafe(lhead);
}

#endif /* WALB_LOG_RECORD_H */
