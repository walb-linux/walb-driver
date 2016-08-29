/**
 * Definitions for Walb log record.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_LOG_RECORD_H
#define WALB_LOG_RECORD_H

#ifdef __cplusplus
extern "C" {
#endif

#include "walb.h"
#include "check.h"
#include "util.h"
#include "u32bits.h"
#include "checksum.h"
#if 0
#include "logger.h"
#endif

/*******************************************************************************
 * Definition of structures.
 *******************************************************************************/

enum {
	LOG_RECORD_EXIST = 0,
	LOG_RECORD_PADDING, /* Non-zero if this is padding log */
	LOG_RECORD_DISCARD, /* Discard IO */
};

/**
 * Log record.
 */
struct walb_log_record {

	/* (4 + 4) + 8 + (4 + 2 + 2) + 8 = 32 bytes */

	/* Just sum of the array assuming data contents
	   as an array of u32 integer.
	   If is_padding non-zero, checksum is not calcurated.
	   You must use the salt that is unique for each device. */
	u32 checksum;

	/* Flags with LOG_RECORD_XXX indicators. */
	u32 flags;

	/* IO offset [logical sector]. */
	u64 offset;

	/* IO size [logical sector].
	 * A discard IO size can be UINT32_MAX,
	 * while normal IO size must be less than UINT16_MAX. */
	u32 io_size;

	/* Local sequence id as the data offset in the log record.
	   lsid - lsid_local is logpack lsid. */
	u16 lsid_local;

	u16 reserved1;

	/* Log sequence id of the record. */
	u64 lsid;

} __attribute__((packed, aligned(8)));

/**
 * Logpack header data inside sector.
 *
 * sizeof(struct walb_logpack_header) <= walb_super_sector.sector_size.
 */
struct walb_logpack_header {

	/* Checksum of the logpack header.
	   You must use the salt that is unique for each device. */
	u32 checksum;

	/* Type identifier */
	u16 sector_type;

	/* Total io size in the log pack [physical sector].
	   Log pack size is total_io_size + 1.
	   Discard request's size is not included. */
	u16 total_io_size;

	/* logpack lsid [physical sector]. */
	u64 logpack_lsid;

	/* Number of log records in the log pack.
	   This includes padding records also. */
	u16 n_records;

	/* Number of padding record. 0 or 1. */
	u16 n_padding;

	u32 reserved1;

	struct walb_log_record record[0];
	/* continuous records */

} __attribute__((packed, aligned(8)));


/*******************************************************************************
 * Macros.
 *******************************************************************************/

#define MAX_TOTAL_IO_SIZE_IN_LOGPACK_HEADER ((1U << 16) - 1)

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
	for (i = 0; i < lhead->n_records && (lrec = &lhead->record[i], 1); i++)

/*******************************************************************************
 * Prototype of static inline functions.
 *******************************************************************************/

static inline unsigned int max_n_log_record_in_sector(unsigned int pbs);
static inline void log_record_init(struct walb_log_record *rec);
static inline int is_valid_log_record(struct walb_log_record *rec);
static inline int is_valid_log_record_const(const struct walb_log_record *rec);
static inline int is_valid_logpack_header(const struct walb_logpack_header *lhead);
static inline int is_valid_logpack_header_with_checksum(
	const struct walb_logpack_header* lhead, unsigned int pbs, u32 salt);
static inline int is_valid_logpack_header_and_records(
	const struct walb_logpack_header *lhead);
static inline int is_valid_logpack_header_and_records_with_checksum(
	const struct walb_logpack_header* lhead, unsigned int pbs, u32 salt);
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
	memset(rec, 0, sizeof(*rec));
}

/**
 * This is for validation of log record.
 *
 * @return Non-zero if valid, or 0.
 */
static inline int is_valid_log_record(struct walb_log_record *rec)
{
	CHECKd(rec);
	CHECKd(test_bit_u32(LOG_RECORD_EXIST, &rec->flags));
	if (!test_bit_u32(LOG_RECORD_PADDING, &rec->flags)) {
		CHECKd(rec->io_size > 0);
	}
	if (!test_bit_u32(LOG_RECORD_DISCARD, &rec->flags)) {
		CHECKd(rec->io_size <= WALB_MAX_NORMAL_IO_SECTORS);
	}
	CHECKd(rec->lsid_local > 0);
	CHECKd(rec->lsid <= MAX_LSID);

	return 1; /* valid */
error:
	return 0; /* invalid */
}

static inline int is_valid_log_record_const(
	const struct walb_log_record *rec)
{
	return is_valid_log_record((struct walb_log_record *)rec);
}

/**
 * Check a logpack header block is end.
 *
 * @return Non-zero if the block is end, or 0.
 */
static inline int is_end_logpack_header(
	const struct walb_logpack_header *lhead)
{
	CHECKd(lhead);
	return lhead->n_records == 0 && lhead->logpack_lsid == (u64)(-1);
error:
	return 0;
}

/**
 * Check validness of a logpack header.
 * This does not validate checksum.
 * End header block is valid.
 *
 * @logpack logpack to be checked.
 *
 * @return Non-zero in success, or 0.
 */
static inline int is_valid_logpack_header(
	const struct walb_logpack_header *lhead)
{

	CHECKd(lhead);
	CHECKd(lhead->sector_type == SECTOR_TYPE_LOGPACK);
	if (lhead->n_records == 0) {
		CHECKd(lhead->total_io_size == 0);
		CHECKd(lhead->n_padding == 0);
	} else {
		CHECKd(lhead->n_padding <= 1);
		CHECKd(lhead->n_padding <= lhead->n_records);

		/* logpack_lsid overflow check. */
		CHECKd(lhead->logpack_lsid <
			lhead->logpack_lsid + 1 + lhead->total_io_size);
	}
	return 1;
error:
#if 0
	LOGd("log pack header is invalid "
		"(n_records: %u total_io_size %u sector_type %u).\n",
		lhead->n_records, lhead->total_io_size,
		lhead->sector_type);
#endif
	return 0;
}

/**
 * Check validness of a logpack header.
 *
 * @logpack logpack to be checked.
 * @pbs physical block size.
 *   (This is logpack header size.)
 *
 * @return Non-zero in success, or 0.
 */
static inline int is_valid_logpack_header_with_checksum(
	const struct walb_logpack_header* lhead, unsigned int pbs, u32 salt)
{
	CHECKld(error0, is_valid_logpack_header(lhead));
	if (lhead->n_records > 0) {
		CHECKld(error1, checksum((const u8 *)lhead, pbs, salt) == 0);
	}
	return 1;
error0:
	return 0;
error1:
#if 0
	LOGd("logpack header checksum is invalid (lsid %" PRIu64").\n",
		lhead->logpack_lsid);
#endif
	return 0;
}

/**
 * Check validness of a logpack header and records.
 */
static inline int is_valid_logpack_header_and_records(
	const struct walb_logpack_header *lhead)
{
	unsigned int i;

	if (!is_valid_logpack_header(lhead)) {
#if 0
		LOGd("header invalid.\n");
#endif
		return 0;
	}
	for (i = 0; i < lhead->n_records; i++) {
		const struct walb_log_record *rec = &lhead->record[i];
		if (!is_valid_log_record_const(rec)) {
#if 0
			LOGd("record %u invalid.\n", i);
#endif
			return 0;
		}
		if (rec->lsid - rec->lsid_local != lhead->logpack_lsid) {
#if 0
			LOGd("lsid(%" PRIu64 ") - lsid_local(%u)"
				" != logpack_lsid(%" PRIu64 ")\n",
				rec->lsid, rec->lsid_local, lhead->logpack_lsid);
#endif
			return 0;
		}
	}
	return 1;
}

static inline int is_valid_logpack_header_and_records_with_checksum(
	const struct walb_logpack_header* lhead, unsigned int pbs, u32 salt)
{
	if (lhead->n_records > 0) {
		if (checksum((const u8 *)lhead, pbs, salt) != 0) {
			return 0;
		}
	}
	return is_valid_logpack_header_and_records(lhead);
}

/**
 * Get next lsid of a logpack header.
 * This does not validate the logpack header.
 */
static inline u64 get_next_lsid_unsafe(const struct walb_logpack_header *lhead)
{
	if (lhead->total_io_size == 0 && lhead->n_records == 0) {
		/* Zero-flush only. */
		return lhead->logpack_lsid;
	}
	return lhead->logpack_lsid + 1 + lhead->total_io_size;
}

/**
 * Get next lsid of a logpack header.
 */
static inline u64 get_next_lsid(const struct walb_logpack_header *lhead)
{
	ASSERT(is_valid_logpack_header(lhead));
	return get_next_lsid_unsafe(lhead);
}

#ifdef __cplusplus
}
#endif

#endif /* WALB_LOG_RECORD_H */
