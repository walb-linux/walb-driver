/**
 * logpack.c - Logpack operations.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/blkdev.h>

#include "check_kernel.h"
#include "logpack.h"

/**
 * Debug print of logpack header.
 *
 */
void walb_logpack_header_print(
	const char *level, const struct walb_logpack_header *lhead)
{
	int i;
	printk("%s*****logpack header*****\n"
		"checksum: %08x\n"
		"n_records: %u\n"
		"n_padding: %u\n"
		"total_io_size: %u\n"
		"logpack_lsid: %"PRIu64"\n",
		level,
		lhead->checksum,
		lhead->n_records,
		lhead->n_padding,
		lhead->total_io_size,
		lhead->logpack_lsid);
	for (i = 0; i < lhead->n_records; i++) {
		printk("%srecord %d\n"
			"  checksum: %08x\n"
			"  lsid: %llu\n"
			"  lsid_local: %u\n"
			"  is_exist: %u\n"
			"  is_padding: %u\n"
			"  is_discard: %u\n"
			"  offset: %"PRIu64"\n"
			"  io_size: %u\n",
			level, i,
			lhead->record[i].checksum,
			lhead->record[i].lsid,
			lhead->record[i].lsid_local,
			test_bit_u32(LOG_RECORD_EXIST, &lhead->record[i].flags),
			test_bit_u32(LOG_RECORD_PADDING, &lhead->record[i].flags),
			test_bit_u32(LOG_RECORD_DISCARD, &lhead->record[i].flags),
			lhead->record[i].offset,
			lhead->record[i].io_size);
		printk("%slogpack lsid: %llu\n", level,
			lhead->record[i].lsid - lhead->record[i].lsid_local);
	}
}

/**
 * Add a request to a logpack header.
 * Do not validate checksum.
 *
 * REQ_DISCARD is not supported.

 * @lhead log pack header.
 *   lhead->logpack_lsid must be set correctly.
 *   lhead->sector_type must be set correctly.
 * @logpack_lsid lsid of the log pack.
 * @req request to add. must be write and its size >= 0.
 *	size == 0 is permitted with flush requests only.
 * @pbs physical block size.
 * @ring_buffer_size ring buffer size [physical block]
 *
 * RETURN:
 *   true in success, or false.
 */
bool walb_logpack_header_add_req(
	struct walb_logpack_header *lhead,
	const struct request *req,
	unsigned int pbs, u64 ring_buffer_size)
{
	u64 logpack_lsid;
	u64 req_lsid;
	unsigned int req_lb, req_pb;
	u64 padding_pb;
	unsigned int max_n_rec;
	int idx;

	ASSERT(lhead);
	ASSERT(lhead->sector_type == SECTOR_TYPE_LOGPACK);
	ASSERT(req);
	ASSERT_PBS(pbs);
	ASSERT(req->cmd_flags & REQ_WRITE);

	logpack_lsid = lhead->logpack_lsid;
	max_n_rec = max_n_log_record_in_sector(pbs);
	idx = lhead->n_records;

	ASSERT(lhead->n_records <= max_n_rec);
	if (lhead->n_records == max_n_rec) {
		LOGd("no more request can not be added.\n");
		return false;
	}

	req_lsid = logpack_lsid + 1 + lhead->total_io_size;
	req_lb = blk_rq_sectors(req);
	if (req_lb == 0) {
		/* Currently only the flush request can have size 0. */
		ASSERT(req->cmd_flags & REQ_FLUSH);
		return true;
	}
	ASSERT(0 < req_lb);
	ASSERT(65536 > req_lb); /* can be u16. */
	req_pb = capacity_pb(pbs, req_lb);

	padding_pb = ring_buffer_size - (req_lsid % ring_buffer_size);
	if (padding_pb < req_pb) {
		/* Log of this request will cross the end of ring buffer.
		   So padding is required. */
		if (lhead->total_io_size + padding_pb
			> MAX_TOTAL_IO_SIZE_IN_LOGPACK_HEADER) {
			LOGd("no more request can not be added.\n");
			return false;
		}

		/* Fill the padding record contents. */
		set_bit_u32(LOG_RECORD_PADDING, &lhead->record[idx].flags);
		set_bit_u32(LOG_RECORD_EXIST, &lhead->record[idx].flags);
		lhead->record[idx].lsid = req_lsid;
		lhead->record[idx].lsid_local = req_lsid - logpack_lsid;
		lhead->record[idx].offset = 0;
		lhead->record[idx].io_size = (u16)capacity_lb(pbs, padding_pb);
		lhead->n_padding++;
		lhead->n_records++;
		lhead->total_io_size += padding_pb;

		req_lsid += padding_pb;
		idx++;

		if (lhead->n_records == max_n_rec) {
			LOGd("no more request can not be added.\n");
			return false;
		}
	}

	if (lhead->total_io_size + req_pb
		> MAX_TOTAL_IO_SIZE_IN_LOGPACK_HEADER) {
		LOGd("no more request can not be added.\n");
		return false;
	}

	/* Fill the log record contents. */
	clear_bit_u32(LOG_RECORD_PADDING, &lhead->record[idx].flags);
	set_bit_u32(LOG_RECORD_EXIST, &lhead->record[idx].flags);
	lhead->record[idx].lsid = req_lsid;
	lhead->record[idx].lsid_local = req_lsid - logpack_lsid;
	lhead->record[idx].offset = (u64)blk_rq_pos(req);
	lhead->record[idx].io_size = (u16)req_lb;
	lhead->n_records++;
	lhead->total_io_size += req_pb;

	return true;
}

/**
 * Add a bio to a logpack header.
 * Almost the same as walb_logpack_header_add_req().
 * Do not validate checksum.
 *
 * REQ_DISCARD is supported.
 *
 * @lhead log pack header.
 *   lhead->logpack_lsid must be set correctly.
 *   lhead->sector_type must be set correctly.
 * @logpack_lsid lsid of the log pack.
 * @bio bio to add. must be write and its size >= 0.
 *	size == 0 is permitted with flush requests only.
 * @pbs physical block size.
 * @ring_buffer_size ring buffer size [physical block]
 *
 * RETURN:
 *   true in success, or false (you must create new logpack for the bio).
 */
bool walb_logpack_header_add_bio(
	struct walb_logpack_header *lhead,
	const struct bio *bio,
	unsigned int pbs, u64 ring_buffer_size)
{
	u64 logpack_lsid;
	u64 bio_lsid;
	unsigned int bio_lb, bio_pb;
	u64 padding_pb;
	unsigned int max_n_rec;
	int idx;
	bool is_discard;
	UNUSED const char no_more_bio_msg[] = "no more bio can not be added.\n";

	ASSERT(lhead);
	ASSERT(lhead->sector_type == SECTOR_TYPE_LOGPACK);
	ASSERT(bio);
	ASSERT_PBS(pbs);
	ASSERT(bio->bi_rw & REQ_WRITE);
	ASSERT(ring_buffer_size > 0);

	logpack_lsid = lhead->logpack_lsid;
	max_n_rec = max_n_log_record_in_sector(pbs);
	idx = lhead->n_records;

	ASSERT(lhead->n_records <= max_n_rec);
	if (lhead->n_records == max_n_rec) {
		LOGd_(no_more_bio_msg);
		return false;
	}

	bio_lsid = logpack_lsid + 1 + lhead->total_io_size;
	bio_lb = bio_sectors(bio);
	if (bio_lb == 0) {
		/* Only flush requests can have zero-size. */
		ASSERT(bio->bi_rw & REQ_FLUSH);
		/* Currently a zero-flush must be alone. */
		ASSERT(idx == 0);
		return true;
	}
	ASSERT(0 < bio_lb);
	ASSERT((1U << 16) > bio_lb); /* can be u16. */
	bio_pb = capacity_pb(pbs, bio_lb);
	is_discard = ((bio->bi_rw & REQ_DISCARD) != 0);

	/* Padding check. */
	padding_pb = ring_buffer_size - bio_lsid % ring_buffer_size;
	if (!is_discard && padding_pb < bio_pb) {
		/* Log of this request will cross the end of ring buffer.
		   So padding is required. */

		if (lhead->total_io_size + padding_pb
			> MAX_TOTAL_IO_SIZE_IN_LOGPACK_HEADER) {
			LOGd_(no_more_bio_msg);
			return false;
		}

		/* Fill the padding record contents. */
		set_bit_u32(LOG_RECORD_PADDING, &lhead->record[idx].flags);
		set_bit_u32(LOG_RECORD_EXIST, &lhead->record[idx].flags);
		lhead->record[idx].lsid = bio_lsid;
		lhead->record[idx].lsid_local = (u16)(bio_lsid - logpack_lsid);
		lhead->record[idx].offset = 0;
		lhead->record[idx].io_size = (u16)capacity_lb(pbs, padding_pb);
		lhead->n_padding++;
		lhead->n_records++;
		lhead->total_io_size += padding_pb;

		bio_lsid += padding_pb;
		idx++;
		ASSERT(bio_lsid == logpack_lsid + 1 + lhead->total_io_size);

		if (lhead->n_records == max_n_rec) {
			LOGd_(no_more_bio_msg);
			return false;
		}
	}

	if (!is_discard &&
		lhead->total_io_size + bio_pb
		> MAX_TOTAL_IO_SIZE_IN_LOGPACK_HEADER) {
		LOGd_(no_more_bio_msg);
		return false;
	}

	/* Fill the log record contents. */
	set_bit_u32(LOG_RECORD_EXIST, &lhead->record[idx].flags);
	clear_bit_u32(LOG_RECORD_PADDING, &lhead->record[idx].flags);
	lhead->record[idx].lsid = bio_lsid;
	lhead->record[idx].lsid_local = (u16)(bio_lsid - logpack_lsid);
	lhead->record[idx].offset = (u64)bio->bi_sector;
	lhead->record[idx].io_size = (u16)bio_lb;
	lhead->n_records++;
	if (is_discard) {
		set_bit_u32(LOG_RECORD_DISCARD, &lhead->record[idx].flags);
		/* lhead->total_io_size will not be added. */
	} else {
		clear_bit_u32(LOG_RECORD_DISCARD, &lhead->record[idx].flags);
		lhead->total_io_size += bio_pb;
	}
	return true;
}

MODULE_LICENSE("Dual BSD/GPL");
