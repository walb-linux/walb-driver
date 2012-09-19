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
void walb_logpack_header_print(const char *level,
                               struct walb_logpack_header *lhead)
{
        int i;
        printk("%s*****logpack header*****\n"
               "checksum: %08x\n"
               "n_records: %u\n"
               "n_padding: %u\n"
               "total_io_size: %u\n"
               "logpack_lsid: %llu",
               level,
               lhead->checksum,
               lhead->n_records,
               lhead->n_padding,
               lhead->total_io_size,
               lhead->logpack_lsid);
        for (i = 0; i < lhead->n_records; i ++) {
                printk("%srecord %d\n"
                       "  checksum: %08x\n"
                       "  lsid: %llu\n"
                       "  lsid_local: %u\n"
                       "  is_padding: %u\n"
                       "  io_size: %u\n"
                       "  is_exist: %u\n"
                       "  offset: %llu\n",
                       level, i,
                       lhead->record[i].checksum,
                       lhead->record[i].lsid,
                       lhead->record[i].lsid_local,
                       lhead->record[i].is_padding,
                       lhead->record[i].io_size,
                       lhead->record[i].is_exist,
                       lhead->record[i].offset);
                printk("%slogpack lsid: %llu\n", level,
                       lhead->record[i].lsid - lhead->record[i].lsid_local);
        }
}

/**
 * Add a request to a logpack header.
 * Do not validate checksum.
 *
 * @lhead log pack header.
 *   lhead->logpack_lsid must be set correctly.
 *   lhead->sector_type must be set correctly.
 * @logpack_lsid lsid of the log pack.
 * @req request to add. must be write and its size >= 0.
 *      size == 0 is permitted with flush requests only.
 * @pbs physical block size.
 * @ring_buffer_size ring buffer size [physical block]
 *
 * RETURN:
 *   true in success, or false.
 */
bool walb_logpack_header_add_req(
	struct walb_logpack_header *lhead,
	struct request *req,
	unsigned int pbs, u64 ring_buffer_size)
{
	u64 logpack_lsid;
	u64 req_lsid;
	unsigned int req_lb, req_pb;
	unsigned int padding_pb;
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
		goto error0;
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

	if (req_lsid % ring_buffer_size + req_pb > ring_buffer_size) {
		/* Log of this request will cross the end of ring buffer.
		   So padding is required. */
		padding_pb = ring_buffer_size - (req_lsid % ring_buffer_size);

		if ((unsigned int)lhead->total_io_size + padding_pb
			> MAX_TOTAL_IO_SIZE_IN_LOGPACK_HEADER) {
			LOGd("no more request can not be added.\n");
			goto error0;
		}

		/* Fill the padding record contents. */
		lhead->record[idx].is_exist = 1;
		lhead->record[idx].lsid = req_lsid;
		lhead->record[idx].lsid_local = req_lsid - logpack_lsid;
		lhead->record[idx].is_padding = 1;
		lhead->record[idx].offset = 0;
		lhead->record[idx].io_size = (u16)capacity_lb(pbs, padding_pb);
		lhead->n_padding ++;
		lhead->n_records ++;
		lhead->total_io_size += padding_pb;

		req_lsid += padding_pb;
		idx ++;
		
		if (lhead->n_records == max_n_rec) {
			LOGd("no more request can not be added.\n");
			goto error0;
		}
	}

	if ((unsigned int)lhead->total_io_size + req_pb
		> MAX_TOTAL_IO_SIZE_IN_LOGPACK_HEADER) {
		LOGd("no more request can not be added.\n");
		goto error0;
	}

	/* Fill the log record contents. */
	lhead->record[idx].is_exist = 1;
	lhead->record[idx].lsid = req_lsid;
	lhead->record[idx].lsid_local = req_lsid - logpack_lsid;
	lhead->record[idx].is_padding = 0;
	lhead->record[idx].offset = (u64)blk_rq_pos(req);
	lhead->record[idx].io_size = (u16)req_lb;
	lhead->n_records ++;
	lhead->total_io_size += req_pb;

	req_lsid += req_pb;
	idx ++;
	
	return true;
error0:
	return false;
}

/**
 * NOT TESTED YET.
 *
 * Add a bio to a logpack header.
 * Almost the same as walb_logpack_header_add_req().
 * Do not validate checksum.
 *
 * @lhead log pack header.
 *   lhead->logpack_lsid must be set correctly.
 *   lhead->sector_type must be set correctly.
 * @logpack_lsid lsid of the log pack.
 * @bio bio to add. must be write and its size >= 0.
 *      size == 0 is permitted with flush requests only.
 * @pbs physical block size.
 * @ring_buffer_size ring buffer size [physical block]
 *
 * RETURN:
 *   true in success, or false.
 */
bool walb_logpack_header_add_bio(
	struct walb_logpack_header *lhead,
	struct bio *bio,
	unsigned int pbs, u64 ring_buffer_size)
{
	u64 logpack_lsid;
	u64 bio_lsid;
	unsigned int bio_lb, bio_pb;
	unsigned int padding_pb;
	unsigned int max_n_rec;
	int idx;
	
	ASSERT(lhead);
	ASSERT(lhead->sector_type == SECTOR_TYPE_LOGPACK);
	ASSERT(bio);
	ASSERT_PBS(pbs);
	ASSERT(bio->bi_rw & REQ_WRITE);

	logpack_lsid = lhead->logpack_lsid;
	max_n_rec = max_n_log_record_in_sector(pbs);
	idx = lhead->n_records;
	
	ASSERT(lhead->n_records <= max_n_rec);
	if (lhead->n_records == max_n_rec) {
		LOGd("no more bio can not be added.\n");
		goto error0;
	}

	bio_lsid = logpack_lsid + 1 + lhead->total_io_size;
	bio_lb = bio_sectors(bio);
	if (bio_lb == 0) {
		/* Currently only the flush request can have size 0. */
		ASSERT(bio->bi_rw & REQ_FLUSH);
		return true;
	}
	ASSERT(0 < bio_lb);
	ASSERT(65536 > bio_lb); /* can be u16. */
	bio_pb = capacity_pb(pbs, bio_lb);

	if (bio_lsid % ring_buffer_size + bio_pb > ring_buffer_size) {
		/* Log of this request will cross the end of ring buffer.
		   So padding is required. */
		padding_pb = ring_buffer_size - (bio_lsid % ring_buffer_size);

		if ((unsigned int)lhead->total_io_size + padding_pb
			> MAX_TOTAL_IO_SIZE_IN_LOGPACK_HEADER) {
			LOGd("no more request can not be added.\n");
			goto error0;
		}

		/* Fill the padding record contents. */
		lhead->record[idx].is_exist = 1;
		lhead->record[idx].lsid = bio_lsid;
		lhead->record[idx].lsid_local = bio_lsid - logpack_lsid;
		lhead->record[idx].is_padding = 1;
		lhead->record[idx].offset = 0;
		lhead->record[idx].io_size = (u16)capacity_lb(pbs, padding_pb);
		lhead->n_padding ++;
		lhead->n_records ++;
		lhead->total_io_size += padding_pb;

		bio_lsid += padding_pb;
		idx ++;
		
		if (lhead->n_records == max_n_rec) {
			LOGd("no more request can not be added.\n");
			goto error0;
		}
	}

	if ((unsigned int)lhead->total_io_size + bio_pb
		> MAX_TOTAL_IO_SIZE_IN_LOGPACK_HEADER) {
		LOGd("no more request can not be added.\n");
		goto error0;
	}

	/* Fill the log record contents. */
	lhead->record[idx].is_exist = 1;
	lhead->record[idx].lsid = bio_lsid;
	lhead->record[idx].lsid_local = bio_lsid - logpack_lsid;
	lhead->record[idx].is_padding = 0;
	lhead->record[idx].offset = (u64)bio->bi_sector;
	lhead->record[idx].io_size = (u16)bio_lb;
	lhead->n_records ++;
	lhead->total_io_size += bio_pb;

	bio_lsid += bio_pb;
	idx ++;
	
	return true;
error0:
	return false;
}

MODULE_LICENSE("Dual BSD/GPL");
