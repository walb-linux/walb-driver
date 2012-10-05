/**
 * Logpack functions for walbctl.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <string.h>

#include "walb/block_size.h"
#include "util.h"
#include "logpack.h"

/*******************************************************************************
 * Private functions.
 *******************************************************************************/

/*******************************************************************************
 * Public functions.
 *******************************************************************************/

/**
 * Check validness of a logpack header.
 *
 * @logpack logpack to be checked.
 * @physical_bs physical block size.
 *		(This is logpack header size.)
 *
 * @return Non-zero in success, or 0.
 */
bool is_valid_logpack_header_with_checksum(
	const struct walb_logpack_header* lhead, int physical_bs)
{
	CHECK(is_valid_logpack_header(lhead));
	CHECK(checksum((const u8 *)lhead, physical_bs) == 0);
	return true;
error:
	LOGe("logpack header checksum is invalid (lsid %"PRIu64").\n",
		lhead->logpack_lsid);
	return false;
}	

/**
 * Read logpack header sector from log device.
 *
 * @fd log device fd opened.
 * @super_sectp super sector.
 * @lsid logpack lsid to read.
 * @logpack buffer to store logpack header data.
 *	    This allocated size must be sector size.
 *
 * @return ture in success, or false.
 */
bool read_logpack_header_from_wldev(int fd,
				const struct walb_super_sector* super_sectp,
				u64 lsid, struct walb_logpack_header* lhead)
{
	/* calc offset in the ring buffer */
	u64 ring_buffer_offset = get_ring_buffer_offset_2(super_sectp);
	u64 ring_buffer_size = super_sectp->ring_buffer_size;

	u64 off = ring_buffer_offset + lsid % ring_buffer_size;

	/* read sector */
	if (! read_sector(fd, (u8 *)lhead, super_sectp->physical_bs, off)) {
		LOGe("read logpack header (lsid %"PRIu64") failed.\n", lsid);
		goto error0;
	}

	/* check lsid */
	if (lsid != lhead->logpack_lsid) {
		LOGe("lsid (given %"PRIu64" read %"PRIu64") is invalid.\n",
			lsid, lhead->logpack_lsid);
		goto error0;
	}

	if (! is_valid_logpack_header_with_checksum(lhead, super_sectp->physical_bs)) {
		LOGe("check logpack header failed.\n");
		goto error0;
	}

	return true;
	
error0:
	return false;
}

/**
 * Print logpack header.
 *
 * @logpack log pack data.
 */
void print_logpack_header(const struct walb_logpack_header* lhead)
{
	ASSERT(lhead != NULL);
	int i;
	printf("*****logpack header*****\n"
		"checksum: %08x\n"
		"n_records: %u\n"
		"n_padding: %u\n"
		"total_io_size: %u\n"
		"logpack_lsid: %"PRIu64"\n",
		lhead->checksum,
		lhead->n_records,
		lhead->n_padding,
		lhead->total_io_size,
		lhead->logpack_lsid);
	for (i = 0; i < lhead->n_records; i ++) {
		printf("record %d\n"
			"  checksum: %08x\n"
			"  lsid: %"PRIu64"\n"
			"  lsid_local: %u\n"
			"  is_padding: %u\n"
			"  io_size: %u\n"
			"  is_exist: %u\n"
			"  offset: %"PRIu64"\n",
			i,
			lhead->record[i].checksum,
			lhead->record[i].lsid,
			lhead->record[i].lsid_local,
			lhead->record[i].is_padding,
			lhead->record[i].io_size,
			lhead->record[i].is_exist,
			lhead->record[i].offset);
		printf("logpack lsid: %"PRIu64"\n", 
			lhead->record[i].lsid - lhead->record[i].lsid_local);
	}
}

/**
 * Write logpack header.
 *
 * @fd file descriptor to write.
 * @physical_bs physical block size.
 * @logpack logpack to be written.
 *
 * @return true in success, or false.
 */
bool write_logpack_header(int fd,
			int physical_bs,
			const struct walb_logpack_header* lhead)
{
	return write_data(fd, (const u8 *)lhead, physical_bs);
}

/**
 * Read logpack data.
 * Padding area will be also read.
 *
 * @fd file descriptor of log device.
 * @super_sectp super sector.
 * @logpack logpack header.
 * @buf buffer.
 * @bufsize buffer size in bytes.
 *
 * @return true in success, or false.
 */
bool read_logpack_data_from_wldev(int fd,
				const struct walb_super_sector* super_sectp,
				const struct walb_logpack_header* lhead,
				u8* buf, size_t bufsize)
{
	int logical_bs = super_sectp->logical_bs;
	int physical_bs = super_sectp->physical_bs;
	int n_lb_in_pb = physical_bs / logical_bs;
	ASSERT(physical_bs % logical_bs == 0);

	if (lhead->total_io_size * physical_bs > (ssize_t)bufsize) {
		LOGe("buffer size is not enough.\n");
		return false;
	}

	int i;
	int n_req = lhead->n_records;
	int total_pb;
	u64 log_off;
	u32 log_lb, log_pb;
	u8 *buf_off;
	
	buf_off = 0;
	total_pb = 0;
	for (i = 0; i < n_req; i ++) {

		log_lb = lhead->record[i].io_size;

		/* Calculate num of physical blocks. */
		log_pb = log_lb / n_lb_in_pb;
		if (log_lb % n_lb_in_pb != 0) { log_pb ++; }

		log_off = get_offset_of_lsid_2
			(super_sectp, lhead->record[i].lsid);
		LOGd("lsid: %"PRIu64" log_off: %"PRIu64"\n",
			lhead->record[i].lsid,
			log_off);

		buf_off = buf + (total_pb * physical_bs);
		if (lhead->record[i].is_padding == 0) {

			/* Read data for the log record. */
			if (! read_sectors(fd, buf_off, physical_bs, log_off, log_pb)) {
				LOGe("read sectors failed.\n");
				goto error0;
			}

			/* Confirm checksum */
			u32 csum = checksum((const u8 *)buf_off, logical_bs * log_lb);
			if (csum != lhead->record[i].checksum) {
				LOGe("log header checksum is invalid. %08x %08x\n",
					csum, lhead->record[i].checksum);
				goto error0;
			}
		} else {
			/* memset zero instead of read due to padding area. */
			memset(buf_off, 0, log_pb * physical_bs);
		}
		total_pb += log_pb;
	}

	return true;
	
error0:	       
	return false;
}

/**
 * Read logpack header from fd.
 *
 * @fd file descriptor (opened, seeked)
 * @physical_bs physical block size (logpack header size)
 * @logpack logpack to be filled. (allocated size must be physical_bs).
 *
 * @return true in success, or false.
 */
bool read_logpack_header(int fd,
			int physical_bs,
			struct walb_logpack_header* lhead)
{
	/* Read */
	if (! read_data(fd, (u8 *)lhead, physical_bs)) {
		return false;
	}

	/* Check */
	if (! is_valid_logpack_header_with_checksum(lhead, physical_bs)) {
		return false;
	}

	return true;
}

/**
 * Read logpack data from ds.
 *
 * @fd file descriptor (opened, seeked)
 * @logical_bs logical block size.
 * @phycial_bs physical block size.
 * @logpack corresponding logpack header.
 * @buf buffer to be filled.
 * @bufsize buffser size.
 *
 * @return true in success, or false.
 */
bool read_logpack_data(int fd,
		int logical_bs, int physical_bs,
		const struct walb_logpack_header* lhead,
		u8* buf, size_t bufsize)
{
	ASSERT(physical_bs % logical_bs == 0);
	const int n_lb_in_pb = physical_bs / logical_bs;

	if (lhead->total_io_size * physical_bs > (ssize_t)bufsize) {
		LOGe("buffer size is not enough.\n");
		goto error0;
	}

	int i;
	const int n_req = lhead->n_records;
	u32 total_pb;

	total_pb = 0;
	for (i = 0; i < n_req; i ++) {

		u32 log_lb = lhead->record[i].io_size;

		u32 log_pb = log_lb / n_lb_in_pb;
		if (log_lb % n_lb_in_pb != 0) { log_pb ++; }

		u8 *buf_off = buf + (total_pb * physical_bs);
		if (lhead->record[i].is_padding == 0) {

			/* Read data of the log record. */
			if (! read_data(fd, buf_off, log_pb * physical_bs)) {
				LOGe("read log data failed.\n");
				goto error0;
			}

			/* Confirm checksum. */
			u32 csum = checksum((const u8 *)buf_off, log_lb * logical_bs);
			if (csum != lhead->record[i].checksum) {
				LOGe("log header checksum in invalid. %08x %08x\n",
					csum, lhead->record[i].checksum);
				goto error0;
			}
		} else {
			memset(buf_off, 0, log_pb * physical_bs);
		}
		total_pb += log_pb;
	}
	
	return true;
		
error0:
	return false;
}

/**
 * Redo logpack.
 *
 * @fd file descriptor of data device (opened).
 * @logical_bs logical block size.
 * @physical_bs physical block size.
 * @logpack logpack header to be redo.
 * @buf logpack data. (meaning data size: lhead->total_io_size * physical_bs)
 */
bool redo_logpack(int fd,
		int logical_bs, int physical_bs,
		const struct walb_logpack_header* lhead,
		const u8* buf)
{
	int i;
	int n_req = lhead->n_records;
	
	for (i = 0; i < n_req; i ++) {

		if (lhead->record[i].is_padding != 0) {
			continue;
		}

		int buf_off = (int)(lhead->record[i].lsid_local - 1) * physical_bs;
		u64 off_lb = lhead->record[i].offset;
		int size_lb = lhead->record[i].io_size;

		if (! write_sectors(fd, buf + buf_off, logical_bs, off_lb, size_lb)) {
			LOGe("write sectors failed.\n");
			goto error0;
		}
	}

	return true;
error0:	       
	return false;
}

/**
 * Write invalid logpack header.
 * This just fill zero.
 *
 * @fd file descriptor of data device (opened).
 * @physical_bs physical block size.
 * @lsid lsid for logpack header.
 *
 * @return true in success, or false.
 */
bool write_invalid_logpack_header(int fd, const struct walb_super_sector* super_sectp, int physical_bs, u64 lsid)
{
	u64 off = get_offset_of_lsid_2(super_sectp, lsid);

	u8* sect = alloc_sector_zero(physical_bs);
	if (!sect) {
		LOGe("Allocate sector failed.\n");
		goto error0;
	}
	if (!write_sector(fd, sect, physical_bs, off)) {
		LOGe("Write sector %"PRIu64" for lsid %"PRIu64" failed.\n", off, lsid);
		goto error1;
	}
	free(sect);
	return true;
error1:
	free(sect);
error0:
	return false;
}

/**
 * Alloate empty logpack data.
 *
 * @physical_bs physical block size.
 * @n_sectors initial number of sectors. n_sectors > 0.
 *
 * @return pointer to allocated logpack in success, or NULL.
 */
logpack_t* alloc_logpack(unsigned int physical_bs, unsigned int n_sectors)
{
	ASSERT_PBS(physical_bs);
	ASSERT(n_sectors > 0);

	/* Allocate for itself. */
	logpack_t* logpack = (logpack_t *)malloc(sizeof(logpack_t));
	if (! logpack) { goto error; }
	logpack->logical_bs = LOGICAL_BLOCK_SIZE;
	logpack->physical_bs = physical_bs;

	/* Header sector. */
	logpack->head_sect = sector_alloc_zero(physical_bs);
	if (! logpack->head_sect) { goto error; }

	/* Data sectors. */
	logpack->data_sects = sector_array_alloc(physical_bs, n_sectors);
	if (! logpack->data_sects) { goto error; }

	struct walb_logpack_header *lhead = logpack_get_header(logpack);
	ASSERT(lhead);
	lhead->checksum = 0;
	lhead->sector_type = SECTOR_TYPE_LOGPACK;
	lhead->total_io_size = 0;
	lhead->logpack_lsid = INVALID_LSID;
	lhead->n_records = 0;
	lhead->n_padding = 0;
	int i;
	int n_max = max_n_log_record_in_sector(physical_bs);
	for (i = 0; i < n_max; i ++) {
		lhead->record[i].is_exist = 0;
	}
	return logpack;

error:
	free_logpack(logpack);
	return NULL;
}

/**
 * Free logpack.
 */
void free_logpack(logpack_t* logpack)
{
	if (logpack) {
		sector_array_free(logpack->data_sects);
		sector_free(logpack->head_sect);
		free(logpack);
	}
}

/**
 * Realloc logpack.
 */
bool realloc_logpack(logpack_t* logpack, int n_sectors)
{
	ASSERT(n_sectors > 0);
	ASSERT_LOGPACK(logpack, false);

	int ret = sector_array_realloc(logpack->data_sects, n_sectors);
	return (ret ? true : false);
}

/**
 * Check logpack is allocated or not.
 *
 * @logpack logpack to be checked.
 * @is_checksum check checksum or not.
 *
 * @return true if valid, or false.
 */
bool is_valid_logpack(logpack_t* logpack, bool is_checksum)
{
	CHECK(logpack != NULL);
	CHECK(logpack->head_sect != NULL);
	CHECK(logpack->data_sects != NULL);
	CHECK(logpack->logical_bs > 0);
	CHECK(logpack->physical_bs >= logpack->logical_bs);
	CHECK(logpack->physical_bs % logpack->logical_bs == 0);
	CHECK(logpack->physical_bs == logpack->head_sect->size);
	CHECK(is_valid_sector_data(logpack->head_sect));
	CHECK(is_valid_sector_data_array(logpack->data_sects));

	if (is_checksum) {
		CHECK(is_valid_logpack_header_with_checksum(
				logpack->head_sect->data,
				logpack->head_sect->size));
	} else {
		CHECK(is_valid_logpack_header(logpack->head_sect->data));
	}
	return true;
error:
	return false;
}

/**
 * NOT TESTED YET.
 *
 * Add an IO request to a logpack.
 * IO data will be copied.
 *
 * @logpack logpack to be modified.
 * @offset Offset of a write IO in logical blocks.
 * @data written data by the write IO.
 * @size IO size in bytes.
 *	 This must be a multiple of logical block size.
 * @is_padding True if this is padding data.
 *	 offset, data, and size is not used.
 *
 * @return True in success, or false.
 */
bool logpack_add_io_request(logpack_t* logpack,
			u64 offset, const u8* data, int size,
			bool is_padding)
{
	/* Check parameters. */
    
	ASSERT_LOGPACK(logpack, false);
	if (is_padding) { ASSERT(data); }
	ASSERT(size % logpack->logical_bs == 0);
	ASSERT(offset <= MAX_LSID);

	/* Short name. */
	u32 pbs = logpack->physical_bs;

	/* Initialize log record. */
	struct walb_logpack_header* lhead = logpack_get_header(logpack);
	u16 rec_id = lhead->n_records;
	struct walb_log_record *rec = &lhead->record[rec_id];
	log_record_init(rec);

	/* Set IO offset. */
	rec->offset = offset;
    
	/* Calc data size in logical blocks. */
	rec->io_size = size / LOGICAL_BLOCK_SIZE;
	unsigned int n_pb = capacity_pb(pbs, rec->io_size);
    
	/* Calc data offset in the logpack in physical bs. */
	if (rec_id == 0) {
		rec->lsid_local = 1;
	} else {
		struct walb_log_record *rec_prev = &lhead->record[rec_id - 1];
		rec->lsid_local = rec_prev->lsid_local +
			(u16)capacity_pb(pbs, rec_prev->io_size);
	}

	/* Padding */
	if (is_padding) { rec->is_padding = 1; }

	/* Realloc sector data array if needed. */
	int current_size = 0;
	int i;
	struct walb_log_record *lrec;
	for_each_logpack_record(i, lrec, lhead) {
		current_size += capacity_pb(pbs, lrec->io_size);
	}
	if (current_size + n_pb > logpack->data_sects->size) {
		if (! realloc_logpack(logpack, current_size + n_pb)) {
			goto error;
		}
	}
    
	/* Copy data to suitable offset in sector data array. */
	

	
    
    
	/* now editing */
    
	/* Modify metadata in logpack header. */

	/* Finalize logpack header. */
	rec->is_exist = 1;
	lhead->n_records ++;
	if (is_padding) { lhead->n_padding ++; }

	return true;
    
error:
	return false;
}

/**
 * Create random logpack data.
 */
struct walb_logpack_header* create_random_logpack(int logical_bs, int physical_bs, const u8* buf)
{
	/* now editing */

	return NULL;
}



/* end of file */
