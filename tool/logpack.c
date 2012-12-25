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
#include "walb_util.h"
#include "logpack.h"

/*******************************************************************************
 * Private functions.
 *******************************************************************************/

/*******************************************************************************
 * Public functions.
 *******************************************************************************/

/**
 * Read logpack header sector from log device.
 *
 * @fd log device fd opened.
 * @super_sectp super sector.
 * @lsid logpack lsid to read.
 * @lhead_sect buffer to store logpack header data.
 *   This allocated size must be sector size.
 * @salt log checksum salt.
 *
 * @return ture in success, or false.
 */
bool read_logpack_header_from_wldev(
	int fd, const struct walb_super_sector* super_sectp,
	u64 lsid, u32 salt, struct sector_data *lhead_sect)
{
	/* calc offset in the ring buffer */
	u64 ring_buffer_offset = get_ring_buffer_offset_2(super_sectp);
	u64 ring_buffer_size = super_sectp->ring_buffer_size;

	u64 off = ring_buffer_offset + lsid % ring_buffer_size;

	/* read sector */
	if (!sector_read(fd, off, lhead_sect)) {
		LOGe("read logpack header (lsid %"PRIu64") failed.\n", lsid);
		goto error0;
	}
	struct walb_logpack_header *lhead =
		get_logpack_header(lhead_sect);

	/* check lsid */
	if (lsid != lhead->logpack_lsid) {
		LOGe("lsid (given %"PRIu64" read %"PRIu64") is invalid.\n",
			lsid, lhead->logpack_lsid);
		goto error0;
	}
	if (!is_valid_logpack_header_with_checksum(
			lhead, super_sectp->physical_bs, salt)) {
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
	for (i = 0; i < lhead->n_records; i++) {
		printf("record %d\n"
			"  checksum: %08x\n"
			"  lsid: %"PRIu64"\n"
			"  lsid_local: %u\n"
			"  is_exist: %u\n"
			"  is_padding: %u\n"
			"  is_discard: %u\n"
			"  offset: %"PRIu64"\n"
			"  io_size: %u\n",
			i,
			lhead->record[i].checksum,
			lhead->record[i].lsid,
			lhead->record[i].lsid_local,
			test_bit_u32(LOG_RECORD_EXIST, &lhead->record[i].flags),
			test_bit_u32(LOG_RECORD_PADDING, &lhead->record[i].flags),
			test_bit_u32(LOG_RECORD_DISCARD, &lhead->record[i].flags),
			lhead->record[i].offset,
			lhead->record[i].io_size);
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
 * @sect_ary sector array.
 *
 * @return true in success, or false.
 */
bool read_logpack_data_from_wldev(
	int fd,
	const struct walb_super_sector* super,
	const struct walb_logpack_header* lhead, u32 salt,
	struct sector_data_array *sect_ary)
{
	const int lbs = super->logical_bs;
	const int pbs = super->physical_bs;
	ASSERT(lbs == LOGICAL_BLOCK_SIZE);
	ASSERT_PBS(pbs);

	if (lhead->total_io_size > sect_ary->size) {
		LOGe("buffer size is not enough.\n");
		return false;
	}

	int i;
	int n_req = lhead->n_records;
	int total_pb;
	u64 log_off;
	u32 log_lb, log_pb;

	total_pb = 0;
	for (i = 0; i < n_req; i++) {
		if (test_bit_u32(LOG_RECORD_DISCARD, &lhead->record[i].flags)) {
			continue;
		}

		log_lb = lhead->record[i].io_size;

		/* Calculate num of physical blocks. */
		log_pb = capacity_pb(pbs, log_lb);

		log_off = get_offset_of_lsid_2
			(super, lhead->record[i].lsid);
		LOGd("lsid: %"PRIu64" log_off: %"PRIu64"\n",
			lhead->record[i].lsid,
			log_off);

		if (test_bit_u32(LOG_RECORD_PADDING, &lhead->record[i].flags)) {
			/* memset zero instead of read due to padding area. */
			sector_array_memset(
				sect_ary, total_pb * pbs, log_pb * pbs, 0);
		} else {
			/* Read data for the log record. */
			if (!sector_array_pread(
					fd, log_off, sect_ary,
					total_pb, log_pb)) {
				LOGe("read sectors failed.\n");
				goto error0;
			}
			/* Confirm checksum */
			u32 csum = sector_array_checksum(
				sect_ary, total_pb * pbs,
				log_lb * lbs, salt);
			if (csum != lhead->record[i].checksum) {
				LOGe("log header checksum is invalid. %08x %08x\n",
					csum, lhead->record[i].checksum);
				goto error0;
			}
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
 * @pbs physical block size [byte].
 * @salt checksum salt.
 * @logpack logpack to be filled. (allocated size must be physical_bs).
 *
 * @return true in success, or false.
 */
bool read_logpack_header(
	int fd,
	unsigned int pbs, u32 salt,
	struct walb_logpack_header* lhead)
{
	/* Read */
	if (!read_data(fd, (u8 *)lhead, pbs)) {
		return false;
	}

	/* Check */
	if (!is_valid_logpack_header_with_checksum(lhead, pbs, salt)) {
		return false;
	}

	return true;
}

/**
 * Read logpack data from ds.
 *
 * @fd file descriptor (opened, seeked)
 * @pbs physical block size.
 * @salt checksum salt.
 * @logpack corresponding logpack header.
 * @buf buffer to be filled.
 * @bufsize buffser size.
 *
 * @return true in success, or false.
 */
bool read_logpack_data_raw(
	int fd, unsigned int pbs, u32 salt,
	const struct walb_logpack_header* lhead,
	u8* buf, size_t bufsize)
{
	ASSERT(fd >= 0);
	ASSERT_PBS(pbs);

	if (lhead->total_io_size * pbs > bufsize) {
		LOGe("buffer size is not enough.\n");
		goto error0;
	}

	int i;
	const int n_req = lhead->n_records;
	u32 total_pb;

	total_pb = 0;
	for (i = 0; i < n_req; i++) {
		u32 log_lb = lhead->record[i].io_size;
		u32 log_pb = capacity_pb(pbs, log_lb);
		u8 *buf_off = buf + (total_pb * pbs);
		if (!test_bit_u32(LOG_RECORD_PADDING, &lhead->record[i].flags)) {

			/* Read data of the log record. */
			if (!read_data(fd, buf_off, log_pb * pbs)) {
				LOGe("read log data failed.\n");
				goto error0;
			}

			/* Confirm checksum. */
			u32 csum = checksum((const u8 *)buf_off,
					log_lb * LOGICAL_BLOCK_SIZE, salt);
			if (csum != lhead->record[i].checksum) {
				LOGe("log header checksum in invalid. %08x %08x\n",
					csum, lhead->record[i].checksum);
				goto error0;
			}
		} else {
			memset(buf_off, 0, log_pb * pbs);
		}
		total_pb += log_pb;
	}
	ASSERT(total_pb == lhead->total_io_size);
	return true;

error0:
	return false;
}

/**
 * Read logpack data from ds.
 *
 * @fd file descriptor (opened, seeked)
 * @lhead corresponding logpack header.
 * @salt checksum salt.
 * @sect_ary sector data array to be store data.
 *
 * @return true in success, or false.
 */
bool read_logpack_data(
	int fd,
	const struct walb_logpack_header* lhead, u32 salt,
	struct sector_data_array *sect_ary)
{
	ASSERT(fd >= 0);
	ASSERT(lhead);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);

	unsigned int pbs = sect_ary->sector_size;
	ASSERT_PBS(pbs);

	if (lhead->total_io_size > sect_ary->size) {
		LOGe("sect_ary size is not enough.\n");
		goto error0;
	}

	int i;
	const int n_req = lhead->n_records;
	u32 total_pb;
	unsigned int idx_pb, log_lb, log_pb;

	total_pb = 0;
	for (i = 0; i < n_req; i++) {
		if (test_bit_u32(LOG_RECORD_DISCARD, &lhead->record[i].flags)) {
			continue;
		}
		idx_pb = lhead->record[i].lsid_local - 1;
		log_lb = lhead->record[i].io_size;
		log_pb = capacity_pb(pbs, log_lb);
		if (!test_bit_u32(LOG_RECORD_PADDING, &lhead->record[i].flags)) {
			/* Read data of the log record. */
			if (!sector_array_read(fd, sect_ary, idx_pb, log_pb)) {
				LOGe("read log data failed.\n");
				goto error0;
			}

			/* Confirm checksum. */
			u32 csum = sector_array_checksum(
				sect_ary,
				idx_pb * pbs,
				log_lb * LOGICAL_BLOCK_SIZE, salt);
			if (csum != lhead->record[i].checksum) {
				LOGe("log header checksum in invalid. %08x %08x\n",
					csum, lhead->record[i].checksum);
				goto error0;
			}
		} else {
			sector_array_memset(
				sect_ary, idx_pb * pbs, log_pb * pbs, 0);
		}
		total_pb += log_pb;
	}
	ASSERT(total_pb == lhead->total_io_size);
	return true;

error0:
	return false;
}

/**
 * Redo logpack.
 *
 * @fd file descriptor of data device (opened).
 * @logpack logpack header to be redo.
 * @buf logpack data. (meaning data size: lhead->total_io_size * physical_bs)
 */
bool redo_logpack(
	int fd,
	const struct walb_logpack_header* lhead,
	const struct sector_data_array *sect_ary)
{
	ASSERT(lhead);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);

	int i;
	int n_req = lhead->n_records;

	unsigned int idx_lb, n_lb;
	u64 off_lb;

	for (i = 0; i < n_req; i++) {
		if (test_bit_u32(LOG_RECORD_PADDING, &lhead->record[i].flags)) {
			continue;
		}
		off_lb = lhead->record[i].offset;
		idx_lb = addr_lb(sect_ary->sector_size,
				lhead->record[i].lsid_local - 1);
		n_lb = lhead->record[i].io_size;
		if (!sector_array_pwrite_lb(fd, off_lb, sect_ary, idx_lb, n_lb)) {
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
 * @super_sect super sector.
 * @lsid lsid for logpack header.
 *
 * @return true in success, or false.
 */
bool write_invalid_logpack_header(
	int fd, const struct sector_data *super_sect, u64 lsid)
{
	const struct walb_super_sector *super
		= get_super_sector_const(super_sect);
	u64 off = get_offset_of_lsid_2(super, lsid);
	unsigned int pbs = super->physical_bs;

	struct sector_data *sect = sector_alloc_zero(pbs);
	if (!sect) {
		LOGe("Allocate sector failed.\n");
		goto error0;
	}
	if (!sector_write(fd, off, sect)) {
		LOGe("Write sector %"PRIu64" for lsid %"PRIu64" failed.\n", off, lsid);
		goto error1;
	}
	sector_free(sect);

	return true;
error1:
	sector_free(sect);
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
struct logpack* alloc_logpack(unsigned int physical_bs, unsigned int n_sectors)
{
	ASSERT_PBS(physical_bs);
	ASSERT(n_sectors > 0);

	/* Allocate for itself. */
	struct logpack* logpack = (struct logpack *)malloc(sizeof(struct logpack));
	if (!logpack) { goto error; }
	logpack->logical_bs = LOGICAL_BLOCK_SIZE;
	logpack->physical_bs = physical_bs;

	/* Header sector. */
	logpack->head_sect = sector_alloc_zero(physical_bs);
	if (!logpack->head_sect) { goto error; }

	/* Data sectors. */
	logpack->data_sects = sector_array_alloc(physical_bs, n_sectors);
	if (!logpack->data_sects) { goto error; }

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
	for (i = 0; i < n_max; i++) {
		clear_bit_u32(LOG_RECORD_EXIST, &lhead->record[i].flags);
	}
	return logpack;

error:
	free_logpack(logpack);
	return NULL;
}

/**
 * Free logpack.
 */
void free_logpack(struct logpack* logpack)
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
bool realloc_logpack(struct logpack* logpack, int n_sectors)
{
	ASSERT(n_sectors > 0);
	ASSERT_LOGPACK(logpack);

	int ret = sector_array_realloc(logpack->data_sects, n_sectors);
	return (ret ? true : false);
}

/**
 * Check logpack is allocated or not.
 *
 * @logpack logpack to be checked.
 * @is_checksum check checksum or not.
 * @salt checksum salt.
 *
 * @return true if valid, or false.
 */
bool is_valid_logpack(struct logpack* logpack, bool is_checksum, u32 salt)
{
	CHECKd(logpack != NULL);
	CHECKd(logpack->head_sect != NULL);
	CHECKd(logpack->data_sects != NULL);
	CHECKd(logpack->logical_bs > 0);
	CHECKd(logpack->physical_bs >= logpack->logical_bs);
	CHECKd(logpack->physical_bs % logpack->logical_bs == 0);
	CHECKd(logpack->physical_bs == logpack->head_sect->size);
	CHECKd(is_valid_sector_data(logpack->head_sect));
	CHECKd(is_valid_sector_data_array(logpack->data_sects));

	if (is_checksum) {
		CHECKd(is_valid_logpack_header_with_checksum(
				logpack->head_sect->data,
				logpack->head_sect->size, salt));
	} else {
		CHECKd(is_valid_logpack_header(logpack->head_sect->data));
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
bool logpack_add_io_request(
	struct logpack* logpack,
	u64 offset, UNUSED const u8* data, int size,
	bool is_padding)
{
	/* Check parameters. */
	ASSERT_LOGPACK(logpack);
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
	if (is_padding) {
		set_bit_u32(LOG_RECORD_PADDING, &rec->flags);
	}

	/* Realloc sector data array if needed. */
	int current_size = 0;
	int i;
	struct walb_log_record *lrec;
	for_each_logpack_record(i, lrec, lhead) {
		current_size += capacity_pb(pbs, lrec->io_size);
	}
	if (current_size + n_pb > logpack->data_sects->size) {
		if (!realloc_logpack(logpack, current_size + n_pb)) {
			goto error;
		}
	}

	/* Copy data to suitable offset in sector data array. */





	/* now editing */

	/* Modify metadata in logpack header. */

	/* Finalize logpack header. */
	set_bit_u32(LOG_RECORD_EXIST, &rec->flags);
	lhead->n_records++;
	if (is_padding) { lhead->n_padding++; }

	return true;

error:
	return false;
}

/**
 * Create random logpack data.
 */
struct walb_logpack_header* create_random_logpack(
	UNUSED  unsigned int lbs, UNUSED unsigned int pbs, UNUSED const u8* buf)
{
	/* now editing */

	return NULL;
}


/* end of file */
