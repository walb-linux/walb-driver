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
 * @logh_sect buffer to store logpack header data.
 *   This allocated size must be sector size.
 * @salt log checksum salt.
 *
 * RETURN:
 *   ture in success, or false.
 */
bool read_logpack_header_from_wldev(
	int fd, const struct walb_super_sector* super_sectp,
	u64 lsid, u32 salt, struct sector_data *logh_sect)
{
	/* calc offset in the ring buffer */
	u64 ring_buffer_offset = get_ring_buffer_offset_2(super_sectp);
	u64 ring_buffer_size = super_sectp->ring_buffer_size;
	u64 off = ring_buffer_offset + lsid % ring_buffer_size;
	struct walb_logpack_header *logh = get_logpack_header(logh_sect);

	/* read sector */
	if (!sector_read(fd, off, logh_sect)) {
		LOGe("read logpack header (lsid %"PRIu64") failed.\n", lsid);
		return false;
	}

	/* check lsid */
	if (lsid != logh->logpack_lsid) {
		LOGe("lsid (given %"PRIu64" read %"PRIu64") is invalid.\n",
			lsid, logh->logpack_lsid);
		return false;
	}
	if (!is_valid_logpack_header_with_checksum(
			logh, super_sectp->physical_bs, salt)) {
		LOGe("check logpack header failed.\n");
		return false;
	}
	return true;
}

/**
 * Print logpack header.
 *
 * @loghk log pack header.
 */
void print_logpack_header(const struct walb_logpack_header* logh)
{
	ASSERT(logh);
	int i;
	printf("*****logpack header*****\n"
		"checksum: %08x\n"
		"n_records: %u\n"
		"n_padding: %u\n"
		"total_io_size: %u\n"
		"logpack_lsid: %"PRIu64"\n",
		logh->checksum,
		logh->n_records,
		logh->n_padding,
		logh->total_io_size,
		logh->logpack_lsid);
	for (i = 0; i < logh->n_records; i++) {
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
			logh->record[i].checksum,
			logh->record[i].lsid,
			logh->record[i].lsid_local,
			test_bit_u32(LOG_RECORD_EXIST, &logh->record[i].flags),
			test_bit_u32(LOG_RECORD_PADDING, &logh->record[i].flags),
			test_bit_u32(LOG_RECORD_DISCARD, &logh->record[i].flags),
			logh->record[i].offset,
			logh->record[i].io_size);
		printf("logpack lsid: %"PRIu64"\n",
			logh->record[i].lsid - logh->record[i].lsid_local);
	}
}

/**
 * Write logpack header.
 *
 * @fd file descriptor to write.
 * @pbs physical block size.
 * @logpack logpack to be written.
 *
 * RETURN:
 *   true in success, or false.
 */
bool write_logpack_header(
	int fd, unsigned int pbs,
	const struct walb_logpack_header* logh)
{
	return write_data(fd, (const u8 *)logh, pbs);
}

/**
 * Read logpack data.
 * Padding area will be also read.
 *
 * @fd file descriptor of log device.
 * @super super sector.
 * @logh logpack header.
 * @salt checksum salt.
 * @sect_ary sector array.
 *
 * RETURN:
 *   index of invalid record found at first.
 *   logh->n_records if the whole logpack is valid.
 */
unsigned int read_logpack_data_from_wldev(
	int fd,
	const struct walb_super_sector* super,
	const struct walb_logpack_header* logh, u32 salt,
	struct sector_data_array *sect_ary)
{
	const int lbs = super->logical_bs;
	const int pbs = super->physical_bs;
	int i;
	int total_pb;

	ASSERT(lbs == LOGICAL_BLOCK_SIZE);
	ASSERT_PBS(pbs);

	if (logh->total_io_size > sect_ary->size) {
		LOGe("buffer size is not enough.\n");
		return false;
	}

	total_pb = 0;
	for (i = 0; i < logh->n_records; i++) {
		u64 log_off;
		u32 log_lb, log_pb;

		if (test_bit_u32(LOG_RECORD_DISCARD, &logh->record[i].flags)) {
			continue;
		}
		log_lb = logh->record[i].io_size;
		log_pb = capacity_pb(pbs, log_lb);
		log_off = get_offset_of_lsid_2
			(super, logh->record[i].lsid);
		LOGd("lsid: %"PRIu64" log_off: %"PRIu64"\n",
			logh->record[i].lsid,
			log_off);

		/* Read data for the log record. */
		if (!sector_array_pread(
				fd, log_off, sect_ary,
				total_pb, log_pb)) {
			LOGe("read sectors failed.\n");
			return i;
		}

		if (test_bit_u32(LOG_RECORD_PADDING, &logh->record[i].flags)) {
			total_pb += log_pb;
			continue;
		}
		/* Confirm checksum */
		u32 csum = sector_array_checksum(
			sect_ary, total_pb * pbs,
			log_lb * lbs, salt);
		if (csum != logh->record[i].checksum) {
			LOGe("log header checksum is invalid. %08x %08x\n",
				csum, logh->record[i].checksum);
			return i;
		}
		total_pb += log_pb;
	}
	ASSERT(i == logh->n_records);
	return logh->n_records;
}

/**
 * Read logpack header from fd.
 *
 * @fd file descriptor (opened, seeked)
 * @pbs physical block size [byte].
 * @salt checksum salt.
 * @logpack logpack to be filled. (allocated size must be physical_bs).
 *
 * RETURN:
 *   true in success, or false.
 */
bool read_logpack_header(
	int fd, unsigned int pbs, u32 salt,
	struct walb_logpack_header* logh)
{
	/* Read */
	if (!read_data(fd, (u8 *)logh, pbs)) {
		return false;
	}

	/* Check */
	if (!is_valid_logpack_header_with_checksum(logh, pbs, salt)) {
		return false;
	}

	return true;
}

/**
 * Read a logpack data from a stream.
 *
 * @fd file descriptor (opened, seeked)
 * @logh corresponding logpack header.
 * @salt checksum salt.
 * @sect_ary sector data array to be store data.
 *
 * RETURN:
 *   true in success, or false.
 */
bool read_logpack_data(
	int fd,
	const struct walb_logpack_header* logh, u32 salt,
	struct sector_data_array *sect_ary)
{
	unsigned int pbs;
	u32 total_pb;
	int i;
	int n_req;

	ASSERT(fd >= 0);
	ASSERT(logh);
	n_req = logh->n_records;
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	pbs = sect_ary->sector_size;
	ASSERT_PBS(pbs);

	if (logh->total_io_size > sect_ary->size) {
		LOGe("sect_ary size is not enough.\n");
		return false;
	}

	total_pb = 0;
	for (i = 0; i < n_req; i++) {
		unsigned int idx_pb, log_lb, log_pb;
		u32 csum;
		const struct walb_log_record *rec = &logh->record[i];

		if (test_bit_u32(LOG_RECORD_DISCARD, &rec->flags)) {
			continue;
		}
		idx_pb = rec->lsid_local - 1;
		log_lb = rec->io_size;
		log_pb = capacity_pb(pbs, log_lb);
		/* Read data of the log record. */
		if (!sector_array_read(fd, sect_ary, idx_pb, log_pb)) {
			LOGe("read log data failed.\n");
			return false;
		}

		if (test_bit_u32(LOG_RECORD_PADDING, &rec->flags)) {
			total_pb += log_pb;
			continue;
		}
		/* Confirm checksum. */
		csum = sector_array_checksum(
			sect_ary,
			idx_pb * pbs,
			log_lb * LOGICAL_BLOCK_SIZE, salt);
		if (csum != rec->checksum) {
			LOGe("log record[%d] checksum is invalid. %08x %08x\n",
				i, csum, rec->checksum);
			return false;
		}
		total_pb += log_pb;
	}
	ASSERT(total_pb == logh->total_io_size);
	return true;
}

/**
 * Redo logpack.
 *
 * @fd file descriptor of data device (opened).
 * @logpack logpack header to be redo.
 * @buf logpack data.
 *   (data size: logh->total_io_size * physical_bs)
 *
 * RETURN:
 *  true in success, or false.
 */
bool redo_logpack(
	int fd,
	const struct walb_logpack_header* logh,
	const struct sector_data_array *sect_ary)
{
	int i, n_req;

	ASSERT(logh);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	n_req = logh->n_records;

	for (i = 0; i < n_req; i++) {
		unsigned int idx_lb, n_lb;
		u64 off_lb;
		const struct walb_log_record *rec = &logh->record[i];

		if (test_bit_u32(LOG_RECORD_PADDING, &rec->flags)) {
			continue;
		}
		off_lb = rec->offset;
		idx_lb = addr_lb(sect_ary->sector_size, rec->lsid_local - 1);
		n_lb = rec->io_size;
		if (test_bit_u32(LOG_RECORD_DISCARD, &rec->flags)) {
			/* If the data device supports discard request,
			   you must issue discard requests. */
			/* now editing */
			continue;
		}
		if (!sector_array_pwrite_lb(fd, off_lb, sect_ary, idx_lb, n_lb)) {
			LOGe("write sectors failed.\n");
			return false;
		}
	}
	return true;
}

/**
 * Write invalid logpack header.
 * This just fill zero.
 *
 * @fd file descriptor of data device (opened).
 * @super_sect super sector.
 * @lsid lsid to invalidate.
 *
 * RETURN:
 *   true in success, or false.
 */
bool write_invalid_logpack_header(
	int fd, const struct sector_data *super_sect, u64 lsid)
{
	struct sector_data *sect;
	bool ret;
	const struct walb_super_sector *super
		= get_super_sector_const(super_sect);
	u64 off = get_offset_of_lsid_2(super, lsid);

	sect = sector_alloc_zero(super->physical_bs);
	if (!sect) {
		LOGe("Allocate sector failed.\n");
		return false;
	}

	ret = sector_write(fd, off, sect);
	if (!ret) {
		LOGe("Write sector %"PRIu64" for lsid %"PRIu64" failed.\n", off, lsid);
	}
	sector_free(sect);
	return ret;
}

/**
 * Shrink logpack header.
 *
 * @logh logpack header to shrink.
 * @invalid_idx new logpack header's n_records must be invalid_idx.
 * @pbs physical block size [byte].
 * @salt checksum salt.
 */
void shrink_logpack_header(
	struct walb_logpack_header *logh, unsigned int invalid_idx,
	unsigned int pbs, u32 salt)
{
	unsigned int i;

	/* Invalidate records. */
	ASSERT(invalid_idx < logh->n_records);
	for (i = invalid_idx; i < logh->n_records; i++) {
		log_record_init(&logh->record[i]);
	}

	/* Set n_records, n_padding, and total_io_size. */
	logh->n_records = invalid_idx;
	logh->n_padding = 0;
	logh->total_io_size = 0;
	for (i = 0; i < invalid_idx; i++) {
		const struct walb_log_record *rec = &logh->record[i];
		if (!test_bit_u32(LOG_RECORD_DISCARD, &rec->flags)) {
			logh->total_io_size += capacity_pb(pbs, rec->io_size);
		}
		if (test_bit_u32(LOG_RECORD_PADDING, &rec->flags)) {
			logh->n_padding++;
		}
	}

	/* Calculate checksum. */
	logh->checksum = 0;
	logh->checksum = checksum((const u8 *)logh, pbs, salt);
	ASSERT(is_valid_logpack_header_with_checksum(logh, pbs, salt));
}

/**
 * Get total size of padding data in a logpack header.
 *
 * RETURN:
 *   total padding size [physical block].
 */
unsigned int get_padding_size_in_logpack_header(
	const struct walb_logpack_header *logh, unsigned int pbs)
{
	unsigned int total_padding_size = 0;
	const struct walb_log_record *rec;
	int i;

	for_each_logpack_record(i, rec, logh) {
		if (test_bit_u32(LOG_RECORD_PADDING, &rec->flags)) {
			total_padding_size += capacity_pb(pbs, rec->io_size);
		}
	}
	return total_padding_size;
}

/**
 * Create a logpack.
 *
 * @logh_sectdp pointer to sector data pointer
 *   for logpack header (will be set).
 * @logd_sect_aryp pointer to sector data array pointer
 *   for logpack data (will be set).
 * @pbs physical block size [byte].
 * @bufsize buffer size for log data [byte].
 *
 * RETURN:
 *    allocated logpack in success, or NULL.
 */
struct logpack *alloc_logpack(
	unsigned int pbs, unsigned int n_sectors)
{
	struct logpack *pack;

	ASSERT(is_valid_pbs(pbs));
	ASSERT(0 < n_sectors);

	pack = (struct logpack *)malloc(sizeof(*pack));
	if (!pack) { goto error1; }
	memset(pack, 0, sizeof(*pack));

	/* Buffer for logpack header. */
	pack->sectd = sector_alloc(pbs);
	if (!pack->sectd) { goto error1; }
	pack->header = get_logpack_header(pack->sectd);

	/* Buffer for logpack data. */
	pack->sectd_ary = sector_array_alloc(pbs, n_sectors);
	if (!pack->sectd_ary) { goto error1; }
	return pack;

error1:
	LOGe("Memory allocation failure.\n");
	free_logpack(pack);
	return NULL;
}

/**
 * Free memories for a logpack.
 */
void free_logpack(struct logpack *pack)
{
	if (pack) {
		sector_free(pack->sectd);
		sector_array_free(pack->sectd_ary);
		free(pack);
	}
}

/**
 * Resize logpack if necessary.
 */
bool resize_logpack_if_necessary(struct logpack *pack, unsigned int n_sectors)
{
	if (n_sectors <= pack->sectd_ary->size) {
		return true;
	}
	return sector_array_realloc(pack->sectd_ary, n_sectors);
}

/* end of file */
