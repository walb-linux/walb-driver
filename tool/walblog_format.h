/**
 * Walblog format header.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALBLOG_FORMAT_USER_H
#define _WALBLOG_FORMAT_USER_H

#include "walb/walb.h"

/* Header size of walblog file. */
#define WALBLOG_HEADER_SIZE 4096

/**
 * For walblog_header.flags.
 * Currently not used.
 */
#if 0
enum {
	/* The log means full backup. */
	WL_HEADER_FULL_BACKUP,
	/* The log is stream. */
	WL_HEADER_STREAM,
	/* The log is consolidated (lsid of each log is meaningless). */
	WL_HEADER_CONSOLIDATED,
};
#endif

/**
 * Compression type.
 * Currently not used.
 */
#if 0
enum {
	COMPRESS_NONE = 0,
	COMPRESS_SNAPPY,
	COMPRESS_GZIP,
	COMPRESS_LZMA2,
};
#endif

/**
 * Walblog file header.
 */
struct walblog_header
{
	/* Must be SECTOR_TYPE_WALBLOG_HEADER */
	u16 sector_type;

	/* WalB version. */
	u16 version;

	/* Must be WALBLOG_HEADER_SIZE */
	u16 header_size;

	u16 reserved1;

	/* Checksum of walblog_header. */
	u32 checksum;

	/**************************************************
	 * The above properties must be shared
	 * by all version of walblog_header.
	 **************************************************/

	/* Checksum salt for log header and IO data.
	   Walblog headers do not use the salt. */
	u32 log_checksum_salt;

	/* Block size */
	u32 logical_bs;
	u32 physical_bs;

	/* uuid of walb device. */
	u8 uuid[16];

	/* lsid. */
	u64 begin_lsid;
	u64 end_lsid; /* may be larger than lsid of
			 the next of the end logpack. */
#if 0
	/* Flags. */
	u32 flags;

	/* Compression type. */
	u16 compress_type;

	u16 reserved2;
#endif
} __attribute__((packed));

/**
 * Print walblog header.
 */
inline void print_wlog_header(struct walblog_header* wh)
{
	const int str_size = 16 * 3 + 1;
	char uuidstr[str_size];

	ASSERT(wh->header_size == WALBLOG_HEADER_SIZE);
	ASSERT(wh->sector_type == SECTOR_TYPE_WALBLOG_HEADER);

	sprint_uuid(uuidstr, str_size, wh->uuid);

	printf("*****walblog header*****\n"
		"checksum: %08x\n"
		"version: %"PRIu32"\n"
		"log_checksum_salt: %"PRIu32"\n"
		"logical_bs: %"PRIu32"\n"
		"physical_bs: %"PRIu32"\n"
		"uuid: %s\n"
		"begin_lsid: %"PRIu64"\n"
		"end_lsid: %"PRIu64"\n",
		wh->checksum,
		wh->version,
		wh->log_checksum_salt,
		wh->logical_bs,
		wh->physical_bs,
		uuidstr,
		wh->begin_lsid,
		wh->end_lsid);
}

/**
 * Check wlog header is valid.
 *
 * @return true in valid, or false.
 */
inline bool check_wlog_header(struct walblog_header* wh)
{
	if (checksum((const u8 *)wh, WALBLOG_HEADER_SIZE, 0) != 0) {
		LOGe("wlog checksum is invalid.\n");
		goto error0;
	}
	if (wh->sector_type != SECTOR_TYPE_WALBLOG_HEADER) {
		LOGe("wlog header sector type is invalid.\n");
		goto error0;
	}
	if (wh->version != WALB_VERSION) {
		LOGe("wlog header version is invalid.\n");
		goto error0;
	}
	if (wh->begin_lsid >= wh->end_lsid) {
		LOGe("wlog header does not satisfy begin_lsid < end_lsid.\n");
		goto error0;
	}

	return true;

error0:
	return false;
}

#endif /* _WALBLOG_FORMAT_USER_H */
