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
 * Walblog file header.
 */
typedef struct walblog_header
{
	u16 header_size; /* must be WALBLOG_HEADER_SIZE */
	u16 sector_type; /* must be SECTOR_TYPE_WALBLOG_HEADER */
	u32 checksum; /* walblog_header checksum. */
	
	/* walb version */
	u32 version;
	u32 reserved1;

	/* Block size */
	u32 logical_bs;
	u32 physical_bs;

	/* uuid of walb device. */
	u8 uuid[16];

	/* lsid. */
	u64 begin_lsid;
	u64 end_lsid; /* may be larger than lsid of
			 the next of the end logpack. */
} walblog_header_t;

/**
 * Print walblog header.
 */
inline void print_wlog_header(walblog_header_t* wh)
{
	const int str_size = 16 * 3 + 1;
	char uuidstr[str_size];
	
	ASSERT(wh->header_size == WALBLOG_HEADER_SIZE);
	ASSERT(wh->sector_type == SECTOR_TYPE_WALBLOG_HEADER);

	sprint_uuid(uuidstr, str_size, wh->uuid);
	
	printf("*****walblog header*****\n"
		"checksum: %08x\n"
		"version: %"PRIu32"\n"
		"logical_bs: %"PRIu32"\n"
		"physical_bs: %"PRIu32"\n"
		"uuid: %s\n"
		"begin_lsid: %"PRIu64"\n"
		"end_lsid: %"PRIu64"\n",
		wh->checksum,
		wh->version,
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
inline bool check_wlog_header(walblog_header_t* wh)
{
	if (checksum((const u8 *)wh, WALBLOG_HEADER_SIZE) != 0) {
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
