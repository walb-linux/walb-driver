/**
 * walb.h - General definitions for Walb.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_H
#define WALB_H

#include "common.h"
#include "disk_name.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Walb log device format version.
 *
 * ChangeLog:
 * ver2
 *   enlarge max IO size to 32bit from 16bit unsigned int.
 *   Still max IO size with data is limited to 16bit due to other reasons.
 */
#define WALB_LOG_VERSION 2

/**
 * Maximum IO size [logical block or sector].
 */
#define WALB_MAX_DISCARD_IO_SECTORS UINT32_MAX
#define WALB_MAX_NORMAL_IO_SECTORS UINT16_MAX

/**
 * Device name prefix/suffix.
 *
 * walb control: /dev/walb/control
 * walb device: /dev/walb/NAME
 * walblog device: /dev/walb/NAME_log
 */
#define WALB_NAME "walb"
#define WALB_DIR_NAME "walb"
#define WALB_CONTROL_NAME "control"
#define WALBLOG_NAME_SUFFIX "_log"
#define WALB_CONTROL_PATH "/dev/" WALB_DIR_NAME "/" WALB_CONTROL_NAME

/**
 * Maximum length of the device name.
 * This must include WALB_DIR_NAME, "/" and '\0' terminator.
 *
 * walb device file:	("%s/%s",  WALB_DIR_NAME, name)
 * walblog device file: ("%s/L%s", WALB_DIR_NAME, name)
 */
#define WALB_DEV_NAME_MAX_LEN (DISK_NAME_LEN				\
				- (sizeof(WALB_DIR_NAME) - 1)		\
				- (sizeof("/L") - 1)			\
				- 1) /* '\0' */

/**
 * Identification to confirm sector type (u16).
 */
#define SECTOR_TYPE_SUPER	     0x0001
#define SECTOR_TYPE_SNAPSHOT	     0x0002
#define SECTOR_TYPE_LOGPACK	     0x0003
#define SECTOR_TYPE_WALBLOG_HEADER  0x0004

/**
 * Constants for lsid.
 */
#define INVALID_LSID ((u64)(-1))
#define MAX_LSID     ((u64)(-2))

/**
 * Validate lsid range.
 */
static inline bool is_lsid_range_valid(u64 lsid0, u64 lsid1)
{
	return lsid0 < lsid1 && lsid1 <= MAX_LSID + 1;
}

/**
 * Maximum pending data size allowed.
 */
#define MAX_PENDING_MB 16384 /* 16GB */

#ifdef __cplusplus
}
#endif

#endif /* WALB_H */
