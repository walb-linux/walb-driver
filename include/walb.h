/**
 * General definitions for Walb.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_H
#define _WALB_H

#define WALB_VERSION 1

#ifdef __KERNEL__
#include <linux/types.h>
#include <linux/kdev_t.h>
#include "inttypes_kern.h"
#ifdef WALB_DEBUG
#define ASSERT(cond) BUG_ON(!(cond))
#else /* WALB_DEBUG */
#define ASSERT(cond)
#endif /* WALB_DEBUG */
#else /* __KERNEL__ */
#include "userland.h"
#include <assert.h>
#include <string.h>
#define ASSERT(cond) assert(cond)
#endif /* __KERNEL__ */

/**
 * Disk name length.
 */
#define DISK_NAME_LEN_USER 32
#ifdef __KERNEL__
#include <linux/genhd.h>
#else
#define DISK_NAME_LEN DISK_NAME_LEN_USER
#endif
#define ASSERT_DISK_NAME_LEN() ASSERT(DISK_NAME_LEN == DISK_NAME_LEN_USER)

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
 * walb device file:    ("%s/%s",  WALB_DIR_NAME, name)
 * walblog device file: ("%s/L%s", WALB_DIR_NAME, name)
 */
#define WALB_DEV_NAME_MAX_LEN (DISK_NAME_LEN - sizeof(WALB_DIR_NAME) - 3)

/**
 * Identification to confirm sector type.
 */
#define SECTOR_TYPE_SUPER    0x0001
#define SECTOR_TYPE_SNAPSHOT 0x0002
#define SECTOR_TYPE_LOGPACK  0x0003
#define SECTOR_TYPE_WALBLOG_HEADER  0x0004

/**
 * Max length of snapshot name.
 */
#define SNAPSHOT_NAME_MAX_LEN 64


static inline u64 checksum_partial(u64 sum, const u8 *data, u32 size)
{
        u32 n = size / sizeof(u32);
        u32 i;

        ASSERT(size % sizeof(u32) == 0);

        for (i = 0; i < n; i ++) {
                sum += *(u32 *)(data + (sizeof(u32) * i));
        }
        return sum;
}

static inline u32 checksum_finish(u64 sum)
{
        u32 ret;
        
        ret = ~(u32)((sum >> 32) + (sum << 32 >> 32)) + 1;
        return (ret == (u32)(-1) ? 0 : ret);
}

static inline u32 checksum(const u8 *data, u32 size)
{
        return checksum_finish(checksum_partial(0, data, size));
}

/**
 * Sprint uuid.
 *
 * @buf buffer to store result. Its size must be 16 * 2 + 1.
 * @uuid uuid ary. Its size must be 16.
 */
static inline void sprint_uuid(char *buf, const u8 *uuid)
{
        char tmp[3];
        int i;

        buf[0] = '\0';
        for (i = 0; i < 16; i ++) {
                sprintf(tmp, "%02x", uuid[i]);
                strcat(buf, tmp);
        }
}

/**
 * Check the name satisfy snapshot name spec.
 *
 * @name name of snapshot.
 *
 * @return 1 if valid, or 0.
 */
static inline int is_valid_snapshot_name(char *name)
{
        size_t len, i;
        char n;

        /* Length check. */
        len = strnlen(name, SNAPSHOT_NAME_MAX_LEN);
        if (len == SNAPSHOT_NAME_MAX_LEN) {
                return 0;
        }

        /* Character code check. */
        for (i = 0; i < len; i ++) {
                n = name[i];
                if (! (n == '_' ||
                       n == '-' ||
                       ('0' <= n && n <= '9') ||
                       ('a' <= n && n <= 'z') ||
                       ('A' <= n && n <= 'Z'))) { return 0; }
        }
        return 1;
}


#endif /* _WALB_H */
