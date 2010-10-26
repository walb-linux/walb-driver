/**
 * General definitions for Walb.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_H
#define _WALB_H

#ifdef __KERNEL__
#include <linux/types.h>
#include <linux/kdev_t.h>
#else /* __KERNEL__ */
#include "userland.h"
#endif /* __KERNEL__ */

inline u32 checksum(const u8 *data, int size)
{
        u32 sum = 0;
        u32 n = size / sizeof(u32);
        int i;
        
        if (size % sizeof(u32) != 0) {
                return 0; /* error */
        }
        
        for (i = 0; i < n; i ++) {
                sum += *(u32 *)(data + (sizeof(u32) * i));
        }
        
        return sum;
}

inline u32 walb_get_sector_size(dev_t devt)
{
        /* We must support devices with 4096 bytes sector. */
        return 512;
}

dev_t walb_get_devt(const char *filename);

#endif /* _WALB_H */
