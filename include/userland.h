/**
 * Definitions for user-land walb programs.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_USERLAND_H
#define _WALB_USERLAND_H

/* Integer types */
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int8_t   s8;
typedef int16_t  s16;
typedef int32_t  s32;
typedef int64_t  s64;

/* dev_t,
   major(), minor(), makedev() */
#include <sys/types.h>
#define MAJOR(dev) major(dev)
#define MINOR(dev) minor(dev)
#define MKDEV(dev) makedev(dev)

/* page size */
#ifndef PAGE_SIZE
#include <unistd.h>
#define PAGE_SIZE getpagesize()
#endif

/* bool */
#include <stdbool.h>

#endif /* _WALB_USERLAND_H */
