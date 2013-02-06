/**
 * Definitions for user-land walb programs.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_USERLAND_H
#define WALB_USERLAND_H

#ifdef __cplusplus
extern "C" {
#endif

/* Integer types */
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int8_t s8;
typedef int16_t s16;
typedef int32_t s32;
typedef int64_t s64;

/* dev_t,
   major(), minor(), makedev(),

   Do not share dev_t variables by kernel and userland.
   sizeof(dev_t) is different.
   Use pairs of unsigned int as major and minor,
   and macros MAJOR, MINOR, MKDEV to share code between kernel and userland.
*/
#include <sys/types.h>
#define MAJOR(dev) major(dev)
#define MINOR(dev) minor(dev)
#define MKDEV(dev) makedev(dev)

/* page size */
#ifndef PAGE_SIZE
#include <unistd.h>
#define PAGE_SIZE ((unsigned int)getpagesize())
#endif

/* bool */
#include <stdbool.h>

#ifdef __cplusplus
}
#endif

#endif /* WALB_USERLAND_H */
