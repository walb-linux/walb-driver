/**
 * inttypes_kernel.h - Int types for kernel code.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_INT_TYPES_KERNEL_H
#define _WALB_INT_TYPES_KERNEL_H

#ifdef CONFIG_64BIT
#define __PRI64_PREFIX "ll"
#else
#define __PRI64_PREFIX "l"
#endif

#define PRId8  "d"
#define PRId16 "d"
#define PRId32 "d"
#define PRId64 __PRI64_PREFIX "d"

#define PRIu8  "u"
#define PRIu16 "u"
#define PRIu32 "u"
#define PRIu64 __PRI64_PREFIX "u"

#endif /* _WALB_INT_TYPES_KERNEL_H */
