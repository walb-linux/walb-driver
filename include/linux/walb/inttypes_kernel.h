/**
 * inttypes_kernel.h - Int types for kernel code.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_INT_TYPES_KERNEL_H
#define WALB_INT_TYPES_KERNEL_H

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

#define UINT8_MAX ((u8)~0U)
#define UINT16_MAX ((u16)~0U)
#define UINT32_MAX ((u32)~0U)
#define UINT64_MAX ((u64)~0ULL)

#endif /* WALB_INT_TYPES_KERNEL_H */
