/**
 * inttypes_kernel.h - Int types for kernel code.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_INT_TYPES_KERNEL_H
#define WALB_INT_TYPES_KERNEL_H

#ifdef CONFIG_64BIT
/*
 * u64 is unsinged long long in kernel even with 64bit.
 */
#define __PRI64_PREFIX "ll"
#else
#define __PRI64_PREFIX "ll"
#endif

#define PRId8  "d"
#define PRId16 "d"
#define PRId32 "d"
#define PRId64 __PRI64_PREFIX "d"

#define PRIu8  "u"
#define PRIu16 "u"
#define PRIu32 "u"
#define PRIu64 __PRI64_PREFIX "u"


#endif /* WALB_INT_TYPES_KERNEL_H */
