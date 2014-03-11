/**
 * printf/printk wrapper.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#ifndef WALB_PRINT_H
#define WALB_PRINT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "common.h"

/**
 * Print macro for debug.
 */
#ifdef __KERNEL__
#include <linux/kernel.h>
#define PRINT(flag, fmt, args...) printk(flag fmt, ##args)
#define PRINT_E(fmt, args...) PRINT(KERN_ERR, fmt, ##args)
#define PRINT_W(fmt, args...) PRINT(KERN_WARNING, fmt, ##args)
#define PRINT_N(fmt, args...) PRINT(KERN_NOTICE, fmt, ##args)
#define PRINT_I(fmt, args...) PRINT(KERN_INFO, fmt, ##args)
#ifdef WALB_DEBUG
#define PRINT_D(fmt, args...) PRINT(KERN_DEBUG, fmt, ##args)
#else
#define PRINT_D(fmt, args...)
#endif
#define PRINTV_E(fmt, args...) PRINT_E("walb(%s) " fmt, __func__, ##args)
#define PRINTV_W(fmt, args...) PRINT_W("walb(%s) " fmt, __func__, ##args)
#define PRINTV_N(fmt, args...) PRINT_N("walb(%s) " fmt, __func__, ##args)
#define PRINTV_I(fmt, args...) PRINT_I("walb(%s) " fmt, __func__, ##args)
#define PRINTV_D(fmt, args...) PRINT_D(					\
		"walb(%s:%d:%s) " fmt, SRC_FILE, __LINE__, __func__, ##args)
#else /* __KERNEL__ */
#include <stdio.h>
#ifdef WALB_DEBUG
#define PRINT_D(...) fprintf(stderr, __VA_ARGS__)
#else
#define PRINT_D(...)
#endif
#define PRINT_E(...) fprintf(stderr, fmt, __VA_ARGS__)
#define PRINT_W PRINT_E
#define PRINT_N PRINT_E
#define PRINT_I PRINT_E
#define PRINT(flag, ...) fprintf(stderr, __VA_ARGS__)
#define PRINT_X(type, ...) PRINT_X2(type, __VA_ARGS__, "")
#define PRINT_X2(type, fmt, ...) fprintf(stderr, type "(%s) " fmt "%s", __func__, __VA_ARGS__)
#define PRINTV_E(...) PRINT_X("ERROR", __VA_ARGS__)
#define PRINTV_W(...) PRINT_X("WARNING", __VA_ARGS__)
#define PRINTV_N(...) PRINT_X("NOTICE", __VA_ARGS__)
#define PRINTV_I(...) PRINT_X("INFO", __VA_ARGS__)
#define PRINTV_D(...) PRINTV_D2(__VA_ARGS__, "")
#define PRINTV_D2(fmt, ...) fprintf(stderr, "DEBUG(%s:%d:%s) " fmt "%s", SRC_FILE, __LINE__, __func__, __VA_ARGS__)

#endif /* __KERNEL__ */
#define PRINT_(...)

#ifdef __cplusplus
}
#endif

#endif /* WALB_PRINT_H */
