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
#define PRINT_D(fmt, args...) fprintf(stderr, fmt, ##args)
#else
#define PRINT_D(fmt, args...)
#endif
#define PRINT_E(fmt, args...) fprintf(stderr, fmt, ##args)
#define PRINT_W PRINT_E
#define PRINT_N PRINT_E
#define PRINT_I PRINT_E
#define PRINT(flag, fmt, args...) fprintf(stderr, fmt, ##args)
#define PRINTV_E(fmt, args...) PRINT_E("ERROR(%s) " fmt, __func__, ##args)
#define PRINTV_W(fmt, args...) PRINT_W("WARNING(%s) " fmt, __func__, ##args)
#define PRINTV_N(fmt, args...) PRINT_N("NOTICE(%s) " fmt, __func__, ##args)
#define PRINTV_I(fmt, args...) PRINT_I("INFO(%s) " fmt, __func__, ##args)
#define PRINTV_D(fmt, args...) PRINT_D(					\
		"DEBUG(%s:%d:%s) " fmt, SRC_FILE, __LINE__, __func__, ##args)
#endif /* __KERNEL__ */
#define PRINT_(fmt, args...)

#ifdef __cplusplus
}
#endif

#endif /* WALB_PRINT_H */
