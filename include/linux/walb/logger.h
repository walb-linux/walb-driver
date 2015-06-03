/**
 * a simple logger.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#ifndef WALB_LOGGER_H
#define WALB_LOGGER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "print.h"

/**
 * Simple logger.
 */
#define LOG_ PRINT_
#define LOGd_ PRINT_
#define LOGi_ PRINT_
#define LOGn_ PRINT_
#define LOGw_ PRINT_
#define LOGe_ PRINT_
#ifdef USE_DYNAMIC_DEBUG
#define LOGd pr_debug
#else
#define LOGd PRINTV_D
#endif
#define LOGi PRINTV_I
#define LOGn PRINTV_N
#define LOGw PRINTV_W
#define LOGe PRINTV_E

#ifdef __cplusplus
}
#endif

#endif /* WALB_LOGGER_H */
