/**
 * pending_io.h - Pending IO processing.
 *
 * (C) 2012 Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_PENDING_IO_H_KERNEL
#define WALB_PENDING_IO_H_KERNEL

#include "check_kernel.h"
#include "kern.h"
#include "treemap.h"
#include "bio_wrapper.h"

/* Pending data functions. */
#ifdef WALB_FAST_ALGORITHM
bool pending_insert(
	struct multimap *pending_data, unsigned int *max_sectors_p,
	struct bio_wrapper *biow, gfp_t gfp_mask);
void pending_delete(
	struct multimap *pending_data, unsigned int *max_sectors_p,
	struct bio_wrapper *biow);
bool pending_check_and_copy(
	struct multimap *pending_data, unsigned int max_sectors,
	struct bio_wrapper *biow, gfp_t gfp_mask);
void pending_delete_fully_overwritten(
	struct multimap *pending_data, const struct bio_wrapper *biow);
bool pending_insert_and_delete_fully_overwritten(
	struct multimap *pending_data, unsigned int *max_sectors_p,
	struct bio_wrapper *biow, gfp_t gfp_mask);
#endif

#endif /* WALB_PENDING_IO_H_KERNEL */
