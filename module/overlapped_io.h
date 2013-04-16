/**
 * overlapped_io.h - Overlapped IO processing.
 *
 * (C) 2012 Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_OVERLAPPED_IO_H_KERNEL
#define WALB_OVERLAPPED_IO_H_KERNEL

#include "check_kernel.h"

#include <linux/list.h>
#include "treemap.h"
#include "bio_wrapper.h"

/* Overlapped data functions. */
#ifdef WALB_OVERLAPPED_SERIALIZE
bool overlapped_check_and_insert(
	struct multimap *overlapped_data, unsigned int *max_sectors_p,
	struct bio_wrapper *biow, gfp_t gfp_mask
#ifdef WALB_DEBUG
	, u64 *overlapped_in_id
#endif
	);
unsigned int overlapped_delete_and_notify(
	struct multimap *overlapped_data, unsigned int *max_sectors_p,
	struct list_head *should_submit_list, struct bio_wrapper *biow
#ifdef WALB_DEBUG
	, u64 *overlapped_out_id
#endif
	);
#endif

#endif /* WALB_OVERLAPPED_IO_H_KERNEL */
