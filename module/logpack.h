/**
 * logpack.h - Header for logpack.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_LOGPACK_H_KERNEL
#define WALB_LOGPACK_H_KERNEL

#include "check_kernel.h"
#include <linux/blkdev.h>
#include <linux/bio.h>
#include <linux/workqueue.h>
#include <linux/list.h>
#include "walb/block_size.h"
#include "walb/log_device.h"

/*******************************************************************************
 * Data definitions.
 *******************************************************************************/

/*******************************************************************************
 * Functions prototype.
 *******************************************************************************/

void walb_logpack_header_print(const char *level,
			struct walb_logpack_header *lhead);
bool walb_logpack_header_add_req(
	struct walb_logpack_header *lhead,
	struct request *req,
	unsigned int pbs, u64 ring_buffer_size);
bool walb_logpack_header_add_bio(
	struct walb_logpack_header *lhead,
	struct bio *bio,
	unsigned int pbs, u64 ring_buffer_size);

#endif /* WALB_LOGPACK_H_KERNEL */
