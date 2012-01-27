/**
 * memblk_io.h - Definition for memblk io functions.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_MEMBLK_IO_H_KERNEL
#define _WALB_MEMBLK_IO_H_KERNEL

#include "check_kernel.h"

#include <linux/blkdev.h>

int memblk_make_request(struct request_queue *q, struct bio *bio);


#endif /* _WALB_MEMBLK_IO_H_KERNEL */
