/**
 * memblk_io.h - Definition for memblk io functions.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_MEMBLK_IO_H_KERNEL
#define _WALB_MEMBLK_IO_H_KERNEL

#include "check_kernel.h"

#include <linux/blkdev.h>


/*******************************************************************************
 * Global variables defined in memblk.c
 *******************************************************************************/

extern struct workqueue_struct *wqs_; /* single-thread */
extern struct workqueue_struct *wqm_; /* multi-thread */

/*******************************************************************************
 * Global functions prototype.
 *******************************************************************************/

int memblk_make_request(struct request_queue *q, struct bio *bio);








#endif /* _WALB_MEMBLK_IO_H_KERNEL */
