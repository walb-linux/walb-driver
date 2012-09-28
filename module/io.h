/**
 * io.h - IO processing core of WalB.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_IO_H_KERNEL
#define WALB_IO_H_KERNEL

#include "check_kernel.h"
#include <linux/bio.h>
#include <linux/blkdev.h>

/* make_requrest callback. */
void walb_make_request(struct request_queue *q, struct bio *bio);
void walblog_make_request(struct request_queue *q, struct bio *bio);

#endif /* WALB_IO_H_KERNEL */
