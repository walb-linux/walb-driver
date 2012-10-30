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

/* For iocore interface. */
bool iocore_initialize(struct walb_dev *wdev);
void iocore_finalize(struct walb_dev *wdev);
void iocore_stop(struct walb_dev *wdev);
void iocore_start(struct walb_dev *wdev);
void iocore_make_request(struct walb_dev *wdev, struct bio *bio);
void iocore_log_make_request(struct walb_dev *wdev, struct bio *bio);
void iocore_flush(struct walb_dev *wdev);

#endif /* WALB_IO_H_KERNEL */
