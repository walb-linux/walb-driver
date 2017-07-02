/**
 * queue_util.h - request queue utility.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef QUEUE_UTIL_H_KERNEL
#define QUEUE_UTIL_H_KERNEL

#include <linux/version.h>
#include <linux/blkdev.h>


static inline bool is_queue_flush_enabled(const struct request_queue *q)
{
#if LINUX_VERSION_CODE < KERNEL_VERSION(4, 7, 0)
	return (q->flush_flags & REQ_FLUSH) != 0;
#else
	return test_bit(QUEUE_FLAG_WC, &q->queue_flags);
#endif
}

static inline bool is_queue_fua_enabled(const struct request_queue *q)
{
#if LINUX_VERSION_CODE < KERNEL_VERSION(4, 7, 0)
	return (q->flush_flags & REQ_FUA) != 0;
#else
	return test_bit(QUEUE_FLAG_FUA, &q->queue_flags);
#endif
}

static inline bool supports_flush_request_bdev(struct block_device *bdev)
{
	return is_queue_flush_enabled(bdev_get_queue(bdev));
}

#endif /* QUEUE_UTIL_H_KERNEL */
