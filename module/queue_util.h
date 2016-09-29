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
	return test_bit(QUEUE_FLAG_WC, &q->queue_flags);
}

static inline bool is_queue_fua_enabled(const struct request_queue *q)
{
	return test_bit(QUEUE_FLAG_FUA, &q->queue_flags);
}

#endif /* QUEUE_UTIL_H_KERNEL */
