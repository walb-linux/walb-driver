/**
 * io.h - IO processing core of WalB.
 *
 * (C) 2012 Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_IO_H_KERNEL
#define WALB_IO_H_KERNEL

#include "check_kernel.h"
#include <linux/bio.h>
#include <linux/blkdev.h>
#include <linux/list.h>
#include "kern.h"
#include "bio_wrapper.h"
#include "worker.h"
#include "treemap.h"

/**
 * iocored->flags bit.
 */
enum {
	IOCORE_STATE_FAILURE = 0,
	IOCORE_STATE_READ_ONLY,
	IOCORE_STATE_LOG_OVERFLOW,

	/* These are for workqueue tasks management. */
	IOCORE_STATE_SUBMIT_LOG_TASK_WORKING,
	IOCORE_STATE_WAIT_LOG_TASK_WORKING,
	IOCORE_STATE_SUBMIT_DATA_TASK_WORKING,
	IOCORE_STATE_WAIT_DATA_TASK_WORKING,
};

/**
 * (struct walb_dev *)->private_data.
 */
struct iocore_data
{
	/* See IOCORE_STATE_XXXXX */
	unsigned long flags;

	/* IO core can process IOs during only stopper is 0. */
	atomic_t n_stoppers;

	/*
	 * There are four queues.
	 * Each queue must be accessed with its own lock held.
	 *
	 * logpack_submit_queue:
	 *   writepack list.
	 * logpack_wait_queue:
	 *   writepack list.
	 * datapack_submit_queue:
	 *   bio_wrapper list.
	 * datapack_wait_queue:
	 *   bio_wrapper list.
	 * logpack_gc_queue:
	 *   writepack list.
	 */
	spinlock_t logpack_submit_queue_lock;
	struct list_head logpack_submit_queue;
	spinlock_t logpack_wait_queue_lock;
	struct list_head logpack_wait_queue;
	spinlock_t datapack_submit_queue_lock;
	struct list_head datapack_submit_queue;
	spinlock_t datapack_wait_queue_lock;
	struct list_head datapack_wait_queue;
	spinlock_t logpack_gc_queue_lock;
	struct list_head logpack_gc_queue;

	/* Number of pending bio(s). */
	atomic_t n_pending_bio;
	/* Number of started write bio(s).
	   n_started_write_bio <= n_pending_write_bio.
	   n_pending_write_bio + n_pending_read_bio = n_pending_bio. */
	atomic_t n_started_write_bio;
	/* Number of pending packs to be garbage-collected. */
	atomic_t n_pending_gc;

	/* for gc worker. */
	struct worker_data gc_worker_data;

#ifdef WALB_OVERLAPPED_SERIALIZE
	/**
	 * All req_entry data may not keep reqe->bioe_list.
	 * You must keep address and size information in another way.
	 */
	spinlock_t overlapped_data_lock; /* Use spin_lock()/spin_unlock(). */
	struct multimap *overlapped_data; /* key: blk_rq_pos(req),
					     val: pointer to req_entry. */

	/* Maximum request size [logical block]. */
	unsigned int max_sectors_in_overlapped;

#ifdef WALB_DEBUG
	/* In order to check FIFO property. */
	u64 overlapped_in_id;
	u64 overlapped_out_id;
#endif

#endif /* WALB_OVERLAPPED_SERIALIZE */

#ifdef WALB_FAST_ALGORITHM
	/**
	 * All bio_wrapper data must keep
	 * biow->bioe_list while they are stored in the pending_data.
	 */
	/* Use spin_lock()/spin_unlock(). */
	spinlock_t pending_data_lock;

	/* key: biow->pos,
	   val: pointer to bio_wrapper. */
	struct multimap *pending_data;

	/* Number of sectors pending
	   [logical block]. */
	unsigned int pending_sectors;

	/* Maximum request size [logical block]. */
	unsigned int max_sectors_in_pending;

	/* For queue stopped timeout check. */
	unsigned long queue_restart_jiffies;

	/* true if queue is stopped. */
	bool is_under_throttling;
#endif
	/* To check that we should flush log device. */
	unsigned long log_flush_jiffies;

#ifdef WALB_DEBUG
	atomic_t n_flush_io;
	atomic_t n_flush_logpack;
	atomic_t n_flush_force;
#endif
};

/* Completion timeout [msec]. */
static const unsigned long completion_timeo_ms_ = 10000; /* 10 seconds. */

/**
 * Get iocore data from wdev.
 */
static inline struct iocore_data* get_iocored_from_wdev(
	struct walb_dev *wdev)
{
	ASSERT(wdev);
	return (struct iocore_data *)wdev->private_data;
}

/* make_requrest callback. */
void walb_make_request(struct request_queue *q, struct bio *bio);
void walblog_make_request(struct request_queue *q, struct bio *bio);

/* For iocore interface. */
bool iocore_initialize(struct walb_dev *wdev);
void iocore_finalize(struct walb_dev *wdev);
void iocore_freeze(struct walb_dev *wdev);
void iocore_melt(struct walb_dev *wdev);
void iocore_make_request(struct walb_dev *wdev, struct bio *bio);
void iocore_log_make_request(struct walb_dev *wdev, struct bio *bio);
void iocore_flush(struct walb_dev *wdev);
void iocore_set_failure(struct walb_dev *wdev);
void iocore_set_readonly(struct walb_dev *wdev);
bool iocore_is_readonly(struct walb_dev *wdev);
void iocore_clear_log_overflow(struct walb_dev *wdev);
bool iocore_is_log_overflow(struct walb_dev *wdev);

/* Iocore utilities. */
void wait_for_all_pending_io_done(struct walb_dev *wdev);
struct bio_wrapper* alloc_bio_wrapper_inc(
	struct walb_dev *wdev, gfp_t gfp_mask);
void destroy_bio_wrapper_dec(
	struct walb_dev *wdev, struct bio_wrapper *biow);

#endif /* WALB_IO_H_KERNEL */
