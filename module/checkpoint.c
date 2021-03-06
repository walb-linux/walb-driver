/**
 * chckpoint.c - Checkpointing functions.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"

#include <linux/module.h>
#include "super.h"
#include "checkpoint.h"
#include "kern.h"

/**
 * Initialize checkpointing.
 */
void init_checkpointing(struct checkpoint_data *cpd)
{
	ASSERT(cpd);

	init_rwsem(&cpd->lock);
	cpd->interval = WALB_DEFAULT_CHECKPOINT_INTERVAL;
	cpd->state = CP_STOPPED;
}

/**
 * Take a checkpoint immediately.
 *
 * @cpd checkpoint data.
 *
 * RETURN:
 *   ture in success, or false.
 */
bool take_checkpoint(struct checkpoint_data *cpd)
{
	bool skip;
	struct walb_dev *wdev;

	ASSERT(cpd);
	wdev = get_wdev_from_checkpoint_data(cpd);
	ASSERT(wdev);

	/* Check the need of writing superblock. */
	spin_lock(&wdev->lsid_lock);
	skip = wdev->lsids.written == wdev->lsids.prev_written;
	spin_unlock(&wdev->lsid_lock);
	if (skip) {
		WLOG_(wdev, "skip superblock sync.\n");
		return true;
	}

	/* Write and flush super block at log device. */
	return walb_sync_super_block(wdev);
}

/**
 * Do checkpointing.
 */
void task_do_checkpointing(struct work_struct *work)
{
	unsigned long j0, j1;
	unsigned long interval, sync_time_ms;
	long delay, sync_time, next_delay;
	int ret;

	struct delayed_work *dwork =
		container_of(work, struct delayed_work, work);
	struct checkpoint_data *cpd =
		container_of(dwork, struct checkpoint_data, dwork);
	struct walb_dev *wdev = get_wdev_from_checkpoint_data(cpd);

	/* CP_WAITING --> CP_RUNNING. */
	down_write(&cpd->lock);
	interval = cpd->interval;

	ASSERT(interval > 0);
	switch (cpd->state) {
	case CP_STOPPING:
		WLOGd(wdev, "do_checkpointing should stop.\n");
		up_write(&cpd->lock);
		return;
	case CP_WAITING:
		cpd->state = CP_RUNNING;
		break;
	default:
		WARN(1, "BUG: invalid cpd->state (wdev: %p state: %u)\n"
			, wdev, cpd->state);
	}
	up_write(&cpd->lock);

	/* Take a checkpoint. */
	j0 = jiffies;
	if (!take_checkpoint(cpd)) {
		/* CP_RUNNING --> CP_STOPPED. */
		down_write(&cpd->lock);
		cpd->state = CP_STOPPED;
		up_write(&cpd->lock);
		return;
	}
	j1 = jiffies;

	/* Calc next delay. */
	delay = msecs_to_jiffies(interval);
	sync_time = (long)(j1 - j0);
	next_delay = delay - sync_time;
	sync_time_ms = jiffies_to_msecs(sync_time);
	WLOG_(wdev, "delay %ld sync_time %ld next_delay %ld\n",
		delay, sync_time, next_delay);
	if (checkpoint_threshold_ms_ > 0 && sync_time_ms > checkpoint_threshold_ms_) {
		WLOGw(wdev, "Checkpoint running time exceeds threshold: %lu\n"
			, sync_time_ms);
	}
	if (next_delay <= 0) {
		WLOGw(wdev, "Checkpoint interval is too small. "
			"Should be more than %lu.\n"
			, sync_time_ms);
		next_delay = 1;
	}
	ASSERT(next_delay > 0);

	/* CP_RUNNING --> CP_WAITING. */
	down_write(&cpd->lock);
	if (cpd->state == CP_RUNNING) {
		/* Register delayed work for next time */
		INIT_DELAYED_WORK(&cpd->dwork, task_do_checkpointing);
		ret = queue_delayed_work(wq_misc_, &cpd->dwork, next_delay);
		ASSERT(ret);
		cpd->state = CP_WAITING;
	} else {
		/* Do nothing */
		ASSERT(cpd->state == CP_STOPPING);
	}
	up_write(&cpd->lock);
}

/**
 * Start checkpointing.
 *
 * Do nothing if
 *   cpd->interval is 0.
 */
void start_checkpointing(struct checkpoint_data *cpd)
{
	unsigned long delay;
	unsigned long interval;
	struct walb_dev *wdev = get_wdev_from_checkpoint_data(cpd);

	down_write(&cpd->lock);
	if (cpd->state != CP_STOPPED) {
		WLOGw(wdev, "Checkpoint state is not stopped.\n");
		up_write(&cpd->lock);
		return;
	}

	interval = cpd->interval;
	if (interval == 0) { /* This is not error. */
		WLOGi(wdev, "checkpoint_interval is 0.\n");
		up_write(&cpd->lock);
		return;
	}
	ASSERT(interval > 0);

	delay = msecs_to_jiffies(interval);
	ASSERT(delay > 0);
	INIT_DELAYED_WORK(&cpd->dwork, task_do_checkpointing);

	queue_delayed_work(wq_misc_, &cpd->dwork, delay);
	cpd->state = CP_WAITING;
	WLOGd(wdev, "state change to CP_WAITING\n");
	up_write(&cpd->lock);
}

/**
 * Stop checkpointing.
 */
void stop_checkpointing(struct checkpoint_data *cpd)
{
	int ret;
	u8 state;
	struct walb_dev *wdev = get_wdev_from_checkpoint_data(cpd);

	down_write(&cpd->lock);
	state = cpd->state;
	if (state != CP_WAITING && state != CP_RUNNING) {
		WLOGw(wdev, "Checkpointing is not running.\n");
		up_write(&cpd->lock);
		return;
	}
	cpd->state = CP_STOPPING;
	WLOGd(wdev, "state change to CP_STOPPING\n");
	up_write(&cpd->lock);

	/* We must unlock before calling this to avoid deadlock. */
	ret = cancel_delayed_work_sync(&cpd->dwork);
	WLOGd(wdev, "cancel_delayed_work_sync: %d\n", ret);

	down_write(&cpd->lock);
	cpd->state = CP_STOPPED;
	WLOGd(wdev, "state change to CP_STOPPED\n");
	up_write(&cpd->lock);
}

/**
 * Get checkpoint interval
 *
 * @cpd checkpoint data.
 *
 * @return current checkpoint interval [ms].
 */
u32 get_checkpoint_interval(struct checkpoint_data *cpd)
{
	u32 interval;

	down_read(&cpd->lock);
	interval = cpd->interval;
	up_read(&cpd->lock);

	return interval;
}

/**
 * Set checkpoint interval.
 *
 * @cpd checkpoint data.
 * @val new checkpoint interval [ms].
 */
void set_checkpoint_interval(struct checkpoint_data *cpd, u32 interval)
{
	down_write(&cpd->lock);
	cpd->interval = interval;
	up_write(&cpd->lock);

	stop_checkpointing(cpd);
	start_checkpointing(cpd);
}

MODULE_LICENSE("GPL");
