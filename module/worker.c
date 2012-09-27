/**
 * worker.c - A thin kthread wrapper.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/kthread.h>
#include <linux/completion.h>
#include "worker.h"
#include "kern.h"

static int generic_worker(void *data);

/**
 * Generic worker.
 *
 * @data struct worker_data.
 */
static int generic_worker(void *data)
{
	struct worker_data *wd = (struct worker_data *)data;
	int ret;
	
	ASSERT(wd);

	while (!kthread_should_stop()) {
		wait_event_interruptible(
			wd->wait_q,
			test_bit(THREAD_WAKEUP, &wd->flags) || kthread_should_stop());

		ret = test_and_clear_bit(THREAD_WAKEUP, &wd->flags);
		ASSERT(ret);
		
		if (!kthread_should_stop()) {
			wd->run(wd->data);
		}
	}
	complete(&wd->done);
	return 0;
}

/**
 * Initialize worker.
 */
void initialize_worker(
	struct worker_data *wd,
	void (*run)(void *data),
	void *data,
	const char *name)
{
	ASSERT(wd);
	ASSERT(wdev);
	ASSERT(task);
	ASSERT(name);

	wd->flags = 0;
	init_waitqueue_head(&wd->wait_q);
	init_completion(&wd->done);
	wd->run = run;
	wd->data = data;
	wd->count = 0; /* debug */
	
	wd->tsk = kthread_run(generic_worker, wd, name);
	ASSERT(wd->tsk);
}

/**
 * Wakeup worker.
 */
void wakeup_worker(struct worker_data *wd)
{
	ASSERT(wd);
	
	if (test_and_set_bit(THREAD_WAKEUP, &wd->flags) == 0) {
		wake_up_interruptible(&wd->wait_q);
		wd->count++; /* debug */
	}
}

/**
 * Finalize worker.
 */
void finalize_worker(struct worker_data *wd)
{
	ASSERT(wd);
	
	kthread_stop(wd->tsk);
	wait_for_completion(&wd->done);
	LOGn("worker counter %lu\n", wd->count); /* debug */
}

MODULE_LICENSE("Dual BSD/GPL");
