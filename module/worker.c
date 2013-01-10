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

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

static int generic_worker(void *data);

/*******************************************************************************
 * Static functions implementation.
 *******************************************************************************/

/**
 * Generic worker.
 *
 * @data struct worker_data.
 */
static int generic_worker(void *data)
{
	struct worker_data *wd = (struct worker_data *)data;

	ASSERT(wd);

	while (!kthread_should_stop()) {
		wait_event_interruptible(
			wd->wait_q,
			test_bit(THREAD_WAKEUP, &wd->flags) || kthread_should_stop());

		clear_bit(THREAD_WAKEUP, &wd->flags);

		if (!kthread_should_stop()) {
			wd->run(wd->data);
		}
	}
	complete(&wd->done);
	return 0;
}

/*******************************************************************************
 * Global functions implementation.
 *******************************************************************************/

/*
	紆余曲折でこうなったのでしょうけど
	ここはシンプルに
	return kmalloc(sizeof(struct worker_data), gfp_mask);
	がわかりやすいかと。
*/
/**
 * Allocate a worker.
 */
struct worker_data* alloc_worker(gfp_t gfp_mask)
{
	struct worker_data *worker;

	worker = kmalloc(sizeof(struct worker_data), gfp_mask);
	if (!worker) {
		goto error0;
	}

	return worker;
error0:
	return NULL;
}

/**
 * Free a worker.
 */
void free_worker(struct worker_data* worker)
{
	kfree(worker);
}

/**
 * Initialize worker.
 *
 * @worker_data
 * @run a function to run when wakeup_worker() is called.
 * @data the agrument of the run().
 */
void initialize_worker(
	struct worker_data *wd,
	void (*run)(void *data),
	void *data)
{
	size_t len;

	ASSERT(wd);
	ASSERT(run);

	len = strnlen(wd->name, WORKER_NAME_MAX_LEN);
	BUG_ON(len >= WORKER_NAME_MAX_LEN);

	wd->flags = 0; /* clear bit */
	init_waitqueue_head(&wd->wait_q);
	init_completion(&wd->done);
	wd->run = run;
	wd->data = data;
#ifdef WORKER_DEBUG
	wd->count = 0;
#endif

	wd->tsk = kthread_run(generic_worker, wd, wd->name);
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
#ifdef WORKER_DEBUG
		wd->count++;
#endif
	}
}

/**
 * Finalize worker.
 *
 * This will wait the last execution of the task.
 */
void finalize_worker(struct worker_data *wd)
{
	ASSERT(wd);

	kthread_stop(wd->tsk);
	wait_for_completion(&wd->done);
#ifdef WORKER_DEBUG
	LOGn("worker counter %lu\n", wd->count);
#endif
}

MODULE_LICENSE("Dual BSD/GPL");
