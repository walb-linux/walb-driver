/**
 * test_kthread.c - Test kthread_* functions.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include <linux/mm.h>
#include <linux/kthread.h>
#include <linux/wait.h>
#include <linux/delay.h>
#include "walb/util.h"
#include "build_date.h"
#include "util.h"

#define THREAD_WAKEUP 0

struct thread_data
{
	unsigned int id;
	struct task_struct *tsk; /* kthread task_struct */
	wait_queue_head_t wait_q; /* Wait queue */
	unsigned long timeout_ms; /* Timeout */
	unsigned long flags;
};

struct thread_data tdata_[16];


int worker(void *data) 
{
	struct thread_data *tdata = (struct thread_data *)data;

	LOGd("worker %u start.\n", tdata->id);
	while (!kthread_should_stop()) {

		LOGd("worker %u sleeps.\n", tdata->id);
		wait_event_interruptible_timeout(
			tdata->wait_q,
			test_bit(THREAD_WAKEUP, &tdata->flags) || kthread_should_stop(),
			msecs_to_jiffies(tdata->timeout_ms));

		clear_bit(THREAD_WAKEUP, &tdata->flags);
		LOGd("worker %u woke up.\n", tdata->id);
	}
	LOGd("worker %u stop.\n", tdata->id);

	return 0;
}

static void run_kthread_test(unsigned int n_threads)
{
	unsigned int i;

	LOGd("run_test begin.\n");
	ASSERT(n_threads > 0);

	for (i = 0; i < n_threads; i++) {
		tdata_[i].id = i;
		clear_bit(THREAD_WAKEUP, &tdata_[i].flags);
		init_waitqueue_head(&tdata_[i].wait_q);
		tdata_[i].timeout_ms = MAX_SCHEDULE_TIMEOUT;
		LOGd("tdata id %u flags %lu wait_q %p timeout %lu.\n",
			tdata_[i].id, tdata_[i].flags, &tdata_[i].wait_q, tdata_[i].timeout_ms);
		tdata_[i].tsk = kthread_run(worker, &tdata_[i], "test_worker%u", i);
		ASSERT(tdata_[i].tsk);
	}

	msleep_interruptible(1000);
	
	for (i = 0; i < n_threads; i++) {
		LOGd("wake up tdata_[%u].\n", i);
		set_bit(THREAD_WAKEUP, &tdata_[i].flags);
		wake_up_interruptible(&tdata_[i].wait_q);
	}
	
	msleep_interruptible(1000);
	
	for (i = 0; i < n_threads; i++) {
		kthread_stop(tdata_[i].tsk);
	}

	LOGd("run_test end.\n");
}

static int __init test_init(void)
{
	LOGe("BUILD_DATE %s\n", BUILD_DATE);
	run_kthread_test(10);
	return -1;
}

static void test_exit(void)
{
}

module_init(test_init);
module_exit(test_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("test of kthread");
MODULE_ALIAS("test_kthread");
