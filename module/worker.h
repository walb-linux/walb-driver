/**
 * worker.h - A thin kthread wrapper.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_WORKER_H_KERNEL
#define WALB_WORKER_H_KERNEL

#include <linux/sched.h>
#include <linux/wait.h>
#include <linux/completion.h>

/* #define WORKER_DEBUG */

struct worker_data
{
	struct task_struct *tsk; /* kthread task_struct. */
	wait_queue_head_t wait_q; /* Wait queue. */
	unsigned long flags;
	struct completion done;

	void (*run)(void *data); /* task pointer. */
	void *data; /* task argument. */

#ifdef WORKER_DEBUG
	unsigned long count;
#endif
};

/* For worker_data.flags */
enum {
	THREAD_WAKEUP = 0,
};

void initialize_worker(struct worker_data *wd,
		void (*run)(void *data), void *data, const char *name);
void wakeup_worker(struct worker_data *wd);
void finalize_worker(struct worker_data *wd);

#endif /* WALB_WORKER_H_KERNEL */
