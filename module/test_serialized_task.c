/**
 * test_serialized_task.c - test two task serialization methods.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include <linux/slab.h>
#include <linux/workqueue.h>
#include <linux/time.h>
#include <linux/delay.h>
#include <linux/completion.h>

#include <linux/spinlock.h>
#include <linux/kthread.h>
#include <linux/wait.h>
#include <linux/mutex.h>

#include "walb/common.h"
#include "walb/util.h"


/**
 * Variables/constants for workqueues.
 */
struct workqueue_struct *wq_single_;
struct workqueue_struct *wq_normal_;

struct test_work
{
	struct work_struct work;
	struct list_head list; /* list entry */
	unsigned int *countp;
	unsigned int cpuid; /* id of the cpu that enqueues the task. */
};

typedef void (test_work_task_fn)(struct work_struct *work);
typedef struct test_work * (create_test_work_fn)(gfp_t gfp_mask);

/* For benchmark_normal_list. */
LIST_HEAD(test_work_list_);

/* For benchmak_normal_mutex and benchmark_normal_list. */
DEFINE_MUTEX(mutex_);

struct thread_data
{
	unsigned int id;
	struct task_struct *tsk;
	wait_queue_head_t wait_q;
	struct completion done;
	const char *name;
	void (*bench)(unsigned int n_tasks, unsigned int *countp);
	unsigned int n_tasks;
	unsigned int *countp;
};

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

static int worker(void *data);
static void run_benchmark(
	unsigned int n_threads,
	const char *name,
	void (*bench)(unsigned int n_tasks, unsigned int *countp),
	unsigned int n_tasks);

static void test_work_task_single(struct work_struct *work);
static void test_work_task_normal_list(struct work_struct *work);
static void test_work_task_normal_mutex(struct work_struct *work);
static void benchmark_single(unsigned int n_tasks, unsigned int *countp);
static void benchmark_normal(unsigned int n_tasks, unsigned int *countp);
static void benchmark_normal_list(unsigned int n_tasks, unsigned int *countp);
static void benchmark_normal_mutex(unsigned int n_tasks, unsigned int *countp);
static void malloc_and_free(unsigned int n_tasks);

static struct test_work* create_test_work(gfp_t gfp_mask);
static void destroy_test_work(struct test_work *test_work);

static void init_workqueue(void);
static void fin_workqueue(void);

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

static int worker(void *data)
{
	struct thread_data *tdata = (struct thread_data *)data;
	tdata->bench(tdata->n_tasks, tdata->countp);
	complete(&tdata->done);
	return 0;
}

static void run_benchmark(
	unsigned int n_threads,
	const char *name,
	void (*bench)(unsigned int n_tasks, unsigned int *countp),
	unsigned int n_tasks)
{
	unsigned int i;
	struct thread_data *tdata;
	struct timespec bgn_ts, end_ts, sub_ts;
	unsigned int count = 0;
	ASSERT(n_threads > 0);

	tdata = kmalloc(sizeof(struct thread_data) * n_threads, GFP_KERNEL);
	ASSERT(tdata);

	getnstimeofday(&bgn_ts);
	for (i = 0; i < n_threads; i++) {
		tdata[i].id = i;
		init_completion(&tdata[i].done);
		init_waitqueue_head(&tdata[i].wait_q);
		tdata[i].bench = bench;
		tdata[i].n_tasks = n_tasks;
		tdata[i].countp = &count;
		tdata[i].tsk = kthread_run(worker, &tdata[i], "test_worker%u", i);
	}
	for (i = 0; i < n_threads; i++) {
		wait_for_completion(&tdata[i].done);
	}
	getnstimeofday(&end_ts);
	sub_ts = timespec_sub(end_ts, bgn_ts);
	kfree(tdata);
	LOGn("%s %u %ld.%09ld\n", name, count, sub_ts.tv_sec, sub_ts.tv_nsec);
}

static void test_work_task_single(struct work_struct *work)
{
	struct test_work *w = container_of(work, struct test_work, work);
	LOGd_("enqueue %u dequeue %u\n", w->cpuid, raw_smp_processor_id());
	(*w->countp)++;
	destroy_test_work(w);
}

static void test_work_task_normal_list(struct work_struct *work)
{
	struct test_work *w;
	bool should_loop = true;

	while (true) {
		
		mutex_lock(&mutex_);
		if (list_empty(&test_work_list_)) {
			w = NULL;
			should_loop = false;
		} else {
			w = list_first_entry(&test_work_list_, struct test_work, list);
		}
		mutex_unlock(&mutex_);

		if (!should_loop) {
			break;
		}
		
		LOGd_("enqueue %u dequeue %u\n", w->cpuid, raw_smp_processor_id());
#if 0
		if (*w->countp % 100000 == 0) {
			LOGn("count %u\n", *w->countp);
		}
#endif
		(*w->countp)++;
		
		mutex_lock(&mutex_);
		list_del(&w->list);
		mutex_unlock(&mutex_);

		destroy_test_work(w);
	}
}

static void test_work_task_normal_mutex(struct work_struct *work)
{
	struct test_work *w = container_of(work, struct test_work, work);
	mutex_lock(&mutex_);
	LOGd_("enqueue %u dequeue %u\n", w->cpuid, raw_smp_processor_id());
	(*w->countp)++;
	mutex_unlock(&mutex_);
	destroy_test_work(w);
}

static void benchmark_normal_list(
	unsigned int n_tasks, unsigned int *countp)
{
	unsigned int i;
	struct test_work *w;
	bool is_empty;
	ASSERT(n_tasks > 0);
	
	for (i = 0; i < n_tasks; i ++) {
		w = create_test_work(GFP_KERNEL);
		w->countp = countp;
		ASSERT(w);
		INIT_WORK(&w->work, test_work_task_normal_list);
		mutex_lock(&mutex_);
		is_empty = list_empty(&test_work_list_);
		list_add_tail(&w->list, &test_work_list_);
		mutex_unlock(&mutex_);
		if (is_empty) {
			queue_work(wq_normal_, &w->work);
		}
	}
	flush_workqueue(wq_normal_);
}

static void benchmark_normal_mutex(
	unsigned int n_tasks, unsigned int *countp)
{
	unsigned int i;
	struct test_work *w;
	ASSERT(n_tasks > 0);

	for (i = 0; i < n_tasks; i ++) {
		w = create_test_work(GFP_KERNEL);
		w->countp = countp;
		ASSERT(w);
		INIT_WORK(&w->work, test_work_task_normal_mutex);
		queue_work(wq_normal_, &w->work);
	}
	flush_workqueue(wq_normal_);
}

static void benchmark_normal(
	unsigned int n_tasks, unsigned int *countp)
{
	unsigned int i;
	struct test_work *w;
	ASSERT(n_tasks > 0);

	for (i = 0; i < n_tasks; i ++) {
		w = create_test_work(GFP_KERNEL);
		w->countp = countp;
		ASSERT(w);
		INIT_WORK(&w->work, test_work_task_single);
		queue_work(wq_normal_, &w->work);	
	}
	flush_workqueue(wq_normal_);
}

static void benchmark_single(
	unsigned int n_tasks, unsigned int *countp)
{
	unsigned int i;
	struct timespec bgn_ts, end_ts, sub_ts;
	struct test_work *w;
	
	ASSERT(n_tasks > 0);

	getnstimeofday(&bgn_ts);
	for (i = 0; i < n_tasks; i ++) {
		w = create_test_work(GFP_KERNEL);
		w->countp = countp;
		ASSERT(w);
		INIT_WORK(&w->work, test_work_task_single);
		queue_work(wq_single_, &w->work);	
	}
	flush_workqueue(wq_single_);
	getnstimeofday(&end_ts);

	sub_ts = timespec_sub(end_ts, bgn_ts);
	/* LOGn("wq_single %u %ld.%09ld\n", count, sub_ts.tv_sec, sub_ts.tv_nsec); */
}

static void malloc_and_free(unsigned int n_tasks)
{
	unsigned int i;
	struct timespec bgn_ts, end_ts, sub_ts;
	struct test_work *w;
	
	ASSERT(n_tasks > 0);

	getnstimeofday(&bgn_ts);
	for (i = 0; i < n_tasks; i ++) {
		w = create_test_work(GFP_KERNEL);
		ASSERT(w);
		destroy_test_work(w);
	}
	getnstimeofday(&end_ts);

	sub_ts = timespec_sub(end_ts, bgn_ts);
	LOGn("baseline %ld.%09ld\n", sub_ts.tv_sec, sub_ts.tv_nsec);
}

static struct test_work* create_test_work(gfp_t gfp_mask)
{
	struct test_work *w = kmalloc(sizeof(struct test_work), gfp_mask);
	ASSERT(w);
	w->cpuid = raw_smp_processor_id();
	return w;
}

static void destroy_test_work(struct test_work *test_work)
{
	FREE(test_work);
}


static void init_workqueue(void)
{
	wq_single_ = create_singlethread_workqueue("test_serialize_single");
	ASSERT(wq_single_);
	wq_normal_ = alloc_workqueue("test_serialize_normal", WQ_MEM_RECLAIM, 1);
	ASSERT(wq_normal_);
}

static void fin_workqueue(void)
{
	if (wq_normal_) {
		destroy_workqueue(wq_normal_);
	}
	if (wq_single_) {
		destroy_workqueue(wq_single_);
	}
}

static int __init test_init(void)
{
	unsigned int n_threads = 4;
	unsigned int n_tasks = 1000000 / n_threads;
	
	init_workqueue();

	run_benchmark(n_threads, "single", benchmark_single, n_tasks);
	run_benchmark(n_threads, "normal", benchmark_normal, n_tasks);
	run_benchmark(n_threads, "normal_l", benchmark_normal_list, n_tasks);
	run_benchmark(n_threads, "normal_m", benchmark_normal_mutex, n_tasks);
	malloc_and_free(n_tasks);
	
	fin_workqueue();

	return -1;
}

static void test_exit(void)
{
}

module_init(test_init);
module_exit(test_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Test workqueue.");
MODULE_ALIAS("test_serialized_task");
