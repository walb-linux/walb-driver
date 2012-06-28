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
	struct completion done;
	struct test_work *next;
	unsigned int cpuid; /* id of the cpu that enqueues the task. */
};

typedef void (test_work_task_fn)(struct work_struct *work);
typedef struct test_work * (create_test_work_fn)(gfp_t gfp_mask);


/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

static void test_work_task_single(struct work_struct *work);
static void test_work_task_normal(struct work_struct *work);
static void benchmark_single(unsigned int n_tasks);
static void benchmark_normal(unsigned int n_tasks);
static void malloc_and_free(unsigned int n_tasks);

static struct test_work* create_test_work_single(gfp_t gfp_mask);
static struct test_work* create_test_work_normal(gfp_t gfp_mask);
static void destroy_test_work(struct test_work *test_work);

static void init_workqueue(void);
static void fin_workqueue(void);


/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

static void test_work_task_single(struct work_struct *work)
{
	struct test_work *w = container_of(work, struct test_work, work);
	LOGd_("enqueue %u dequeue %u\n", w->cpuid, raw_smp_processor_id());
	destroy_test_work(w);
}

static void test_work_task_normal(struct work_struct *work)
{
	struct test_work *w = container_of(work, struct test_work, work);
	LOGd_("enqueue %u dequeue %u\n", w->cpuid, raw_smp_processor_id());
	wait_for_completion(&w->done);
	if (w->next) {
		complete(&w->next->done);
	}
	destroy_test_work(w);
}

static void benchmark_normal(unsigned int n_tasks)
{
	unsigned int i;
	struct timespec bgn_ts, end_ts, sub_ts;
	struct test_work *w, *prev;
	
	ASSERT(n_tasks > 0);

	getnstimeofday(&bgn_ts);
	prev = NULL;
	for (i = 0; i < n_tasks; i ++) {

		w = create_test_work_normal(GFP_KERNEL);
		ASSERT(w);
		INIT_WORK(&w->work, test_work_task_normal);
		if (prev) {
			prev->next = w;
			queue_work(wq_normal_, &prev->work);
		} else {
			/* Kick the first task. */
			complete(&w->done);
		}
		prev = w;
	}
	ASSERT(w);
	queue_work(wq_normal_, &w->work);
	
	flush_workqueue(wq_normal_);
	getnstimeofday(&end_ts);

	sub_ts = timespec_sub(end_ts, bgn_ts);
	LOGn("wq_normal %ld.%09ld\n", sub_ts.tv_sec, sub_ts.tv_nsec);
}

static void benchmark_single(unsigned int n_tasks)
{
	unsigned int i;
	struct timespec bgn_ts, end_ts, sub_ts;
	struct test_work *w;
	
	ASSERT(n_tasks > 0);

	getnstimeofday(&bgn_ts);
	for (i = 0; i < n_tasks; i ++) {
		w = create_test_work_single(GFP_KERNEL);
		ASSERT(w);
		INIT_WORK(&w->work, test_work_task_single);
		queue_work(wq_single_, &w->work);	
	}
	flush_workqueue(wq_single_);
	getnstimeofday(&end_ts);

	sub_ts = timespec_sub(end_ts, bgn_ts);
	LOGn("wq_single %ld.%09ld\n", sub_ts.tv_sec, sub_ts.tv_nsec);
}

static void malloc_and_free(unsigned int n_tasks)
{
	unsigned int i;
	struct timespec bgn_ts, end_ts, sub_ts;
	struct test_work *w;
	
	ASSERT(n_tasks > 0);

	getnstimeofday(&bgn_ts);
	for (i = 0; i < n_tasks; i ++) {
		w = create_test_work_single(GFP_KERNEL);
		ASSERT(w);
		destroy_test_work(w);
	}
	getnstimeofday(&end_ts);

	sub_ts = timespec_sub(end_ts, bgn_ts);
	LOGn("baseline %ld.%09ld\n", sub_ts.tv_sec, sub_ts.tv_nsec);
}


static struct test_work* create_test_work_normal(gfp_t gfp_mask)
{
	struct test_work *w = kmalloc(sizeof(struct test_work), gfp_mask);
	ASSERT(w);
	init_completion(&w->done);
	w->next = NULL;
	w->cpuid = raw_smp_processor_id();
	return w;
}

static struct test_work* create_test_work_single(gfp_t gfp_mask)
{
	struct test_work *w = kmalloc(sizeof(struct test_work), gfp_mask);
	ASSERT(w);
	init_completion(&w->done);
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
	wq_normal_ = alloc_workqueue("test_serialize_normal", WQ_MEM_RECLAIM, 0);
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
	unsigned int n_tasks = 1000000;
	/* unsigned int n_tasks = 20; */
	
        init_workqueue();
	benchmark_single(n_tasks);
	benchmark_normal(n_tasks);
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
