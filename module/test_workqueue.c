/**
 * test_workqueue.c - test workqueue behavior.
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
#include <linux/sched.h>
#include <linux/completion.h>
#include <linux/spinlock.h>
#include <linux/mutex.h>

#include "walb/common.h"
#include "walb/util.h"


/**
 * Variables/constants for workqueues.
 */
#define N_WQ 4
struct workqueue_struct *wq_[N_WQ];
#define WQ_NAME_PREFIX "test_workqueue_"
char *wq_name_[N_WQ];

struct workqueue_struct *wq_single_;
#define WQ_NAME_SINGLE "test_workqueue_single"

struct workqueue_struct *wq_unbound_;
#define WQ_NAME_UNBOUND "test_workqueue_unbound"

/**
 * A wrapper of work.
 */
struct test_work
{
	struct work_struct work;
	unsigned int msec_sleep; /* milliseconds to sleep */

	struct timespec bgn_ts; /* begin */
	struct timespec enq_ts; /* enqueue */
	struct timespec deq_ts; /* dequeue */
	struct timespec end_ts; /* end */

	unsigned int id;
};

typedef void (test_work_task_fn)(struct work_struct *work);


/**
 * Simply tail-recursion task.
 */
struct tail_recur_work
{
	int i;
	struct work_struct work;
	struct completion done;
};

/**
 * Test work for shared mutex.
 */
struct test_work_mutex
{
	struct work_struct work;
	struct mutex *mutex;
	unsigned int id;
	unsigned int n_trial;
};

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

static void test_work_task_detail(struct work_struct *work, int id);
static void test_work_task_0(struct work_struct *work);
static void test_work_task_1(struct work_struct *work);
static void test_work_task_2(struct work_struct *work);
static void test_work_task_3(struct work_struct *work);
static void create_and_enqueue_task(
	unsigned int wq_id,
	test_work_task_fn *test_work_task_fn,
	unsigned int msec_sleep);

static struct test_work* create_test_work(gfp_t gfp_mask);
static void destroy_test_work(struct test_work *test_work);

static void init_workqueue(void);
static void fin_workqueue(void);

static struct tail_recur_work* create_tail_recur_work(int i);
static void destroy_tail_recur_work(struct tail_recur_work* work);
static void tail_recur_task(struct work_struct *work);

static void test_recursive_enqueue(void);
static void test_workqueue(void);

static void test_mutex_task(struct work_struct *work);
static void test_mutex(void);

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

static void test_work_task_detail(struct work_struct *work, int id)
{
	UNUSED struct timespec delta[3];
	struct test_work* test_work = container_of(work, struct test_work, work);

	getnstimeofday(&test_work->deq_ts);

	/* Sleep */
	msleep_interruptible(test_work->msec_sleep);

	getnstimeofday(&test_work->end_ts);
	
	delta[0] = timespec_sub(test_work->enq_ts, test_work->bgn_ts);
	delta[1] = timespec_sub(test_work->deq_ts, test_work->enq_ts);
	delta[2] = timespec_sub(test_work->end_ts, test_work->deq_ts);

	LOGd("test_work_task_%d: %ld.%09ld --(%ld.%09ld)-- enq --(%ld.%09ld)-- deq --(%ld.%09ld)-- end\n",
		id,
		test_work->bgn_ts.tv_sec,
		test_work->bgn_ts.tv_nsec,
		delta[0].tv_sec, delta[0].tv_nsec,
		delta[1].tv_sec, delta[1].tv_nsec,
		delta[2].tv_sec, delta[2].tv_nsec);
		
	destroy_test_work(test_work);
}

static void test_work_task_0(struct work_struct *work)
{
	test_work_task_detail(work, 0);
}

static void test_work_task_1(struct work_struct *work)
{
	test_work_task_detail(work, 1);
}

static void test_work_task_2(struct work_struct *work)
{
	test_work_task_detail(work, 2);
}

static void test_work_task_3(struct work_struct *work)
{
	test_work_task_detail(work, 3);
}


static void create_and_enqueue_task(
	unsigned int wq_id,
	test_work_task_fn *test_work_task_fn,
	unsigned int msec_sleep)
{
	struct test_work* test_work;
	
	test_work = create_test_work(GFP_KERNEL);
	ASSERT(test_work);
	test_work->msec_sleep = msec_sleep;
	getnstimeofday(&test_work->bgn_ts);
	INIT_WORK(&test_work->work, test_work_task_fn);
	queue_work(wq_[wq_id], &test_work->work);
	getnstimeofday(&test_work->enq_ts);
}


static struct test_work* create_test_work(gfp_t gfp_mask)
{
	return (struct test_work *)MALLOC(sizeof(struct test_work), gfp_mask);
}

static void destroy_test_work(struct test_work *test_work)
{
	FREE(test_work);
}


static void init_workqueue(void)
{
	int i;
	
	for (i = 0; i < N_WQ; i ++) {
		wq_name_[i] = (char *)MALLOC(64, GFP_KERNEL);
		ASSERT(wq_name_[i]);
		snprintf(wq_name_[i], 64, WQ_NAME_PREFIX "%d", i);
#if 0
		wq_[i] = create_workqueue(wq_name_[i]);
#elif 0
		wq_[i] = create_singlethread_workqueue(wq_name_[i]);
#else
		wq_[i] = alloc_workqueue(wq_name_[i], WQ_MEM_RECLAIM, 0);
#endif
		ASSERT(wq_[i]);
	}


	/* (1) is slower than (2). */
#if 0
	/* (1) */
	wq_single_ = alloc_workqueue(WQ_NAME_SINGLE, WQ_MEM_RECLAIM |WQ_NON_REENTRANT, 1);
#else
	/* (2) */
	wq_single_ = alloc_workqueue(WQ_NAME_SINGLE, WQ_MEM_RECLAIM |WQ_UNBOUND, 1);
#endif
	ASSERT(wq_single_);

	wq_unbound_ = alloc_workqueue(WQ_NAME_UNBOUND, WQ_MEM_RECLAIM | WQ_UNBOUND, 32);
	ASSERT(wq_unbound_);
}

static void fin_workqueue(void)
{
	int i;
	
	for (i = 0; i < N_WQ; i ++) {
		flush_workqueue(wq_[i]);
		destroy_workqueue(wq_[i]);
		wq_[i] = NULL;
		FREE(wq_name_[i]);
		wq_name_[i] = NULL;
	}
	flush_workqueue(wq_single_);
	destroy_workqueue(wq_single_);
	flush_workqueue(wq_unbound_);
	destroy_workqueue(wq_unbound_);
}

static struct tail_recur_work* create_tail_recur_work(int i)
{
	struct tail_recur_work *work;
	work = kmalloc(sizeof(struct tail_recur_work), GFP_KERNEL);
	if (!work) {
		goto end;
	}
	work->i = i;
	INIT_WORK(&work->work, tail_recur_task);
	init_completion(&work->done);
end:
	return work;
}

static void destroy_tail_recur_work(struct tail_recur_work* work)
{
	if (work) {
		kfree(work);
	}
}

static void tail_recur_task(struct work_struct *work)
{
	struct tail_recur_work *trwork =
		container_of(work, struct tail_recur_work, work);

	LOGn("i: %d\n", trwork->i);
	if (trwork->i > 0) {
		trwork->i --;
		INIT_WORK(&trwork->work, tail_recur_task);
		queue_work(wq_[0], &trwork->work);
	} else {
		complete(&trwork->done);
		LOGn("tail recursion done.\n");
	}
}

/**
 * Recursive enqueue test.
 */
static void test_recursive_enqueue(void)
{
	struct tail_recur_work *work;
	LOGn("begin.\n");
	work = create_tail_recur_work(100);
	if (!work) {
		LOGe("create_tail_recur_work() failed.\n");
		return;
	}
	queue_work(wq_[0], &work->work);
	wait_for_completion(&work->done);
	destroy_tail_recur_work(work);
	LOGn("flush_workqueue done.\n");
}

static void test_wq_single_task(struct work_struct *work)
{
	struct test_work *w =
		container_of(work, struct test_work, work);
	/* struct timespec ts; */

	/* getnstimeofday(&ts); */

	/* LOGn("%u BEGIN\n", w->id); */
	/* msleep_interruptible(w->msec_sleep); */
	/* LOGn("%u END\n", w->id); */

	destroy_test_work(w);
	
	/* LOGn("test_wq_single_task timestamp %ld.%09ld\n", */
	/*	ts.tv_sec, ts.tv_nsec); */
}

/**
 * NON_REENTRANT flag test.
 */
static void test_wq_single(void)
{
	const unsigned int N_TRIAL = 1000000;
	struct test_work *w;
	unsigned int i;

	struct timespec bgn_ts, end_ts, sub_ts;

	getnstimeofday(&bgn_ts);
	for (i = 0; i < N_TRIAL; i ++) {
		w = create_test_work(GFP_KERNEL);
		ASSERT(w);
		w->msec_sleep = 100;
		w->id = i;
		INIT_WORK(&w->work, test_wq_single_task);
		queue_work(wq_single_, &w->work);
	}
	flush_workqueue(wq_single_);
	getnstimeofday(&end_ts);

	sub_ts = timespec_sub(end_ts, bgn_ts);
	LOGn("test_wq_single: %ld.%09ld", sub_ts.tv_sec, sub_ts.tv_nsec);
}

/**
 * Spinlock overhead.
 */
static void test_spinlock(void)
{
	const unsigned int N_TRIAL = 1000000;
	spinlock_t lock;
	int i;
	struct timespec bgn_ts, end_ts, sub_ts;
	
	spin_lock_init(&lock);
	getnstimeofday(&bgn_ts);
	for (i = 0; i < N_TRIAL; i ++) {
		spin_lock(&lock);
		spin_unlock(&lock);
	}
	getnstimeofday(&end_ts);
	
	sub_ts = timespec_sub(end_ts, bgn_ts);
	LOGn("test_spinlock: %ld.%09ld\n", sub_ts.tv_sec, sub_ts.tv_nsec);
}

/**
 * Task for test_mutex().
 */
static void test_mutex_task(struct work_struct *work)
{
	int i;
	struct test_work_mutex *twork =
		container_of(work, struct test_work_mutex, work);
	struct timespec bgn_ts, end_ts, sub_ts;
	unsigned long period = 0;
	ASSERT(twork);

	LOGn("start id %u processor %u\n", twork->id, get_cpu());
	put_cpu();
	for (i = 0; i < twork->n_trial; i++) {
		mutex_lock(twork->mutex);
		getnstimeofday(&bgn_ts);
#if 0
		udelay(4000);
		msleep(4);
#else
		schedule();
#endif
		getnstimeofday(&end_ts);
		sub_ts = timespec_sub(end_ts, bgn_ts);
		period += sub_ts.tv_nsec / 1000;
		mutex_unlock(twork->mutex);
		msleep(8);
		
	}
	LOGn("end id %u\n", twork->id);
	LOGn("critial section takes %lu us (average)\n", period / twork->n_trial);
}

/**
 * Mutex overhead.
 */
static void test_mutex(void)
{
	const unsigned int N_TASK = 8;
	struct mutex mutex;
	struct test_work_mutex twork[N_TASK];
	int i;
	struct timespec bgn_ts, end_ts, sub_ts;

	mutex_init(&mutex);
	getnstimeofday(&bgn_ts);
	for (i = 0; i < N_TASK; i++) {
		INIT_WORK(&twork[i].work, test_mutex_task);
		twork[i].mutex = &mutex;
		twork[i].n_trial = 250;
		twork[i].id = i;
		queue_work(wq_[0], &twork[i].work);
	}
	flush_workqueue(wq_[0]);
	getnstimeofday(&end_ts);

	sub_ts = timespec_sub(end_ts, bgn_ts);
	LOGn("test_mutex: %ld.%09ld sec.\n", sub_ts.tv_sec, sub_ts.tv_nsec);
}

static void test_workqueue(void)
{
	/* Test1 */
	create_and_enqueue_task(0, test_work_task_0, 100);
	create_and_enqueue_task(0, test_work_task_0, 100);
	create_and_enqueue_task(0, test_work_task_0, 100);
	create_and_enqueue_task(0, test_work_task_0, 100);
	flush_workqueue(wq_[0]);

	/* Test2 */
	create_and_enqueue_task(0, test_work_task_0, 100);
	create_and_enqueue_task(1, test_work_task_0, 100);
	create_and_enqueue_task(2, test_work_task_0, 100);
	create_and_enqueue_task(3, test_work_task_0, 100);
	flush_workqueue(wq_[0]);
	flush_workqueue(wq_[1]);
	flush_workqueue(wq_[2]);
	flush_workqueue(wq_[3]);

	/* Test3 */
	create_and_enqueue_task(0, test_work_task_0, 100);
	create_and_enqueue_task(0, test_work_task_1, 100);
	create_and_enqueue_task(0, test_work_task_2, 100);
	create_and_enqueue_task(0, test_work_task_3, 100);
	flush_workqueue(wq_[0]);

	/* Test4 */
	create_and_enqueue_task(0, test_work_task_0, 100);
	create_and_enqueue_task(1, test_work_task_0, 100);
	create_and_enqueue_task(0, test_work_task_1, 100);
	create_and_enqueue_task(1, test_work_task_1, 100);
	flush_workqueue(wq_[0]);
	flush_workqueue(wq_[1]);

	/* Test5 */
	test_recursive_enqueue();

	/* Test6 */
	test_wq_single();

	/* test7 */
	test_spinlock();

	/* test8 */
	test_mutex();
}

static int __init test_init(void)
{
	init_workqueue();
	test_workqueue();
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
MODULE_ALIAS("test_workqueue");
