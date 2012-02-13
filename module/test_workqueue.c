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

#include "walb/common.h"
#include "walb/util.h"


/**
 * Variables/constants for workqueues.
 */
#define N_WQ 4
struct workqueue_struct *wq_[N_WQ];
#define WQ_NAME_PREFIX "test_workqueue_"
char *wq_name_[N_WQ];


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
};

typedef void (test_work_task_fn)(struct work_struct *work);

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

static void test_workqueue(void);


/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

static void test_work_task_detail(struct work_struct *work, int id)
{
        __UNUSED struct timespec delta[3];
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
