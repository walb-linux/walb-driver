/**
 * pack_work.c - pack_work implementation.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/sched.h>
#include "walb/common.h"
#include "pack_work.h"

/*******************************************************************************
 * Static variables definition.
 *******************************************************************************/

/**
 * kmem_cache for pack_work.
 */
#define KMEM_CACHE_PACK_WORK_NAME "pack_work_cache"
static struct kmem_cache *pack_work_cache_ = NULL;
static atomic_t n_users_ = ATOMIC_INIT(0);

/*******************************************************************************
 * Global function definitions.
 *******************************************************************************/

/**
 * Create a pack_work.
 *
 * RETURN:
 * NULL if failed.
 * CONTEXT:
 * Any.
 */
struct pack_work* create_pack_work(void *data, gfp_t gfp_mask)
{
	struct pack_work *pwork;

	ASSERT(pack_work_cache_);

	pwork = kmem_cache_alloc(pack_work_cache_, gfp_mask);
	if (!pwork) { return NULL; }
	pwork->data = data;
	return pwork;
}

/**
 * Destory a pack_work.
 */
void destroy_pack_work(struct pack_work *work)
{
	if (!work) { return; }
	kmem_cache_free(pack_work_cache_, work);
}

/**
 * Helper function for tasks.
 *
 * @data any data.
 * @nr flag bit number.
 * @flags_p pointer to flags data.
 * @wq workqueue.
 * @task task.
 *
 * RETURN:
 *   pack_work if really enqueued, or NULL.
 */
struct pack_work* enqueue_task_if_necessary(
	void *data, int nr, unsigned long *flags_p,
	struct workqueue_struct *wq, void (*task)(struct work_struct *))
{
	struct pack_work *pwork = NULL;
	int ret;

	ASSERT(task);
	ASSERT(wq);

retry:
	if (!test_and_set_bit(nr, flags_p)) {
		pwork = create_pack_work(data, GFP_NOIO);
		if (!pwork) {
			LOGn("memory allocation failed.\n");
			clear_bit(nr, flags_p);
			schedule();
			goto retry;
		}
		LOGd_("enqueue task for %d\n", nr);
		INIT_WORK(&pwork->work, task);
		ret = queue_work(wq, &pwork->work);
		if (!ret) {
			LOGe("work is already on the queue.\n");
		}
	}
	return pwork;
}

/**
 * Helper function for tasks.
 *
 * @data any data.
 * @nr flag bit number.
 * @flags_p pointer to flags data.
 * @wq workqueue.
 * @task task.
 * @delay delay [jiffies].
 *
 * RETURN:
 *   pack_work if really enqueued, or NULL.
 */
#if 0
struct pack_work* enqueue_delayed_task_if_necessary(
	void *data, int nr, unsigned long *flags_p,
	struct workqueue_struct *wq, void (*task)(struct work_struct *),
	unsigned int delay)
{
	struct pack_work *pwork;
	int ret;

	ASSERT(task);
	ASSERT(wq);

retry:
	if (!test_and_set_bit(nr, flags_p)) {
		pwork = create_pack_work(data, GFP_NOIO);
		if (!pwork) {
			LOGn("memory allocation failed.\n");
			clear_bit(nr, flags_p);
			schedule();
			goto retry;
		}
		LOGd_("enqueue delayed task for %d\n", nr);
		INIT_DELAYED_WORK(&pwork->dwork, task);
		ret = queue_delayed_work(wq, &pwork->dwork, delay);
		if (!ret) {
			LOGe("work is already on the queue.\n");
		}
	}
	return pwork;
}
#endif

/*******************************************************************************
 * Init/exit.
 *******************************************************************************/

bool pack_work_init(void)
{
	if (atomic_inc_return(&n_users_) == 1) {
		/* Prepare kmem_cache data. */
		pack_work_cache_ = kmem_cache_create(
			KMEM_CACHE_PACK_WORK_NAME,
			sizeof(struct pack_work), 0, 0, NULL);
		if (!pack_work_cache_) {
			atomic_dec(&n_users_);
			return false;
		}
	}
	return true;
}

void pack_work_exit(void)
{
	if (atomic_dec_return(&n_users_) == 0) {
		ASSERT(pack_work_cache_);
		kmem_cache_destroy(pack_work_cache_);
		pack_work_cache_ = NULL;
	}
}

MODULE_LICENSE("Dual BSD/GPL");
