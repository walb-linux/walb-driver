/**
 * pack_work.h - Definition for pack_work.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_PACK_WORK_H_KERNEL
#define WALB_PACK_WORK_H_KERNEL

#include "check_kernel.h"

/**
 * Pack work.
 */
struct pack_work
{
	struct work_struct work;
#if 0
	struct delayed_work dwork;
#endif
	void *data;
};

/* Pack work helper functions. */
struct pack_work* create_pack_work(void *data, gfp_t gfp_mask);
void destroy_pack_work(struct pack_work *work);

/* Helper function for an original queuing feature. */
struct pack_work* enqueue_task_if_necessary(
	void *data, int nr, unsigned long *flags,
	struct workqueue_struct *wq,
	void (*task)(struct work_struct *));
#if 0
struct pack_work* enqueue_delayed_task_if_necessary(
	void *data, int nr, unsigned long *flags,
	struct workqueue_struct *wq, void (*task)(struct work_struct *),
	unsigned int delay);
#endif

/* init/exit. */
bool pack_work_init(void);
void pack_work_exit(void);

#endif /* WALB_PACK_WORK_H_KERNEL */
