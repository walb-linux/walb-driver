/**
 * test_completion.c - test completion.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include <linux/completion.h>
#include <linux/delay.h>
#include <linux/workqueue.h>
#include "walb/common.h"
#include "build_date.h"

struct my_work
{
	unsigned int id;
	struct work_struct work;
	struct completion *done;
};

void task0(struct work_struct *work)
{
	struct my_work *mwork = container_of(work, struct my_work, work);
	ASSERT(mwork);

	LOGn("start\n");
	wait_for_completion(mwork->done);
	LOGn("end\n");
}

void task1(struct work_struct *work)
{
	struct my_work *mwork = container_of(work, struct my_work, work);
	ASSERT(mwork);

	LOGn("start\n");
	complete(mwork->done);
	LOGn("end\n");
}


static int __init test_init(void)
{
	struct completion done;
	struct my_work mwork0, mwork1;

        LOGn("BUILD_DATE %s\n", BUILD_DATE);
	
	init_completion(&done);
	mwork0.id = 0;
	mwork1.id = 1;
	mwork0.done = &done;
	mwork1.done = &done;
	INIT_WORK(&mwork0.work, task0);
	INIT_WORK(&mwork1.work, task1);
	
	queue_work(system_wq, &mwork0.work);
	mdelay(1);
	queue_work(system_wq, &mwork1.work);
	
	flush_workqueue(system_wq);
	
        return -1;
}

static void test_exit(void)
{
}

module_init(test_init);
module_exit(test_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Test of completion.");
MODULE_ALIAS("test_completion");
/* MODULE_ALIAS_BLOCKDEV_MAJOR(MEMBLK_MAJOR); */
