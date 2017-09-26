/**
 * test_deadlock.c - Test deadlock.
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
#include "linux/walb/util.h"
#include "linux/walb/logger.h"
#include "build_date.h"

DEFINE_SPINLOCK(lock1);
DEFINE_SPINLOCK(lock2);

int thread2_flag;

static int spinlock_stall_thread1(void *data)
{
	spin_lock(&lock1);
	while (1) {
		if (thread2_flag == 1) break;
		mdelay(200);
	}
	spin_lock(&lock2);
	return 0;
}

static int spinlock_stall_thread2(void *data)
{
	spin_lock(&lock2);
	thread2_flag = 1;
	spin_lock(&lock1);
	return 0;
}


static int __init test_init(void)
{
	struct task_struct *kthread1;
	struct task_struct *kthread2;

	LOGe("BUILD_DATE %s\n", BUILD_DATE);

	spin_lock_init(&lock1);
	spin_lock_init(&lock2);

	kthread1 = kthread_run(spinlock_stall_thread1, NULL, "spinlock1");
	kthread2 = kthread_run(spinlock_stall_thread2, NULL, "spinlock2");

	return 0;
}

static void test_exit(void)
{
}

module_init(test_init);
module_exit(test_exit);
MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("test of deadlock");
MODULE_ALIAS("test_deadlock");
