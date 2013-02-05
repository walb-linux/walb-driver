/**
 * simple_blk.h - Definition for simple_blk driver.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_SIMPLE_BLK_H_KERNEL
#define WALB_SIMPLE_BLK_H_KERNEL

#include "check_kernel.h"

#include <linux/blkdev.h>
#include <linux/genhd.h>
#include <linux/workqueue.h>

#include "walb/common.h"
#include "walb/disk_name.h"

/*******************************************************************************
 * Macros definition.
 *******************************************************************************/

#define SIMPLE_BLK_NAME "simple_blk"
#define SIMPLE_BLK_DIR_NAME "simple_blk"
/*
	sizeof("..")で想定した長さになってる?
*/
#define SIMPLE_BLK_DEV_NAME_MAX_LEN					\
	(DISK_NAME_LEN - sizeof(SIMPLE_BLK_DIR_NAME) - sizeof("/dev//"))

/*******************************************************************************
 * Data definition.
 *******************************************************************************/

/**
 * Memory block device.
 */
struct simple_blk_dev
{
	unsigned int minor; /* Minor device id. */
	u64 capacity; /* Device capacity [logical block] */

	/* name of the device. terminated by '\0'. */
	char name[SIMPLE_BLK_DEV_NAME_MAX_LEN];

	unsigned int pbs; /* physical block size. */

	spinlock_t lock; /* Lock data for this struct and queue if need. */
	struct request_queue *queue; /* request queue */
	bool use_make_request_fn; /* true if using sdev_register_with_bio(). */
	union {
		/* for bio. */
		make_request_fn *make_request_fn;
		/* for request. */
		request_fn_proc *request_fn_proc;
	};

	struct gendisk *gd; /* disk */
	unsigned long is_started; /* If started, bit 0 is on, or off. */

	void *private_data; /* You can use this for any purpose. */
};

/**
 * Workqueue type for IO.
 */
enum workqueue_type {
	WQ_TYPE_SINGLE, WQ_TYPE_UNBOUND, WQ_TYPE_NORMAL
};

/*******************************************************************************
 * Exported functions prototype.
 *******************************************************************************/

/* (Un)register a new block device. */
bool sdev_register_with_bio(
	unsigned int minor, u64 capacity, unsigned int pbs,
	make_request_fn *make_request_fn);
bool sdev_register_with_req(
	unsigned int minor, u64 capacity, unsigned int pbs,
	request_fn_proc *request_fn_proc);
bool sdev_unregister(unsigned int minor);

/* Start/stop a registered device. */
bool sdev_start(unsigned int minor);
bool sdev_stop(unsigned int minor);

/* Get a device. */
struct simple_blk_dev* sdev_get(unsigned minor);

/* Get a device from a queue. */
static inline struct simple_blk_dev* get_sdev_from_queue(struct request_queue *q)
{
	struct simple_blk_dev* sdev;

	ASSERT(q);
	sdev = (struct simple_blk_dev *)q->queuedata;
	return sdev;
}

/* Create a workqueue with a type. */
struct workqueue_struct* create_wq_io(
	const char *name, enum workqueue_type type);

#endif /* WALB_SIMPLE_BLK_H_KERNEL */
