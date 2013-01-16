/**
 * req_entry.h - Definition for req_entry.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_REQ_ENTRY_H_KERNEL
#define WALB_REQ_ENTRY_H_KERNEL

#include "check_kernel.h"
#include <linux/types.h>
#include <linux/blkdev.h>
#include <linux/list.h>
#include <linux/completion.h>
#include "bio_entry.h"
#include "walb/common.h"

/**
 * Request entry struct.
 */
struct req_entry
{
	struct list_head list; /* list entry */

	/**
	 * These are used for workqueue.
	 */
	struct work_struct work;
	void *data;

	/**
	 * Target request and related bio_entry list for data device.
	 */
	struct request *req;
	struct list_head bio_ent_list; /* list head of bio_entry */

	/* Notification from write_req_task to gc_task.
	   read_req_task does not use this. */
	struct completion done;

#ifdef WALB_OVERLAPPED_SERIALIZE
	struct completion overlapped_done;
	int n_overlapped; /* initial value is -1. */
#endif

	u64 req_pos; /* request address [logical block] */
	unsigned int req_sectors; /* request size [logical block] */
};

/* print for debug. */
UNUSED void print_req_entry(const char *level, struct req_entry *reqe);

/* req_entry related. */
struct req_entry* create_req_entry(
	struct request *req, void *data, gfp_t gfp_mask);
void destroy_req_entry(struct req_entry *reqe);
UNUSED void req_entry_get(struct req_entry *reqe);
UNUSED void req_entry_put(struct req_entry *reqe);

#ifdef WALB_FAST_ALGORITHM
void get_overlapped_pos_and_sectors(
	struct req_entry *reqe0,  struct req_entry *reqe1,
	u64 *ol_req_pos_p, unsigned int *ol_req_sectors_p);
bool data_copy_req_entry(
	struct req_entry *dst_reqe,  struct req_entry *src_reqe,
	gfp_t gfp_mask);
#endif

/* init/exit */
bool req_entry_init(void);
void req_entry_exit(void);

#endif /* WALB_REQ_ENTRY_H_KERNEL */
