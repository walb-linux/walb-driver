/**
 * wdev_util.h - walb device utiltity.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_DEV_UTIL_H_KERNEL
#define WALB_DEV_UTIL_H_KERNEL

#include "check_kernel.h"
#include "kern.h"

/*******************************************************************************
 * Data definition.
 *******************************************************************************/

/**
 * For (walb_dev *)->freeze_state.
 *
 * FRZ_MELTED -> FRZ_FREEZED
 * FRZ_MELTED -> FRZ_FREEZED_WITH_TIMEOUT
 * FRZ_FREEZED -> FRZ_FREEZED_WITH_TIMEOUT
 * FRZ_FREEZED -> FRZ_MELTED
 * FRZ_FREEZED_WITH_TIMEOUT -> FRZ_MELTED
 */
enum {
	FRZ_MELTED = 0,
	FRZ_FREEZED,
	FRZ_FREEZED_WITH_TIMEOUT,
};

/*******************************************************************************
 * Functions prototype.
 *******************************************************************************/

/* Logpack check function. */
int walb_check_lsid_valid(struct walb_dev *wdev, u64 lsid);

/* Utility functions for walb_dev. */
u64 get_oldest_lsid(struct walb_dev *wdev);
u64 get_written_lsid(struct walb_dev *wdev);
u64 get_permanent_lsid(struct walb_dev *wdev);
u64 get_completed_lsid(struct walb_dev *wdev);
int walb_set_name(struct walb_dev *wdev, unsigned int minor,
		const char *name);
void walb_decide_flush_support(struct walb_dev *wdev);
void walb_discard_support(struct walb_dev *wdev, bool support);
bool resize_disk(struct gendisk *gd, u64 new_size);
bool invalidate_lsid(struct walb_dev *wdev, u64 lsid);
void backup_lsid_set(struct walb_dev *wdev, struct lsid_set *lsids);
void restore_lsid_set(struct walb_dev *wdev, const struct lsid_set *lsids);
void task_melt(struct work_struct *work);
void cancel_melt_work(struct walb_dev *wdev);
bool freeze_if_melted(struct walb_dev *wdev, u32 timeout_sec);
bool melt_if_frozen(struct walb_dev *wdev, bool restarts_checkpointing);
void set_geometry(struct hd_geometry *geo, u64 n_sectors);
bool get_lsid_range_from_ctl(
	u64 *lsid0, u64 *lsid1, const struct walb_ctl *ctl);
void set_chunk_sectors(
	unsigned int *chunk_sectors, unsigned int pbs,
	const struct request_queue *q);
void print_queue_limits(
	const char *level, const char *msg,
	const struct queue_limits *limits);
u64 walb_get_log_usage(struct walb_dev *wdev);
u64 walb_get_log_capacity(struct walb_dev *wdev);

#endif /* WALB_DEV_UTIL_H_KERNEL */
