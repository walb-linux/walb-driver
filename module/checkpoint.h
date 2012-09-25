/**
 * checkpoint.h - Checkpointing functions.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_CHECKPOINT_H_KERNEL
#define WALB_CHECKPOINT_H_KERNEL

#include "check_kernel.h"
#include "kern.h"

/*
 * Default checkpoint interval [ms]
 */
#define WALB_DEFAULT_CHECKPOINT_INTERVAL 10000
#define WALB_MAX_CHECKPOINT_INTERVAL (24 * 60 * 60 * 1000) /* 1 day */

/**
 * Checkpointing state.
 *
 * Permitted state transition:
 *   stoppped -> waiting @start_checkpionting()
 *   waiting -> running  @do_checkpointing()
 *   running -> waiting  @do_checkpointing()
 *   waiting -> stopped  @do_checkpointing()
 *   waiting -> stopping @stop_checkpointing()
 *   running -> stopping @stop_checkpointing()
 *   stopping -> stopped @stop_checkpointing()
 */
enum {
        CP_STOPPED = 0,
        CP_STOPPING,
        CP_WAITING,
        CP_RUNNING,
};

void init_checkpointing(struct walb_dev *wdev);
void task_do_checkpointing(struct work_struct *work);
void start_checkpointing(struct walb_dev *wdev);
void stop_checkpointing(struct walb_dev *wdev);
u32 get_checkpoint_interval(struct walb_dev *wdev);
void set_checkpoint_interval(struct walb_dev *wdev, u32 val);

#endif /* WALB_CHECKPOINT_H_KERNEL */
