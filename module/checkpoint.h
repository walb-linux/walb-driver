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
 *   waiting -> running	 @task_do_checkpointing()
 *   running -> waiting	 @task_do_checkpointing()
 *   waiting -> stopped	 @task_do_checkpointing()
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

/**
 * For checkpointing.
 */
struct checkpoint_data
{
	/*
	 * checkpoint_lock is used to access 
	 *   checkpoint_interval,
	 *   checkpoint_state.
	 */
	struct rw_semaphore lock;

	/*
	 * Checkpointing interval [ms].
	 * 0 means the device does not do checkpointing.
	 */
	u32 interval;

	/*
	 * CP_XXX
	 */
	u8 state;

	/*
	 * checkpoint_work accesses are automatically
	 * serialized by checkpoint_state.
	 */
	struct delayed_work dwork;

	/*
	 * Checkpointing updates written_lsid
	 * where log and data has been already persistent
	 * for all lsid < written_lsid.
	 *
	 * Checkpointing functionality will not sync superblock
	 * when written_lsid == prev_written_lsid
	 * that means any write IO did not occur from previous checkpointing.
	 */
	spinlock_t written_lsid_lock;
	u64 written_lsid;
	u64 prev_written_lsid;
};

void init_checkpointing(struct checkpoint_data *cpd, u64 written_lsid);
bool take_checkpoint(struct checkpoint_data *cpd);
void task_do_checkpointing(struct work_struct *work);
void start_checkpointing(struct checkpoint_data *cpd);
void stop_checkpointing(struct checkpoint_data *cpd);
u32 get_checkpoint_interval(struct checkpoint_data *cpd);
void set_checkpoint_interval(struct checkpoint_data *cpd, u32 val);

#endif /* WALB_CHECKPOINT_H_KERNEL */
