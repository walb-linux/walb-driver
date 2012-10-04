/**
 * chckpoint.c - Checkpointing functions.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"

#include <linux/module.h>
#include "super.h"
#include "checkpoint.h"

/**
 * Initialize checkpointing.
 */
void init_checkpointing(struct checkpoint_data *cpd, u64 written_lsid)
{
	ASSERT(cpd);
	ASSERT(written_lsid != INVALID_LSID);
	
        init_rwsem(&cpd->lock);
        cpd->interval = WALB_DEFAULT_CHECKPOINT_INTERVAL;
        cpd->state = CP_STOPPED;

	spin_lock_init(&cpd->written_lsid_lock);
	cpd->written_lsid = written_lsid;
	cpd->prev_written_lsid = written_lsid;
}

/**
 * Do checkpointing.
 */
void task_do_checkpointing(struct work_struct *work)
{
        unsigned long j0, j1;
        unsigned long interval;
        long delay, sync_time, next_delay;
        int ret;
        u64 written_lsid, prev_written_lsid;
        
        struct delayed_work *dwork =
                container_of(work, struct delayed_work, work);
        struct checkpoint_data *cpd =
                container_of(dwork, struct checkpoint_data, dwork);
	struct walb_dev *wdev =
		container_of(cpd, struct walb_dev, cpd);

        LOGd("called.\n");

        /* Get written_lsid and prev_written_lsid. */
        spin_lock(&cpd->written_lsid_lock);
        written_lsid = cpd->written_lsid;
        prev_written_lsid = cpd->prev_written_lsid;
        spin_unlock(&cpd->written_lsid_lock);

        /* Lock */
        down_write(&cpd->lock);
        interval = cpd->interval;

        ASSERT(interval > 0);
        switch (cpd->state) {
        case CP_STOPPING:
                LOGd("do_checkpointing should stop.\n");
                up_write(&cpd->lock);
                return;
        case CP_WAITING:
                cpd->state = CP_RUNNING;
                break;
        default:
                BUG();
        }
        up_write(&cpd->lock);

        /* Write superblock */
        j0 = jiffies;
        if (written_lsid == prev_written_lsid) {

                LOGd("skip superblock sync.\n");
        } else {
                if (walb_sync_super_block(wdev) != 0) {

                        atomic_set(&wdev->is_read_only, 1);
                        LOGe("superblock sync failed.\n");

                        down_write(&cpd->lock);
                        cpd->state = CP_STOPPED;
                        up_write(&cpd->lock);
                        return;
                }
        }
        j1 = jiffies;

        delay = msecs_to_jiffies(interval);
        sync_time = (long)(j1 - j0);
        next_delay = (long)delay - sync_time;

        LOGd("do_checkpinting: delay %ld sync_time %ld next_delay %ld\n",
                 delay, sync_time, next_delay);

        if (next_delay <= 0) {
                LOGw("Checkpoint interval is too small. "
                         "Should be more than %d.\n", jiffies_to_msecs(sync_time));
                next_delay = 1;
        }
        ASSERT(next_delay > 0);
        
        down_write(&cpd->lock);
        if (cpd->state == CP_RUNNING) {
                /* Register delayed work for next time */
                INIT_DELAYED_WORK(&cpd->dwork, task_do_checkpointing);
                ret = queue_delayed_work(wq_misc_, &cpd->dwork, next_delay);
                ASSERT(ret);
                cpd->state = CP_WAITING;
        } else {
                /* Do nothing */
                ASSERT(cpd->state == CP_STOPPING);
        }
        up_write(&cpd->lock);
}

/**
 * Start checkpointing.
 *
 * Do nothing if
 *   cpd->interval is 0.
 */
void start_checkpointing(struct checkpoint_data *cpd)
{
        unsigned long delay;
        unsigned long interval;

        down_write(&cpd->lock);
        if (cpd->state != CP_STOPPED) {
                LOGw("Checkpoint state is not stopped.\n");
                up_write(&cpd->lock);
                return;
        }

        interval = cpd->interval;
        if (interval == 0) { /* This is not error. */
                LOGn("checkpoint_interval is 0.\n");
                up_write(&cpd->lock);
                return;
        }
        ASSERT(interval > 0);
        
        delay = msecs_to_jiffies(interval);
        ASSERT(delay > 0);
        INIT_DELAYED_WORK(&cpd->dwork, task_do_checkpointing);

        queue_delayed_work(wq_misc_, &cpd->dwork, delay);
        cpd->state = CP_WAITING;
        LOGd("state change to CP_WAITING\n");
        up_write(&cpd->lock);
}

/**
 * Stop checkpointing.
 */
void stop_checkpointing(struct checkpoint_data *cpd)
{
        int ret;
        u8 state;

        down_write(&cpd->lock);
        state = cpd->state;
        if (state != CP_WAITING && state != CP_RUNNING) {
                LOGw("Checkpointing is not running.\n");
                up_write(&cpd->lock);
                return;
        }
        cpd->state = CP_STOPPING;
        LOGd("state change to CP_STOPPING\n");
        up_write(&cpd->lock);

        /* We must unlock before calling this to avoid deadlock. */
        ret = cancel_delayed_work_sync(&cpd->dwork);
        LOGd("cancel_delayed_work_sync: %d\n", ret);

        down_write(&cpd->lock);
        cpd->state = CP_STOPPED;
        LOGd("state change to CP_STOPPED\n");
        up_write(&cpd->lock);
}

/**
 * Get checkpoint interval
 *
 * @cpd checkpoint data.
 *
 * @return current checkpoint interval.
 */
u32 get_checkpoint_interval(struct checkpoint_data *cpd)
{
        u32 interval;
        
        down_read(&cpd->lock);
        interval = cpd->interval;
        up_read(&cpd->lock);

        return interval;
}

/**
 * Set checkpoint interval.
 *
 * @cpd checkpoint data.
 * @val new checkpoint interval.
 */
void set_checkpoint_interval(struct checkpoint_data *cpd, u32 interval)
{
        down_write(&cpd->lock);
        cpd->interval = interval;
        up_write(&cpd->lock);
        
        stop_checkpointing(cpd);
        start_checkpointing(cpd);
}

MODULE_LICENSE("Dual BSD/GPL");
