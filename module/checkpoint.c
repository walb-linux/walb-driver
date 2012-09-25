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
void init_checkpointing(struct walb_dev *wdev)
{
	ASSERT(wdev);
        init_rwsem(&wdev->checkpoint_lock);
        wdev->checkpoint_interval = WALB_DEFAULT_CHECKPOINT_INTERVAL;
        wdev->checkpoint_state = CP_STOPPED;
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
        struct walb_dev *wdev =
                container_of(dwork, struct walb_dev, checkpoint_work);

        LOGd("do_checkpointing called.\n");

        /* Get written_lsid and prev_written_lsid. */
        spin_lock(&wdev->datapack_list_lock);
        written_lsid = wdev->written_lsid;
        prev_written_lsid = wdev->prev_written_lsid;
        spin_unlock(&wdev->datapack_list_lock);

        /* Lock */
        down_write(&wdev->checkpoint_lock);
        interval = wdev->checkpoint_interval;

        ASSERT(interval > 0);
        switch (wdev->checkpoint_state) {
        case CP_STOPPING:
                LOGd("do_checkpointing should stop.\n");
                up_write(&wdev->checkpoint_lock);
                return;
        case CP_WAITING:
                wdev->checkpoint_state = CP_RUNNING;
                break;
        default:
                BUG();
        }
        up_write(&wdev->checkpoint_lock);

        /* Write superblock */
        j0 = jiffies;
        if (written_lsid == prev_written_lsid) {

                LOGd("skip superblock sync.\n");
        } else {
                if (walb_sync_super_block(wdev) != 0) {

                        atomic_set(&wdev->is_read_only, 1);
                        LOGe("superblock sync failed.\n");

                        down_write(&wdev->checkpoint_lock);
                        wdev->checkpoint_state = CP_STOPPED;
                        up_write(&wdev->checkpoint_lock);
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
        
        down_write(&wdev->checkpoint_lock);
        if (wdev->checkpoint_state == CP_RUNNING) {
                /* Register delayed work for next time */
                INIT_DELAYED_WORK(&wdev->checkpoint_work, task_do_checkpointing);
                ret = queue_delayed_work(wq_misc_, &wdev->checkpoint_work, next_delay);
                ASSERT(ret);
                wdev->checkpoint_state = CP_WAITING;
        } else {
                /* Do nothing */
                ASSERT(wdev->checkpoint_state == CP_STOPPING);
        }
        up_write(&wdev->checkpoint_lock);
}

/**
 * Start checkpointing.
 *
 * Do nothing if
 *   wdev->is_checkpoint_running is 1 or
 *   wdev->checkpoint_interval is 0.
 */
void start_checkpointing(struct walb_dev *wdev)
{
        unsigned long delay;
        unsigned long interval;

        down_write(&wdev->checkpoint_lock);
        if (wdev->checkpoint_state != CP_STOPPED) {
                LOGw("Checkpoint state is not stopped.\n");
                up_write(&wdev->checkpoint_lock);
                return;
        }

        interval = wdev->checkpoint_interval;
        if (interval == 0) { /* This is not error. */
                LOGn("checkpoint_interval is 0.\n");
                up_write(&wdev->checkpoint_lock);
                return;
        }
        ASSERT(interval > 0);
        
        delay = msecs_to_jiffies(interval);
        ASSERT(delay > 0);
        INIT_DELAYED_WORK(&wdev->checkpoint_work, task_do_checkpointing);

        queue_delayed_work(wq_misc_, &wdev->checkpoint_work, delay);
        wdev->checkpoint_state = CP_WAITING;
        LOGd("state change to CP_WAITING\n");
        up_write(&wdev->checkpoint_lock);
}

/**
 * Stop checkpointing.
 *
 * Do nothing if 
 *   wdev->is_checkpoint_running is not 1 or
 *   wdev->should_checkpoint_stop is not 0.
 */
void stop_checkpointing(struct walb_dev *wdev)
{
        int ret;
        u8 state;

        down_write(&wdev->checkpoint_lock);
        state = wdev->checkpoint_state;
        if (state != CP_WAITING && state != CP_RUNNING) {
                LOGw("Checkpointing is not running.\n");
                up_write(&wdev->checkpoint_lock);
                return;
        }
        wdev->checkpoint_state = CP_STOPPING;
        LOGd("state change to CP_STOPPING\n");
        up_write(&wdev->checkpoint_lock);

        /* We must unlock before calling this to avoid deadlock. */
        ret = cancel_delayed_work_sync(&wdev->checkpoint_work);
        LOGd("cancel_delayed_work_sync: %d\n", ret);

        down_write(&wdev->checkpoint_lock);
        wdev->checkpoint_state = CP_STOPPED;
        LOGd("state change to CP_STOPPED\n");
        up_write(&wdev->checkpoint_lock);
}

/**
 * Get checkpoint interval
 *
 * @wdev walb device.
 *
 * @return current checkpoint interval.
 */
u32 get_checkpoint_interval(struct walb_dev *wdev)
{
        u32 interval;
        
        down_read(&wdev->checkpoint_lock);
        interval = wdev->checkpoint_interval;
        up_read(&wdev->checkpoint_lock);

        return interval;
}

/**
 * Set checkpoint interval.
 *
 * @wdev walb device.
 * @val new checkpoint interval.
 */
void set_checkpoint_interval(struct walb_dev *wdev, u32 val)
{
        down_write(&wdev->checkpoint_lock);
        wdev->checkpoint_interval = val;
        up_write(&wdev->checkpoint_lock);
        
        stop_checkpointing(wdev);
        start_checkpointing(wdev);
}

MODULE_LICENSE("Dual BSD/GPL");
