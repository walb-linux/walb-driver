/**
 * test_memblk_data_perf.c - Test performance potential of memblk_data.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>

#include "memblk_data.h"

#include <linux/mm.h>
#include <linux/kthread.h>
#include <linux/wait.h>
#include <linux/time.h>
#include <linux/delay.h>
#include "walb/util.h"
#include "build_date.h"
#include "util.h"

enum io_mode
{
        IO_READ, IO_WRITE, IO_RW, IO_MAX,
};

#define THREAD_DONE 0
#define THREAD_WORKING 1

struct thread_data
{
        unsigned int id;
        struct task_struct *tsk; /* kthread task_struct */
        wait_queue_head_t wait_q; /* Wait queue */
        unsigned long timeout_ms; /* Timeout */
        unsigned long flags;

        enum io_mode mode;
        unsigned int n_io; /* Number of IO to execute. */
};



#define BLOCK_SIZE 512
struct memblk_data *mdata_ = NULL;
#define MAX_N_THREADS 16
u8 *buffer_[MAX_N_THREADS];
struct thread_data tdata_[MAX_N_THREADS];


static void create_test_data(u64 capacity)
{
        unsigned int i;
        ASSERT(capacity > 0);
        mdata_ = mdata_create(capacity, BLOCK_SIZE, GFP_KERNEL);
        ASSERT(mdata_);

        for (i = 0; i < MAX_N_THREADS; i ++) {
                buffer_[i] = (u8 *)__get_free_page(GFP_KERNEL);
                ASSERT(buffer_[i]);
        }
}

static void destroy_test_data(void)
{
        unsigned int i;

        for (i = 0; i < MAX_N_THREADS; i ++) {
                ASSERT(buffer_[i]);
                free_page((unsigned long)buffer_[i]);
        }
        
        ASSERT(mdata_);
        mdata_destroy(mdata_);
        mdata_ = NULL;
}


void random_io(unsigned int id, enum io_mode mode)
{
        u32 addr;
        bool is_write = false;
        
        ASSERT((u32)mdata_->capacity < (u32)UINT_MAX);
        addr = get_random_u32_max((u32)mdata_->capacity);

        switch (mode) {
        case IO_READ:
                is_write = true;
                break;
        case IO_WRITE:
                is_write = false;
                break;
        case IO_RW:
                is_write = (get_random_u32_max(2) == 0);
                break;
        default:
                BUG();
        }

        if (is_write) {
#if 0
                LOGd("ID %u W %"PRIu32".\n", id, addr);
#endif
                mdata_write_block(mdata_, (u64)addr, buffer_[id]);
        } else {
#if 0
                LOGd("ID %u R %"PRIu32".\n", id, addr);
#endif
                mdata_read_block(mdata_, (u64)addr, buffer_[id]);
        }
}



int worker(void *data) 
{
        struct thread_data *tdata = (struct thread_data *)data;
        unsigned int i;

        LOGd("worker %u start.\n", tdata->id);
        
        set_bit(THREAD_WORKING, &tdata->flags);
        for (i = 0; i < tdata->n_io; i ++) {
                random_io(tdata->id, tdata->mode);
        };
        set_bit(THREAD_DONE, &tdata->flags);
        wake_up_interruptible(&tdata->wait_q);
        LOGd("worker %u stop.\n", tdata->id);

        return 0;
}

static void run_benchmark(unsigned int n_threads, unsigned int n_io, enum io_mode mode)
{
        unsigned int i;
        struct timespec ts_bgn, ts_end, ts_time;

        LOGd("run_benchmark begin.\n");
        ASSERT(n_threads > 0);

        getnstimeofday(&ts_bgn);

        for (i = 0; i < n_threads; i ++) {
                tdata_[i].id = i;
                clear_bit(THREAD_DONE, &tdata_[i].flags);
                clear_bit(THREAD_WORKING, &tdata_[i].flags);
                init_waitqueue_head(&tdata_[i].wait_q);
                tdata_[i].timeout_ms = MAX_SCHEDULE_TIMEOUT;
                tdata_[i].n_io = n_io / n_threads;
                tdata_[i].mode = mode;
                LOGd("tdata id %u flags %lu wait_q %p timeout %lu.\n",
                     tdata_[i].id, tdata_[i].flags, &tdata_[i].wait_q, tdata_[i].timeout_ms);
                tdata_[i].tsk = kthread_run(worker, &tdata_[i], "test_worker%u", i);
                ASSERT(tdata_[i].tsk);
        }

        for (i = 0; i < n_threads; i ++) {

                while (!test_bit(THREAD_WORKING, &tdata_[i].flags));
                wait_event_interruptible(
                        tdata_[i].wait_q,
                        test_bit(THREAD_DONE, &tdata_[i].flags));

                /* kthread_stop(&tdata_[i].tsk); */
                LOGd("thread %u done.\n", tdata_[i].id);
        }

        getnstimeofday(&ts_end);

        ts_time = timespec_sub(ts_end, ts_bgn);
        LOGd("run_benchmark end.\n");
        LOGi("MODE: %d N_THREADS: %3u ELAPSED_TIME: %ld.%09ld\n",
             mode, n_threads, ts_time.tv_sec, ts_time.tv_nsec);
}

static int __init test_init(void)
{
        int i, j;
        LOGe("BUILD_DATE %s\n", BUILD_DATE);

	mdata_init();
        create_test_data(1048576);
        for (j = 0; j < 5; j ++) {
                for (i = 1; i <= 8; i ++) {
                        run_benchmark(i,  1000000, IO_READ);
                }
                for (i = 1; i <= 8; i ++) {
                        run_benchmark(i,  1000000, IO_WRITE);
                }
                for (i = 1; i <= 8; i ++) {
                        run_benchmark(i,  1000000, IO_RW);
                }
        }
        destroy_test_data();
	mdata_exit();
        
        return -1;
}

static void test_exit(void)
{
}

module_init(test_init);
module_exit(test_exit);
MODULE_LICENSE("Dual BSD/GPL");
MODULE_DESCRIPTION("Performance test of memblk_data.");
MODULE_ALIAS("test_memblk_data_perf");
/* MODULE_ALIAS_BLOCKDEV_MAJOR(MEMBLK_MAJOR); */
