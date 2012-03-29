/**
 * simple_blk_req_mem_barrier.c -
 * request_fn which do memory read/write with barriers.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"
#include <linux/blkdev.h>
#include <linux/workqueue.h>
#include <linux/slab.h>
#include <linux/time.h>
#include <linux/timer.h>
#include <linux/delay.h>
#include "simple_blk_req.h"
#include "memblk_data.h"


/* #define PERFORMANCE_DEBUG */

#define WQ_IO_NAME "simple_blk_req_mem_barrier_io"

/*******************************************************************************
 * Global data definition.
 *******************************************************************************/

/* module parameters. */
extern int sleep_ms_;

/*******************************************************************************
 * Static data definition.
 *******************************************************************************/

/* for debug */
static atomic_t id_counter_ = ATOMIC_INIT(0);

struct req_work
{
        struct request *req;
        struct simple_blk_dev *sdev;
        struct work_struct work;
        /* struct delayed_work dwork; */
        /* struct timer_list end_timer; */
        unsigned int id;
        
#ifdef PERFORMANCE_DEBUG
        struct timespec ts_start;
        struct timespec ts_enq1;
        struct timespec ts_deq1;
        struct timespec ts_end;
#endif
};

struct kmem_cache *req_work_cache_ = NULL;
struct workqueue_struct *wq_io_ = NULL;

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

static void log_bi_rw_flag(struct bio *bio);

static void mdata_exec_discard(struct memblk_data *mdata, u64 block_id, unsigned int n_blocks);
static void mdata_exec_req(struct memblk_data *mdata, struct request *req);
static void mdata_exec_req_cur(struct memblk_data *mdata, struct request *req);

static struct memblk_data* get_mdata_from_sdev(struct simple_blk_dev *sdev);
__UNUSED static struct memblk_data* get_mdata_from_queue(struct request_queue *q);

static struct req_work* create_req_work(
        struct request *req, struct simple_blk_dev *sdev, gfp_t gfp_mask,
        void (*worker)(struct work_struct *work));
static void destroy_req_work(struct req_work *work);

static bool mdata_exec_req_special(struct memblk_data *mdata, struct request *req);


/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * For debug.
 */
static void log_bi_rw_flag(struct bio *bio)
{
        LOGd("bio bi_sector %"PRIu64" %0lx bi_size %u bi_vcnt %hu "
             "bi_rw %0lx [%s][%s][%s][%s][%s][%s].\n",
             (u64)bio->bi_sector, bio->bi_sector, bio->bi_size, bio->bi_vcnt,
             bio->bi_rw, 
             (bio->bi_rw & REQ_WRITE ? "REQ_WRITE" : ""),
             (bio->bi_rw & REQ_RAHEAD? "REQ_RAHEAD" : ""),
             (bio->bi_rw & REQ_FLUSH ? "REQ_FLUSH" : ""),
             (bio->bi_rw & REQ_FUA ? "REQ_FUA" : ""),
             (bio->bi_rw & REQ_DISCARD ? "REQ_DISCARD" : ""),
             (bio->bi_rw & REQ_SECURE ? "REQ_SECURE" : ""));
}

/**
 * Currently discard just fills zero.
 */
static void mdata_exec_discard(struct memblk_data *mdata, u64 block_id, unsigned int n_blocks)
{
        unsigned int i;
        for (i = 0; i < n_blocks; i ++) {
                memset(mdata_get_block(mdata, block_id + i), 0, mdata->block_size);
        }
}

/**
 * Get mdata from a sdev.
 */
static struct memblk_data* get_mdata_from_sdev(struct simple_blk_dev *sdev)
{
        ASSERT(sdev);
        return (struct memblk_data *)sdev->private_data;
}

/**
 * Get mdata from a queue.
 */
static struct memblk_data* get_mdata_from_queue(struct request_queue *q)
{
        return get_mdata_from_sdev(sdev_get_from_queue(q));
}


/**
 * Create a req_work.
 *
 * @req started request.
 *
 * RETURN:
 * NULL if failed.
 * CONTEXT:
 * Any.
 */
static struct req_work* create_req_work(
        struct request *req,
        struct simple_blk_dev *sdev,
        gfp_t gfp_mask,
        void (*worker)(struct work_struct *work))
{
        struct req_work *work;

        ASSERT(req);
        ASSERT(sdev);

        work = kmem_cache_alloc(req_work_cache_, gfp_mask);
        if (!work) {
                goto error0;
        }
        work->req = req;
        work->sdev = sdev;
        work->id = atomic_inc_return(&id_counter_);
        INIT_WORK(&work->work, worker);
        return work;
error0:
        return NULL;
}

/**
 * Destory a bio_work.
 */
static void destroy_req_work(struct req_work *work)
{
        ASSERT(work);
        kmem_cache_free(req_work_cache_, work);
}

static void req_worker(struct work_struct *work)
{
        struct req_work *req_work = container_of(work, struct req_work, work);
        struct simple_blk_dev *sdev = req_work->sdev;
        struct memblk_data *mdata = get_mdata_from_sdev(sdev);
        struct request *req = req_work->req;

        mdata_exec_req(mdata, req);
#if 0
        LOGd("REQ %u: %"PRIu64" (%u).\n", req_work->id, blk_rq_pos(req), blk_rq_bytes(req));
#endif
        if (unlikely(sleep_ms_ > 0)) {
                msleep_interruptible(sleep_ms_);
        }
        
        blk_end_request_all(req, 0);
        destroy_req_work(req_work);
}

/**
 * Exec current of a request.
 * CONTEXT:
 * Non-IRQ.
 */
static void mdata_exec_req_cur(struct memblk_data *mdata, struct request *req)
{
        unsigned int io_size;
        sector_t block_id;
        unsigned int n_blk;
        unsigned int is_write;
        
        ASSERT(req);
        io_size = blk_rq_cur_bytes(req);
        block_id = (u64)blk_rq_pos(req);

        is_write = req->cmd_flags & REQ_WRITE;

        n_blk = io_size / mdata->block_size;
        ASSERT(io_size % mdata->block_size == 0);

        if (is_write) {
                mdata_write_blocks(mdata, block_id, n_blk, req->buffer);
        } else {
                mdata_read_blocks(mdata, block_id, n_blk, req->buffer);
        }
}

/**
 * Execute a special request.
 *
 * RETURN:
 *   true when the request is special, or false.
 */
static bool mdata_exec_req_special(struct memblk_data *mdata, struct request *req)
{
        unsigned int io_size = blk_rq_bytes(req);
        u64 block_id = (u64)blk_rq_pos(req);
        
        if (req->cmd_flags & REQ_DISCARD) {
                mdata_exec_discard(mdata, block_id, io_size / mdata->block_size);
                return true;
        }

        if (req->cmd_flags & REQ_FLUSH && io_size == 0) {
                LOGd("REQ_FLUSH\n");
                return true;
        }

        if (req->cmd_flags & REQ_FUA && io_size == 0) {
                LOGd("REQ_FUA\n");
                return true;
        }

        return false;
}


/**
 * Exec whole request.
 * CONTEXT:
 * Non-IRQ.
 */
static void mdata_exec_req(struct memblk_data *mdata, struct request *req)
{
        int i;
        sector_t sector;
        u64 block_id;
        struct bio_vec *bvec;
        u8 *buffer_bio;
        unsigned int is_write;
        unsigned int n_blk;
        unsigned io_size;
        struct req_iterator iter;
        unsigned long flags;
        u8 *buf;

        ASSERT(req);
        io_size = blk_rq_bytes(req);
        block_id = (u64)blk_rq_pos(req);

        /* log_bi_rw_flag(bio); */

        if (req->cmd_flags & REQ_DISCARD) {
                mdata_exec_discard(mdata, block_id, io_size / mdata->block_size);
                return;
        }

        if (req->cmd_flags & REQ_FLUSH && io_size == 0) {
                LOGd("REQ_FLUSH\n");
                return;
        }

        if (req->cmd_flags & REQ_FUA && io_size == 0) {
                LOGd("REQ_FUA\n");
                return;
        }
        
        is_write = req->cmd_flags & REQ_WRITE;

        rq_for_each_segment(bvec, req, iter) {
                buf = bvec_kmap_irq(bvec, &flags);
#if 0
                LOGd("bvec->bv_len: %u mdata->block_size %u\n",
                     bvec->bv_len, mdata->block_size);
#endif
                ASSERT(bvec->bv_len % mdata->block_size == 0);
                n_blk = bvec->bv_len / mdata->block_size;

                if (is_write) {
                        mdata_write_blocks(mdata, block_id, n_blk, buf);
                } else {
                        mdata_read_blocks(mdata, block_id, n_blk, buf);
                }
                
                block_id += n_blk;
                flush_kernel_dcache_page(bvec->bv_page);
                bvec_kunmap_irq(bvec, &flags);
        }
}

/*******************************************************************************
 * Global functions definition.
 *******************************************************************************/

#if 0
/**
 * Without workqueue.
 */
void simple_blk_req_request_fn(struct request_queue *q)
{
        struct memblk_data *mdata = get_mdata_from_queue(q);
        struct request *req;

        req = blk_fetch_request(q);
        while (req) {
                /* LOGd("REQ: %"PRIu64" (%u)\n", (u64)blk_rq_pos(req), blk_rq_bytes(req)); */

                if (mdata_exec_req_special(mdata, req)) {
                        __blk_end_request_all(req, 0);
                        req = blk_fetch_request(q);
                } else {
                        mdata_exec_req_cur(mdata, req);
                        if (!__blk_end_request_cur(req, 0)) {
                                req = blk_fetch_request(q);
                        }
                }
        }
}
#else
/**
 * With workqueue.
 */ 
void simple_blk_req_request_fn(struct request_queue *q)
{
        struct memblk_data *mdata;
        struct simple_blk_dev *sdev = sdev_get_from_queue(q);
        struct request *req;
        struct req_work *req_work;

        req = blk_fetch_request(q);
        while (req) {
                /* LOGd("REQ: %"PRIu64" (%u)\n", (u64)blk_rq_pos(req), blk_rq_bytes(req)); */

                if (req->cmd_flags & REQ_FLUSH) {

                        flush_workqueue(wq_io_);
                        __blk_end_request_all(req, 0);
                } else {
                        req_work = create_req_work(req, sdev, GFP_ATOMIC, req_worker);
                        if (req_work) {
                                queue_work(wq_io_, &req_work->work);
                        } else {
                                __blk_end_request_all(req, -EIO);
                        }
                }
                req = blk_fetch_request(q);
        }
}
#endif

/**
 * Create memory data.
 * @sdev a simple block device. must not be NULL.
 * REUTRN:
 * true in success, or false.
 * CONTEXT:
 * Non-IRQ.
 */
bool create_private_data(struct simple_blk_dev *sdev)
{
        struct memblk_data *mdata = NULL;
        u64 capacity;
        unsigned int block_size;

        ASSERT(sdev);
        
        capacity = sdev->capacity;
        block_size = sdev->blksiz.lbs;
        mdata = mdata_create(capacity, block_size, GFP_KERNEL);
        
        if (!mdata) {
                goto error0;
        }
        sdev->private_data = (void *)mdata;
        return true;
#if 0
error1:
        mdata_destroy(mdata);
#endif
error0:
        return false;
}

/**
 * Destory memory data.
 * @sdev a simple block device. must not be NULL.
 * RETURN:
 * true in success, or false.
 * CONTEXT:
 * Non-IRQ.
 */
void destroy_private_data(struct simple_blk_dev *sdev)
{
        ASSERT(sdev);
        mdata_destroy(sdev->private_data);
}

/**
 * Accept REQ_DISCARD, REQ_FLUSH, and REQ_FUA.
 */
void customize_sdev(struct simple_blk_dev *sdev)
{
        struct request_queue *q;
        ASSERT(sdev);
        q = sdev->queue;
        
        /* Accept REQ_DISCARD. */
        q->limits.discard_granularity = PAGE_SIZE;
        q->limits.discard_granularity = sdev->blksiz.lbs;
	q->limits.max_discard_sectors = UINT_MAX;
	q->limits.discard_zeroes_data = 1;
	queue_flag_set_unlocked(QUEUE_FLAG_DISCARD, q);
	/* queue_flag_set_unlocked(QUEUE_FLAG_SECDISCARD, q); */

        /* Accept REQ_FLUSH and REQ_FUA. */
        /* blk_queue_flush(q, REQ_FLUSH | REQ_FUA); */
        blk_queue_flush(q, REQ_FLUSH);
}

/**
 * Initialize kmem_cache.
 */
bool pre_register(void)
{
        req_work_cache_ = kmem_cache_create("req_work_cache", sizeof(struct req_work), 0, 0, NULL);
        if (!req_work_cache_) {
                LOGe("req_work_cache creation failed.\n");
                goto error0;
        }
        
#ifdef USE_WQ_SINGLE
        /* Single thread workqueue. This may be slow. */
        wq_io_ = create_singlethread_workqueue(WQ_IO_NAME);
        LOGe("USE_WQ_SINGLE");
#elif defined USE_WQ_UNBOUND
        /* Worker may not use the same CPU with enqueuer. */
        wq_io_ = alloc_workqueue(WQ_IO_NAME, WQ_MEM_RECLAIM | WQ_UNBOUND , 0);
        LOGe("USE_WQ_UNBOUND");
#else
        /* Default. This is the fastest. */
        wq_io_ = alloc_workqueue(WQ_IO_NAME, WQ_MEM_RECLAIM, 0);
        LOGe("USE_WQ_NORMAL");
#endif
                                 
        if (!wq_io_) {
                LOGe("create io queue failed.\n");
                goto error1;
        }
        return true;
#if 0
error2:
        destroy_workqueue(wq_io_);
#endif
error1:
        kmem_cache_destroy(req_work_cache_);
error0:
        return false;
}

/**
 * Finalize kmem_cache.
 */
void post_unregister(void)
{
        destroy_workqueue(wq_io_);
        kmem_cache_destroy(req_work_cache_);
}

/* end of file. */
