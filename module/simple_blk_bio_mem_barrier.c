/**
 * simple_blk_bio_mem_barrier.c -
 * make_request_fn which do memory read/write with barriers.
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
#include <linux/spinlock.h>

#include "walb/block_size.h"
#include "simple_blk_bio.h"
#include "memblk_data.h"

/*******************************************************************************
 * Static data definition.
 *******************************************************************************/

/* #define PERFORMANCE_DEBUG */

/* For debug. */
static atomic_t id_counter_ = ATOMIC_INIT(0);

struct bio_work
{
        struct bio *bio;
        struct simple_blk_dev *sdev;
        struct work_struct work;
	struct list_head list; /* list entry */
	int id; /* for debug */
};

#define BIO_WORK_CACHE_NAME "bio_work_cache"
struct kmem_cache *bio_work_cache_ = NULL;
#define WQ_IO_NAME "simple_blk_bio_mem_barrier_io"
struct workqueue_struct *wq_io_ = NULL;
#define WQ_FLUSH_NAME "simple_blk_bio_mem_barrier_flush"
struct workqueue_struct *wq_flush_ = NULL;

/**
 * sdev->private_data should be this.
 */
struct pdata
{
	struct memblk_data *mdata; /* mdata */

	spinlock_t lock; /* bio_work_list and under_flush must be accessed
			    with the lock. */
	struct list_head bio_work_list; /* head of bio_work list. */
	bool under_flush; /* true during flushing. */
};

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

static void log_bi_rw_flag(struct bio *bio);

static void mdata_exec_discard(struct memblk_data *mdata, u64 block_id, unsigned int n_blocks);
static void mdata_exec_bio(struct memblk_data *mdata, struct bio *bio);

UNUSED static struct memblk_data* get_mdata_from_sdev(struct simple_blk_dev *sdev);
UNUSED static struct memblk_data* get_mdata_from_queue(struct request_queue *q);
UNUSED static struct pdata* get_pdata_from_sdev(struct simple_blk_dev *sdev);
UNUSED static struct pdata* get_pdata_from_queue(struct request_queue *q);

static struct bio_work* create_bio_work(struct bio *bio, struct simple_blk_dev *sdev, gfp_t gfp_mask);
static void destroy_bio_work(struct bio_work *work);

static void bio_work_io_task(struct work_struct *work);
static void bio_work_flush_task(struct work_struct *work);

static struct pdata* pdata_create(struct memblk_data *mdata, gfp_t gfp_mask);
static void pdata_destroy(struct pdata *pdata);

static void queue_bio_work(struct bio_work *work);

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
 * Execute IO on a mdata.
 *
 * You must call bio_end_io() by yourself after this function.
 *
 * CONTEXT:
 * Non-IRQ.
 */
static void mdata_exec_bio(struct memblk_data *mdata, struct bio *bio)
{
        int i;
        sector_t sector;
        u64 block_id;
        struct bio_vec *bvec;
        u8 *buffer_bio;
        unsigned int is_write;
        unsigned int n_blk;

        ASSERT(bio);

        sector = bio->bi_sector;
        block_id = (u64)sector;

        log_bi_rw_flag(bio);

        if (bio->bi_rw & REQ_DISCARD) {
                log_bi_rw_flag(bio);
                if (bio->bi_rw & REQ_SECURE) {
                        mdata_exec_discard(mdata, block_id, bio->bi_size / mdata->block_size);
                }
                return;
        }

        if (bio->bi_rw & REQ_FLUSH && bio->bi_size == 0) {
                log_bi_rw_flag(bio);
                LOGd("REQ_FLUSH\n");
                return;
        }

        if (bio->bi_rw & REQ_FUA && bio->bi_size == 0) {
                log_bi_rw_flag(bio);
                LOGd("REQ_FUA\n");
                return;
        }
        
        is_write = bio->bi_rw & REQ_WRITE;
        
        bio_for_each_segment(bvec, bio, i) {
                buffer_bio = (u8 *)__bio_kmap_atomic(bio, i, KM_USER0);
                ASSERT(bio_cur_bytes(bio) % mdata->block_size == 0);

                n_blk = bio_cur_bytes(bio) / mdata->block_size;
                if (is_write) {
                        mdata_write_blocks(mdata, block_id, n_blk, buffer_bio);
                } else {
                        mdata_read_blocks(mdata, block_id, n_blk, buffer_bio);
                }
                block_id += n_blk;
                flush_kernel_dcache_page(bvec->bv_page);
               __bio_kunmap_atomic(bio, KM_USER0);
        }
}

/**
 * Get mdata from a sdev.
 */
static inline struct memblk_data* get_mdata_from_sdev(struct simple_blk_dev *sdev)
{
        ASSERT(sdev);
	return get_pdata_from_sdev(sdev)->mdata;
}

/**
 * Get mdata from a queue.
 */
static inline struct memblk_data* get_mdata_from_queue(struct request_queue *q)
{
	ASSERT(q);
        return get_mdata_from_sdev(sdev_get_from_queue(q));
}


static inline struct pdata* get_pdata_from_sdev(struct simple_blk_dev *sdev)
{
	ASSERT(sdev);
	return (struct pdata *)sdev->private_data;
}

static inline struct pdata* get_pdata_from_queue(struct request_queue *q)
{
	ASSERT(q);
	return get_pdata_from_sdev(sdev_get_from_queue(q));
}


/**
 * Create a bio_work.
 *
 * RETURN:
 * NULL if failed.
 * CONTEXT:
 * Any.
 */
static struct bio_work* create_bio_work(struct bio *bio, struct simple_blk_dev *sdev, gfp_t gfp_mask)
{
        struct bio_work *work;

        ASSERT(bio);
        ASSERT(sdev);

        work = kmem_cache_alloc(bio_work_cache_, gfp_mask);
        if (!work) {
                goto error0;
        }
        work->bio = bio;
        work->sdev = sdev;
        work->id = atomic_inc_return(&id_counter_);
        return work;
error0:
        return NULL;
}

/**
 * Destory a bio_work.
 */
static void destroy_bio_work(struct bio_work *work)
{
        ASSERT(work);
        kmem_cache_free(bio_work_cache_, work);
}

/**
 * Normla bio task.
 */
static void bio_work_io_task(struct work_struct *work)
{
        struct bio_work *bio_work = container_of(work, struct bio_work, work);
        struct simple_blk_dev *sdev = bio_work->sdev;
	struct memblk_data *mdata = get_mdata_from_sdev(sdev);
        struct bio *bio = bio_work->bio;

	ASSERT(!(bio->bi_rw & REQ_FLUSH));

	mdata_exec_bio(mdata, bio);
	bio_endio(bio, 0);
	
	destroy_bio_work(bio_work);
}

/**
 * Flush bio task.
 */
static void bio_work_flush_task(struct work_struct *work)
{
        struct bio_work *bio_work = container_of(work, struct bio_work, work);
        struct simple_blk_dev *sdev = bio_work->sdev;
	struct pdata *pdata = get_pdata_from_sdev(sdev);
	struct memblk_data *mdata = get_mdata_from_sdev(sdev);
        struct bio *bio = bio_work->bio;
	unsigned long flags;
	struct bio_work *child, *next;
	struct list_head listh;

	ASSERT(bio->bi_rw & REQ_FLUSH);
	INIT_LIST_HEAD(&listh);
	
	spin_lock_irqsave(&pdata->lock, flags);
	LOGd("spin_lock\n");
	ASSERT(!pdata->under_flush);
	pdata->under_flush = true;
	list_for_each_entry_safe(child, next, &pdata->bio_work_list, list) {
		list_del(&child->list);
		list_add_tail(&child->list, &listh);
	}
	ASSERT(list_empty(&pdata->bio_work_list));
	LOGd("spin_unlock\n");
	spin_unlock_irqrestore(&pdata->lock, flags);
		
	flush_workqueue(wq_io_);
	mdata_exec_bio(mdata, bio);
	bio_endio(bio, 0);
	list_for_each_entry_safe(child, next, &listh, list) {
		INIT_WORK(&child->work, bio_work_io_task);
		queue_work(wq_io_, &child->work);
	}
	
	spin_lock_irqsave(&pdata->lock, flags);
	LOGd("spin_lock\n");
	ASSERT(pdata->under_flush);
	pdata->under_flush = false;
	LOGd("spin_unlock\n");
	spin_unlock_irqrestore(&pdata->lock, flags);
	
	destroy_bio_work(bio_work);
}

static struct pdata* pdata_create(struct memblk_data *mdata, gfp_t gfp_mask)
{
	struct pdata *pdata;
	unsigned long flags;

	ASSERT(mdata);
	
	pdata = kmalloc(sizeof(struct pdata), gfp_mask);
	if (!pdata) {
		return NULL;
	}
	pdata->mdata = mdata;
	spin_lock_init(&pdata->lock);

	spin_lock_irqsave(&pdata->lock, flags);
	INIT_LIST_HEAD(&pdata->bio_work_list);
	pdata->under_flush = false;
	spin_unlock_irqrestore(&pdata->lock, flags);

	return pdata;
}

static void pdata_destroy(struct pdata *pdata)
{
	if (pdata) {
		kfree(pdata);
	}
}

/**
 * Enqueue bio_work data to an appropriate queue.
 */
static void queue_bio_work(struct bio_work *work)
{
	ASSERT(work);
	ASSERT(work->bio);

	if (work->bio->bi_rw & REQ_FLUSH) {
		INIT_WORK(&work->work, bio_work_flush_task);
		queue_work(wq_flush_, &work->work);
	} else {
		INIT_WORK(&work->work, bio_work_io_task);
		queue_work(wq_io_, &work->work);
	}
}

/*******************************************************************************
 * Global functions definition.
 *******************************************************************************/

/**
 * Make request.
 * CONTEXT:
 *     in_interrupt() is 0. in_atomic() is 0.
 */
void simple_blk_bio_make_request(struct request_queue *q, struct bio *bio)
{
	struct simple_blk_dev *sdev = sdev_get_from_queue(q);
	struct pdata *pdata = get_pdata_from_queue(q);
	struct bio_work *work;
	unsigned long flags;
	bool under_flush = false;
	
	LOGd("in_interrupt: %lu in_atomic: %u\n",
		in_interrupt(), in_atomic());

	ASSERT(bio);

	work = create_bio_work(bio, sdev, GFP_NOIO);
	if (!work) {
		LOGe("create_bio_work() failed.\n");
		goto error0;
	}
	
	spin_lock_irqsave(&pdata->lock, flags);
	LOGd("spin_lock\n");
	if (pdata->under_flush) {
		under_flush = true;
		list_add_tail(&work->list, &pdata->bio_work_list);
	}
	LOGd("spin_unlock\n");
	spin_unlock_irqrestore(&pdata->lock, flags);

	if (!under_flush) {
		queue_bio_work(work);
	}
	return;
error0:
	bio_endio(bio, -EIO);
}

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
        struct memblk_data *mdata;
	struct pdata *pdata;
        u64 capacity;
        unsigned int block_size;

        ASSERT(sdev);
        
        capacity = sdev->capacity;
        block_size = LOGICAL_BLOCK_SIZE;
        mdata = mdata_create(capacity, block_size, GFP_KERNEL);
        if (!mdata) {
                goto error0;
        }
	pdata = pdata_create(mdata, GFP_KERNEL);
	if (!pdata) {
		goto error1;
	}
        sdev->private_data = pdata;
        return true;
#if 0
error2:
	pdata_destroy(pdata);
#endif
error1:
        mdata_destroy(mdata);
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
	struct pdata *pdata;

        ASSERT(sdev);
	pdata = sdev->private_data;
	ASSERT(pdata);
	
        mdata_destroy(pdata->mdata);
	pdata_destroy(pdata);
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
        /* q->limits.discard_granularity = PAGE_SIZE; */
        /* q->limits.discard_granularity = LOGICAL_BLOCK_SIZE; */
	/* q->limits.max_discard_sectors = UINT_MAX; */
	/* q->limits.discard_zeroes_data = 1; */
	/* queue_flag_set_unlocked(QUEUE_FLAG_DISCARD, q); */
	/* queue_flag_set_unlocked(QUEUE_FLAG_SECDISCARD, q); */

        /* Accept REQ_FLUSH and REQ_FUA. */
        /* blk_queue_flush(q, REQ_FLUSH | REQ_FUA); */

        /* Accept REQ_FLUSH only. */
        blk_queue_flush(q, REQ_FLUSH);
}

/**
 * Initialize kmem_cache.
 */
bool pre_register(void)
{
        bio_work_cache_ = kmem_cache_create(BIO_WORK_CACHE_NAME, sizeof(struct bio_work), 0, 0, NULL);
        if (!bio_work_cache_) {
                LOGe("bio_work_cache creation failed.\n");
                goto error0;
        }

        wq_io_ = create_wq_io(WQ_IO_NAME, get_workqueue_type());
        if (!wq_io_) {
                LOGe("create io workqueue failed.\n");
                goto error1;
        }
        wq_flush_ = create_singlethread_workqueue(WQ_FLUSH_NAME);
        if (!wq_flush_) {
                LOGe("create flush workqueue failed.\n");
                goto error2;
        }

	if (!mdata_init()) {
		goto error3;
	}
	
        return true;
#if 0
error4:
	mdata_exit();
#endif
error3:
        destroy_workqueue(wq_flush_);
error2:
        destroy_workqueue(wq_io_);
error1:
        kmem_cache_destroy(bio_work_cache_);
error0:
        return false;
}

/**
 * Finalize kmem_cache.
 */
void post_unregister(void)
{
	mdata_exit();
        destroy_workqueue(wq_flush_);
        destroy_workqueue(wq_io_);
        kmem_cache_destroy(bio_work_cache_);
}

/* end of file. */
