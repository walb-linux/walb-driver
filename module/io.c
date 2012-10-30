/**
 * io.c - IO processing core of WalB.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include "kern.h"
#include "io.h"
#include "bio_wrapper.h"
#include "treemap.h"

/*******************************************************************************
 * Static data definition.
 *******************************************************************************/

/**
 * iocored->flags bit.
 */
enum {
	IOCORE_STATE_READ_ONLY = 0,
	IOCORE_STATE_FAILURE,
	IOCORE_STATE_QUEUE_STOPPED,
	IOCORE_STATE_SUBMIT_TASK_WORKING,
	IOCORE_STATE_WAIT_TASK_WORKING,
	IOCORE_STATE_SUBMIT_DATA_TASK_WORKING,
};

struct walb_dev_sould_have
{
	/* To avoid lock lsuper0 during request processing. */
	u64 ring_buffer_off; 
	u64 ring_buffer_size;

	/* chunk sectors.
	   if chunk_sectors > 0:
	   (1) bio size must not exceed the size.
	   (2) bio must not cross over multiple chunks.
	   else:
	   no limitation. */
	unsigned int ldev_chunk_sectors;
	unsigned int ddev_chunk_sectors;

	/* Maximum logpack size [physical block].
	   This will be used for logpack size
	   not to be too long
	   This will avoid decrease of
	   sequential write performance. */
	unsigned int max_logpack_pb; 

#ifdef WALB_OVERLAPPING_SERIALIZE
	/* Maximum request size [logical block]. */
	unsigned int max_sectors_in_overlapping;
#endif
	
#ifdef WALB_FAST_ALGORITHM
	/* Maximum request size [logical block]. */
	unsigned int max_sectors_in_pending;

	/* max_pending_sectors < pending_sectors
	   we must stop the queue. */
	unsigned int max_pending_sectors; 

	/* min_pending_sectors > pending_sectors
	   we can restart the queue. */	
	unsigned int min_pending_sectors;

	/* queue stopped period must not exceed
	   queue_stop_time_ms. */
	unsigned int queue_stop_timeout_ms; 
#endif
};
	
/**
 * (struct walb_dev *)->private_data.
 */
struct iocore_data
{
	/* See IOCORE_STATE_XXXXX */
	unsigned long flags;

	/*
	 * There are four queues.
	 * Each queue must be accessed with its own lock held.
	 *
	 * logpack_submit_queue:
	 *   writepack list.
	 * logpack_wait_queue:
	 *   writepack list.
	 * datapack_submit_queue:
	 *   bio_wrapper list.
	 * logpack_gc_queue:
	 *   writepack list.
	 */
	spinlock_t logpack_submit_queue_lock;
	struct list_head logpack_submit_queue;
	spinlock_t logpack_wait_queue_lock;
	struct list_head logpack_wait_queue;
	spinlock_t datapack_submit_queue_lock;
	struct list_head datapack_submit_queue;
	spinlock_t logpack_gc_queue_lock;
	struct list_head logpack_gc_queue;

	/* for gc worker. */
	struct worker_data gc_worker_data;

	atomic_t n_pending_bio; /* Number of pending bio(s).
				   This is used for device exit. */

#ifdef WALB_OVERLAPPING_SERIALIZE
	/**
	 * All req_entry data may not keep reqe->bioe_list.
	 * You must keep address and size information in another way.
	 */
	spinlock_t overlapping_data_lock; /* Use spin_lock()/spin_unlock(). */
	struct multimap *overlapping_data; /* key: blk_rq_pos(req),
					      val: pointer to req_entry. */
#endif

#ifdef WALB_FAST_ALGORITHM
	/**
	 * All bio_wrapper data must keep
	 * biow->bioe_list while they are stored in the pending_data.
	 */
	spinlock_t pending_data_lock; /* Use spin_lock()/spin_unlock(). */
	struct multimap *pending_data; /* key: biow->pos,
					  val: pointer to bio_wrapper. */
	unsigned int pending_sectors; /* Number of sectors pending
					 [logical block]. */
	unsigned long queue_restart_jiffies; /* For queue stopped timeout check. */
	bool is_queue_stopped; /* true if queue is stopped. */
#endif
};

/* All treemap(s) in this module will share a treemap memory manager. */
static atomic_t n_users_of_memory_manager_ = ATOMIC_INIT(0);
static struct treemap_memory_manager mmgr_;
#define TREE_NODE_CACHE_NAME "walb_iocore_bio_node_cache"
#define TREE_CELL_HEAD_CACHE_NAME "walb_iocore_bio_cell_head_cache"
#define TREE_CELL_CACHE_NAME "walb_iocore_bio_cell_cache"
#define N_ITEMS_IN_MEMPOOL (128 * 2) /* for pending data and overlapping data. */

/*******************************************************************************
 * Macros definition.
 *******************************************************************************/

#define WORKER_NAME_GC "walb_gc"
#define N_PACK_BULK 32
#define N_IO_BULK 128

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

static inline struct iocore_data* get_iocored_from_wdev(
	struct walb_dev *wdev);

static struct iocore_data* create_iocore_data(gfp_t gfp_mask);
static void destroy_iocore_data(struct iocore_data *iocored);

static struct bio_wrapper* alloc_bio_wrapper_inc(
	struct walb_dev *wdev, gfp_t gfp_mask);
static void destroy_bio_wrapper_dec(
	struct walb_dev *wdev, struct bio_wrapper *biow);

/* For treemap memory manager. */
static bool treemap_memory_manager_inc(void);
static void treemap_memory_manager_dec(void);

/*******************************************************************************
 * Static functions implementation.
 *******************************************************************************/

/**
 * Get iocore data from wdev.
 */
static inline struct iocore_data* get_iocored_from_wdev(
	struct walb_dev *wdev)
{
	return (struct iocore_data *)wdev->private_data;
}

/**
 * Create iocore data.
 * GC worker will not be started inside this function.
 */
static struct iocore_data* create_iocore_data(gfp_t gfp_mask)
{
	struct iocore_data *iocored;

	iocored = kmalloc(sizeof(struct iocore_data), gfp_mask);
	if (!iocored) {
		LOGe("memory allocation failure.\n");
		goto error0;
	}

	iocored->flags = 0;
	
	spin_lock_init(&iocored->logpack_submit_queue_lock);
	spin_lock_init(&iocored->logpack_wait_queue_lock);
	spin_lock_init(&iocored->datapack_submit_queue_lock);
	spin_lock_init(&iocored->logpack_gc_queue_lock);
	INIT_LIST_HEAD(&iocored->logpack_submit_queue);
	INIT_LIST_HEAD(&iocored->logpack_wait_queue);
	INIT_LIST_HEAD(&iocored->datapack_submit_queue);
	INIT_LIST_HEAD(&iocored->logpack_gc_queue);

	atomic_set(&iocored->n_pending_bio, 0);

#ifdef WALB_OVERLAPPING_SERIALIZE
	spin_lock_init(&iocored->overlapping_data_lock);
	iocored->overlapping_data = multimap_create(gfp_mask, &mmgr_);
	if (!iocored->overlapping_data) {
		LOGe();
		goto error1;
	}
#endif

#ifdef WALB_FAST_ALGORITHM
	spin_lock_init(&iocored->pending_data_lock);
	iocored->pending_data = multimap_create(gfp_mask, &mmgr_);
	if (!iocored->pending_data) {
		LOGe();
		goto error2;
	}
	iocored->pending_sectors = 0;
	iocored->queue_restart_jiffies = jiffies;
	iocored->is_queue_stopped = false;
#endif
	return iocored;

#ifdef WALB_FAST_ALGORITHM
error2:
	multimap_destroy(iocored->pending_data);
	
#endif
#ifdef WALB_OVERLAPPING_SERIALIZE
error1:
	multimap_destroy(iocored->overlapping_data);
#endif
	kfree(iocored);
error0:
	return NULL;
}

/**
 * Destroy iocore data.
 */
static void destroy_iocore_data(struct iocore_data *iocored)
{
	ASSERT(iocored);
	
#ifdef WALB_FAST_ALGORITHM
	multimap_destroy(iocored->pending_data);
#endif
#ifdef WALB_OVERLAPPING_SERIALIZE
	multimap_destroy(iocored->overlapping_data);
#endif
	kfree(iocored);
}

/**
 * Allocate a bio wrapper and increment n_pending_bio.
 */
static struct bio_wrapper* alloc_bio_wrapper_inc(
	struct walb_dev *wdev, gfp_t gfp_mask)
{
	struct bio_wrapper *biow;
	ASSERT(wdev);
	
	biow = alloc_bio_wrapper(gfp_mask);
	if (!biow) {
		goto error0;
	}
	atomic_inc(&wdev->n_pending_bio);
	return biow;
error0:
	return NULL;
}

/**
 * Destroy a bio wrapper and decrement n_pending_bio.
 */
static void destroy_bio_wrapper_dec(
	struct walb_dev *wdev, struct bio_wrapper *biow)
{
	ASSERT(wdev);
	ASSERT(biow);
	
	destroy_bio_wrapper(biow);
	atomic_dec(&wdev->n_pending_bio);
}

/**
 * Increment n_users of treemap memory manager and
 * iniitialize mmgr_ if necessary.
 */
static bool treemap_memory_manager_inc(void)
{
	bool ret;
	
	if (atomic_inc_return(&n_users_of_memory_manager_) == 1) {
		ret = initialize_treemap_memory_manager(
			&mmgr_, N_ITEMS_IN_MEMPOOL,
			TREE_NODE_CACHE_NAME,
			TREE_CELL_HEAD_CACHE_NAME,
			TREE_CELL_CACHE_NAME);
		if (!ret) {
			atomic_dec(&n_users_of_memory_manager_);
			goto error;
		}
	}
	return true;
error:
	return false;
}

/**
 * Decrement n_users of treemap memory manager and
 * finalize mmgr_ if necessary.
 */
static void treemap_memory_manager_dec(void)
{
	if (atomic_dec_return(&n_users_of_memory_manager_) == 0) {
		finalize_treemap_memory_manager(&mmgr_);
	}
}
					
/*******************************************************************************
 * Interfaces.
 *******************************************************************************/

#if 0
/**
 * WalB operations.
 */
static struct walb_iocore_operations iocore_ops_ = {
	.initialize       = iocore_initialize,
	.finalize         = iocore_finalize,
	.make_request     = iocore_make_request,
	.log_make_request = iocore_log_make_request,
	.stop             = iocore_stop,
	.start            = iocore_start,
};
#endif

/*******************************************************************************
 * Global functions implementation.
 *******************************************************************************/

/**
 * Initialize iocore data for a wdev.
 */
bool iocore_initialize(struct walb_dev *wdev)
{
	/* now editing */
	
	int ret;
	struct iocore_data *iocored;

	iocored = create_iocore_data(GFP_KERNEL);
	if (!iocored) {
		LOGe("Memory allocation failed.\n");
		goto error0;
	}
	wdev->private_data = iocored;
	
	if (!treemap_memory_manager_inc()) {
		LOGe("Treemap memory manager inc failed.\n");
		goto error1;
	}

	/* Decide gc worker name and start it. */
	ret = snprintf(iocored->gc_worker_data.name, WORKER_NAME_MAX_LEN,
		"%s/%u", WORKER_NAME_GC, MINOR(wdev->devt));
	if (ret >= WORKER_NAME_MAX_LEN) {
		LOGe("Thread name size too long.\n");
		goto error2;
	}
	initialize_worker(&iocored->gc_worker_data,
			run_logpack_gc_worker, (void *)wdev);
	
	return true;

error2:
	treemap_memory_manager_dec();
error1:
	destroy_iocore_data(iocored);
	wdev->private_data = NULL;
error0:
	return false;
}

/**
 * Finalize iocore data for a wdev.
 */
void iocore_finalize(struct walb_dev *wdev)
{
	/* now editing */
	
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);
	
	finalize_worker(&iocored->gc_worker_data);
	destroy_iocore_data(iocored);
	wdev->private_data = NULL;
	
	treemap_memory_manager_dec();
}

/**
 * Stop IO processing.
 *
 * After stopped, there is no IO pending underlying
 * data/log devices.
 * Upper layer can submit IOs but the walb driver
 * just queues them and does not start processing during stopped.
 */
void iocore_stop(struct walb_dev *wdev)
{
	/* now editing */
}

/**
 * (Re)start IO processing.
 */
void iocore_start(struct walb_dev *wdev)
{
	/* now editing */
}

/**
 * Make request.
 */
void iocore_make_request(struct walb_dev *wdev, struct bio *bio)
{
	struct bio_wrapper *biow;
	int error = -EIO;

	/* Failure state. */
	if (test_bit(IOCORE_STATE_FAILURE, &wdev->flags)) {
		bio_endio(bio, -EIO);
		return;
	}

	/* Create bio wrapper. */
	biow = alloc_bio_wrapper_inc(wdev, GFP_NOIO);
	if (!biow) {
		error = -ENOMEM;
		goto error0;
	}
	init_bio_wrapper(biow, bio);
	biow->private_data = wdev;

	if (biow->bio->bi_rw & REQ_WRITE) {
		/* Calculate checksum. */
		biow->csum = bio_calc_checksum(biow->bio);
		
		/* Push into queue. */
		spin_lock(&wdev->logpack_submit_queue_lock);
		list_add_tail(&biow->list, &wdev->logpack_submit_queue);
		spin_unlock(&wdev->logpack_submit_queue_lock);

		/* Enqueue logpack-submit task. */
		if (!test_bit(IOCORE_STATE_QUEUE_STOPPED, &wdev->flags)) {
			enqueue_submit_task_if_necessary(wdev);
		}
	} else { /* read */
		submit_read_bio_wrapper(wdev, biow);

		/* TODO: support IOCORE_STATE_QUEUE_STOPPED for read also. */
	}
	return;
#if 0
error1:
	destroy_bio_wrapper_dec(wdev, biow);
#endif
error0:
	bio_endio(bio, error);
}
#endif

/**
 * Make request for wrapper log device.
 */
void iocore_log_make_request(struct walb_dev *wdev, struct bio *bio)
{
	if (bio->bi_rw & WRITE) {
		bio_endio(bio, -EIO);
	} else {
		bio->bi_bdev = wdev->ldev;
		generic_make_request(bio);
	}
}

/**
 * Wait for all pending IO(s) for underlying data/log devices.
 */
void iocore_flush(struct walb_dev *wdev)
{
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);

	set_bit(IOCORE_STATE_FAILURE, &iocored->flags);
	LOGn("n_pending_bio %d\n", atomic_read(&iocored->n_pending_bio));
	while (atomic_read(&iocored->n_pending_bio) > 0) {
		LOGn("n_pending_bio %d\n", atomic_read(&iocored->n_pending_bio));
		msleep(100);
	}
	/* flush_all_wq(); */
	LOGn("n_pending_bio %d\n", atomic_read(&iocored->n_pending_bio));
}

/**
 * Make request.
 */
#if 0
void walb_make_request(struct request_queue *q, struct bio *bio)
{
	UNUSED struct walb_dev *wdev = get_wdev_from_queue(q);

	/* Set a clock ahead. */
	spin_lock(&wdev->latest_lsid_lock);
	wdev->latest_lsid++;
	spin_unlock(&wdev->latest_lsid_lock);

#ifdef WALB_FAST_ALGORITHM
	spin_lock(&wdev->completed_lsid_lock);
	wdev->completed_lsid++;
	spin_unlock(&wdev->completed_lsid_lock);
#endif
	spin_lock(&wdev->cpd.written_lsid_lock);
	wdev->cpd.written_lsid++;
	spin_unlock(&wdev->cpd.written_lsid_lock);

	/* not yet implemented. */
	set_bit(BIO_UPTODATE, &bio->bi_flags);
	bio_endio(bio, 0);
}
#else
void walb_make_request(struct request_queue *q, struct bio *bio)
{
	struct walb_dev *wdev = get_wdev_from_queue(q);
	iocore_make_request(wdev, bio);
}
#endif

/**
 * Walblog device make request.
 *
 * 1. Completion with error if write.
 * 2. Just forward to underlying log device if read.
 *
 * @q request queue.
 * @bio bio.
 */
void walblog_make_request(struct request_queue *q, struct bio *bio)
{
	struct walb_dev *wdev = get_wdev_from_queue(q);
	iocore_log_make_request(wdev, bio);
}

MODULE_LICENSE("Dual BSD/GPL");
