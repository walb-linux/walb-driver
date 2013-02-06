/**
 * redo.c - Redo code.
 *
 * Copyright(C) 2013, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/delay.h>
#include "kern.h"
#include "io.h"
#include "bio_wrapper.h"
#include "bio_entry.h"
#include "treemap.h"
#include "worker.h"
#include "bio_util.h"
#include "pack_work.h"
#include "logpack.h"
#include "super.h"
#include "io.h"
#include "redo.h"

/*******************************************************************************
 * Static data definition.
 *******************************************************************************/

/**
 * For redo tasks.
 */
struct redo_data
{
	struct walb_dev *wdev;
	u64 lsid;
	int error;

	/* These are shared with worker and master.
	   Use queue_lock to access them. */
	spinlock_t queue_lock;
	struct list_head queue;
	unsigned int queue_len;
};

/**
 * Logpack for redo.
 */
struct redo_pack
{
	struct bio_wrapper *logh_biow;
	struct list_head biow_list;
};

/*******************************************************************************
 * Macros definition.
 *******************************************************************************/

/* Maximum size of log to read ahead for redo [logical block].
   Currently 8MB. */
#define READ_AHEAD_LB (8 * 1024 * 1024 / LOGICAL_BLOCK_SIZE)

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

static struct redo_data* create_redo_data(struct walb_dev *wdev, u64 lsid);
static void destroy_redo_data(struct redo_data* data);
static void run_read_log_in_redo(void *data);
static void run_gc_log_in_redo(void *data);
static struct bio_wrapper* create_log_bio_wrapper_for_redo(
	struct walb_dev *wdev, u64 lsid, struct sector_data *sectd);
static bool prepare_data_bio_for_redo(
	struct walb_dev *wdev, struct bio_wrapper *biow,
	u64 pos, unsigned int len);
static struct bio_wrapper* create_discard_bio_wrapper_for_redo(
	struct walb_dev *wdev, u64 pos, unsigned int len);
static void destroy_bio_wrapper_for_redo(
	struct walb_dev *wdev, struct bio_wrapper* biow);
static void bio_end_io_for_redo(struct bio *bio, int error);
static void wait_for_all_read_io_and_destroy(
	struct redo_data *read_rd);
static void wait_for_all_write_io_for_redo(struct walb_dev *wdev);
static void wait_for_all_gc_tasks_for_redo(struct redo_data *gc_rd);
static unsigned int get_bio_wrapper_from_read_queue(
	struct redo_data *read_rd, struct list_head *biow_list,
	unsigned int n);
static struct bio_wrapper* get_logpack_header_for_redo(
	struct worker_data *read_wd, struct redo_data *read_rd,
	u64 written_lsid);
static bool redo_logpack(
	struct worker_data *read_wd, struct redo_data *read_rd,
	struct redo_data *gc_rd,
	struct bio_wrapper *logh_biow, u64 *written_lsid_p,
	bool *should_terminate);
static u32 calc_checksum_for_redo(
	unsigned int n_lb, unsigned int pbs, u32 salt,
	struct list_head *biow_list);
static void create_data_io_for_redo(
	struct walb_dev *wdev,
	struct walb_log_record *rec,
	struct list_head *biow_list);
static void create_discard_data_io_for_redo(
	struct walb_dev *wdev,
	struct walb_log_record *rec,
	struct list_head *biow_list);
static void submit_data_bio_for_redo(
	UNUSED struct walb_dev *wdev, struct bio_wrapper *biow);

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * Create redo_data.
 */
static struct redo_data* create_redo_data(struct walb_dev *wdev, u64 lsid)
{
	struct redo_data *data;

	ASSERT(wdev);

	data = kmalloc(sizeof(*data), GFP_KERNEL);
	if (!data) { return NULL; }

	data->wdev = wdev;
	data->lsid = lsid;
	spin_lock_init(&data->queue_lock);
	INIT_LIST_HEAD(&data->queue);
	data->queue_len = 0;
	data->error = 0;
	return data;
}

/**
 * Destroy redo_data.
 */
static void destroy_redo_data(struct redo_data* data)
{
	ASSERT(data);
	ASSERT(list_empty(&data->queue));
	ASSERT(data->queue_len == 0);

	kfree(data);
}

/**
 * Read log device worker.
 *
 * What this function will do:
 *   while queue is not occupied:
 *     create buf/bio/biow and submit it.
 *     enqueue the biow.
 *
 * You must call wakeup_worker() to read more data.
 *
 * @data struct redo_data pointer.
 */
static void run_read_log_in_redo(void *data)
{
	struct redo_data *redod;
	struct walb_dev *wdev;
	struct list_head biow_list;
	unsigned int queue_len;
	unsigned int pbs;
	unsigned int max_len;
	struct bio_wrapper *biow, *biow_next;
	struct blk_plug plug;

	redod = (struct redo_data *)data;
	ASSERT(redod);
	wdev = redod->wdev;
	ASSERT(wdev);
	pbs = wdev->physical_bs;
	max_len = capacity_pb(pbs, READ_AHEAD_LB);

	INIT_LIST_HEAD(&biow_list);

	spin_lock(&redod->queue_lock);
	queue_len = redod->queue_len;
	spin_unlock(&redod->queue_lock);

	while (queue_len < max_len) {
		/* Create biow for redo. */
	retry:
		biow = create_log_bio_wrapper_for_redo(
			wdev, redod->lsid, NULL);
		if (!biow) {
			schedule();
			goto retry;
		}

		list_add_tail(&biow->list, &biow_list);

		/* Iterate. */
		queue_len++;
		redod->lsid++;
	}

	if (list_empty(&biow_list)) {
		goto fin;
	}

	/* Submit biow(s). */
	blk_start_plug(&plug);
	list_for_each_entry_safe(biow, biow_next, &biow_list, list) {
		generic_make_request(biow->bio);
	}
	blk_finish_plug(&plug);

	/* Enqueue submitted biow(s). */
	spin_lock(&redod->queue_lock);
	list_for_each_entry_safe(biow, biow_next, &biow_list, list) {
		list_move_tail(&biow->list, &redod->queue);
		redod->queue_len++;
	}
	spin_unlock(&redod->queue_lock);
	ASSERT(list_empty(&biow_list));

fin:
	redod->error = 0;
	return;
#if 0
error0:
	redod->error = -1;
#endif
}

/**
 * GC the allocated data for redo.
 */
static void run_gc_log_in_redo(void *data)
{
	struct redo_data *redod;
	struct list_head biow_list;
	unsigned int n_biow, c;
	struct bio_wrapper *biow, *biow_next;
	const unsigned long timeo = msecs_to_jiffies(completion_timeo_ms_);
	unsigned long rtimeo;
#ifdef WALB_OVERLAPPED_SERIALIZE
	struct iocore_data *iocored;
	struct list_head should_submit_list;
	struct bio_wrapper *biow_tmp, *biow_tmp_next;
#endif

	redod = (struct redo_data *)data;
	ASSERT(redod);
	INIT_LIST_HEAD(&biow_list);
#ifdef WALB_OVERLAPPED_SERIALIZE
	iocored = get_iocored_from_wdev(redod->wdev);
	INIT_LIST_HEAD(&should_submit_list);
#endif

	while (true) {
		ASSERT(list_empty(&biow_list));
		spin_lock(&redod->queue_lock);
		n_biow = 0;
		list_for_each_entry_safe(biow, biow_next, &redod->queue, list) {
			list_move_tail(&biow->list, &biow_list);
			n_biow++;
			redod->queue_len--;
		}
		spin_unlock(&redod->queue_lock);
		if (n_biow == 0) {
			break;
		}

		list_for_each_entry_safe(biow, biow_next, &biow_list, list) {
			list_del(&biow->list);
			c = 0;
		retry:
			rtimeo = wait_for_completion_timeout(&biow->done, timeo);
			if (rtimeo == 0) {
				LOGn("timeout(%d): biow %p pos %"PRIu64" len %u\n",
					c, biow, (u64)biow->pos, biow->len);
				c++;
				goto retry;
			}

#ifdef WALB_OVERLAPPED_SERIALIZE
			/* Delete from overlapped detection data. */
			spin_lock(&iocored->overlapped_data_lock);
			overlapped_delete_and_notify(
				iocored->overlapped_data,
				&iocored->max_sectors_in_overlapped,
				&should_submit_list, biow
#ifdef WALB_DEBUG
				, &iocored->overlapped_out_id
#endif
				);
			spin_unlock(&iocored->overlapped_data_lock);

			/* Submit overlapped. */
			list_for_each_entry_safe(
				biow_tmp, biow_tmp_next, &should_submit_list, list4) {
				ASSERT(biow_tmp->n_overlapped == 0);
				list_del(&biow_tmp->list4);
				generic_make_request(biow_tmp->bio);
			}
			ASSERT(list_empty(&should_submit_list));
#endif
			if (biow->error) {
				redod->error = biow->error;
			}
			destroy_bio_wrapper_for_redo(redod->wdev, biow);
		}
	}
}

/**
 * Create a bio wrapper for log read in redo.
 * You can submit the bio of returned bio wrapper.
 *
 * @wdev walb device (log device will be used for target).
 * @lsid target lsid to read.
 * @sectd sector data. if NULL then newly allocated.
 *
 * RETURN:
 *   bio wrapper in success, or false.
 */
static struct bio_wrapper* create_log_bio_wrapper_for_redo(
	struct walb_dev *wdev, u64 lsid, struct sector_data *sectd)
{
	struct bio *bio;
	struct bio_wrapper *biow;
	const unsigned int pbs = wdev->physical_bs;
	u64 off_lb, off_pb;
	int len;
	bool is_sectd_alloc = false;

	ASSERT(pbs <= PAGE_SIZE);

	if (!sectd) {
		is_sectd_alloc = true;
		sectd = sector_alloc(pbs, GFP_NOIO);
		if (!sectd) { goto error0; }
	}
	bio = bio_alloc(GFP_NOIO, 1);
	if (!bio) { goto error1; }
	biow = alloc_bio_wrapper_inc(wdev, GFP_NOIO);
	if (!biow) { goto error2; }

	bio->bi_bdev = wdev->ldev;
	off_pb = lsid % wdev->ring_buffer_size + wdev->ring_buffer_off;
	LOGd_("lsid: %"PRIu64" off_pb: %"PRIu64"\n", lsid, off_pb);
	off_lb = addr_lb(pbs, off_pb);
	bio->bi_sector = off_lb;
	bio->bi_rw = READ;
	bio->bi_end_io = bio_end_io_for_redo;
	bio->bi_private = biow;
	len = bio_add_page(bio, virt_to_page(sectd->data),
			pbs, offset_in_page(sectd->data));
	ASSERT(len == pbs);
	ASSERT(bio->bi_size == pbs);

	init_bio_wrapper(biow, bio);
	biow->private_data = sectd;

	return biow;
#if 0
error3:
	destroy_bio_wrapper_dec(wdev, biow);
#endif
error2:
	bio_put(bio);
error1:
	if (is_sectd_alloc) {
		sector_free(sectd);
	}
error0:
	return NULL;
}

/**
 * Prepare data bio for redo and assign in a bio wrapper.
 *
 * @wdev walb device.
 * @biow bio wrapper.
 *   biow->bio must be NULL.
 *   biow->private_data must be sector data to be written.
 * @pos IO position (address) in the deta device [logical block].
 * @len IO size [logical block].
 */
static bool prepare_data_bio_for_redo(
	struct walb_dev *wdev, struct bio_wrapper *biow,
	u64 pos, unsigned int len)
{
	struct bio *bio;
	struct sector_data *sectd;

	ASSERT(biow);
	ASSERT(!biow->bio);
	sectd = biow->private_data;
	ASSERT(sectd);

	bio = bio_alloc(GFP_NOIO, 1);
	if (!bio) { return false; }

	bio->bi_bdev = wdev->ddev;
	bio->bi_sector = pos;
	bio->bi_rw = WRITE;
	bio->bi_end_io = bio_end_io_for_redo;
	bio->bi_private = biow;
	bio_add_page(bio, virt_to_page(sectd->data),
		len * LOGICAL_BLOCK_SIZE, offset_in_page(sectd->data));
	ASSERT(bio->bi_size == len * LOGICAL_BLOCK_SIZE);

	init_bio_wrapper(biow, bio);
	biow->private_data = sectd;

	return true;
}

/**
 * Create discard bio wrapper for redo.
 *
 * @wdev walb device.
 * @pos IO position [logical block].
 * @len IO size [logical block].
 *
 * RETURN:
 *   Created bio_wrapper data in success, or NULL.
 */
static struct bio_wrapper* create_discard_bio_wrapper_for_redo(
	struct walb_dev *wdev, u64 pos, unsigned int len)
{
	struct bio *bio;
	struct bio_wrapper *biow;

	/* bio_alloc_(GFP_NOIO, 0) will cause kernel panic. */
	bio = bio_alloc(GFP_NOIO, 1);
	if (!bio) { goto error0; }
	biow = alloc_bio_wrapper_inc(wdev, GFP_NOIO);
	if (!biow) { goto error1; }

	bio->bi_bdev = wdev->ddev;
	bio->bi_sector = (sector_t)pos;
	bio->bi_rw = WRITE | REQ_DISCARD;
	bio->bi_end_io = bio_end_io_for_redo;
	bio->bi_private = biow;
	bio->bi_size = len;

	init_bio_wrapper(biow, bio);
	ASSERT(bio_wrapper_state_is_discard(biow));
	ASSERT(!biow->private_data);
	return biow;
#if 0
error2:
	destroy_bio_wrapper_dec(wdev, biow);
#endif
error1:
	bio_put(bio);
error0:
	return NULL;
}

/**
 * Destroy bio wrapper created by create_bio_wrapper_for_redo().
 */
static void destroy_bio_wrapper_for_redo(
	struct walb_dev *wdev, struct bio_wrapper* biow)
{
	struct sector_data *sectd;

	if (!biow) { return; }

	ASSERT(list_empty(&biow->bioe_list));

	if (biow->private_data) {
		sectd = biow->private_data;
		sector_free(sectd);
		biow->private_data = NULL;
	}
	if (biow->bio) {
		bio_put(biow->bio);
		biow->bio = NULL;
	}
	destroy_bio_wrapper_dec(wdev, biow);
}

/**
 * bio_end_io for redo.
 */
static void bio_end_io_for_redo(struct bio *bio, int error)
{
	struct bio_wrapper *biow;

	biow = bio->bi_private;
	ASSERT(biow);

	LOGd_("pos %"PRIu64"\n", (u64)biow->pos);
#ifdef WALB_DEBUG
	if (bio_wrapper_state_is_discard(biow)) {
		ASSERT(!biow->private_data);
	} else {
		ASSERT(biow->private_data); /* sector data */
	}
#endif

	biow->error = error;
	bio_put(bio);
	biow->bio = NULL;
	complete(&biow->done);
}

/**
 * Wait for all IOs for log read and destroy.
 */
static void wait_for_all_read_io_and_destroy(struct redo_data *read_rd)
{
	struct list_head biow_list;
	struct bio_wrapper *biow, *biow_next;
#ifdef WALB_DEBUG
	unsigned int len;
	bool is_empty;
#endif

	ASSERT(read_rd);
	INIT_LIST_HEAD(&biow_list);

	/* Get from queue. */
	spin_lock(&read_rd->queue_lock);
	list_for_each_entry_safe(biow, biow_next, &read_rd->queue, list) {
		list_move_tail(&biow->list, &biow_list);
		read_rd->queue_len--;
	}
#ifdef WALB_DEBUG
	len = read_rd->queue_len;
	is_empty = list_empty(&read_rd->queue);
#endif
	spin_unlock(&read_rd->queue_lock);
#ifdef WALB_DEBUG
	ASSERT(len == 0);
	ASSERT(is_empty);
#endif

	/* Wait for completion and destroy. */
	list_for_each_entry_safe(biow, biow_next, &biow_list, list) {
		const unsigned long timeo =
			msecs_to_jiffies(completion_timeo_ms_);
		unsigned long rtimeo;
		int c = 0;
		list_del(&biow->list);
	retry:
		rtimeo = wait_for_completion_timeout(&biow->done, timeo);
		if (rtimeo == 0) {
			LOGw("timeout(%d): biow %p pos %"PRIu64" len %u\n",
				c, biow, (u64)biow->pos, biow->len);
			c++;
			goto retry;
		}
		destroy_bio_wrapper_for_redo(read_rd->wdev, biow);
	}
	ASSERT(list_empty(&biow_list));
}

/**
 * Wait for all write io(s) for redo.
 */
static void wait_for_all_write_io_for_redo(struct walb_dev *wdev)
{
	wait_for_all_pending_io_done(wdev);
}

/**
 * Wait for all gc tasks for redo.
 */
static void wait_for_all_gc_tasks_for_redo(struct redo_data *gc_rd)
{
	bool is_empty;

	while (true) {
		spin_lock(&gc_rd->queue_lock);
		is_empty = list_empty(&gc_rd->queue);
		spin_unlock(&gc_rd->queue_lock);

		if (is_empty) { break; }
		msleep(100);
	}
}

/**
 * Get bio wrapper(s) from a read queue.
 *
 * @read_rd redo data for log read.
 * @biow_list biow will be inserted to the list.
 * @n number of bio wrappers to try to get.
 *
 * RETURN:
 *   Number of bio wrapper(s) gotten.
 */
static unsigned int get_bio_wrapper_from_read_queue(
	struct redo_data *read_rd, struct list_head *biow_list,
	unsigned int n)
{
	unsigned int n_biow = 0;
	struct bio_wrapper *biow, *biow_next;

	ASSERT(read_rd);
	ASSERT(biow_list);

	if (n == 0) { goto fin; }

	spin_lock(&read_rd->queue_lock);
	list_for_each_entry_safe(biow, biow_next, &read_rd->queue, list) {
		list_move_tail(&biow->list, biow_list);
		read_rd->queue_len--;
		n_biow++;
		if (n_biow == n) {
			break;
		}
	}
	spin_unlock(&read_rd->queue_lock);
fin:
	return n_biow;
}

/**
 * Get logpack header biow.
 *
 * @read_rd redo data for read.
 * @written_lsid written_lsid.
 *
 * RETURN:
 *   bio wrapper if it is valid logpack header, or NULL.
 */
static struct bio_wrapper* get_logpack_header_for_redo(
	struct worker_data *read_wd, struct redo_data *read_rd,
	u64 written_lsid)
{
	unsigned int n;
	struct list_head biow_list;
	struct bio_wrapper *biow;
	struct sector_data *sectd;
	const struct walb_logpack_header *logh;

	ASSERT(read_rd);
	INIT_LIST_HEAD(&biow_list);
retry:
	n = get_bio_wrapper_from_read_queue(read_rd, &biow_list, 1);
	if (n < 1) {
		wakeup_worker(read_wd);
		schedule();
		goto retry;
	}
	ASSERT(!list_empty(&biow_list));
	biow = list_first_entry(&biow_list, struct bio_wrapper, list);

	/* Wait for completion */
	LOGd_("wait_for_completion %"PRIu64"\n", written_lsid);
	wait_for_completion(&biow->done);

	/* Logpack header check. */
	ASSERT(biow);
	sectd = biow->private_data;
	ASSERT_SECTOR_DATA(sectd);
	logh = get_logpack_header_const(sectd);
	if (is_valid_logpack_header_with_checksum(
			logh, sectd->size, read_rd->wdev->log_checksum_salt)
		&& logh->logpack_lsid == written_lsid) {
		return biow;
	} else {
		destroy_bio_wrapper_for_redo(read_rd->wdev, biow);
		return NULL;
	}
}

/**
 * Redo logpack.
 *
 * If the logpack is partially valid,
 * invalid IOs records will be deleted from the logpack header
 * and the updated logpack header will be written to the log device.
 *
 * @read_rd redo data for read.
 * @gc_rd redo data for gc.
 * @logh_biow !!!valid!!! logpack header biow.
 *   This logpack header will be updated
 *   if the logpack is partially invalid.
 *   logh_biow will be destroyed in the function.
 * @written_lsid_p pointer to written_lsid.
 * @should_terminate when true redo should be terminated.
 *
 * RETURN:
 *   true if redo succeeded, or false (due to IO error etc.)
 */
static bool redo_logpack(
	struct worker_data *read_wd, struct redo_data *read_rd,
	struct redo_data *gc_rd,
	struct bio_wrapper *logh_biow, u64 *written_lsid_p,
	bool *should_terminate)
{
	struct walb_dev *wdev;
	struct sector_data *sectd;
	struct walb_logpack_header *logh;
	unsigned int i, invalid_idx = 0;
	struct list_head biow_list_pack, biow_list_io, biow_list_ready;
	unsigned int n_pb, n_lb, n;
	unsigned int pbs;
	struct bio_wrapper *biow, *biow_next;
	u32 csum;
	bool is_valid = true;
	int error = 0;
	struct blk_plug plug;
	bool retb = true;

	ASSERT(read_rd);
	wdev = read_rd->wdev;
	ASSERT(wdev);
	pbs = wdev->physical_bs;
	ASSERT(gc_rd);
	INIT_LIST_HEAD(&biow_list_pack);
	INIT_LIST_HEAD(&biow_list_io);
	INIT_LIST_HEAD(&biow_list_ready);
	ASSERT(logh_biow);
	sectd = logh_biow->private_data;
	ASSERT_SECTOR_DATA(sectd);

	logh = get_logpack_header(sectd);
	ASSERT(logh);

	n_pb = 0;
retry1:
	n_pb += get_bio_wrapper_from_read_queue(
		read_rd, &biow_list_pack,
		logh->total_io_size - n_pb);
	if (n_pb < logh->total_io_size) {
		wakeup_worker(read_wd);
		LOGd_("n_pb %u total_io_size %u\n", n_pb, logh->total_io_size);
		schedule();
		goto retry1;
	}
	ASSERT(n_pb == logh->total_io_size);

	/* Wait for log read IO completion. */
	list_for_each_entry(biow, &biow_list_pack, list) {
		wait_for_completion(&biow->done);
	}

	for (i = 0; i < logh->n_records; i++) {
		struct walb_log_record *rec = &logh->record[i];
		const bool is_discard =
			test_bit_u32(LOG_RECORD_DISCARD, &rec->flags);
		const bool is_padding =
			test_bit_u32(LOG_RECORD_PADDING, &rec->flags);

		ASSERT(test_bit_u32(LOG_RECORD_EXIST, &rec->flags));
		ASSERT(list_empty(&biow_list_io));

		n_lb = rec->io_size;
		if (n_lb == 0) {
			/* zero-sized IO. */
			continue;
		}
		n_pb = capacity_pb(pbs, n_lb);

		if (is_discard) {
			if (blk_queue_discard(bdev_get_queue(wdev->ddev))) {
				create_discard_data_io_for_redo(
					wdev, rec, &biow_list_ready);
			} else {
				/* Do nothing. */
			}
			continue;
		}

		/*
		 * Normal IO.
		 */

		/* Move the corresponding biow to biow_list_io. */
		ASSERT(list_empty(&biow_list_io));
		n = 0;
		list_for_each_entry_safe(biow, biow_next, &biow_list_pack, list) {
			if (biow->error) {
				error = biow->error;
			}
			list_move_tail(&biow->list, &biow_list_io);
			n++;
			if (n == n_pb) { break; }
		}
		if (error) {
			retb = false;
			goto fin;
		}

		/* Padding record and data is just ignored. */
		if (is_padding) {
			list_for_each_entry_safe(biow, biow_next,
						&biow_list_io, list) {
				list_del(&biow->list);
				destroy_bio_wrapper_for_redo(wdev, biow);
			}
			continue;
		}

		/* Validate checksum. */
		csum = calc_checksum_for_redo(
			rec->io_size, pbs,
			wdev->log_checksum_salt, &biow_list_io);
		if (csum != rec->checksum) {
			is_valid = false;
			invalid_idx = i;
			break;
		}

		/* Create data bio. */
		create_data_io_for_redo(wdev, rec, &biow_list_io);
		list_for_each_entry_safe(biow, biow_next, &biow_list_io, list) {
			list_move_tail(&biow->list, &biow_list_ready);
		}
	}

	/* Submit ready biow(s). */
	blk_start_plug(&plug);
	list_for_each_entry(biow, &biow_list_ready, list) {
		LOGd_("submit data bio pos %"PRIu64" len %u\n",
			(u64)biow->pos, biow->len);
		submit_data_bio_for_redo(wdev, biow);
	}
	blk_finish_plug(&plug);

	/* Enqueue submitted biow(s) for gc. */
	spin_lock(&gc_rd->queue_lock);
	list_for_each_entry_safe(biow, biow_next, &biow_list_ready, list) {
		list_move_tail(&biow->list, &gc_rd->queue);
		gc_rd->queue_len++;
	}
	spin_unlock(&gc_rd->queue_lock);
	ASSERT(list_empty(&biow_list_ready));

	/*
	 * Case (1): valid.
	 */
	if (is_valid) {
		ASSERT(list_empty(&biow_list_pack));
		*written_lsid_p = logh->logpack_lsid + 1 + logh->total_io_size;
		*should_terminate = false;
		retb = true;
		goto fin;
	}

	/*
	 * Case (2): fully invalid.
	 */
	if (invalid_idx == 0) {
		/* The whole logpack will be discarded. */
		*written_lsid_p = logh->logpack_lsid;
		*should_terminate = true;
		retb = true;
		goto fin;
	}

	/*
	 * Case (3): partially invalid.
	 */
	/* Update logpack header. */
	for (i = invalid_idx; i < logh->n_records; i++) {
		log_record_init(&logh->record[i]);
	}
	logh->n_records = invalid_idx;
	/* Re-calculate total_io_size and n_padding. */
	logh->total_io_size = 0;
	logh->n_padding = 0;
	for (i = 0; i < logh->n_records; i++) {
		struct walb_log_record *rec = &logh->record[i];
		if (!test_bit_u32(LOG_RECORD_DISCARD, &rec->flags)) {
			logh->total_io_size += capacity_pb(
				pbs, rec->io_size);
		}
		if (test_bit_u32(LOG_RECORD_PADDING, &rec->flags)) {
			logh->n_padding++;
		}
	}
	ASSERT(logh->total_io_size > 0);
	logh->checksum = 0;
	logh->checksum = checksum(
		(const u8 *)logh, pbs, wdev->log_checksum_salt);
	/* Try to overwrite the last logpack header block. */
	logh_biow->private_data = NULL;
	destroy_bio_wrapper_for_redo(wdev, logh_biow);
retry2:
	logh_biow = create_log_bio_wrapper_for_redo(
		wdev, logh->logpack_lsid, sectd);
	if (!logh_biow) {
		schedule();
		goto retry2;
	}
	logh_biow->bio->bi_rw = WRITE_FLUSH_FUA;
	generic_make_request(logh_biow->bio);
	wait_for_completion(&logh_biow->done);
	if (logh_biow->error) {
		LOGe("Updated logpack header IO failed.");
		retb = false;
		goto fin;
	}
	*written_lsid_p = logh->logpack_lsid + 1 + logh->total_io_size;
	*should_terminate = true;
	retb = true;

fin:
	/* Destroy remaining biow(s). */
	list_for_each_entry_safe(biow, biow_next, &biow_list_io, list) {
		list_del(&biow->list);
		destroy_bio_wrapper_for_redo(wdev, biow);
	}
	list_for_each_entry_safe(biow, biow_next, &biow_list_pack, list) {
		list_del(&biow->list);
		destroy_bio_wrapper_for_redo(wdev, biow);
	}
	destroy_bio_wrapper_for_redo(wdev, logh_biow);
	return retb;
}

/**
 * Calculate checksum for redo.
 *
 * @n_lb io size [logical block].
 * @pbs physical block size [bytes].
 * @salt checksum salt.
 * @biow_list biow list where each biow size is pbs.
 *
 * RETURN:
 *   checksum of the IO data.
 */
static u32 calc_checksum_for_redo(
	unsigned int n_lb, unsigned int pbs, u32 salt,
	struct list_head *biow_list)
{
	struct bio_wrapper *biow;
	u32 csum = salt;
	unsigned int len;
	struct sector_data *sectd;

	ASSERT(n_lb > 0);
	ASSERT_PBS(pbs);
	ASSERT(biow_list);
	ASSERT(!list_empty(biow_list));

	list_for_each_entry(biow, biow_list, list) {
		sectd = biow->private_data;
		ASSERT_SECTOR_DATA(sectd);
		ASSERT(sectd->size == pbs);
		ASSERT(biow->len == n_lb_in_pb(pbs));
		ASSERT(n_lb > 0);

		if (biow->len <= n_lb) {
			len = biow->len;
		} else {
			len = n_lb;
		}
		csum = checksum_partial(
			csum, sectd->data, len * LOGICAL_BLOCK_SIZE);
		n_lb -= len;
	}
	ASSERT(n_lb == 0);
	return checksum_finish(csum);
}

/**
 * Create data io for redo.
 *
 * @wdev walb device.
 * @rec log record.
 * @biow_list biow list
 *   where each biow->private data is sector data.
 *   Each biow will be replaced for data io.
 */
static void create_data_io_for_redo(
	struct walb_dev *wdev,
	struct walb_log_record *rec,
	struct list_head *biow_list)
{
	unsigned int n_lb, n_pb;
	unsigned int pbs = wdev->physical_bs;
	struct bio_wrapper *biow, *biow_next;
	u64 off;
	unsigned int len;
	struct list_head new_list;

	ASSERT(rec);
	ASSERT_PBS(pbs);
	ASSERT(biow_list);
	ASSERT(!list_empty(biow_list));
	ASSERT(!test_bit_u32(LOG_RECORD_DISCARD, &rec->flags));

	off = rec->offset;
	n_lb = rec->io_size;
	n_pb = capacity_pb(pbs, n_lb);

	INIT_LIST_HEAD(&new_list);
	list_for_each_entry_safe(biow, biow_next, biow_list, list) {
		if (biow->len <= n_lb) {
			len = biow->len;
		} else {
			len = n_lb;
		}
		list_del(&biow->list);
	retry:
		if (!prepare_data_bio_for_redo(wdev, biow, off, len)) {
			schedule();
			goto retry;
		}
		list_add_tail(&biow->list, &new_list);

		n_lb -= len;
		off += len;
		n_pb--;
	}
	ASSERT(n_lb == 0);
	ASSERT(n_pb == 0);
	ASSERT(list_empty(biow_list));

	list_for_each_entry_safe(biow, biow_next, &new_list, list) {
		list_move_tail(&biow->list, biow_list);
	}
	ASSERT(list_empty(&new_list));
}

/**
 * Create discard data io for redo.
 *
 * @wdev walb device.
 * @rec log record (must be discard)
 * @biow_list biow list
 *   created bio wrapper will be added to the tail.
 */
static void create_discard_data_io_for_redo(
	struct walb_dev *wdev,
	struct walb_log_record *rec,
	struct list_head *biow_list)
{
	struct bio_wrapper *biow;

	ASSERT(rec);
	ASSERT(test_bit_u32(LOG_RECORD_DISCARD, &rec->flags));

retry:
	biow = create_discard_bio_wrapper_for_redo(wdev, rec->offset, rec->io_size);
	if (!biow) {
		schedule();
		goto retry;
	}
	list_add_tail(&biow->list, biow_list);
}

/**
 * Submit data bio for redo.
 *
 * @wdev walb device.
 * @biow target bio is biow->bio.
 */
static void submit_data_bio_for_redo(
	UNUSED struct walb_dev *wdev, struct bio_wrapper *biow)
{
#ifdef WALB_OVERLAPPED_SERIALIZE
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);
	bool is_overlapped_insert_succeeded;
#endif

	ASSERT(biow);

#ifdef WALB_OVERLAPPED_SERIALIZE
	/* check and insert to overlapped detection data. */
retry_insert_ol:
	spin_lock(&iocored->overlapped_data_lock);
	is_overlapped_insert_succeeded =
		overlapped_check_and_insert(
			iocored->overlapped_data,
			&iocored->max_sectors_in_overlapped,
			biow, GFP_ATOMIC
#ifdef WALB_DEBUG
			, &iocored->overlapped_in_id
#endif
			);
	spin_unlock(&iocored->overlapped_data_lock);
	if (!is_overlapped_insert_succeeded) {
		schedule();
		goto retry_insert_ol;
	}
	ASSERT(biow->n_overlapped >= 0);
	if (biow->n_overlapped == 0) {
		generic_make_request(biow->bio);
	} else {
		LOGd_("n_overlapped %u\n", biow->n_overlapped);
	}
#else /* WALB_OVERLAPPED_SERIALIZE */
	generic_make_request(biow->bio);
#endif /* WALB_OVERLAPPED_SERIALIZE */
}

/*******************************************************************************
 * Global functions definition.
 *******************************************************************************/

/**
 * Execute redo.
 *
 * Start: wdev->written_lsid
 * End: lsid with checksum invalid.
 *
 * written_lsid will be updated by the new
 * written_lsid, completed_lsid, and latest_lsid.
 */
bool execute_redo(struct walb_dev *wdev)
{
	unsigned int minor;
	struct worker_data *read_wd, *gc_wd;
	struct redo_data *read_rd, *gc_rd;
	struct list_head biow_list;
	struct bio_wrapper *logh_biow;
	unsigned int pbs;
	u64 written_lsid, start_lsid;
	int err;
	bool failed = false;
	bool should_terminate;
	bool retb;
	int ret;
	struct timespec ts[2];
	u64 n_logpack = 0;

	ASSERT(wdev);
	minor = MINOR(wdev->devt);
	pbs = wdev->physical_bs;

	/* Allocate resources and prepare workers.. */
	read_wd = alloc_worker(GFP_KERNEL);
	if (!read_wd) { goto error0; }
	gc_wd = alloc_worker(GFP_KERNEL);
	if (!gc_wd) { goto error1; }

	ret = snprintf(read_wd->name, WORKER_NAME_MAX_LEN,
		"%s/%u", "redo_read", minor / 2);
	ASSERT(ret < WORKER_NAME_MAX_LEN);
	ret = snprintf(gc_wd->name, WORKER_NAME_MAX_LEN,
		"%s/%u", "redo_gc", minor / 2);
	ASSERT(ret < WORKER_NAME_MAX_LEN);

	spin_lock(&wdev->lsid_lock);
	written_lsid = wdev->lsids.written;
	spin_unlock(&wdev->lsid_lock);
	start_lsid = written_lsid;
	read_rd = create_redo_data(wdev, written_lsid);
	if (!read_rd) { goto error2; }
	gc_rd = create_redo_data(wdev, written_lsid);
	if (!gc_rd) { goto error3; }

	LOGn("Redo will start from lsid %"PRIu64".\n", written_lsid);

	/* Run workers. */
	initialize_worker(read_wd,
			run_read_log_in_redo, (void *)read_rd);
	initialize_worker(gc_wd,
			run_gc_log_in_redo, (void *)gc_rd);

	/* Get biow and construct log pack and submit redo IOs. */
	INIT_LIST_HEAD(&biow_list);
	getnstimeofday(&ts[0]);
	while (true) {
		/* Get logpack header. */
		logh_biow = get_logpack_header_for_redo(
			read_wd, read_rd, written_lsid);
		if (!logh_biow) {
			/* Redo should be terminated. */
			break;
		}

		/* Check IO error of the logpack header. */
		if (logh_biow->error) {
			destroy_bio_wrapper_for_redo(wdev, logh_biow);
			failed = true;
			break;
		}

		/* Try to redo the logpack. */
		LOGd_("Try to redo (lsid %"PRIu64")\n", written_lsid);
		if (!redo_logpack(read_wd, read_rd, gc_rd,
					logh_biow, &written_lsid,
					&should_terminate)) {
			/* IO error occurred. */
			failed = true;
			break;
		}
		n_logpack++;

		if (should_terminate) {
			break;
		}
		wakeup_worker(gc_wd);
		wakeup_worker(read_wd);
	}

	/* Finalize. */
	finalize_worker(read_wd);
	wait_for_all_read_io_and_destroy(read_rd);
	wakeup_worker(gc_wd);
	wait_for_all_write_io_for_redo(wdev);
	wait_for_all_gc_tasks_for_redo(gc_rd);
	finalize_worker(gc_wd);

	/* Now the redo task has done. */

	/* Free resources. */
	destroy_redo_data(gc_rd);
	destroy_redo_data(read_rd);
	free_worker(gc_wd);
	free_worker(read_wd);

	if (failed) {
		LOGe("IO error occurred during redo.\n");
		return false;
	}

	/* flush data device. */
	err = blkdev_issue_flush(wdev->ddev, GFP_KERNEL, NULL);
	if (err) {
		LOGe("Data device flush failed.");
		return false;
	} else {
		LOGn("Redo has done with lsid %"PRIu64".\n", written_lsid);
	}

	/* Update lsid variables. */
	spin_lock(&wdev->lsid_lock);
	wdev->lsids.prev_written = written_lsid;
	wdev->lsids.written = written_lsid;
#ifdef WALB_FAST_ALGORITHM
	wdev->lsids.completed = written_lsid;
#endif
	wdev->lsids.permanent = written_lsid;
	wdev->lsids.flush = written_lsid;
	wdev->lsids.latest = written_lsid;
	spin_unlock(&wdev->lsid_lock);

	/* Update superblock. */
	retb = walb_sync_super_block(wdev);
	if (!retb) {
		LOGe("Superblock sync failed.\n");
		return false;
	}

	/* Get end time. */
	getnstimeofday(&ts[1]);
	ts[0] = timespec_sub(ts[1], ts[0]);
	LOGn("Redo period: %ld.%09ld second\n", ts[0].tv_sec, ts[0].tv_nsec);
	LOGn("Redo %"PRIu64" logpack of totally %"PRIu64" physical blocks.\n",
		n_logpack, written_lsid - start_lsid);

	return true;
#if 0
error4:
	destroy_redo_data(gc_rd);
#endif
error3:
	destroy_redo_data(read_rd);
error2:
	free_worker(gc_wd);
error1:
	free_worker(read_wd);
error0:
	return false;
}

MODULE_LICENSE("Dual BSD/GPL");
