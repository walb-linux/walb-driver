/**
 * simple_blk_bio_mem.c - make_request_fn which do memory read/write.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"
#include <linux/blkdev.h>

#include "walb/block_size.h"
#include "simple_blk_bio.h"
#include "memblk_data.h"

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

UNUSED static void log_bi_rw_flag(struct bio *bio);
static void mdata_exec_bio(struct memblk_data *mdata, struct bio *bio);
static struct memblk_data* get_mdata_from_queue(struct request_queue *q);

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * For debug.
 */
static void log_bi_rw_flag(struct bio *bio)
{
        LOGd("bio bi_sector %"PRIu64" bi_rw %0lx bi_size %u bi_vcnt %hu\n",
             (u64)bio->bi_sector, bio->bi_rw, bio->bi_size, bio->bi_vcnt);
        LOGd("bi_rw: %s %s %s %s %s.\n",
             (bio->bi_rw & REQ_WRITE ? "REQ_WRITE" : ""),
             (bio->bi_rw & REQ_RAHEAD? "REQ_RAHEAD" : "") ,
             (bio->bi_rw & REQ_FLUSH ? "REQ_FLUSH" : ""),
             (bio->bi_rw & REQ_FUA ? "REQ_FUA" : ""),
             (bio->bi_rw & REQ_DISCARD ? "REQ_DISCARD" : ""));
}

/**
 * Read/write from/to mdata.
 * CONTEXT:
 * IRQ.
 */
static void mdata_exec_bio(struct memblk_data *mdata, struct bio *bio)
{
        int i;
        sector_t sector;
        u64 block_id;
        UNUSED struct bio_vec *bvec;
        u8 *buffer_bio;
        unsigned int is_write;
        unsigned int n_blk;

        ASSERT(bio);

        sector = bio->bi_sector;
        block_id = (u64)sector;

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
               __bio_kunmap_atomic(bio, KM_USER0);
        }
}

/**
 * Get mdata from a queue.
 */
static struct memblk_data* get_mdata_from_queue(struct request_queue *q)
{
        return (struct memblk_data *)sdev_get_from_queue(q)->private_data;
}

/*******************************************************************************
 * Global functions definition.
 *******************************************************************************/

/**
 * Make request.
 *
 * CONTEXT:
 * IRQ.
 */
void simple_blk_bio_make_request(struct request_queue *q, struct bio *bio)
{
        ASSERT(bio);
        mdata_exec_bio(get_mdata_from_queue(q), bio);
        bio_endio(bio, 0);
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
        struct memblk_data *mdata = NULL;
        u64 capacity;
        unsigned int block_size;

        ASSERT(sdev);

        capacity = sdev->capacity;
        block_size = LOGICAL_BLOCK_SIZE;
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
 * Do nothing.
 */
void customize_sdev(struct simple_blk_dev *sdev)
{
}

/**
 * Do nothing.
 */
bool pre_register(void)
{
        return true;
}

/**
 * Do nothing.
 */
void post_unregister(void)
{
}

/* end of file. */
