/**
 * memblk_io_simple.c - make_request which simply read/write blocks.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"

#include <linux/blkdev.h>
#include "walb/common.h"
#include "memblk_io.h"
#include "memblk.h"

/*******************************************************************************
 * Static functions prototype.
 *******************************************************************************/

static int execute_bio(struct memblk_dev *mdev, struct bio *bio);

/*******************************************************************************
 * Static functions definition.
 *******************************************************************************/

/**
 * Read/write memory block devices.
 * @mdev Memory block device.
 * @bio bio to be executed.
 * CONTEXT:
 * Inside IRQ.
 * mdev->index is not locked inside.
 * RETURN:
 * Status. status argument for bio_endio(bio, status);
 */
static int execute_bio(struct memblk_dev *mdev, struct bio *bio)
{
        int i;
        u8 *buf_mem, *buf_bvec;
        struct bio_vec *bvec;
        u64 sector_in_lb;
        u64 sector_in_pb;
        unsigned long off_mem;
        u32 bvec_size;
        
        ASSERT(mdev);
        ASSERT(bio);
        sector_in_lb = (u64)bio->bi_sector;
        sector_in_pb = MCALL(&mdev->bs_op, required_n_pb, sector_in_lb);
        off_mem = (unsigned long)MCALL(&mdev->bs_op, off_in_p, sector_in_lb);

        ASSERT(bio->bi_size <= mdev->bs_op.physical_bs - (u32)off_mem);
        
        buf_mem = get_physical_sector(mdev, sector_in_pb);
        ASSERT(buf_mem);
        
        bio_for_each_segment(bvec, bio, i) {
                buf_bvec = __bio_kmap_atomic(bio, i, KM_USER0);
                bvec_size = bio_cur_bytes(bio);
                if (bio_data_dir(bio) == WRITE) {
                        memcpy(buf_mem + off_mem, buf_bvec, bvec_size);
                } else {
                        memcpy(buf_bvec, buf_mem + off_mem, bvec_size);
                }
                ASSERT(bvec_size % mdev->bs_op.logical_bs == 0);
                off_mem += bvec_size;
                __bio_kunmap_atomic(bio, KM_USER0);
        }
        return 0; /* always success. */
}

/*******************************************************************************
 * Global functions definition.
 *******************************************************************************/

/**
 * Make request.
 * Currently do nothing.
 */
int memblk_make_request(struct request_queue *q, struct bio *bio)
{
        int status;
        struct memblk_dev *mdev = q->queuedata;
        ASSERT(mdev);

        status = execute_bio(mdev, bio);
        bio_endio(bio, status);

        if (status) {
                LOGe("IO failed (%"PRIu64"u:%"PRIu64"u)\n",
                     bio->bi_sector, bio->bi_size);
        }
        return 0; /* never retry */
}

/* end of file */
