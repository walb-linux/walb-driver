/**
 * io.h - IO operations.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_IO_H_KERNEL
#define _WALB_IO_H_KERNEL

#include "check_kernel.h"

#include "walb/bitmap.h"
#include "walb/sector.h"
#include "util.h"

/**
 * BIO wrapper flag.
 */
#define WALB_BIO_INIT    0
#define WALB_BIO_END     1
#define WALB_BIO_ERROR   2

struct walb_ddev_bio {

        struct request *req; /* wrapper-level request */

        struct list_head *head; /* list head */
        struct list_head list;
        
        /* sector_t offset; /\* io offset *\/ */
        /* int iosize;      /\* io size *\/ */

        int status;
        
        struct bio *bio; /* bio for underlying device */

};

/**
 * Work to deal with multiple bio(s).
 */
struct walb_submit_bio_work
{
        struct list_head list; /* list of walb_ddev_bio */
        spinlock_t lock; /* lock for the list */
        struct work_struct work;
};

/**
 * Bio wrapper with completion.
 */
struct walb_bio_with_completion
{
        struct bio *bio;
        struct completion wait;
        int status;
        struct list_head list;
};

/**
 * Work to deal with multiple bio(s).
 * Using bitmap instead list.
 */
struct walb_bios_work
{
        struct work_struct work;
        struct block_device *bdev;
        struct request *req_orig; /* Original request. */
        
        int n_bio; /* Number of bio(s) managed in this object. */
        struct walb_bitmap *end_bmp; /* Bitmap size is n_bio. */
        struct bio **biop_ary; /* Array of bio pointer with n_bio size. */
        atomic_t is_fail; /* non-zero if failed. */
};

/**
 * Work to deal with multiple bio(s).
 */
struct walb_bioclist_work
{
        struct work_struct work;
        struct block_device *bdev;
        struct request *req_orig;
};


/*******************************************************************************
 * Prototypes.
 *******************************************************************************/

/* Utility functions. */
int walb_rq_count_bio(struct request *req);

/* Sector IO function. */
int sector_io(int rw, struct block_device *bdev,
              u64 off, struct sector_data *sect);

/* End IO callback for struct walb_bio_with_completion. */
void walb_end_io_with_completion(struct bio *bio, int error);


/* Submit IO functions with
   struct walb_ddev_bio and struct walb_submit_bio_work. */
void walb_make_ddev_request(struct block_device *bdev, struct request *req);
void walb_submit_bio_task(struct work_struct *work);
void walb_ddev_end_io(struct bio *bio, int error);
void walb_init_ddev_bio(struct walb_ddev_bio *dbio);


/* Submit IO function with
   struct walb_bios_work. */
void walb_forward_request_to_ddev(struct block_device *bdev,
                                  struct request *req);
void walb_bios_work_task(struct work_struct *work);
void walb_bios_work_end_io(struct bio *bio, int error);

struct walb_bios_work* walb_create_bios_work(
        struct block_device *bdev, struct request *req_orig, gfp_t gfp_mask);
void walb_destroy_bios_work(struct walb_bios_work* wk);
int walb_fill_bios_work(struct walb_bios_work* wk, gfp_t gfp_mask);
int walb_clone_bios_work(struct walb_bios_work* wk, gfp_t gfp_mask);
void walb_submit_bios_work(struct walb_bios_work* wk);


/* Submit IO functions with
   struct walb_bioclist_work. */
void walb_forward_request_to_ddev2(struct block_device *bdev,
                                   struct request *req);
void walb_bioclist_work_task(struct work_struct *work);

struct walb_bioclist_work* walb_create_bioclist_work(
        struct block_device *bdev, struct request *req, gfp_t gfp_mask);
void walb_destroy_bioclist_work(struct walb_bioclist_work *wk);


/* Deprecated functions. */
#if 0
void walb_transfer(struct walb_dev *dev, unsigned long sector,
            unsigned long nbytes, char *buffer, int write);
int walb_xfer_bio(struct walb_dev *dev, struct bio *bio);
int walb_xfer_segment(struct walb_dev *dev,
               struct req_iterator *iter);
int walb_xfer_request(struct walb_dev *dev, struct request *req);
void walb_full_request(struct request_queue *q);
int walb_make_request(struct request_queue *q, struct bio *bio);
#endif

#endif /* _WALB_IO_H_KERNEL */
