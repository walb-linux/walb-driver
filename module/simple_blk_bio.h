/**
 * simple_blk_bio.h - Definition for simple_blk_bio operations.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_SIMPLE_BLK_BIO_H_KERNEL
#define WALB_SIMPLE_BLK_BIO_H_KERNEL

#include "check_kernel.h"
#include <linux/blkdev.h>
#include "simple_blk.h"

/*******************************************************************************
 * Global functions prototype.
 *******************************************************************************/

/* Make requrest for simpl_blk_bio_* modules. */
void simple_blk_bio_make_request(struct request_queue *q, struct bio *bio);

/* Called before register. */
bool pre_register(void);
/* Called after unregister. */
void post_unregister(void);

/* Create private data for sdev. */
bool create_private_data(struct simple_blk_dev *sdev);
/* Destroy private data for ssev. */
void destroy_private_data(struct simple_blk_dev *sdev);
/* Customize sdev after register before start. */
void customize_sdev(struct simple_blk_dev *sdev);

/* Create a workqueue that type is specified by module parameter. */
struct workqueue_struct* create_wq_io(const char *name);

#endif /* WALB_SIMPLE_BLK_BIO_H_KERNEL */
