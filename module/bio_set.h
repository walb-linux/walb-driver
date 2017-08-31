/**
 * bio_set.h - bio set definition.
 *
 * (C) 2017 Cybozu Labs, Inc.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_BIO_SET_H_KERNEL
#define WALB_BIO_SET_H_KERNEL

#include "check_kernel.h"
#include <linux/types.h>
#include <linux/blk_types.h>


extern struct bio_set *walb_bio_set_;


bool walb_bio_set_init(void);
void walb_bio_set_exit(void);

#endif /* WALB_BIO_SET_H_KERNEL */
