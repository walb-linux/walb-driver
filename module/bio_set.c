/**
 * bio_set.c - functions for bio_set.
 *
 * (C) 2017 Cybozu Labs, Inc.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"
#include <linux/bio.h>
#include "bio_set.h"


struct bio_set *walb_bio_set_ = NULL;


bool walb_bio_set_init(void)
{
	if (walb_bio_set_)
		return true;

	/*
	 * BIOSET_NEED_BVECS is not set,
	 * so you can use walb_bio_set_ for bio_clone_fast() only.
	 * Do not use bio_clone_bioset().
	 */
	walb_bio_set_ = bioset_create(BIO_POOL_SIZE, 0, BIOSET_NEED_RESCUER);

	if (!walb_bio_set_)
		goto error;

	return true;
error:
	return false;
}


void walb_bio_set_exit(void)
{
	if (walb_bio_set_) {
		bioset_free(walb_bio_set_);
		walb_bio_set_ = NULL;
	}
}
