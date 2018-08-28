/**
 * bio_set.c - functions for bio_set.
 *
 * (C) 2017 Cybozu Labs, Inc.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"
#include <linux/bio.h>
#include <linux/slab.h>
#include <linux/version.h>
#include "bio_set.h"


struct bio_set *walb_bio_set_ = NULL;


#if LINUX_VERSION_CODE < KERNEL_VERSION(4, 18, 0)
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
	if (!walb_bio_set_)
		return;

	bioset_free(walb_bio_set_);
	walb_bio_set_ = NULL;
}

#else /* #if LINUX_VERSION_CODE < KERNEL_VERSION(4, 18, 0) */

bool walb_bio_set_init(void)
{
	if (walb_bio_set_)
		return true;

	walb_bio_set_ = kzalloc(sizeof(struct bio_set), GFP_KERNEL);
	if (!walb_bio_set_)
		goto error0;

	/*
	 * BIOSET_NEED_BVECS is not set,
	 * so you can use walb_bio_set_ for bio_clone_fast() only.
	 * Do not use bio_clone_bioset().
	 */
	if (bioset_init(walb_bio_set_, BIO_POOL_SIZE, 0, BIOSET_NEED_RESCUER))
		goto error1;

	return true;
error1:
	kfree(walb_bio_set_);
	walb_bio_set_ = NULL;
error0:
	return false;
}


void walb_bio_set_exit(void)
{
	if (!walb_bio_set_)
		return;

	bioset_exit(walb_bio_set_);
	kfree(walb_bio_set_);
	walb_bio_set_ = NULL;
}
#endif /* #if LINUX_VERSION_CODE < KERNEL_VERSION(4, 18, 0) */
