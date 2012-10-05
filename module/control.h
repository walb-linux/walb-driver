/**
 * control.h - Header of walb control functions.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_CONTROL_H_KERNEL
#define WALB_CONTROL_H_KERNEL

#include "check_kernel.h"

#include "walb/ioctl.h"

void* walb_alloc_and_copy_from_user(
	void __user *userbuf,
	size_t buf_size,
	gfp_t gfp_mask);
int walb_copy_to_user_and_free(
	void __user *userbuf,
	void *buf,
	size_t buf_size);
struct walb_ctl* walb_get_ctl(void __user *userctl, gfp_t gfp_mask);
int walb_put_ctl(void __user *userctl, struct walb_ctl *ctl);

int walb_control_init(void);
void walb_control_exit(void);

#endif /* WALB_CONTROL_H_KERNEL */
