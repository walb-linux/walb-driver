/**
 * Assesion of userland code for preprocessor.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_CHECK_USERLAND_H_USERLAND
#define _WALB_CHECK_USERLAND_H_USERLAND

#ifdef __KERNEL__
#error This header does not support kernel code.
#endif

#endif /* _WALB_CHECK_USERLAND_H_USERLAND */
