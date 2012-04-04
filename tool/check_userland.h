/**
 * Assesion of userland code for preprocessor.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_CHECK_USERLAND_H_USERLAND
#define WALB_CHECK_USERLAND_H_USERLAND

#ifdef __KERNEL__
#error This header does not support kernel code.
#endif

#endif /* WALB_CHECK_USERLAND_H_USERLAND */
