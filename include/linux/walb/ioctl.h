/**
 * ioctl.h - data structure definictions for walb ioctl interface.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_IOCTL_H
#define WALB_IOCTL_H

#include "walb.h"
#include "print.h"
#include "check.h"

#ifdef __KERNEL__
#include <linux/kernel.h>
#include <linux/ioctl.h>
#else /* __KERNEL__ */
#include <stdio.h>
#include <sys/ioctl.h>
#endif /* __KERNEL__ */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * If you want to assign device minor automatically, specify this.
 */
#define WALB_DYNAMIC_MINOR (-1U)

/**
 * Data structure for walb ioctl.
 */
struct walb_ctl_data {

	unsigned int wmajor; /* walb device major. */
	unsigned int wminor; /* walb device minor.
				walblog device minor is (wminor + 1). */
	unsigned int lmajor;  /* log device major. */
	unsigned int lminor;  /* log device minor. */
	unsigned int dmajor;  /* data device major. */
	unsigned int dminor;  /* data device minor. */

	/* These are used for other struct for each control command. */
	size_t buf_size; /* buffer size. */
#ifdef __KERNEL__
	void __user *buf;
#else
	void *buf; /* buffer pointer if data_size > 0. */
#endif
	void *kbuf; /* used inside kernel. */
} __attribute__((packed));

/**
 * Data structure for walb ioctl.
 */
struct walb_ctl {

	/* Command id. */
	int command;

	/* Used for integer value transfer. */
	int val_int;
	u64 val_u64;
	u32 val_u32;

	int error; /* error no. */

	/* For userland --> kernel. */
	struct walb_ctl_data u2k;

	/* For kernel --> userland. */
	struct walb_ctl_data k2u;
} __attribute__((packed));

/**
 * Print walb_ctl data for debug.
 */
static inline void print_walb_ctl(
	__attribute__((unused)) const struct walb_ctl *ctl)
{
	PRINT(KERN_DEBUG,
		"***** walb_ctl *****\n"
		"command: %d\n"
		"val_int: %d\n"
		"val_u32: %u\n"
		"val_u64: %" PRIu64"\n"
		"error: %d\n"

		"u2k.wdevt: (%u:%u)\n"
		"u2k.ldevt: (%u:%u)\n"
		"u2k.ddevt: (%u:%u)\n"
		"u2k.buf_size: %zu\n"

		"k2u.wdevt: (%u:%u)\n"
		"k2u.ldevt: (%u:%u)\n"
		"k2u.ddevt: (%u:%u)\n"
		"k2u.buf_size: %zu\n",
		ctl->command,
		ctl->val_int,
		ctl->val_u32,
		ctl->val_u64,
		ctl->error,

		ctl->u2k.wmajor, ctl->u2k.wminor,
		ctl->u2k.lmajor, ctl->u2k.lminor,
		ctl->u2k.dmajor, ctl->u2k.dminor,
		ctl->u2k.buf_size,

		ctl->k2u.wmajor, ctl->k2u.wminor,
		ctl->k2u.lmajor, ctl->k2u.lminor,
		ctl->k2u.dmajor, ctl->k2u.dminor,
		ctl->k2u.buf_size);
}

/**
 * Ioctl magic word for walb.
 */
#define WALB_IOCTL_ID 0xfe

/**
 * Ioctl command id.
 */
enum {
	WALB_IOCTL_VERSION_CMD = 0,
	WALB_IOCTL_CONTROL_CMD,
	WALB_IOCTL_WDEV_CMD,
};

/**
 * Ioctl command id.
 *
 * WALB_IOCTL_VERSION is for both. (currently each walb device only.)
 * WALB_IOCTL_CONTROL is for /dev/walb/control device.
 * WALB_IOCTL_WDEV is for each walb device.
 */
#define WALB_IOCTL_VERSION _IOR(WALB_IOCTL_ID, WALB_IOCTL_VERSION_CMD, u32)
#define WALB_IOCTL_CONTROL _IOWR(WALB_IOCTL_ID, WALB_IOCTL_CONTROL_CMD, struct walb_ctl)
#define WALB_IOCTL_WDEV _IOWR(WALB_IOCTL_ID, WALB_IOCTL_WDEV_CMD, struct walb_ctl)

/**
 * For walb_ctl.command.
 */
enum {
	WALB_IOCTL_DUMMY = 0,

	/****************************************
	 * For WALB_IOCTL_CONTROL
	 * The target is /dev/walb/control.
	 ****************************************/

	/*
	 * Start a walb device.
	 *
	 * INPUT:
	 *   ctl->u2k.lmajor, ctl->u2k.lminor as log device major/minor.
	 *   ctl->u2k.dmajor, ctl->u2k.dminor as data device major/minor.
	 *   ctl->u2k.buf as device name (ctl->u2k.buf_size < DISK_NAME_LEN).
	 *     You can specify NULL and 0.
	 *   ctl->u2k.wminor as walb device minor.
	 *     Specify WALB_DYNAMIC_MINOR for automatic assign.
	 *   ctl->u2k.buf as struct walb_start_param.
	 * OUTPUT:
	 *   ctl->k2u.wmajor, ctl->k2u.wminor as walb device major/minor.
	 *   ctl->k2u.buf as device name (ctl->k2u.buf_size >= DISK_NAME_LEN).
	 *   ctl->error as error code.
	 * RETURN:
	 *   0 in success, or -EFAULT.
	 */
	WALB_IOCTL_START_DEV,

	/*
	 * Stop a walb device.
	 *
	 * INPUT:
	 *   ctl->u2k.wmajor, ctl->u2k.wmajor as walb device major/minor.
	 *   Set ctl->val_int to non-zero if you force it to stop.
	 * OUTPUT:
	 *   ctl->error as error code.
	 * RETURN:
	 *   0 in success, -EBUSY if someone uses the device, or -EFAULT.
	 *   If force flag is set,
	 */
	WALB_IOCTL_STOP_DEV,

	/*
	 * Get walb device major number.
	 *
	 * INPUT:
	 *   None.
	 * OUTPUT:
	 *   ctl->k2u.wmajor as major number.
	 * RETURN:
	 *   0.
	 */
	WALB_IOCTL_GET_MAJOR,

	/*
	 * Get walb device data list.
	 *
	 * INPUT:
	 *   ctl->u2k.buf as unsigned int *minor.
	 *     ctl->u2k.buf_size >= sizeof(unsigned int) * 2.
	 *     Range: minor[0] <= minor < minor[1].
	 * OUTPUT:
	 *   ctl->k2u.buf as struct disk_data *ddata.
	 *   ctl->val_int as number of stored devices.
	 * RETURN:
	 *   0 in success, or -EFAULT.
	 */
	WALB_IOCTL_LIST_DEV,

	/*
	 * Get numbr of walb devices.
	 *
	 * INPUT:
	 *   None.
	 * OUTPUT:
	 *   ctl->val_int as number of walb devices.
	 * RETURN:
	 *   0 in success, or -EFAULT.
	 */
	WALB_IOCTL_NUM_OF_DEV,

	/****************************************
	 * For WALB_IOCTL_WDEV
	 * The targets are walb devices.
	 ****************************************/

	/*
	 * Get oldest_lsid.
	 *
	 * INPUT:
	 *   None.
	 * OUTPUT:
	 *   ctl->val_u64 as oldest_lsid
	 */
	WALB_IOCTL_GET_OLDEST_LSID,

	/*
	 * Set oldest_lsid.
	 *
	 * INPUT:
	 *   ctl->val_u64 as new oldest_lsid.
	 * OUTPUT:
	 *   None.
	 */
	WALB_IOCTL_SET_OLDEST_LSID,

	/*
	 * NOT YET IMPLEMENTED (NYI).
	 * ???
	 *
	 * INPUT:
	 * OUTPUT:
	 */
	WALB_IOCTL_STATUS,

	/*
	 * Deprecated. These will be removed.
	 */
	WALB_IOCTL_CREATE_SNAPSHOT,
	WALB_IOCTL_DELETE_SNAPSHOT,
	WALB_IOCTL_DELETE_SNAPSHOT_RANGE,
	WALB_IOCTL_GET_SNAPSHOT,
	WALB_IOCTL_NUM_OF_SNAPSHOT_RANGE,
	WALB_IOCTL_LIST_SNAPSHOT_RANGE,
	WALB_IOCTL_LIST_SNAPSHOT_FROM,

	/*
	 * Get checkpoint interval.
	 *
	 * INPUT:
	 *   None.
	 * OUTPUT:
	 *   ctl->val_u32 as interval [ms].
	 * RETURN:
	 *   0 in success, or -EFAULT.
	 */
	WALB_IOCTL_GET_CHECKPOINT_INTERVAL,

	/*
	 * Take a checkpoint immediately.
	 *
	 * INPUT:
	 *   None.
	 * OUTPUT:
	 *   None.
	 * RETURN:
	 *   0 in success, or -EFAULT.
	 */
	WALB_IOCTL_TAKE_CHECKPOINT,

	/*
	 * Set checkpoint interval.
	 *
	 * INPUT:
	 *   ctl->val_u32 as new interval [ms].
	 * OUTPUT:
	 *   None.
	 * RETURN:
	 *   0 in success, or -EFAULT.
	 */
	WALB_IOCTL_SET_CHECKPOINT_INTERVAL,

	/*
	 * Get written_lsid where all IO(s) which lsid < written_lsid
	 * have been written to the underlying both log and data devices.
	 *
	 * INPUT:
	 *   None
	 * OUTPUT:
	 *   ctl->val_u64 as written_lsid.
	 */
	WALB_IOCTL_GET_WRITTEN_LSID,

	/*
	 * Get permanent_lsid where all IO(s) which lsid < permanent_lsid
	 * have been permanent in the log device.
	 *
	 * INPUT:
	 *   None
	 * OUTPUT:
	 *   ctl->val_u64 as permanent_lsid.
	 */
	WALB_IOCTL_GET_PERMANENT_LSID,

	/*
	 * Get completed_lsid where all IO(s) which lsid < completed_lsid
	 * have been completed.
	 * For easy algorithm, this is the same as written_lsid.
	 *
	 * INPUT:
	 *   None
	 * OUTPUT:
	 *   ctl->val_u64 as completed_lsid.
	 */
	WALB_IOCTL_GET_COMPLETED_LSID,

	/*
	 * Get log space usage.
	 * INPUT:
	 *   None
	 * OUTPUT:
	 *   ctl->val_u64 as log space usage [physical block].
	 *     log space usage > log capacity means log overflow.
	 */
	WALB_IOCTL_GET_LOG_USAGE,

	/*
	 * Get log space capacity.
	 * INPUT:
	 *   None
	 * OUTPUT:
	 *   ctl->val_u64 as log space capacity [physical block].
	 */
	WALB_IOCTL_GET_LOG_CAPACITY,

	/*
	 * Is flush capability.
	 * INPUT:
	 *   None
	 * OUTPUT:
	 *   ctl->val_int as boolean (non-zero is true).
	 */
	WALB_IOCTL_IS_FLUSH_CAPABLE,

	/*
	 * Resize walb device.
	 *
	 * INPUT:
	 *   ctl->val_u64 as new device capacity [logical block].
	 *     This must be equal to or larger than the old size.
	 *     0 means auto-detect new size.
	 * OUTPUT:
	 *   None.
	 * RETURN:
	 *   0 in success, or -EFAULT.
	 */
	WALB_IOCTL_RESIZE,

	/*
	 * Clear all logs.
	 *
	 * This will revalidate the log space size
	 * when log device size has changed.
	 * This will create a new UUID.
	 *
	 * INPUT:
	 *   None
	 * OUTPUT:
	 *   None
	 * RETURN:
	 *   0 in success, or -EFAULT.
	 */
	WALB_IOCTL_CLEAR_LOG,

	/*
	 * Check log space overflow.
	 *
	 * INPUT:
	 *   None.
	 * OUTPUT:
	 *   ctl->val_int as result.
	 *     Non-zero in overflowed, or 0.
	 * RETURN:
	 *   0 in success, or -EFAULT.
	 */
	WALB_IOCTL_IS_LOG_OVERFLOW,

	/*
	 * Freeze the device temporary.
	 *
	 * Stop write IO processing for a specified period.
	 * If the device has been frozen already,
	 * reset the timeout.
	 *
	 * INPUT:
	 *   ctl->val_u32 as timeout [second].
	 *     0 means no timeout. You must melt by yourself.
	 * OUTPUT:
	 *   None.
	 * RETURN:
	 *   0 in success, or -EFAULT.
	 */
	WALB_IOCTL_FREEZE,

	/*
	 * Melt the frozen device.
	 *
	 * INPUT:
	 *   None.
	 * OUTPUT:
	 *   None.
	 * RETURN:
	 *   0 in success, or -EFAULT.
	 */
	WALB_IOCTL_MELT,

	/*
	 * Check whether the device is frozen or not.
	 *
	 * INPUT:
	 *   None.
	 * OUTPUT:
	 *   ctl->val_int as result.
	 *     Non-zero in frozen, or 0.
	 * RETURN:
	 *   0 in success, or -EFAULT.
	 */
	WALB_IOCTL_IS_FROZEN,

	/* NIY means [N]ot [I]mplemented [Y]et. */
};

/**
 * WALB_IOCTL_START_DEV
 */
struct walb_start_param
{
	/* Device name. Terminated by '\0'. */
	char name[DISK_NAME_LEN];

	/* Pending data limit size [MB]. */
	unsigned int max_pending_mb;
	unsigned int min_pending_mb;

	/* Queue stop timeout [msec]. */
	unsigned int queue_stop_timeout_ms;

	/* Maximum logpack size [KB].
	   A logpack containing a requests can exceeds the limitation.
	   This must be the integral multiple of physical block size.
	   0 means there is no limitation of logpack size
	   (practically limited by physical block size for logpack header). */
	unsigned int max_logpack_kb;

	/* Log flush intervals. */
	unsigned int log_flush_interval_ms; /* period [ms]. */
	unsigned int log_flush_interval_mb; /* size [MB]. */

	/* Max number of logpacks to be processed at once. */
	unsigned int n_pack_bulk;

	/* Max number of data IOs to be processed at once. */
	unsigned int n_io_bulk;

} __attribute__((packed));

/**
 * Check start parameter validness.
 */
static inline bool is_walb_start_param_valid(
	const struct walb_start_param *param)
{
	CHECKd(param);
	CHECKd(strnlen(param->name, DISK_NAME_LEN) < DISK_NAME_LEN);
	CHECKd(2 <= param->max_pending_mb);
	CHECKd(param->max_pending_mb <= MAX_PENDING_MB);
	CHECKd(1 <= param->min_pending_mb);
	CHECKd(param->min_pending_mb < param->max_pending_mb);
	CHECKd(1 <= param->queue_stop_timeout_ms);
	/* CHECKd(0 <= param->log_flush_interval_ms); */
	/* CHECKd(0 <= param->log_flush_interval_mb); */
	CHECKd(param->log_flush_interval_mb * 2 <= param->max_pending_mb);
	CHECKd(0 < param->n_pack_bulk);
	CHECKd(0 < param->n_io_bulk);
	return true;
error:
	return false;
};

#ifdef __cplusplus
}
#endif

#endif /* WALB_IOCTL_H */
