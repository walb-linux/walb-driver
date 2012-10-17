/**
 * Control walb device.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>

#include "walb/walb.h"
#include "walb/log_device.h"
#include "walb/log_record.h"
#include "walb/ioctl.h"

#include "random.h"
#include "util.h"
#include "logpack.h"
#include "snapshot.h"
#include "walblog_format.h"

/*******************************************************************************
 * Macros.
 *******************************************************************************/

static char NOMEM_STR[] = "Memory allocation failed.\n";

/* Buffer size for ioctl should be page size due to performance. */
#define BUFFER_SIZE 4096

/*******************************************************************************
 * Static data definition.
 *******************************************************************************/

/**
 * Command-line configuration.
 */
struct config
{
	char *cmd_str; /* command string */
	char *ldev_name; /* log device name */
	char *ddev_name; /* data device name */

	/* size_t log_sector_size; /\* sector size of log device *\/ */
	/* size_t log_dev_size; /\* size of log device by the sector *\/ */

	int n_snapshots; /* maximum number of snapshots to keep */

	char *wdev_name; /* walb device */
	char *wldev_name;  /* walblog device */
	u64 lsid; /* lsid */

	u64 lsid0; /* from lsid */
	u64 lsid1; /* to lsid */

	char *name; /* name of stuff */
	
	/* These will be converted to lsid(s) internally. */
	char *snap0; /* from snapshot */
	char *snap1; /* to snapshot */

	size_t size; /* (size_t)(-1) means undefined. */
};

/**
 * For command string to function.
 */
typedef bool (*command_fn)(const struct config *);

/**
 * Helper data for map command string to function.
 */
struct map_str_to_fn
{
	const char *str;
	command_fn fn;
};

/**
 * Options string.
 */
static const char *helpstr_options_ = 
	"OPTIONS:\n"
	"  N_SNAP: --n_snap [max number of snapshots]\n"
	"  SIZE:   --size [size of stuff]\n"
	"  LRANGE: --lsid0 [from lsid] --lsid1 [to lsid]\n"
	"  TRANGE: --time0 [from time] --time1 [to time]\n"
	"  SRANGE: --snap0 [from snapshot] --snap1 [to snapshot]\n"
	"  LSID:   --lsid [lsid]\n"
	"  DDEV:   --ddev [data device path]\n"
	"  LDEV:   --ldev [log device path]\n"
	"  WDEV:   --wdev [walb device path]\n"
	"  WLDEV:  --wldev [walblog device path]\n"
	"  NAME:   --name [name of stuff]\n"
	"  WLOG:   walb log data as stream\n";

/**
 * Helper data structure for help command.
 */
struct cmdhelp
{
	const char *cmdline;
	const char *description;
};

/**
 * Help string.
 */
static struct cmdhelp cmdhelps_[] = {
	{ "format_ldev LDEV DDEV (NSNAP) (NAME) (N_SNAP)", 
	  "Format log device." },
	{ "create_wdev LDEV DDEV (NAME)",
	  "Make walb/walblog device." },
	{ "delete_wdev WDEV",
	  "Delete walb/walblog device." },
	{ "create_snapshot WDEV NAME",
	  "Create snapshot." },
	{ "delete_snapshot WDEV NAME | LRANGE",
	  "Delete snapshot." },
	{ "num_snapshot WDEV (LRANGE | TRANGE | SRANGE)",
	  "Get number of snapshots." },
	{ "list_snapshot WDEV (LRANGE | TRANGE | SRANGE)",
	  "Get list of snapshots." },
	{ "set_checkpoint_interval WDEV SIZE",
	  "Set checkpoint interval in [ms]." },
	{ "get_checkpoint_interval WDEV",
	  "Get checkpoint interval in [ms]." },
	{ "cat_wldev WLDEV (LRANGE) > WLOG",
	  "Extract wlog from walblog device." },
	{ "show_wldev WLDEV (LRANGE)",
	  "Show wlog in walblog device." },
	{ "show_wlog (LRANGE) < WLOG",
	  "Show wlog in stdin." },
	{ "redo_wlog DDEV (LRANGE) < WLOG",
	  "Redo wlog to data device." },
	{ "redo LDEV DDEV",
	  "Redo logs and get consistent data device." },
	{ "set_oldest_lsid WDEV LSID",
	  "Delete old logs in the device." },
	{ "get_oldest_lsid WDEV",
	  "Get oldest_lsid in the device." },
	{ "get_written_lsid WDEV",
	  "Get written_lsid in the device." },
	{ "get_log_usage WDEV",
	  "Get log usage in the log device." },
	{ "get_log_capacity WDEV",
	  "Get log capacity in the log device." },
	{ "get_version",
	  "Get walb version."},
};

/**
 * For getopt.
 */
enum
{
	OPT_LDEV = 1,
	OPT_DDEV,
	OPT_N_SNAP,
	OPT_WDEV,
	OPT_WLDEV,
	OPT_LSID,
	OPT_LSID0,
	OPT_LSID1,
	OPT_NAME,
	OPT_SNAP0,
	OPT_SNAP1,
	OPT_SIZE,
	OPT_HELP,
};

/*******************************************************************************
 * Prototype of static functions.
 *******************************************************************************/

/* Helper functions. */
static void show_shorthelp();
static void show_help();
static void init_config(struct config* cfg);
static int parse_opt(int argc, char* const argv[], struct config *cfg);
static bool init_walb_metadata(int fd, int logical_bs, int physical_bs,
			u64 ddev_lb, u64 ldev_lb, int n_snapshots,
			const char *name);
static bool invoke_ioctl(const char *wdev_name, struct walb_ctl *ctl, int open_flag);
static u64 get_oldest_lsid(const char* wdev_name);
static u64 get_written_lsid(const char* wdev_name);
static u64 get_completed_lsid(const char* wdev_name);
static u64 get_log_capacity(const char* wdev_name);
static bool dispatch(const struct config *cfg);
static bool delete_snapshot_by_name(const struct config *cfg);
static bool delete_snapshot_by_lsid_range(const struct config *cfg);
static u64 get_lsid_by_snapshot_name(
	const char *wdev_name, const char *snap_name);
static void decide_lsid_range(const struct config *cfg, u64 lsid[2]);
	
/* commands. */
static bool do_format_ldev(const struct config *cfg);
static bool do_create_wdev(const struct config *cfg);
static bool do_delete_wdev(const struct config *cfg);
static bool do_create_snapshot(const struct config *cfg);
static bool do_delete_snapshot(const struct config *cfg);
static bool do_num_snapshot(const struct config *cfg);
static bool do_list_snapshot(const struct config *cfg);
static bool do_take_checkpoint(const struct config *cfg);
static bool do_set_checkpoint_interval(const struct config *cfg);
static bool do_get_checkpoint_interval(const struct config *cfg);
static bool do_cat_wldev(const struct config *cfg);
static bool do_redo_wlog(const struct config *cfg);
static bool do_redo(const struct config *cfg);
static bool do_show_wlog(const struct config *cfg);
static bool do_show_wldev(const struct config *cfg);
static bool do_set_oldest_lsid(const struct config *cfg);
static bool do_get_oldest_lsid(const struct config *cfg);
static bool do_get_written_lsid(const struct config *cfg);
static bool do_get_completed_lsid(const struct config *cfg);
static bool do_get_log_usage(const struct config *cfg);
static bool do_get_log_capacity(const struct config *cfg);
static bool do_get_version(const struct config *cfg);
static bool do_help(const struct config *cfg);

/*******************************************************************************
 * Helper functions.
 *******************************************************************************/

static void show_shorthelp()
{
	printf("Usage: walbctl COMMAND OPTIONS\n"
		"COMMAND:\n");
	int size = sizeof(cmdhelps_) / sizeof(struct cmdhelp);
	int i;
	for (i = 0; i < size; i ++) {
		printf("  %s\n", cmdhelps_[i].cmdline);
	}
	printf("%s"
		"NIY: Not Implemented Yet.\n",
		helpstr_options_);
}

static void show_help()
{
	printf("Usage: walbctl COMMAND OPTIONS\n"
		"COMMAND:\n");
	int size = sizeof(cmdhelps_) / sizeof(struct cmdhelp);
	int i;
	for (i = 0; i < size; i ++) {
		printf("  %s\n"
			"      %s\n",
			cmdhelps_[i].cmdline,
			cmdhelps_[i].description);
	}
	printf("%s"
		"NIY: Not Implemented Yet.\n",
		helpstr_options_);
}


static void init_config(struct config* cfg)
{
	ASSERT(cfg != NULL);

	/* memset(cfg, 0, sizeof(struct config)); */
	
	cfg->n_snapshots = 10000;

	cfg->lsid0 = (u64)(-1);
	cfg->lsid1 = (u64)(-1);

	cfg->name = NULL;

	cfg->size = (size_t)(-1);
}

/**
 * Parse options.
 */
static int parse_opt(int argc, char* const argv[], struct config *cfg)
{
	int c;

	while (1) {
		int option_index = 0;
		static struct option long_options[] = {
			{"ldev", 1, 0, OPT_LDEV}, /* log device */
			{"ddev", 1, 0, OPT_DDEV}, /* data device */
			{"n_snap", 1, 0, OPT_N_SNAP}, /* num of snapshots */
			{"wdev", 1, 0, OPT_WDEV}, /* walb device */
			{"wldev", 1, 0, OPT_WLDEV}, /* walb log device */
			{"lsid", 1, 0, OPT_LSID}, /* lsid */
			{"lsid0", 1, 0, OPT_LSID0}, /* begin */
			{"lsid1", 1, 0, OPT_LSID1}, /* end */
			{"name", 1, 0, OPT_NAME},
			{"snap0", 1, 0, OPT_SNAP0}, /* begin */
			{"snap1", 1, 0, OPT_SNAP1}, /* end */
			{"size", 1, 0, OPT_SIZE},
			{"help", 0, 0, OPT_HELP},
			{0, 0, 0, 0}
		};

		c = getopt_long(argc, argv, "", long_options, &option_index);
		if (c == -1) {
			break;
		}
		switch (c) {
		case OPT_LDEV:
			cfg->ldev_name = strdup(optarg);
			LOGd("ldev: %s\n", optarg);
			break;
		case OPT_DDEV:
			cfg->ddev_name = strdup(optarg);
			LOGd("ddev: %s\n", optarg);
			break;
		case OPT_N_SNAP:
			cfg->n_snapshots = atoi(optarg);
			break;
		case OPT_WDEV:
			cfg->wdev_name = strdup(optarg);
			break;
		case OPT_WLDEV:
			cfg->wldev_name = strdup(optarg);
			break;
		case OPT_LSID:
			cfg->lsid = atoll(optarg);
			break;
		case OPT_LSID0:
			cfg->lsid0 = atoll(optarg);
			break;
		case OPT_LSID1:
			cfg->lsid1 = atoll(optarg);
			break;
		case OPT_NAME:
			cfg->name = strdup(optarg);
			break;
		case OPT_SNAP0:
			cfg->snap0 = strdup(optarg);
			break;
		case OPT_SNAP1:
			cfg->snap1 = strdup(optarg);
			break;
		case OPT_SIZE:
			cfg->size = atoll(optarg);
			break;
		case OPT_HELP:
			cfg->cmd_str = "help";
			return 0;
		default:
			LOGw("unknown option.\n");
		}
	}

	if (optind < argc) {
		LOGd("command: ");
		while (optind < argc) {
			cfg->cmd_str = strdup(argv[optind]);
			LOGd("%s ", argv[optind]);
			optind++;
		}
		LOGd("\n");
	} else {
		show_shorthelp();
		return -1;
	}

	return 0;
}

/**
 * Initialize log device.
 *
 * @fd block device file descripter.
 * @lbs logical block size.
 * @pbs physical block size.
 * @ddev_lb device size [logical block].
 * @ldev_lb log device size [logical block]
 * @n_snapshots number of snapshots to keep.
 * @name name of the walb device, or NULL.
 *
 * @return true in success, or false.
 */
static bool init_walb_metadata(
	int fd, int lbs, int pbs,
	u64 ddev_lb, u64 ldev_lb, int n_snapshots,
	const char *name)
{
	ASSERT(fd >= 0);
	ASSERT(lbs > 0);
	ASSERT(pbs > 0);
	ASSERT(ddev_lb < (u64)(-1));
	ASSERT(ldev_lb < (u64)(-1));
	/* name can be null. */

	/* Alloc super sector. */
	struct sector_data *super_sect_data
		= sector_alloc_zero(pbs);
	if (!super_sect_data) {
		LOGe("alloc sector failed.\n");
		goto error0;
	}
	struct walb_super_sector *super_sect
		= get_super_sector(super_sect_data);

	/* Initialize super sector. */
	init_super_sector(
		super_sect_data, lbs, pbs,
		ddev_lb, ldev_lb, n_snapshots, name);
	
	/* Write super sector */
	if (!write_super_sector(fd, super_sect_data)) {
		LOGe("write super sector failed.\n");
		goto error1;
	}

	/* Prepare a snapshot sectors. */
	struct sector_data *snap_sect_data
		= sector_alloc_zero(pbs);
	if (!snap_sect_data) {
		LOGe("allocate sector failed.\n");
		goto error1;
	}
	struct walb_snapshot_sector *snap_sect
		= get_snapshot_sector(snap_sect_data);
	
	/* Write snapshot sectors */
	int i = 0;
	int n_sectors = (int)super_sect->snapshot_metadata_size;
	for (i = 0; i < n_sectors; i++) {
		init_snapshot_sector(snap_sect_data);
		if (!write_snapshot_sector(fd, super_sect, snap_sect, i)) {
			LOGe("write snapshot sector %d failed.\n", i);
			goto error2;
		}
	}

	/* Write invalid logpack not to run redo. */
	if (!write_invalid_logpack_header(fd, super_sect, pbs, 0)) {
		LOGe("write invalid logpack header for lsid 0 failed.\n");
		goto error2;
	}

#if 1	     
	/* Read super sector and print for debug. */
	sector_zeroclear(super_sect_data);
	if (!read_super_sector(fd, super_sect_data)) {
		goto error2;
	}
	print_super_sector(super_sect_data);

	/* Read first snapshot sector and print for debug. */
	for (i = 0; i < n_sectors; i++) {
		sector_zeroclear(snap_sect_data);
		if (!read_snapshot_sector(fd, super_sect, snap_sect, i)) {
			LOGe("read snapshot sector %d failed.\n", i);
			goto error2;
		}
		print_snapshot_sector(snap_sect, pbs);
	}
#endif
	sector_free(snap_sect_data);
	sector_free(super_sect_data);
	return true;

error2:
	sector_free(snap_sect_data);
error1:
	sector_free(super_sect_data);
error0:
	return false;
}

/**
 * Invoke ioctl to WALB_IOCTL_WDEV.
 *
 * @wdev_name walb device name.
 * @ctl data for input/output.
 * @open_flag open flag.
 *
 * @return true in success, or false.
 */
static bool invoke_ioctl(const char *wdev_name, struct walb_ctl *ctl, int open_flag)
{
	if (wdev_name == NULL) {
		LOGe("Specify walb device.\n");
		goto error0;
	}
	if (check_bdev(wdev_name) < 0) {
		LOGe("invoke_ioctl: check walb device failed %s.\n",
			wdev_name);
		goto error0;
	}
	
	int fd = open(wdev_name, open_flag);
	if (fd < 0) {
		perror("open failed");
		goto error0;
	}

	int ret = ioctl(fd, WALB_IOCTL_WDEV, ctl);
	if (ret < 0) {
		LOGe("invoke_ioctl: ioctl failed.\n");
		goto error1;
	}
	close(fd);
	return true;
	
error1:
	close(fd);
error0:
	return false;
}

/**
 * Get oldest_lsid.
 *
 * @return oldest_lsid in success, or (u64)(-1).
 */
static u64 get_oldest_lsid(const char* wdev_name)
{
	struct walb_ctl ctl = {
		.command = WALB_IOCTL_GET_OLDEST_LSID,
		.u2k = { .buf_size = 0 },
		.k2u = { .buf_size = 0 },
	};
	
	if (invoke_ioctl(wdev_name, &ctl, O_RDONLY)) {
		return ctl.val_u64;
	} else {
		return (u64)(-1);
	}
}

/**
 * Get written_lsid.
 *
 * @return written_lsid in success, or (u64)(-1).
 */
static u64 get_written_lsid(const char* wdev_name)
{
	struct walb_ctl ctl = {
		.command = WALB_IOCTL_GET_WRITTEN_LSID,
		.u2k = { .buf_size = 0 },
		.k2u = { .buf_size = 0 },
	};
	
	if (invoke_ioctl(wdev_name, &ctl, O_RDONLY)) {
		return ctl.val_u64;
	} else {
		return (u64)(-1);
	}
}

/**
 * Get written_lsid.
 *
 * @return written_lsid in success, or (u64)(-1).
 */
static u64 get_completed_lsid(const char* wdev_name)
{
	struct walb_ctl ctl = {
		.command = WALB_IOCTL_GET_COMPLETED_LSID,
		.u2k = { .buf_size = 0 },
		.k2u = { .buf_size = 0 },
	};
	
	if (invoke_ioctl(wdev_name, &ctl, O_RDONLY)) {
		return ctl.val_u64;
	} else {
		return (u64)(-1);
	}
}

/**
 * Get log capacity.
 *
 * @return log capacity [physical sector] in success, or (u64)(-1).
 */
static u64 get_log_capacity(const char* wdev_name)
{
	struct walb_ctl ctl = {
		.command = WALB_IOCTL_GET_LOG_CAPACITY,
		.u2k = { .buf_size = 0 },
		.k2u = { .buf_size = 0 },
	};
	
	if (invoke_ioctl(wdev_name, &ctl, O_RDONLY)) {
		return ctl.val_u64;
	} else {
		return (u64)(-1);
	}
}

/**
 * Dispatch command.
 */
static bool dispatch(const struct config *cfg)
{
	bool ret = false;
	ASSERT(cfg->cmd_str != NULL);

	struct map_str_to_fn map[] = {
		{ "format_ldev", do_format_ldev },
		{ "create_wdev", do_create_wdev },
		{ "delete_wdev", do_delete_wdev },
		{ "create_snapshot", do_create_snapshot },
		{ "delete_snapshot", do_delete_snapshot },
		{ "num_snapshot", do_num_snapshot },
		{ "list_snapshot", do_list_snapshot },
		{ "take_checkpoint", do_take_checkpoint },
		{ "set_checkpoint_interval", do_set_checkpoint_interval },
		{ "get_checkpoint_interval", do_get_checkpoint_interval },
		{ "cat_wldev", do_cat_wldev },
		{ "show_wlog", do_show_wlog },
		{ "show_wldev", do_show_wldev },
		{ "redo_wlog", do_redo_wlog },
		{ "redo", do_redo },
		{ "set_oldest_lsid", do_set_oldest_lsid },
		{ "get_oldest_lsid", do_get_oldest_lsid },
		{ "get_written_lsid", do_get_written_lsid },
		{ "get_completed_lsid", do_get_completed_lsid },
		{ "get_log_usage", do_get_log_usage },
		{ "get_log_capacity", do_get_log_capacity },
		{ "get_version", do_get_version },
		{ "help", do_help },
	};
	int array_size = sizeof(map)/sizeof(map[0]);

	int i;
	for (i = 0; i < array_size; i ++) {
		if (strcmp(cfg->cmd_str, map[i].str) == 0) {
			ret = (*map[i].fn)(cfg);
			break;
		}
	}
	
	return ret;
}

/**
 * Delete a snapshot by name.
 */
static bool delete_snapshot_by_name(const struct config *cfg)
{
	/* Check. */
	if (!is_valid_snapshot_name(cfg->name)) {
		LOGe("snapshot name %s is not valid.\n", cfg->name);
		goto error0;
	}
		
	/* Prepare control data. */
	struct walb_snapshot_record record;
	record.lsid = INVALID_LSID;
	record.timestamp = 0;
	record.snapshot_id = INVALID_SNAPSHOT_ID;
	snprintf(record.name, SNAPSHOT_NAME_MAX_LEN, "%s", cfg->name);
	struct walb_ctl ctl = {
		.command = WALB_IOCTL_DELETE_SNAPSHOT,
		.u2k = { .buf_size = sizeof(struct walb_snapshot_record),
			 .buf = (u8 *)&record },
		.k2u = { .buf_size = 0 },
	};
	
	if (!invoke_ioctl(cfg->wdev_name, &ctl, O_RDWR)) { goto error0; }
	LOGn("Delete snapshot succeeded.\n");
	return true;

error0:
	LOGe("Delete snapshot failed: %d.\n", ctl.error);
	return false;
}

/**
 * Delete snapshots by range.
 */
static bool delete_snapshot_by_lsid_range(const struct config *cfg)
{
	int error = 0;
	ASSERT(is_lsid_range_valid(cfg->lsid0, cfg->lsid1));
	
	/* Decide lsid range. */
	u64 lsid[2];
	lsid[0] = cfg->lsid0;
	lsid[1] = cfg->lsid1;
	
	/* Prepare control data. */
	struct walb_ctl ctl = {
		.command = WALB_IOCTL_DELETE_SNAPSHOT_RANGE,
		.u2k = { .buf_size = sizeof(lsid),
			 .buf = (u8 *)&lsid[0] },
		.k2u = { .buf_size = 0 },
	};
	
	if (!invoke_ioctl(cfg->wdev_name, &ctl, O_RDWR)) {
		error = ctl.error;
		goto error0;
	}
	LOGn("Delete %d snapshots succeeded.\n", ctl.val_int);
	return true;

error0:
	LOGe("Delete snapshots failed: %d.\n", error);
	return false;
}

/**
 * Get lsid by snapshot name.
 *
 * RETURN:
 *   lsid in success, or INVALID_LSID.
 */
static u64 get_lsid_by_snapshot_name(
	const char *wdev_name, const char *snap_name)
{
	ASSERT(is_valid_snapshot_name(snap_name));
	struct walb_snapshot_record srec[2];

	snprintf(srec[0].name, SNAPSHOT_NAME_MAX_LEN, "%s", snap_name);
	
	struct walb_ctl ctl = {
		.command = WALB_IOCTL_GET_SNAPSHOT,
		.u2k = { .buf_size = sizeof(struct walb_snapshot_record),
			 .buf = (u8 *)&srec[0] },
		.k2u = { .buf_size = sizeof(struct walb_snapshot_record),
			 .buf = (u8 *)&srec[1] },
	};
	if (!invoke_ioctl(wdev_name, &ctl, O_RDWR)) { goto error0; }
	ASSERT(srec[1].lsid != INVALID_LSID);
	return srec[1].lsid;
	
error0:
	return INVALID_LSID;
}

/**
 * Decide lsid range using config.
 *
 * @cfg configuration.
 * @lsid u64 array of size 2 to store result.
 */
static void decide_lsid_range(const struct config *cfg, u64 lsid[2])
{
	ASSERT(cfg);
	
	/* Decide lsid[0]. */
	if (cfg->lsid0 != (u64)(-1)) {
		lsid[0] = cfg->lsid0;
	} else if (is_valid_snapshot_name(cfg->snap0)) {
		lsid[0] = get_lsid_by_snapshot_name(cfg->wdev_name, cfg->snap0);
		if (lsid[0] == INVALID_LSID) {
			LOGe("Snapshot %s not found.\n", cfg->snap0);
			goto error0;
		}
	} else {
		lsid[0] = 0;
	}
	
	/* Decide lsid[1]. */
	if (cfg->lsid1 != (u64)(-1)) {
		lsid[1] = cfg->lsid1;
	} else if (is_valid_snapshot_name(cfg->snap1)) {
		lsid[1] = get_lsid_by_snapshot_name(cfg->wdev_name, cfg->snap1);
		if (lsid[1] == INVALID_LSID) {
			LOGe("Snapshot %s not found.\n", cfg->snap1);
			goto error0;
		}
	} else {
		lsid[1] = MAX_LSID + 1;
	}
	return;

error0:
	lsid[0] = INVALID_LSID;
	lsid[1] = INVALID_LSID;
	return;
}

/*******************************************************************************
 * Commands.
 *******************************************************************************/

/**
 * Execute log device format.
 *
 * @return true in success, or false.
 */
static bool do_format_ldev(const struct config *cfg)
{
	ASSERT(cfg->cmd_str);
	ASSERT(strcmp(cfg->cmd_str, "format_ldev") == 0);
	
	/*
	 * Check devices.
	 */
	if (check_bdev(cfg->ldev_name) < 0) {
		LOGe("format_ldev: check log device failed %s.\n",
			cfg->ldev_name);
		goto error0;
	}
	if (check_bdev(cfg->ddev_name) < 0) {
		LOGe("format_ldev: check data device failed %s.\n",
			cfg->ddev_name);
		goto error0;
	}

	/*
	 * Block size.
	 */
	int ldev_logical_bs = get_bdev_logical_block_size(cfg->ldev_name);
	int ddev_logical_bs = get_bdev_logical_block_size(cfg->ddev_name);
	int ldev_physical_bs = get_bdev_physical_block_size(cfg->ldev_name);
	int ddev_physical_bs = get_bdev_physical_block_size(cfg->ddev_name);
	if (ldev_logical_bs != ddev_logical_bs ||
		ldev_physical_bs != ddev_physical_bs) {
		LOGe("logical or physical block size is different.\n");
		goto error0;
	}
	int logical_bs = ldev_logical_bs;
	int physical_bs = ldev_physical_bs;

	/*
	 * Device size.
	 */
	u64 ldev_size = get_bdev_size(cfg->ldev_name);
	u64 ddev_size = get_bdev_size(cfg->ddev_name);

	/*
	 * Debug print.
	 */
	LOGd("logical_bs: %d\n"
		"physical_bs: %d\n"
		"ddev_size: %zu\n"
		"ldev_size: %zu\n",
		logical_bs, physical_bs, ddev_size, ldev_size);
	
	if (logical_bs <= 0 || physical_bs <= 0 ||
		ldev_size == (u64)(-1) || ldev_size == (u64)(-1) ) {
		LOGe("getting block device parameters failed.\n");
		goto error0;
	}
	if (ldev_size % logical_bs != 0 ||
		ddev_size % logical_bs != 0) {
		LOGe("device size is not multiple of logical_bs\n");
		goto error0;
	}
	
	int fd;
	fd = open(cfg->ldev_name, O_RDWR);
	if (fd < 0) {
		perror("open failed");
		goto error0;
	}

	if (!init_walb_metadata(fd, logical_bs, physical_bs,
					ddev_size / logical_bs,
					ldev_size / logical_bs,
					cfg->n_snapshots, cfg->name)) {

		LOGe("initialize walb log device failed.\n");
		goto error1;
	}
	
	close(fd);
	return true;

error1:
	close(fd);
error0:
	return false;
}

/**
 * Create walb device.
 */
static bool do_create_wdev(const struct config *cfg)
{
	ASSERT(cfg->cmd_str);
	ASSERT(strcmp(cfg->cmd_str, "create_wdev") == 0);

	/*
	 * Check devices.
	 */
	if (check_bdev(cfg->ldev_name) < 0) {
		LOGe("create_wdev: check log device failed %s.\n",
			cfg->ldev_name);
		goto error0;
	}
	if (check_bdev(cfg->ddev_name) < 0) {
		LOGe("create_wdev: check data device failed %s.\n",
			cfg->ddev_name);
		goto error0;
	}

	dev_t ldevt = get_bdev_devt(cfg->ldev_name);
	dev_t ddevt = get_bdev_devt(cfg->ddev_name);
	ASSERT(ldevt != (dev_t)(-1) && ddevt != (dev_t)(-1));
	
	/*
	 * Open control device.
	 */
	LOGd("control path: %s\n", WALB_CONTROL_PATH);
	int fd = open(WALB_CONTROL_PATH, O_RDWR);
	if (fd < 0) {
		perror("open failed.");
		goto error0;
	}
	
	/*
	 * Make ioctl data.
	 */
	char u2k_buf[DISK_NAME_LEN];
	char k2u_buf[DISK_NAME_LEN];
	struct walb_ctl ctl = {
		.command = WALB_IOCTL_START_DEV,
		.u2k = { .wminor = WALB_DYNAMIC_MINOR,
			 .lmajor = MAJOR(ldevt),
			 .lminor = MINOR(ldevt),
			 .dmajor = MAJOR(ddevt),
			 .dminor = MINOR(ddevt),
			 .buf_size = DISK_NAME_LEN, .buf = u2k_buf, },
		.k2u = { .buf_size = DISK_NAME_LEN, .buf = k2u_buf, },
	};
	if (cfg->name == NULL) {
		strncpy(u2k_buf, "", DISK_NAME_LEN);
	} else {
		strncpy(u2k_buf, cfg->name, DISK_NAME_LEN);
	}
	
	print_walb_ctl(&ctl); /* debug */
	
	int ret = ioctl(fd, WALB_IOCTL_CONTROL, &ctl);
	if (ret < 0) {
		LOGe("create_wdev: ioctl failed with error %d.\n",
			ctl.error);
		goto error1;
	}
	ASSERT(ctl.error == 0);
	ASSERT(strnlen(ctl.k2u.buf, DISK_NAME_LEN) < DISK_NAME_LEN);
	printf("create_wdev is done successfully.\n"
		"name: %s\n"
		"major: %u\n"
		"minor: %u\n",
		(char *)ctl.k2u.buf,
		ctl.k2u.wmajor, ctl.k2u.wminor);
	close(fd);
	print_walb_ctl(&ctl); /* debug */
	return true;
	
error1:
	close(fd);
error0:
	return false;
}

/**
 * Delete walb device.
 */
static bool do_delete_wdev(const struct config *cfg)
{
	ASSERT(cfg->cmd_str);
	ASSERT(strcmp(cfg->cmd_str, "delete_wdev") == 0);

	/*
	 * Check devices.
	 */
	if (check_bdev(cfg->wdev_name) < 0) {
		LOGe("delete_wdev: check walb device failed %s.\n",
			cfg->wdev_name);
		goto error0;
	}
	dev_t wdevt = get_bdev_devt(cfg->wdev_name);
	ASSERT(wdevt != (dev_t)(-1));
	
	/*
	 * Open control device.
	 */
	int fd = open(WALB_CONTROL_PATH, O_RDWR);
	if (fd < 0) {
		perror("open failed.");
		goto error0;
	}
	
	/*
	 * Make ioctl data.
	 */
	struct walb_ctl ctl = {
		.command = WALB_IOCTL_STOP_DEV,
		.u2k = { .wmajor = MAJOR(wdevt),
			 .wminor = MINOR(wdevt),
			 .buf_size = 0, },
		.k2u = { .buf_size = 0, },
	};
	
	int ret = ioctl(fd, WALB_IOCTL_CONTROL, &ctl);
	if (ret < 0) {
		LOGe("delete_wdev: ioctl failed with error %d.\n",
			ctl.error);
		goto error1;
	}
	ASSERT(ctl.error == 0);
	LOGn("delete_wdev is done successfully.\n");
	close(fd);
	return true;
	
error1:
	close(fd);
error0:
	return false;
}

/**
 * Create snapshot.
 *
 * Input: NAME (default: datetime string).
 * Output: Nothing.
 */
static bool do_create_snapshot(const struct config *cfg)
{
	int error = 0;
	ASSERT(strcmp(cfg->cmd_str, "create_snapshot") == 0);

	char name[SNAPSHOT_NAME_MAX_LEN + 1];
	name[SNAPSHOT_NAME_MAX_LEN] = '\0';

	time_t timestamp = time(0);

	/* Check config. */
	if (cfg->name == NULL) {
		if (!get_datetime_str(timestamp, name, SNAPSHOT_NAME_MAX_LEN)) {
			ASSERT(false);
		}
	} else {
		strncpy(name, cfg->name, SNAPSHOT_NAME_MAX_LEN);
	}
	if (!is_valid_snapshot_name(name)) {
		LOGe("snapshot name %s is not valid.\n", name);
		goto error0;
	}
	LOGd("name: %s\n", name);
	
	/* Prepare control data. */
	struct walb_snapshot_record record;
	record.lsid = INVALID_LSID;
	record.timestamp = (u64)timestamp;
	record.snapshot_id = INVALID_SNAPSHOT_ID;
	strncpy(record.name, name, SNAPSHOT_NAME_MAX_LEN);
	
	struct walb_ctl ctl = {
		.command = WALB_IOCTL_CREATE_SNAPSHOT,
		.u2k = { .buf_size = sizeof(struct walb_snapshot_record),
			 .buf = (u8 *)&record },
		.k2u = { .buf_size = 0 },
	};
	
	if (!invoke_ioctl(cfg->wdev_name, &ctl, O_RDWR)) {
		error = ctl.error;
		goto error0;
	}
	LOGn("Create snapshot succeeded.\n");
	return true;

error0:
	LOGe("Create snapshot failed: %d.\n", error);
	return false;
}

/**
 * Delete one or more snapshots.
 *
 * Specify name or lsid range.
 */
static bool do_delete_snapshot(const struct config *cfg)
{
	bool ret = false;
	ASSERT(strcmp(cfg->cmd_str, "delete_snapshot") == 0);
	
	/* Check config. */
	if (cfg->name) {
		ret = delete_snapshot_by_name(cfg);
	} else if (is_lsid_range_valid(cfg->lsid0, cfg->lsid1)) {
		ret = delete_snapshot_by_lsid_range(cfg);
	} else {
		LOGe("Specify snapshot name or lsid range to delete.\n");
		ret = false;
	}
	return ret;
}

/**
 * Get number of snapshots.
 *
 * Specify a range (optional)
 *   Left edge by --lsid0 or --snap0 (default: 0)
 *   Right edge by --lsid1 or --snap1 (default: MAX_LSID + 1)
 */
static bool do_num_snapshot(const struct config *cfg)
{
	int error = 0;
	ASSERT(strcmp(cfg->cmd_str, "num_snapshot") == 0);

	/* Decide lsid range. */
	u64 lsid[2];
	decide_lsid_range(cfg, lsid);
	if (!is_lsid_range_valid(lsid[0], lsid[1])) {
		LOGe("Specify correct lsid range: (%"PRIu64", %"PRIu64").\n",
			lsid[0], lsid[1]);
		goto error0;
	}
	
	/* Prepare control data. */
	struct walb_ctl ctl = {
		.command = WALB_IOCTL_NUM_OF_SNAPSHOT_RANGE,
		.u2k = { .buf_size = sizeof(lsid),
			 .buf = (u8 *)&lsid[0] },
		.k2u = { .buf_size = 0 },
	};
	
	if (!invoke_ioctl(cfg->wdev_name, &ctl, O_RDWR)) {
		error = ctl.error;
		goto error0;
	}
	ASSERT(ctl.val_int >= 0);
	LOGn("Num of snapshots in range (%"PRIu64", %"PRIu64"): %d.\n",
		lsid[0], lsid[1], ctl.val_int);
	return true;

error0:
	LOGe("Num of snapshots ioctl failed: %d.\n", error);
	return false;
}

/**
 * List snapshots.
 *
 * Specify lsid range.
 */
static bool do_list_snapshot(const struct config *cfg)
{
	int error = 0;
	ASSERT(strcmp(cfg->cmd_str, "list_snapshot") == 0);
	int n_rec, i;

	u8 buf[PAGE_SIZE];
	struct walb_snapshot_record *srec =
		(struct walb_snapshot_record *)buf;

	/* Decide lsid range. */
	u64 lsid[2];
	decide_lsid_range(cfg, lsid);
	if (!is_lsid_range_valid(lsid[0], lsid[1])) {
		LOGe("Specify correct lsid range: (%"PRIu64", %"PRIu64").\n",
			lsid[0], lsid[1]);
		goto error0;
	}
	while (lsid[0] < lsid[1]) {
	
		/* Prepare control data. */
		struct walb_ctl ctl = {
			.command = WALB_IOCTL_LIST_SNAPSHOT_RANGE,
			.u2k = { .buf_size = sizeof(lsid),
				 .buf = (u64 *)&lsid[0] },
			.k2u = { .buf_size = PAGE_SIZE,
				 .buf = &buf[0] },
		};
	
		if (!invoke_ioctl(cfg->wdev_name, &ctl, O_RDWR)) {
			error = ctl.error;
			goto error0;
		}
		n_rec = ctl.val_int;
		for (i = 0; i < n_rec; i++) {
			print_snapshot_record(&srec[i]);
		}
		lsid[0] = ctl.val_u64; /* the first lsid of remaining. */
	}
	return true;

error0:
	LOGe("List snapshots ioctl failed: %d.\n", error);
	return false;
}

/**
 * Make checkpointo immediately.
 */
static bool do_take_checkpoint(const struct config *cfg)
{
	struct walb_ctl ctl = {
		.command = WALB_IOCTL_TAKE_CHECKPOINT,
		.u2k = { .buf_size = 0 },
		.k2u = { .buf_size = 0 },
	};
	if (!invoke_ioctl(cfg->wdev_name, &ctl, O_RDWR)) { goto error0; }
	return true;

error0:
	LOGe("Take snapshot failed\n");
	return false;
}

/**
 * Set checkpoint interval.
 *
 * @return true in success, or false.
 */
static bool do_set_checkpoint_interval(const struct config *cfg)
{
	ASSERT(strcmp(cfg->cmd_str, "set_checkpoint_interval") == 0);

	if (cfg->size == (size_t)(-1)) {
		LOGe("Specify checkpoint interval.\n");
		goto error0;
	}
	if (cfg->size > (size_t)UINT32_MAX) {
		LOGe("Given interval is too big.\n");
		goto error0;
	}
	
	struct walb_ctl ctl = {
		.command = WALB_IOCTL_SET_CHECKPOINT_INTERVAL,
		.val_u32 = (u32)cfg->size,
		.u2k = { .buf_size = 0 },
		.k2u = { .buf_size = 0 },
	};
	
	if (!invoke_ioctl(cfg->wdev_name, &ctl, O_RDWR)) { goto error0; }
	LOGn("checkpoint interval is set to %"PRIu32" successfully.\n", ctl.val_u32);

	return true;
error0:
	return false;
}

/**
 * Get checkpoint interval.
 *
 * @return true in success, or false.
 */
static bool do_get_checkpoint_interval(const struct config *cfg)
{
	ASSERT(strcmp(cfg->cmd_str, "get_checkpoint_interval") == 0);

	struct walb_ctl ctl = {
		.command = WALB_IOCTL_GET_CHECKPOINT_INTERVAL,
		.u2k = { .buf_size = 0 },
		.k2u = { .buf_size = 0 },
	};
	
	if (!invoke_ioctl(cfg->wdev_name, &ctl, O_RDWR)) { goto error0; }
	printf("checkpoint interval is %"PRIu32".\n", ctl.val_u32);

	return true;
error0:
	return false;
}

/**
 * Cat logpack in specified range.
 *
 * @return true in success, or false.
 */
static bool do_cat_wldev(const struct config *cfg)
{
	ASSERT(cfg->cmd_str);
	ASSERT(strcmp(cfg->cmd_str, "cat_wldev") == 0);

	/*
	 * Check device.
	 */
	if (check_bdev(cfg->wldev_name) < 0) {
		LOGe("cat_wldev: check log device failed %s.\n",
			cfg->wldev_name);
		goto error0;
	}
	int logical_bs = get_bdev_logical_block_size(cfg->wldev_name);
	int physical_bs = get_bdev_physical_block_size(cfg->wldev_name);

	int fd = open(cfg->wldev_name, O_RDONLY);
	if (fd < 0) {
		perror("open failed.");
		goto error0;
	}
	
	/* Allocate memory and read super block */
	struct walb_super_sector *super_sectp = 
		(struct walb_super_sector *)alloc_sector(physical_bs);
	if (super_sectp == NULL) {
		LOGe("%s", NOMEM_STR);
		goto error1;
	}

	u64 off0 = get_super_sector0_offset(physical_bs);
	if (!read_sector(fd, (u8 *)super_sectp, physical_bs, off0)) {
		LOGe("read super sector0 failed.\n");
		goto error1;
	}
	if (!__is_valid_super_sector(super_sectp, physical_bs)) {
		LOGe("read super sector is not valid.\n");
		goto error1;
	}
	
	struct walb_logpack_header *logpack =
		(struct walb_logpack_header *)alloc_sector(physical_bs);
	if (logpack == NULL) {
		LOGe("%s", NOMEM_STR);
		goto error2;
	}

	/* print_super_sector(super_sectp); */
	u64 oldest_lsid = super_sectp->oldest_lsid;
	LOGd("oldest_lsid: %"PRIu64"\n", oldest_lsid);

	/* Range check */
	u64 lsid, begin_lsid, end_lsid;
	if (cfg->lsid0 == (u64)(-1)) {
		begin_lsid = oldest_lsid;
	} else {
		begin_lsid = cfg->lsid0;
	}
	if (cfg->lsid0 < oldest_lsid) {
		LOGe("given lsid0 %"PRIu64" < oldest_lsid %"PRIu64"\n",
			cfg->lsid0, oldest_lsid);
		goto error3;
	}
	end_lsid = cfg->lsid1;
	if (begin_lsid > end_lsid) {
		LOGe("lsid0 < lsid1 property is required.\n");
		goto error3;
	}

	size_t bufsize = 1024 * 1024; /* 1MB */
	ASSERT(bufsize % physical_bs == 0);
	u8 *buf = alloc_sectors(physical_bs, bufsize / physical_bs);
	if (buf == NULL) {
		LOGe("%s", NOMEM_STR);
		goto error3;
	}

	/* Prepare and write walblog_header. */
	walblog_header_t* wh = (walblog_header_t *)buf;
	ASSERT(WALBLOG_HEADER_SIZE <= bufsize);
	memset(wh, 0, WALBLOG_HEADER_SIZE);
	wh->header_size = WALBLOG_HEADER_SIZE;
	wh->sector_type = SECTOR_TYPE_WALBLOG_HEADER;
	wh->checksum = 0;
	wh->version = WALB_VERSION;
	wh->logical_bs = logical_bs;
	wh->physical_bs = physical_bs;
	copy_uuid(wh->uuid, super_sectp->uuid);
	wh->begin_lsid = begin_lsid;
	wh->end_lsid = end_lsid;
	/* Checksum */
	u32 wh_sum = checksum((const u8 *)wh, WALBLOG_HEADER_SIZE);
	wh->checksum = wh_sum;
	/* Write */
	write_data(1, buf, WALBLOG_HEADER_SIZE);
	LOGd("lsid %"PRIu64" to %"PRIu64"\n", begin_lsid, end_lsid);

	/* Write each logpack to stdout. */
	lsid = begin_lsid;
	while (lsid < end_lsid) {

		/* Logpack header */
		if (!read_logpack_header_from_wldev(fd, super_sectp, lsid, logpack)) {
			break;
		}
		LOGd("logpack %"PRIu64"\n", logpack->logpack_lsid);
		write_logpack_header(1, physical_bs, logpack);
		
		/* Realloc buffer if buffer size is not enough. */
		if (bufsize / physical_bs < logpack->total_io_size) {
			if (!realloc_sectors(&buf, physical_bs, logpack->total_io_size)) {
				LOGe("realloc_sectors failed.\n");
				goto error3;
			}
			bufsize = (u32)logpack->total_io_size * physical_bs;
			LOGd("realloc_sectors called. %zu bytes\n", bufsize);
		}

		/* Logpack data. */
		if (!read_logpack_data_from_wldev(fd, super_sectp, logpack, buf, bufsize)) {
			LOGe("read logpack data failed.\n");
			goto error4;
		}
		write_data(1, buf, logpack->total_io_size * physical_bs);
		
		lsid += logpack->total_io_size + 1;
	}

	free(buf);
	free(logpack);
	free(super_sectp);
	close(fd);
	return true;

error4:
	free(buf);
error3:
	free(logpack);
error2:
	free(super_sectp);
error1:
	close(fd);
error0:
	return false;
}

/**
 * Redo wlog.
 *
 * wlog is read from stdin.
 * --ddev (required)
 * --lsid0 (optional, default is the first lsid in the wlog.)
 * --lsid1 (optional, default is the last lsid in the wlog.)
 * redo logs of lsid0 <= lsid < lsid1.
 */
static bool do_redo_wlog(const struct config *cfg)
{
	ASSERT(cfg->cmd_str);
	ASSERT(strcmp(cfg->cmd_str, "redo_wlog") == 0);

	/* Check data device. */
	if (check_bdev(cfg->ddev_name) < 0) {
		LOGe("redo_wlog: check data device failed %s.\n",
			cfg->ddev_name);
		goto error0;
	}

	/* Open data device. */
	int fd = open(cfg->ddev_name, O_RDWR);
	if (fd < 0) {
		perror("open failed.");
		goto error0;
	}

	/* Allocate walblog header. */
	walblog_header_t* wh = (walblog_header_t *)malloc(WALBLOG_HEADER_SIZE);
	if (wh == NULL) {
		LOGe("%s", NOMEM_STR);
		goto error1;
	}
	
	/* Read wlog header. */
	read_data(0, (u8 *)wh, WALBLOG_HEADER_SIZE);
	check_wlog_header(wh);
	print_wlog_header(wh); /* debug */

	/* Set block size */
	int lbs = wh->logical_bs;
	int pbs = wh->physical_bs;
	if (pbs % lbs != 0) {
		LOGe("physical_bs %% logical_bs must be 0.\n");
		goto error2;
	}

	int ddev_lbs = get_bdev_logical_block_size(cfg->ddev_name);
	int ddev_pbs = get_bdev_physical_block_size(cfg->ddev_name);
	if (ddev_lbs != lbs || ddev_pbs != pbs) {
		LOGe("block size check is not valid\n"
			"(wlog lbs %d, ddev lbs %d, wlog pbs %d, ddev pbs %d.\n",
			lbs, ddev_lbs, pbs, ddev_pbs);
		goto error2;
	}

	/* Deside begin_lsid and end_lsid. */
	u64 begin_lsid, end_lsid;
	if (cfg->lsid0 == (u64)(-1)) {
		begin_lsid = wh->begin_lsid;
	} else {
		begin_lsid = cfg->lsid0;
	}
	if (cfg->lsid1 == (u64)(-1)) {
		end_lsid = wh->end_lsid;
	} else {
		end_lsid = cfg->lsid1;
	}

	/* Allocate for logpack header. */
	struct walb_logpack_header *logpack =
		(struct walb_logpack_header *)alloc_sector(pbs);
	if (logpack == NULL) {
		LOGe("%s", NOMEM_STR);
		goto error2;
	}

	/* Allocate for logpack data. */
	size_t bufsize = 1024 * 1024; /* 1MB */
	ASSERT(bufsize % pbs == 0);
	u8 *buf = alloc_sectors(pbs, bufsize / pbs);
	if (buf == NULL) {
		LOGe("%s", NOMEM_STR);
		goto error3;
	}
	
	u64 lsid = begin_lsid;
	while (lsid < end_lsid) {

		/* Read logpack header */
		if (!read_logpack_header(0, pbs, logpack)) {
			break;
		}

		/* Realloc buffer if needed. */
		u32 total_io_size = logpack->total_io_size;
		if (total_io_size * pbs > bufsize) {
			if (!realloc_sectors(&buf, pbs, total_io_size)) {
				LOGe("realloc_sectors failed.\n");
				goto error4;
			}
			bufsize = total_io_size * pbs;
		}
		if (!read_logpack_data(0, lbs, pbs, logpack, buf, bufsize)) {
			LOGe("read logpack data failed.\n");
			goto error4;
		}

		/* Decision of skip and end. */
		lsid = logpack->logpack_lsid;
		if (lsid < begin_lsid) { continue; }
		if (end_lsid <= lsid) { break; }
		
		LOGd("logpack %"PRIu64"\n", lsid);

		/* Redo */
		if (!redo_logpack(fd, lbs, pbs, logpack, buf)) {
			LOGe("redo_logpack failed.\n");
			goto error4;
		}
	}

	free(buf);
	free(logpack);
	free(wh);
	close(fd);
	return true;

error4:
	free(buf);
error3:
	free(logpack);
error2:
	free(wh);
error1:
	close(fd);
error0:
	return false;
}

/**
 * Redo
 *
 * Redo logs which are still not written to data device
 * and get consistent data device.
 * Please run this command before creating walb device if need.
 *
 * --ldev (required)
 * --ddev (required)
 */
static bool do_redo(const struct config *cfg)
{
	ASSERT(cfg->cmd_str);
	ASSERT(strcmp(cfg->cmd_str, "redo") == 0);
	
	/*
	 * Check devices.
	 */
	if (check_bdev(cfg->ldev_name) < 0 || check_bdev(cfg->ddev_name) < 0) {
		LOGe("%s or %s is not block device.\n", cfg->ldev_name, cfg->ddev_name);
		goto error0;
	}

	if (!is_same_block_size(cfg->ldev_name, cfg->ddev_name)) {
		LOGe("block size is not the same.\n");
		goto error0;
	}
	
	/* Block size. */
	int lbs = get_bdev_logical_block_size(cfg->ldev_name);
	int pbs = get_bdev_physical_block_size(cfg->ldev_name);

	/* Open devices. */
	int lfd = open(cfg->ldev_name, O_RDWR);
	if (lfd < 0) { perror("open failed."); goto error0; }
	int dfd = open(cfg->ddev_name, O_RDWR);
	if (dfd < 0) { perror("open failed."); goto error1; }

	/* Read super sector. */
	struct walb_super_sector *super = (struct walb_super_sector *)alloc_sector(pbs);
	if (super == NULL) { LOGe("%s", NOMEM_STR); goto error2; }
	u64 off0 = get_super_sector0_offset(pbs);
	if (!read_sector(lfd, (u8 *)super, pbs, off0)) {
		LOGe("Read super sector failed.\n");
		goto error3;
	}
	if (!__is_valid_super_sector(super, pbs)) {
		LOGe("super sector is not valid.\n");
		goto error3;
	}

	size_t bufsize = 1024 * 1024; /* 1MB */
	ASSERT(bufsize % pbs == 0);
	u8 *buf = alloc_sectors(pbs, bufsize / pbs);
	if (buf == NULL) { LOGe("%s", NOMEM_STR); goto error3; }

	struct walb_logpack_header *logpack =
		(struct walb_logpack_header *)alloc_sector(pbs);
	if (logpack == NULL) { LOGe("%s", NOMEM_STR); goto error4; }
	
	u64 lsid = super->written_lsid;
	u64 begin_lsid = lsid;
	/* Read logpack header */
	while (read_logpack_header_from_wldev(lfd, super, lsid, logpack)) {
		
		LOGd("logpack %"PRIu64"\n", logpack->logpack_lsid);
		
		/* Realloc buf if bufsize is not enough. */
		if (bufsize / pbs < logpack->total_io_size) {
			if (!realloc_sectors(&buf, pbs, logpack->total_io_size)) {
				LOGe("realloc_sectors failed.\n");
				goto error5;
			}
			bufsize = (u32)logpack->total_io_size * pbs;
			LOGd("realloc_sectors called. %zu bytes\n", bufsize);
		}

		/* Read logpack data from log device. */
		if (!read_logpack_data_from_wldev(lfd, super, logpack, buf, bufsize)) {
			LOGe("read logpack data failed.\n");
			goto error5;
		}

		/* Write logpack to data device. */
		if (!redo_logpack(dfd, lbs, pbs, logpack, buf)) {
			LOGe("redo_logpack failed.\n");
			goto error5;
		}
		
		lsid += logpack->total_io_size + 1;
	}

	/* Set new written_lsid and sync down. */
	u64 end_lsid = lsid;
	super->written_lsid = end_lsid;
	if (!__write_super_sector(lfd, super)) {
		LOGe("write super sector failed.\n");
		goto error5;
	}
	LOGn("Redo from lsid %"PRIu64" to %"PRIu64"\n",
		begin_lsid, end_lsid);
	
	close(dfd);
	close(lfd);

	return true;

error5:
	free(logpack);
error4:
	free(buf);
error3:
	free(super);
error2:
	close(dfd);
error1:
	close(lfd);
error0:
	return false;
}

/**
 * Show wlog from stdin.
 *
 */
static bool do_show_wlog(const struct config *cfg)
{
	ASSERT(cfg->cmd_str);
	ASSERT(strcmp(cfg->cmd_str, "show_wlog") == 0);

	walblog_header_t* wh = (walblog_header_t *)malloc(WALBLOG_HEADER_SIZE);
	if (wh == NULL) { LOGe("%s", NOMEM_STR); goto error0; }
	
	/* Read and print wlog header. */
	read_data(0, (u8 *)wh, WALBLOG_HEADER_SIZE);
	print_wlog_header(wh);

	/* Check wlog header. */
	check_wlog_header(wh);
	
	/* Set block size. */
	int logical_bs = wh->logical_bs;
	int physical_bs = wh->physical_bs;
	if (physical_bs % logical_bs != 0) {
		LOGe("physical_bs %% logical_bs must be 0.\n");
		goto error1;
	}

	/* Buffer for logpack header. */
	struct walb_logpack_header *logpack;
	logpack = (struct walb_logpack_header *)alloc_sector(physical_bs);
	if (logpack == NULL) { LOGe("%s", NOMEM_STR); goto error1; }

	/* Buffer for logpack data. */
	size_t bufsize = 1024 * 1024; /* 1MB */
	u8 *buf = alloc_sectors(physical_bs, bufsize / physical_bs);
	if (buf == NULL) { LOGe("%s", NOMEM_STR); goto error2; }

	/* Range */
	u64 begin_lsid, end_lsid;
	if (cfg->lsid0 == (u64)(-1)) {
		begin_lsid = 0;
	} else {
		begin_lsid = cfg->lsid0;
	}
	end_lsid = cfg->lsid1;
	
	/* Read, print and check each logpack */
	while (read_logpack_header(0, physical_bs, logpack)) {

		/* Check buffer size. */
		u32 total_io_size = logpack->total_io_size;
		if (total_io_size * physical_bs > bufsize) {
			if (!realloc_sectors(&buf, physical_bs, total_io_size)) {
				LOGe("realloc_sectors failed.\n");
				goto error3;
			}
			bufsize = total_io_size * physical_bs;
		}

		/* Read logpack data. */
		if (!read_logpack_data(0, logical_bs, physical_bs, logpack, buf, bufsize)) {
			LOGe("read logpack data failed.\n");
			goto error3;
		}

		/* Check range. */
		if (logpack->logpack_lsid < begin_lsid) {
			continue; /* skip */
		}
		if (end_lsid <= logpack->logpack_lsid ) {
			break; /* end */
		}   
		
		/* Print logpack header. */
		print_logpack_header(logpack);
	}

	free(buf);
	return true;

error3:
	free(buf);
error2:
	free(logpack);
error1:
	free(wh);
error0:
	return false;
}

/**
 * Show logpack header inside walblog device.
 *
 * @return true in success, or false.
 */
static bool do_show_wldev(const struct config *cfg)
{
	ASSERT(strcmp(cfg->cmd_str, "show_wldev") == 0);

	/*
	 * Check device.
	 */
	if (check_bdev(cfg->wldev_name) < 0) {
		LOGe("check log device failed %s.\n",
			cfg->wldev_name);
		goto error0;
	}
	int physical_bs = get_bdev_physical_block_size(cfg->wldev_name);

	int fd = open(cfg->wldev_name, O_RDONLY);
	if (fd < 0) { perror("open failed"); goto error0; }
	
	/* Allocate memory and read super block */
	struct walb_super_sector *super_sectp = 
		(struct walb_super_sector *)alloc_sector(physical_bs);
	if (super_sectp == NULL) { LOGe("%s", NOMEM_STR); goto error1; }

	u64 off0 = get_super_sector0_offset(physical_bs);
	if (!read_sector(fd, (u8 *)super_sectp, physical_bs, off0)) {
		LOGe("read super sector0 failed.\n");
		goto error1;
	}
	
	struct walb_logpack_header *logpack =
		(struct walb_logpack_header *)alloc_sector(physical_bs);
	if (logpack == NULL) { LOGe("%s", NOMEM_STR); goto error2; }

	__print_super_sector(super_sectp);
	u64 oldest_lsid = super_sectp->oldest_lsid;
	LOGd("oldest_lsid: %"PRIu64"\n", oldest_lsid);

	/* Range check */
	u64 lsid, begin_lsid, end_lsid;
	if (cfg->lsid0 == (u64)(-1)) {
		begin_lsid = oldest_lsid;
	} else {
		begin_lsid = cfg->lsid0;
	}
	if (cfg->lsid0 < oldest_lsid) {
		LOGe("given lsid0 %"PRIu64" < oldest_lsid %"PRIu64"\n",
			cfg->lsid0, oldest_lsid);
		goto error3;
	}
	end_lsid = cfg->lsid1;
	if (begin_lsid > end_lsid) {
		LOGe("lsid0 < lsid1 property is required.\n");
		goto error3;
	}
	
	/* Print each logpack header. */
	lsid = begin_lsid;
	while (lsid < end_lsid) {
		if (!read_logpack_header_from_wldev(fd, super_sectp, lsid, logpack)) {
			break;
		}
		print_logpack_header(logpack);
		lsid += logpack->total_io_size + 1;
	}

	free(logpack);
	free(super_sectp);
	close(fd);
	return true;

error3:
	free(logpack);
error2:
	free(super_sectp);
error1:
	close(fd);
error0:
	return false;
}

/**
 * Set oldest_lsid.
 */
static bool do_set_oldest_lsid(const struct config *cfg)
{
	ASSERT(strcmp(cfg->cmd_str, "set_oldest_lsid") == 0);
	
	struct walb_ctl ctl = {
		.command = WALB_IOCTL_SET_OLDEST_LSID,
		.val_u64 = cfg->lsid,
		.u2k = { .buf_size = 0 },
		.k2u = { .buf_size = 0 },
	};
	
	if (!invoke_ioctl(cfg->wdev_name, &ctl, O_RDWR)) { goto error0; }
	
	LOGn("oldest_lsid is set to %"PRIu64" successfully.\n", cfg->lsid);

	return true;
error0:
	return false;
}

/**
 * Get oldest_lsid.
 */
static bool do_get_oldest_lsid(const struct config *cfg)
{
	ASSERT(strcmp(cfg->cmd_str, "get_oldest_lsid") == 0);
	
	u64 oldest_lsid = get_oldest_lsid(cfg->wdev_name);
	if (oldest_lsid != (u64)(-1)) {
		printf("%"PRIu64"\n", oldest_lsid);
		return true;
	} else {
		return false;
	}
}	 

/**
 * Get written_lsid.
 */
static bool do_get_written_lsid(const struct config *cfg)
{
	ASSERT(strcmp(cfg->cmd_str, "get_written_lsid") == 0);
	
	u64 written_lsid = get_written_lsid(cfg->wdev_name);
	if (written_lsid != (u64)(-1)) {
		printf("%"PRIu64"\n", written_lsid);
		return true;
	} else {
		return false;
	}
}

/**
 * Get completed_lsid.
 */
static bool do_get_completed_lsid(const struct config *cfg)
{
	ASSERT(strcmp(cfg->cmd_str, "get_completed_lsid") == 0);

	u64 lsid = get_completed_lsid(cfg->wdev_name);
	if (lsid != (u64)(-1)) {
		printf("%"PRIu64"\n", lsid);
		return true;
	} else {
		return false;
	}
}

/**
 * Get log usage.
 */
static bool do_get_log_usage(const struct config *cfg)
{
	ASSERT(strcmp(cfg->cmd_str, "get_log_usage") == 0);

	/* This is not strict usage, because
	   there is no method to get oldest_lsid and
	   written_lsid atomically. */
	u64 oldest_lsid = get_oldest_lsid(cfg->wdev_name);
	u64 written_lsid = get_written_lsid(cfg->wdev_name);

	if (oldest_lsid == (u64)(-1) || written_lsid == (u64)(-1)) {
		LOGe("Geting oldest_lsid or written_lsid is failed.\n");
		goto error0;
	}
	if (oldest_lsid > written_lsid) {
		LOGe("This does not satisfy oldest_lsid <= written_lsid "
			"%"PRIu64" %"PRIu64"\n",
			oldest_lsid, written_lsid);
		goto error0;
	}

	printf("%"PRIu64"\n", written_lsid - oldest_lsid);
	return true;
error0:
	return false;
}

/**
 * Get log capacity.
 */
static bool do_get_log_capacity(const struct config *cfg)
{
	ASSERT(strcmp(cfg->cmd_str, "get_log_capacity") == 0);

	u64 log_capacity = get_log_capacity(cfg->wdev_name);

	if (log_capacity == (u64)(-1)) {
		LOGe("Getting log_capacity failed.\n");
		return false;
	}

	printf("%"PRIu64"\n", log_capacity);
	return true;
}

/**
 * Get walb version.
 */
static bool do_get_version(const struct config *cfg)
{
	ASSERT(strcmp(cfg->cmd_str, "get_version") == 0);
	
	if (check_bdev(cfg->wdev_name) < 0) {
		LOGe("device check failed.");
		goto error0;
	}

	int fd = open(cfg->wdev_name, O_RDONLY);
	if (fd < 0) {
		perror("open failed");
		goto error0;
	}
	
	u32 version;
	int ret = ioctl(fd, WALB_IOCTL_VERSION, &version);
	if (ret < 0) {
		LOGe("get version failed.\n");
		goto error1;
	}

	printf("walb version: %"PRIu32"\n", version);
	close(fd);
	return true;


error1:
	close(fd);
error0:
	return false;
}

/**
 * Show help message.
 */
static bool do_help(UNUSED const struct config *cfg)
{
	show_help();
	return true;
}

/*******************************************************************************
 * Functions for main.
 *******************************************************************************/

int main(int argc, char* argv[])
{
	struct config cfgt;

	init_random();
	init_config(&cfgt);
	
	if (parse_opt(argc, argv, &cfgt) != 0) {
		return 1;
	}
	
	if (!dispatch(&cfgt)) {
		LOGe("operation failed.\n");
	}

	return 0;
}
