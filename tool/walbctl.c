/**
 * Control walb device.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
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

#include "walb.h"
#include "walb_log_device.h"
#include "walb_log_record.h"
#include "random.h"
#include "walb_ioctl.h"

#include "util.h"
#include "logpack.h"
#include "walblog_format.h"


#define NOMEM_STR "Memory allocation failed.\n"

/*******************************************************************************
 * Typedefs.
 *******************************************************************************/

typedef struct config
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

        size_t size; /* (size_t)(-1) means undefined. */
        
} config_t;

/*******************************************************************************
 * Helper functions.
 *******************************************************************************/

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

typedef struct
{
        const char *cmdline;
        const char *description;
} cmdhelp_t;

static cmdhelp_t cmdhelps[] = {
        { "format_ldev LDEV DDEV (NSNAP) (NAME) (SIZE)", 
          "Format log device." },
        { "create_wdev LDEV DDEV (NAME)",
          "Make walb/walblog device." },
        { "delete_wdev WDEV",
          "Delete walb/walblog device." },
        { "(NIY)create_snapshot WDEV NAME",
          "Create snapshot." },
        { "(NIY)delete_snapshot WDEV NAME",
          "Delete snapshot." },
        { "(NIY)num_snapshot WDEV (LRANGE | TRANGE | SRANGE)",
          "Get number of snapshots." },
        { "(NIY)list_snapshot WDEV (LRANGE | TRANGE | SRANGE)",
          "Get list of snapshots." },
        { "set_checkpoint_interval WDEV SIZE",
          "Set checkpoint interval in [ms]." },
        { "get_checkpoint_interval WDEV",
          "Get checkpoint interval in [ms]."
          /* "Make checkpoint to reduce redo time after crash." */ },
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

void show_shorthelp()
{
        printf("Usage: walbctl COMMAND OPTIONS\n"
               "COMMAND:\n");
        int size = sizeof(cmdhelps) / sizeof(cmdhelp_t);
        int i;
        for (i = 0; i < size; i ++) {
                printf("  %s\n", cmdhelps[i].cmdline);
        }
        printf("%s"
               "NIY: Not Implemented Yet.\n",
               helpstr_options_);
}

void show_help()
{
        printf("Usage: walbctl COMMAND OPTIONS\n"
               "COMMAND:\n");
        int size = sizeof(cmdhelps) / sizeof(cmdhelp_t);
        int i;
        for (i = 0; i < size; i ++) {
                printf("  %s\n"
                       "      %s\n",
                       cmdhelps[i].cmdline,
                       cmdhelps[i].description);
        }
        printf("%s"
               "NIY: Not Implemented Yet.\n",
               helpstr_options_);
}


void init_config(config_t* cfg)
{
        ASSERT(cfg != NULL);

        cfg->n_snapshots = 10000;

        cfg->lsid0 = (u64)(-1);
        cfg->lsid1 = (u64)(-1);

        cfg->name = NULL;

        cfg->size = (size_t)(-1);
}

/**
 * For getopt.
 */
enum {
        OPT_LDEV = 1,
        OPT_DDEV,
        OPT_N_SNAP,
        OPT_WDEV,
        OPT_WLDEV,
        OPT_LSID,
        OPT_LSID0,
        OPT_LSID1,
        OPT_NAME,
        OPT_SIZE,
        OPT_HELP,
};

/**
 * Parse options.
 */
int parse_opt(int argc, char* const argv[], config_t *cfg)
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
                        {"lsid0", 1, 0, OPT_LSID0},
                        {"lsid1", 1, 0, OPT_LSID1},
                        {"name", 1, 0, OPT_NAME},
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
                        optind ++;
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
 * @logical_bs logical block size.
 * @physical_bs physical block size.
 * @ddev_lb device size [logical block].
 * @ldev_lb log device size [logical block]
 * @n_snapshots number of snapshots to keep.
 * @name name of the walb device, or NULL.
 *
 * @return true in success, or false.
 */
bool init_walb_metadata(int fd, int logical_bs, int physical_bs,
                        u64 ddev_lb, u64 ldev_lb, int n_snapshots,
                        const char *name)
{
        ASSERT(fd >= 0);
        ASSERT(logical_bs > 0);
        ASSERT(physical_bs > 0);
        ASSERT(ddev_lb < (u64)(-1));
        ASSERT(ldev_lb < (u64)(-1));

        walb_super_sector_t super_sect;
        walb_snapshot_sector_t *snap_sectp;

        ASSERT(sizeof(super_sect) <= (size_t)physical_bs);
        ASSERT(sizeof(*snap_sectp) <= (size_t)physical_bs);

        /* Calculate number of snapshot sectors. */
        int n_sectors;
        int t = max_n_snapshots_in_sector(physical_bs);
        n_sectors = (n_snapshots + t - 1) / t;

        LOGd("metadata_size: %d\n", n_sectors);

        /* Prepare super sector */
        memset(&super_sect, 0, sizeof(super_sect));

        super_sect.logical_bs = logical_bs;
        super_sect.physical_bs = physical_bs;
        super_sect.snapshot_metadata_size = n_sectors;
        generate_uuid(super_sect.uuid);
        
        super_sect.ring_buffer_size =
                ldev_lb / (physical_bs / logical_bs)
                - get_ring_buffer_offset(physical_bs, n_snapshots);

        super_sect.oldest_lsid = 0;
        super_sect.written_lsid = 0;
        super_sect.device_size = ddev_lb;
        char *rname = set_super_sector_name(&super_sect, name);
        if (name != NULL && strlen(name) != strlen(rname)) {
                printf("name %s is pruned to %s.\n", name, rname);
        }
        
        /* Write super sector */
        if (! write_super_sector(fd, &super_sect)) {
                LOGe("write super sector failed.\n");
                goto error0;
        }

        /* Prepare super sectors
           Bitmap data will be all 0. */
        snap_sectp = (walb_snapshot_sector_t *)alloc_sector_zero(physical_bs);
        if (snap_sectp == NULL) {
                goto error0;
        }
        
        /* Write metadata sectors */
        int i = 0;
        for (i = 0; i < n_sectors; i ++) {
                if (! write_snapshot_sector(fd, &super_sect, snap_sectp, i)) {
                        goto error1;
                }
        }

#if 1        
        /* Read super sector and print for debug. */
        memset(&super_sect, 0, sizeof(super_sect));
        if (! read_super_sector(fd, &super_sect, physical_bs, n_snapshots)) {
                goto error1;
        }
        /* print_super_sector(&super_sect); */

        /* Read first snapshot sector and print for debug. */
        memset(snap_sectp, 0, physical_bs);
        if (! read_snapshot_sector(fd, &super_sect, snap_sectp, 0)) {
                goto error1;
        }
        /* print_snapshot_sector(snap_sectp, physical_bs); */
        
#endif
        
        return true;

error1:
        free(snap_sectp);
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
bool invoke_ioctl(const char *wdev_name, struct walb_ctl *ctl, int open_flag)
{
        if (check_bdev(wdev_name) < 0) {
                LOGe("invoke_ioctl: check walb device failed %s.\n",
                    wdev_name);
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
u64 get_oldest_lsid(const char* wdev_name)
{
        struct walb_ctl ctl = {
                .command = WALB_IOCTL_OLDEST_LSID_GET,
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
u64 get_written_lsid(const char* wdev_name)
{
        struct walb_ctl ctl = {
                .command = WALB_IOCTL_WRITTEN_LSID_GET,
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
u64 get_log_capacity(const char* wdev_name)
{
        struct walb_ctl ctl = {
                .command = WALB_IOCTL_LOG_CAPACITY_GET,
                .u2k = { .buf_size = 0 },
                .k2u = { .buf_size = 0 },
        };
        
        if (invoke_ioctl(wdev_name, &ctl, O_RDONLY)) {
                return ctl.val_u64;
        } else {
                return (u64)(-1);
        }
}

/*******************************************************************************
 * Commands.
 *******************************************************************************/

/**
 * Execute log device format.
 *
 * @return true in success, or false.
 */
bool do_format_ldev(const config_t *cfg)
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

        if (! init_walb_metadata(fd, logical_bs, physical_bs,
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
bool do_create_wdev(const config_t *cfg)
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
                .command = WALB_IOCTL_DEV_START,
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
bool do_delete_wdev(const config_t *cfg)
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
                .command = WALB_IOCTL_DEV_STOP,
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
 * Set checkpoint interval.
 *
 * @return true in success, or false.
 */
bool do_set_checkpoint_interval(const config_t *cfg)
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
                .command = WALB_IOCTL_CHECKPOINT_INTERVAL_SET,
                .val_u32 = (u32)cfg->size,
                .u2k = { .buf_size = 0 },
                .k2u = { .buf_size = 0 },
        };
        
        if (! invoke_ioctl(cfg->wdev_name, &ctl, O_RDWR)) { goto error0; }
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
bool do_get_checkpoint_interval(const config_t *cfg)
{
        ASSERT(strcmp(cfg->cmd_str, "get_checkpoint_interval") == 0);

        struct walb_ctl ctl = {
                .command = WALB_IOCTL_CHECKPOINT_INTERVAL_GET,
                .u2k = { .buf_size = 0 },
                .k2u = { .buf_size = 0 },
        };
        
        if (! invoke_ioctl(cfg->wdev_name, &ctl, O_RDWR)) { goto error0; }
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
bool do_cat_wldev(const config_t *cfg)
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
        walb_super_sector_t *super_sectp = 
                (walb_super_sector_t *)alloc_sector(physical_bs);
        if (super_sectp == NULL) {
                LOGe(NOMEM_STR);
                goto error1;
        }

        u64 off0 = get_super_sector0_offset(physical_bs);
        if (! read_sector(fd, (u8 *)super_sectp, physical_bs, off0)) {
                LOGe("read super sector0 failed.\n");
                goto error1;
        }
        if (! is_valid_super_sector(super_sectp, physical_bs)) {
                LOGe("read super sector is not valid.\n");
                goto error1;
        }
        
        walb_logpack_header_t *logpack =
                (walb_logpack_header_t *)alloc_sector(physical_bs);
        if (logpack == NULL) {
                LOGe(NOMEM_STR);
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
                LOGe(NOMEM_STR);
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
                if (! read_logpack_header_from_wldev(fd, super_sectp, lsid, logpack)) {
                        break;
                }
                LOGd("logpack %"PRIu64"\n", logpack->logpack_lsid);
                write_logpack_header(1, physical_bs, logpack);
                
                /* Realloc buffer if buffer size is not enough. */
                if (bufsize / physical_bs < logpack->total_io_size) {
                        if (! realloc_sectors(&buf, physical_bs, logpack->total_io_size)) {
                                LOGe("realloc_sectors failed.\n");
                                goto error3;
                        }
                        bufsize = (u32)logpack->total_io_size * physical_bs;
                        LOGd("realloc_sectors called. %zu bytes\n", bufsize);
                }

                /* Logpack data. */
                if (! read_logpack_data_from_wldev(fd, super_sectp, logpack, buf, bufsize)) {
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
bool do_redo_wlog(const config_t *cfg)
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
                LOGe(NOMEM_STR);
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
        walb_logpack_header_t *logpack =
                (walb_logpack_header_t *)alloc_sector(pbs);
        if (logpack == NULL) {
                LOGe(NOMEM_STR);
                goto error2;
        }

        /* Allocate for logpack data. */
        size_t bufsize = 1024 * 1024; /* 1MB */
        ASSERT(bufsize % pbs == 0);
        u8 *buf = alloc_sectors(pbs, bufsize / pbs);
        if (buf == NULL) {
                LOGe(NOMEM_STR);
                goto error3;
        }
        
        u64 lsid = begin_lsid;
        while (lsid < end_lsid) {

                /* Read logpack header */
                if (! read_logpack_header(0, pbs, logpack)) {
                        break;
                }

                /* Realloc buffer if needed. */
                u32 total_io_size = logpack->total_io_size;
                if (total_io_size * pbs > bufsize) {
                        if (! realloc_sectors(&buf, pbs, total_io_size)) {
                                LOGe("realloc_sectors failed.\n");
                                goto error4;
                        }
                        bufsize = total_io_size * pbs;
                }
                if (! read_logpack_data(0, lbs, pbs, logpack, buf, bufsize)) {
                        LOGe("read logpack data failed.\n");
                        goto error4;
                }

                /* Decision of skip and end. */
                lsid = logpack->logpack_lsid;
                if (lsid < begin_lsid) { continue; }
                if (end_lsid <= lsid) { break; }
                
                LOGd("logpack %"PRIu64"\n", lsid);

                /* Redo */
                if (! redo_logpack(fd, lbs, pbs, logpack, buf)) {
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
bool do_redo(const config_t *cfg)
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

        if (! is_same_block_size(cfg->ldev_name, cfg->ddev_name)) {
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
        walb_super_sector_t *super = (walb_super_sector_t *)alloc_sector(pbs);
        if (super == NULL) { LOGe(NOMEM_STR); goto error2; }
        u64 off0 = get_super_sector0_offset(pbs);
        if (! read_sector(lfd, (u8 *)super, pbs, off0)) {
                LOGe("Read super sector failed.\n");
                goto error3;
        }
        if (! is_valid_super_sector(super, pbs)) {
                LOGe("super sector is not valid.\n");
                goto error3;
        }

        size_t bufsize = 1024 * 1024; /* 1MB */
        ASSERT(bufsize % pbs == 0);
        u8 *buf = alloc_sectors(pbs, bufsize / pbs);
        if (buf == NULL) { LOGe(NOMEM_STR); goto error3; }

        walb_logpack_header_t *logpack =
                (walb_logpack_header_t *)alloc_sector(pbs);
        if (logpack == NULL) { LOGe(NOMEM_STR); goto error4; }
        
        u64 lsid = super->written_lsid;
        u64 begin_lsid = lsid;
        /* Read logpack header */
        while (read_logpack_header_from_wldev(lfd, super, lsid, logpack)) {
                
                LOGd("logpack %"PRIu64"\n", logpack->logpack_lsid);
                
                /* Realloc buf if bufsize is not enough. */
                if (bufsize / pbs < logpack->total_io_size) {
                        if (! realloc_sectors(&buf, pbs, logpack->total_io_size)) {
                                LOGe("realloc_sectors failed.\n");
                                goto error5;
                        }
                        bufsize = (u32)logpack->total_io_size * pbs;
                        LOGd("realloc_sectors called. %zu bytes\n", bufsize);
                }

                /* Read logpack data from log device. */
                if (! read_logpack_data_from_wldev(lfd, super, logpack, buf, bufsize)) {
                        LOGe("read logpack data failed.\n");
                        goto error5;
                }

                /* Write logpack to data device. */
                if (! redo_logpack(dfd, lbs, pbs, logpack, buf)) {
                        LOGe("redo_logpack failed.\n");
                        goto error5;
                }
                
                lsid += logpack->total_io_size + 1;
        }

        /* Set new written_lsid and sync down. */
        u64 end_lsid = lsid;
        super->written_lsid = end_lsid;
        if (! write_super_sector(lfd, super)) {
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
bool do_show_wlog(const config_t *cfg)
{
        ASSERT(cfg->cmd_str);
        ASSERT(strcmp(cfg->cmd_str, "show_wlog") == 0);

        walblog_header_t* wh = (walblog_header_t *)malloc(WALBLOG_HEADER_SIZE);
        if (wh == NULL) { LOGe(NOMEM_STR); goto error0; }
        
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
        if (logpack == NULL) { LOGe(NOMEM_STR); goto error1; }

        /* Buffer for logpack data. */
        size_t bufsize = 1024 * 1024; /* 1MB */
        u8 *buf = alloc_sectors(physical_bs, bufsize / physical_bs);
        if (buf == NULL) { LOGe(NOMEM_STR); goto error2; }

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
                        if (! realloc_sectors(&buf, physical_bs, total_io_size)) {
                                LOGe("realloc_sectors failed.\n");
                                goto error3;
                        }
                        bufsize = total_io_size * physical_bs;
                }

                /* Read logpack data. */
                if (! read_logpack_data(0, logical_bs, physical_bs, logpack, buf, bufsize)) {
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
bool do_show_wldev(const config_t *cfg)
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
        walb_super_sector_t *super_sectp = 
                (walb_super_sector_t *)alloc_sector(physical_bs);
        if (super_sectp == NULL) { LOGe(NOMEM_STR); goto error1; }

        u64 off0 = get_super_sector0_offset(physical_bs);
        if (! read_sector(fd, (u8 *)super_sectp, physical_bs, off0)) {
                LOGe("read super sector0 failed.\n");
                goto error1;
        }
        
        walb_logpack_header_t *logpack =
                (walb_logpack_header_t *)alloc_sector(physical_bs);
        if (logpack == NULL) { LOGe(NOMEM_STR); goto error2; }

        print_super_sector(super_sectp);
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
                if (! read_logpack_header_from_wldev(fd, super_sectp, lsid, logpack)) {
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
bool do_set_oldest_lsid(const config_t *cfg)
{
        ASSERT(strcmp(cfg->cmd_str, "set_oldest_lsid") == 0);
        
        struct walb_ctl ctl = {
                .command = WALB_IOCTL_OLDEST_LSID_SET,
                .val_u64 = cfg->lsid,
                .u2k = { .buf_size = 0 },
                .k2u = { .buf_size = 0 },
        };
        
        if (! invoke_ioctl(cfg->wdev_name, &ctl, O_RDWR)) { goto error0; }
        
        LOGn("oldest_lsid is set to %"PRIu64" successfully.\n", cfg->lsid);

        return true;
error0:
        return false;
}

/**
 * Get oldest_lsid.
 */
bool do_get_oldest_lsid(const config_t *cfg)
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
bool do_get_written_lsid(const config_t *cfg)
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
 * Get log usage.
 */
bool do_get_log_usage(const config_t *cfg)
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
bool do_get_log_capacity(const config_t *cfg)
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
bool do_get_version(const config_t *cfg)
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
bool do_help(__attribute__((unused)) const config_t *cfg)
{
        show_help();
        return true;
}

/*******************************************************************************
 * Functions for main.
 *******************************************************************************/

/**
 * For command string to function.
 */
typedef bool (*command_fn)(const config_t *);

struct map_str_to_fn {
        const char *str;
        command_fn fn;
};


/**
 * Dispatch command.
 */
bool dispatch(const config_t *cfg)
{
        bool ret = false;
        ASSERT(cfg->cmd_str != NULL);

        struct map_str_to_fn map[] = {
                { "format_ldev", do_format_ldev },
                { "create_wdev", do_create_wdev },
                { "delete_wdev", do_delete_wdev },
                /* { "create_snapshot", do_create_snapshot }, */
                /* { "delete_snapshot", do_delete_snapshot }, */
                /* { "num_snapshot", do_num_snapshot }, */
                /* { "list_snapshot", do_list_snapshot }, */
                /* { "checkpoint", do_checkpoint }, */
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

int main(int argc, char* argv[])
{
        config_t cfgt;

        init_random();
        init_config(&cfgt);
        
        if (parse_opt(argc, argv, &cfgt) != 0) {
                return 1;
        }
        
        if (! dispatch(&cfgt)) {
                LOGe("operation failed.\n");
        }

        return 0;
}
