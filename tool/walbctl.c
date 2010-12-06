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
        
} config_t;

config_t cfg_;

void show_help()
{
        printf("log format: walbctl mklog --ldev [path] --ddev [path]\n"
               "cat log: walbctl cat --ldev [path]\n"
               "set oldest_lsid: walbctl set_oldest_lsid --wdev [path] --lsid [lsid]\n"
               "get oldest_lsid: walbctl get_oldest_lsid --wdev [path]\n");
}

void init_config(config_t* cfg)
{
        ASSERT(cfg != NULL);

        cfg->n_snapshots = 10000;
}


enum {
        OPT_LDEV = 1,
        OPT_DDEV,
        OPT_N_SNAP,
        OPT_WDEV,
        OPT_WLDEV,
        OPT_LSID
};


int parse_opt(int argc, char* const argv[])
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
                        {0, 0, 0, 0}
                };

                c = getopt_long(argc, argv, "", long_options, &option_index);
                if (c == -1) {
                        break;
                }
                switch (c) {
                case OPT_LDEV:
                        cfg_.ldev_name = strdup(optarg);
                        printf("ldev: %s\n", optarg);
                        break;
                case OPT_DDEV:
                        cfg_.ddev_name = strdup(optarg);
                        printf("ddev: %s\n", optarg);
                        break;
                case OPT_N_SNAP:
                        cfg_.n_snapshots = atoi(optarg);
                        break;
                case OPT_WDEV:
                        cfg_.wdev_name = strdup(optarg);
                        break;
                case OPT_WLDEV:
                        cfg_.wldev_name = strdup(optarg);
                        break;
                case OPT_LSID:
                        cfg_.lsid = atoll(optarg);
                        break;
                default:
                        printf("default\n");
                }
        }

        if (optind < argc) {
                printf("command: ");
                while (optind < argc) {
                        cfg_.cmd_str = strdup(argv[optind]);
                        printf("%s ", argv[optind]);
                        optind ++;
                }
                printf("\n");
        } else {
                show_help();
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
 *
 * @return true in success, or false.
 */
bool init_walb_metadata(int fd, int logical_bs, int physical_bs,
                        u64 ddev_lb, u64 ldev_lb, int n_snapshots)
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

        printf("metadata_size: %d\n", n_sectors);

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

        /* Write super sector */
        if (! write_super_sector(fd, &super_sect)) {
                LOG("write super sector failed.\n");
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
        print_super_sector(&super_sect);

        /* Read first snapshot sector and print for debug. */
        memset(snap_sectp, 0, physical_bs);
        if (! read_snapshot_sector(fd, &super_sect, snap_sectp, 0)) {
                goto error1;
        }
        print_snapshot_sector(snap_sectp, physical_bs);
        
#endif
        
        return true;

error1:
        free(snap_sectp);
error0:
        return false;
}


/**
 * Execute log device format.
 *
 * @return 0 in success, or -1.
 */
int format_log_dev()
{
        ASSERT(cfg_.cmd_str);
        ASSERT(strcmp(cfg_.cmd_str, "mklog") == 0);

        /*
         * Check devices.
         */
        if (check_bdev(cfg_.ldev_name) < 0) {
                printf("format_log_dev: check log device failed %s.\n",
                       cfg_.ldev_name);
        }
        if (check_bdev(cfg_.ddev_name) < 0) {
                printf("format_log_dev: check data device failed %s.\n",
                       cfg_.ddev_name);
        }

        /*
         * Block size.
         */
        int ldev_logical_bs = get_bdev_logical_block_size(cfg_.ldev_name);
        int ddev_logical_bs = get_bdev_logical_block_size(cfg_.ddev_name);
        int ldev_physical_bs = get_bdev_physical_block_size(cfg_.ldev_name);
        int ddev_physical_bs = get_bdev_physical_block_size(cfg_.ddev_name);
        if (ldev_logical_bs != ddev_logical_bs ||
            ldev_physical_bs != ddev_physical_bs) {
                printf("logical or physical block size is different.\n");
                goto error0;
        }
        int logical_bs = ldev_logical_bs;
        int physical_bs = ldev_physical_bs;

        /*
         * Device size.
         */
        u64 ldev_size = get_bdev_size(cfg_.ldev_name);
        u64 ddev_size = get_bdev_size(cfg_.ddev_name);

        /*
         * Debug print.
         */
        printf("logical_bs: %d\n"
               "physical_bs: %d\n"
               "ddev_size: %zu\n"
               "ldev_size: %zu\n",
               logical_bs, physical_bs, ddev_size, ldev_size);
        
        if (logical_bs <= 0 || physical_bs <= 0 ||
            ldev_size == (u64)(-1) || ldev_size == (u64)(-1) ) {
                printf("getting block device parameters failed.\n");
                goto error0;
        }
        if (ldev_size % logical_bs != 0 ||
            ddev_size % logical_bs != 0) {
                printf("device size is not multiple of logical_bs\n");
                goto error0;
        }
        
        int fd;
        fd = open(cfg_.ldev_name, O_RDWR);
        if (fd < 0) {
                perror("open failed");
                goto error0;
        }

        if (! init_walb_metadata(fd, logical_bs, physical_bs,
                                 ddev_size / logical_bs,
                                 ldev_size / logical_bs,
                                 cfg_.n_snapshots)) {

                printf("initialize walb log device failed.\n");
                goto error1;
        }
        
        close(fd);
        return 0;

error1:
        close(fd);
error0:
        return -1;
}


/**
 * Execute log device format.
 *
 * @return 0 in success, or -1.
 */
int cat_log()
{
        ASSERT(cfg_.cmd_str);
        ASSERT(strcmp(cfg_.cmd_str, "cat") == 0);

        /*
         * Check device.
         */
        if (check_bdev(cfg_.ldev_name) < 0) {
                printf("cat_log: check log device failed %s.\n",
                       cfg_.ldev_name);
        }
        int physical_bs = get_bdev_physical_block_size(cfg_.ldev_name);

        int fd = open(cfg_.ldev_name, O_RDONLY);
        if (fd < 0) {
                perror("open failed");
                goto error0;
        }
        
        /* Allocate memory and read super block */
        walb_super_sector_t *super_sectp = 
                (walb_super_sector_t *)alloc_sector(physical_bs);
        if (super_sectp == NULL) { goto error1; }

        u64 off0 = get_super_sector0_offset(physical_bs);
        if (! read_sector(fd, (u8 *)super_sectp, physical_bs, off0)) {
                LOG("read super sector0 failed.\n");
                goto error1;
        }
        
        walb_logpack_header_t *logpack =
                (walb_logpack_header_t *)alloc_sector(physical_bs);
        if (logpack == NULL) { goto error2; }

        print_super_sector(super_sectp);
        u64 lsid = super_sectp->oldest_lsid;
        while (true) {
                if (! read_logpack_header(fd, super_sectp, lsid, logpack)) {
                        break;
                }
                print_logpack_header(logpack);
                lsid += logpack->total_io_size + 1;
        }

        free(logpack);
        free(super_sectp);
        close(fd);
        return 0;

/* error3: */
/*         free(logpack); */
error2:
        free(super_sectp);
error1:
        close(fd);
error0:
        return -1;
}

/**
 * Set oldest_lsid.
 */
int set_oldest_lsid()
{
        if (check_bdev(cfg_.wdev_name) < 0) {
                printf("set_oldest_lsid: check walb device failed %s.\n",
                       cfg_.wdev_name);
        }
        int fd = open(cfg_.wdev_name, O_RDWR);
        if (fd < 0) {
                perror("open failed");
                goto error0;
        }

        u64 lsid = cfg_.lsid;
        int ret = ioctl(fd, WALB_IOCTL_SET_OLDESTLSID, &lsid);
        if (ret < 0) {
                printf("set_oldest_lsid: ioctl failed.\n");
                goto error1;
        }
        printf("oldest_lsid is set to %"PRIu64" successfully.\n", lsid);
        close(fd);
        return 0;
        
error1:
        close(fd);
error0:
        return -1;
}

/**
 * Get oldest_lsid.
 */
int get_oldest_lsid()
{
        if (check_bdev(cfg_.wdev_name) < 0) {
                printf("get_oldest_lsid: check walb device failed %s.\n",
                       cfg_.wdev_name);
        }
        int fd = open(cfg_.wdev_name, O_RDONLY);
        if (fd < 0) {
                perror("open failed");
                goto error0;
        }

        u64 lsid;
        int ret = ioctl(fd, WALB_IOCTL_GET_OLDESTLSID, &lsid);
        if (ret < 0) {
                printf("get_oldest_lsid: ioctl failed.\n");
                goto error1;
        }
        printf("oldest_lsid is %"PRIu64"\n", lsid);
        close(fd);
        return 0;
        
error1:
        close(fd);
error0:
        return -1;
}        


void dispatch()
{
        ASSERT(cfg_.cmd_str != NULL);
        if (strcmp(cfg_.cmd_str, "mklog") == 0) {
                format_log_dev();
        } else if (strcmp(cfg_.cmd_str, "catlog") == 0) {
                cat_log();
        } else if (strcmp(cfg_.cmd_str, "set_oldest_lsid") == 0) {
                set_oldest_lsid();
        } else if (strcmp(cfg_.cmd_str, "get_oldest_lsid") == 0) {
                get_oldest_lsid();
        }
}

int main(int argc, char* argv[])
{
        init_random();
        
        init_config(&cfg_);
        if (parse_opt(argc, argv) != 0) {
                return 0;
        }
        dispatch();
        
        return 0;
}
