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
        
} config_t;

config_t cfg_;

void show_help()
{
        printf("log format: walbctl mklog --ldev [path] --ddev [path]\n"
               "cat log: walbctl catlog --wldev [path] "
               "(--lsid0 [from lsid]) (--lsid1 [to lsid])\n"
               "print log: walbctl printlog --wldev [path] "
               "(--lsid0 [from lsid]) (--lsid1 [to lsid])\n"
               "set oldest_lsid: walbctl set_oldest_lsid --wdev [path] --lsid [lsid]\n"
               "get oldest_lsid: walbctl get_oldest_lsid --wdev [path]\n");
}

void init_config(config_t* cfg)
{
        ASSERT(cfg != NULL);

        cfg->n_snapshots = 10000;

        cfg->lsid0 = (u64)(-1);
        cfg->lsid1 = (u64)(-1);
}


enum {
        OPT_LDEV = 1,
        OPT_DDEV,
        OPT_N_SNAP,
        OPT_WDEV,
        OPT_WLDEV,
        OPT_LSID,
        OPT_LSID0,
        OPT_LSID1
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
                        {"lsid0", 1, 0, OPT_LSID0},
                        {"lsid1", 1, 0, OPT_LSID1},
                        {0, 0, 0, 0}
                };

                c = getopt_long(argc, argv, "", long_options, &option_index);
                if (c == -1) {
                        break;
                }
                switch (c) {
                case OPT_LDEV:
                        cfg_.ldev_name = strdup(optarg);
                        LOG("ldev: %s\n", optarg);
                        break;
                case OPT_DDEV:
                        cfg_.ddev_name = strdup(optarg);
                        LOG("ddev: %s\n", optarg);
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
                case OPT_LSID0:
                        cfg_.lsid0 = atoll(optarg);
                        break;
                case OPT_LSID1:
                        cfg_.lsid1 = atoll(optarg);
                        break;
                default:
                        LOG("unknown option.\n");
                }
        }

        if (optind < argc) {
                LOG("command: ");
                while (optind < argc) {
                        cfg_.cmd_str = strdup(argv[optind]);
                        LOG("%s ", argv[optind]);
                        optind ++;
                }
                LOG("\n");
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

        LOG("metadata_size: %d\n", n_sectors);

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
 * Execute log device format.
 *
 * @return true in success, or false.
 */
bool format_log_dev()
{
        ASSERT(cfg_.cmd_str);
        ASSERT(strcmp(cfg_.cmd_str, "mklog") == 0);

        /*
         * Check devices.
         */
        if (check_bdev(cfg_.ldev_name) < 0) {
                LOG("format_log_dev: check log device failed %s.\n",
                    cfg_.ldev_name);
        }
        if (check_bdev(cfg_.ddev_name) < 0) {
                LOG("format_log_dev: check data device failed %s.\n",
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
                LOG("logical or physical block size is different.\n");
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
        LOG("logical_bs: %d\n"
            "physical_bs: %d\n"
            "ddev_size: %zu\n"
            "ldev_size: %zu\n",
            logical_bs, physical_bs, ddev_size, ldev_size);
        
        if (logical_bs <= 0 || physical_bs <= 0 ||
            ldev_size == (u64)(-1) || ldev_size == (u64)(-1) ) {
                LOG("getting block device parameters failed.\n");
                goto error0;
        }
        if (ldev_size % logical_bs != 0 ||
            ddev_size % logical_bs != 0) {
                LOG("device size is not multiple of logical_bs\n");
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

                LOG("initialize walb log device failed.\n");
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
 * Cat logpack in specified range.
 *
 * @return true in success, or false.
 */
bool cat_log()
{
        ASSERT(cfg_.cmd_str);
        ASSERT(strcmp(cfg_.cmd_str, "catlog") == 0);

        /*
         * Check device.
         */
        if (check_bdev(cfg_.wldev_name) < 0) {
                LOG("cat_log: check log device failed %s.\n",
                    cfg_.wldev_name);
        }
        int logical_bs = get_bdev_logical_block_size(cfg_.wldev_name);
        int physical_bs = get_bdev_physical_block_size(cfg_.wldev_name);

        int fd = open(cfg_.wldev_name, O_RDONLY);
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

        /* print_super_sector(super_sectp); */
        u64 oldest_lsid = super_sectp->oldest_lsid;

        /* Range check */
        u64 lsid, begin_lsid, end_lsid;
        if (cfg_.lsid0 == (u64)(-1)) {
                begin_lsid = 0;
        } else {
                begin_lsid = cfg_.lsid0;
        }
        if (cfg_.lsid0 < oldest_lsid) {
                LOG("given lsid0 %"PRIu64" < oldest_lsid %"PRIu64"\n",
                    cfg_.lsid0, oldest_lsid);
                goto error3;
        }
        end_lsid = cfg_.lsid1;
        if (begin_lsid > end_lsid) {
                LOG("lsid0 < lsid1 property is required.\n");
                goto error3;
        }

        size_t bufsize = 1024 * 1024; /* 1MB */
        u8 *buf = alloc_sectors(physical_bs, bufsize / physical_bs);
        if (buf == NULL) {
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
        LOG("lsid %"PRIu64" to %"PRIu64"\n", begin_lsid, end_lsid);

        /* Write each logpack to stdout. */
        lsid = begin_lsid;
        while (lsid < end_lsid) {

                /* Logpack header */
                if (! read_logpack_header(fd, super_sectp, lsid, logpack)) {
                        break;
                }
                LOG("logpack %"PRIu64"\n", logpack->logpack_lsid);
                write_logpack_header(1, super_sectp, logpack);
                
                /* Realloc buffer if buffer size is not enough. */
                if (bufsize / physical_bs < logpack->total_io_size) {
                        if (! realloc_sectors(&buf, physical_bs, logpack->total_io_size)) {
                                LOG("realloc_sectors failed.\n");
                                goto error3;
                        }
                        bufsize = (u32)logpack->total_io_size * physical_bs;
                        LOG("realloc_sectors called. %zu bytes\n", bufsize);
                }

                /* Logpack data. */
                if (! read_logpack_data(fd, super_sectp, logpack, buf, bufsize)) {
                        LOG("read logpack data failed.\n");
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
 * Print wlog from stdin.
 *
 */
bool print_wlog()
{
        ASSERT(cfg_.cmd_str);
        ASSERT(strcmp(cfg_.cmd_str, "printwlog") == 0);

        walblog_header_t* wh = (walblog_header_t *)malloc(WALBLOG_HEADER_SIZE);
        if (wh == NULL) { goto error0; }
        
        /* Read and print wlog header. */
        read_data(0, (u8 *)wh, WALBLOG_HEADER_SIZE);
        print_wlog_header(wh);

        /* Check wlog header. */
        check_wlog_header(wh);
        
        /* Set block size. */
        int logical_bs = wh->logical_bs;
        int physical_bs = wh->physical_bs;
        if (physical_bs % logical_bs != 0) {
                LOG("physical_bs %% logical_bs must be 0.\n");
                goto error1;
        }
        int n_lb_in_pb = physical_bs / logical_bs;

        /* Buffer for logpack header. */
        struct walb_logpack_header *logpack;
        logpack = (struct walb_logpack_header *)alloc_sector(physical_bs);
        if (logpack == NULL) { goto error1; }

        /* Buffer for logpack data. */
        size_t bufsize = 1024 * 1024; /* 1MB */
        u8 *buf = alloc_sectors(physical_bs, bufsize / physical_bs);
        if (buf == NULL) { goto error2; }
        
        
        /* Read, print and check each logpack */
        while (read_data(0, (u8 *)logpack, physical_bs)) {

                /* Print logpack header. */
                print_logpack_header(logpack);

                /* Check buffer size */
                u32 total_io_size = logpack->total_io_size;
                if (total_io_size * physical_bs > bufsize) {
                        if (! realloc_sectors(&buf, physical_bs, total_io_size)) {
                                LOG("realloc_sectors failed.\n");
                                goto error3;
                        }
                        bufsize = total_io_size * physical_bs;
                }

                /* Read logpack data */
                if (! read_data(0, buf, total_io_size * physical_bs)) {
                        LOG("read logpack data failed.\n");
                        goto error3;
                }

                /* Confirm checksum. */
                int i;
                for (i = 0; i < logpack->n_records; i ++) {

                        if (logpack->record[i].is_padding == 0) {

                                int off_pb = logpack->record[i].lsid_local - 1;

                                int size_lb = logpack->record[i].io_size;                                                     int size_pb;
                                if (size_lb % n_lb_in_pb == 0) {
                                        size_pb = size_lb / n_lb_in_pb;
                                } else {
                                        size_pb = size_lb / n_lb_in_pb + 1;
                                }
                                
                                if (checksum(buf + (off_pb * physical_bs), size_pb * physical_bs) == logpack->record[i].checksum) {
                                        printf("record %d: checksum valid\n", i);
                                } else {
                                        printf("record %d: checksum invalid\n", i);
                                }
                                
                                
                        } else {
                                printf("record %d: padding\n", i);
                        }

                }
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
 * Print logpack in specified range.
 *
 * @return true in success, or false.
 */
bool print_log()
{
        ASSERT(cfg_.cmd_str);
        ASSERT(strcmp(cfg_.cmd_str, "printlog") == 0);

        /*
         * Check device.
         */
        if (check_bdev(cfg_.wldev_name) < 0) {
                LOG("check log device failed %s.\n",
                    cfg_.wldev_name);
        }
        int physical_bs = get_bdev_physical_block_size(cfg_.wldev_name);

        int fd = open(cfg_.wldev_name, O_RDONLY);
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
        u64 oldest_lsid = super_sectp->oldest_lsid;

        /* Range check */
        u64 lsid, begin_lsid, end_lsid;
        if (cfg_.lsid0 == (u64)(-1)) {
                begin_lsid = 0;
        } else {
                begin_lsid = cfg_.lsid0;
        }
        if (cfg_.lsid0 < oldest_lsid) {
                LOG("given lsid0 %"PRIu64" < oldest_lsid %"PRIu64"\n",
                    cfg_.lsid0, oldest_lsid);
                goto error3;
        }
        end_lsid = cfg_.lsid1;
        if (begin_lsid > end_lsid) {
                LOG("lsid0 < lsid1 property is required.\n");
                goto error3;
        }
        
        /* Print each logpack header. */
        lsid = begin_lsid;
        while (lsid < end_lsid) {
                if (! read_logpack_header(fd, super_sectp, lsid, logpack)) {
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
bool set_oldest_lsid()
{
        if (check_bdev(cfg_.wdev_name) < 0) {
                LOG("set_oldest_lsid: check walb device failed %s.\n",
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
                LOG("set_oldest_lsid: ioctl failed.\n");
                goto error1;
        }
        printf("oldest_lsid is set to %"PRIu64" successfully.\n", lsid);
        close(fd);
        return true;
        
error1:
        close(fd);
error0:
        return false;
}

/**
 * Get oldest_lsid.
 */
bool get_oldest_lsid()
{
        if (check_bdev(cfg_.wdev_name) < 0) {
                LOG("get_oldest_lsid: check walb device failed %s.\n",
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
                LOG("get_oldest_lsid: ioctl failed.\n");
                goto error1;
        }
        printf("oldest_lsid is %"PRIu64"\n", lsid);
        close(fd);
        return true;
        
error1:
        close(fd);
error0:
        return false;
}        

bool dispatch()
{
        bool ret = false;
        ASSERT(cfg_.cmd_str != NULL);

        
        if (strcmp(cfg_.cmd_str, "mklog") == 0) {
                ret = format_log_dev();
        } else if (strcmp(cfg_.cmd_str, "catlog") == 0) {
                ret = cat_log();
        } else if (strcmp(cfg_.cmd_str, "printlog") == 0) {
                ret = print_log();
        } else if (strcmp(cfg_.cmd_str, "printwlog") == 0) {
                ret = print_wlog();
        } else if (strcmp(cfg_.cmd_str, "set_oldest_lsid") == 0) {
                ret = set_oldest_lsid();
        } else if (strcmp(cfg_.cmd_str, "get_oldest_lsid") == 0) {
                ret = get_oldest_lsid();
        }
        return ret;
}

int main(int argc, char* argv[])
{
        init_random();
        init_config(&cfg_);
        
        if (parse_opt(argc, argv) != 0) {
                return 1;
        }
        
        if (! dispatch()) {
                LOG("operation failed.\n");
        }
        
        return 0;
}
