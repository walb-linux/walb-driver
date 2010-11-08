/**
 * Control walb device.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */

#include "walb.h"
#include "walb_log_device.h"
#include "walb_log_record.h"

#include "util.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

typedef struct config
{
        char* cmd_str; /* command string */
        char *ldev_name; /* log device name */

        size_t log_sector_size; /* sector size of log device */
        size_t log_dev_size; /* size of log device by the sector */

        int n_snapshots; /* maximum number of snapshots to keep */
        
} config_t;

config_t cfg_;

void show_help()
{
        printf("log format: walbctl mklog --ldev [block device path]\n");
}

void init_config(config_t* cfg)
{
        assert(cfg != NULL);

        cfg->n_snapshots = 10000;
}

int parse_opt(int argc, char* const argv[])
{
        int c;

        while (1) {
                int option_index = 0;
                static struct option long_options[] = {
                        {"ldev", 1, 0, 1}, /* log device */
                        {"n_snap", 1, 0, 2}, /* num of snapshots */
                        {0, 0, 0, 0}
                };

                c = getopt_long(argc, argv, "", long_options, &option_index);
                if (c == -1) {
                        break;
                }
                switch (c) {
                case 1:
                        cfg_.ldev_name = strdup(optarg);
                        printf("ldev: %s\n", optarg);
                        break;
                case 2:
                        cfg_.n_snapshots = atoi(optarg);
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
 * @sector_size sector size.
 * @size device size by the sector.
 * @n_snapshots number of snapshots to keep.
 *
 * @return 0 in success, or -1.
 */
int init_walb_metadata(int fd, int sector_size, u64 size, int n_snapshots)
{
        assert(fd >= 0);
        assert(sector_size > 0);
        assert(size < (u64)(-1));

        walb_super_sector_t super_sect;
        walb_snapshot_sector_t snap_sect;

        assert(sizeof(super_sect) <= sector_size);
        assert(sizeof(snap_sect) <= sector_size);
        

        /* Calculate number of snapshot sectors. */
        int n_sectors;
        int t = max_n_snapshots_in_sector(sector_size);
        n_sectors = (n_snapshots + t - 1) / t;

        printf("metadata_size: %d\n", n_sectors);

        /* Memory image of sector. */
        u8 sector_buf[sector_size];
        
        /* Prepare and write super sector */
        memset(&super_sect, 0, sizeof(super_sect));
        memset(sector_buf, 0, sector_size);

        super_sect.sector_size = sector_size;
        super_sect.snapshot_metadata_size = n_sectors;
        generate_uuid(super_sect.uuid);
        
        super_sect.start_offset = get_ring_buffer_offset(sector_size, n_snapshots);

        
        /* now editing */
        
        /* Prepare and write n_sectors times snapshot sectors */
        memset(&snap_sect, 0, sizeof(snap_sect));
        memset(sector_buf, 0, sector_size);


        
        return 0;

        
error:
        return -1;
}



/**
 * Execute log device format.
 *
 * @return 0 in success, or -1.
 */
int format_log_dev()
{
        assert(cfg_.cmd_str);
        assert(strcmp(cfg_.cmd_str, "mklog") == 0);
        
        if (check_log_dev(cfg_.ldev_name) < 0) {
                printf("format_log_dev: check failed.");
        }
        int sector_size = get_bdev_sector_size(cfg_.ldev_name);
        u64 size = get_bdev_size(cfg_.ldev_name);

        printf("sector_size: %d\n"
               "size: %zu\n", sector_size, size);


        if (sector_size < 0 || size == (u64)(-1)) {
                printf("getting block device parameters failed.\n");
                goto error;
        }
        
        int fd;
        fd = open(cfg_.ldev_name, O_RDWR);
        if (fd < 0) {
                perror("open failed");
                goto error;
        }

        if (init_walb_metadata(fd, sector_size, size, cfg_.n_snapshots) < 0) {

                printf("initialize walb log device failed.\n");
                goto close;
        }
        
        close(fd);
        return 0;

close:
        close(fd);
error:
        return -1;
}

void dispatch()
{
        assert(cfg_.cmd_str != NULL);
        if (strcmp(cfg_.cmd_str, "mklog") == 0) {

                format_log_dev();
        }
}

int main(int argc, char* const argv[])
{
        init_random();
        
        init_config(&cfg_);
        if (parse_opt(argc, argv) != 0) {
                return 0;
        }
        dispatch();
        
        return 0;
}
