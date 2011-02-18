/**
 * test_snapshot.c - Test for snapshot code.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <stdio.h>
#include <unistd.h>

#include "snapshot.h"

#define LOG_DEVICE_FILE "snapshot_test.tmp"

/**
 * @sector_size sector size in bytes.
 * @n_snapshots number of snapshots.
 */
void test(int sector_size, int n_snapshots)
{
        /* Create file as log device. */
        int fd = open(LOG_DEVICE_FILE, O_RDWR | O_TRUNC);
        ASSERT(fd > 0);

        /* Prepare psuedo super sector. */
        
        /* now editing */
        walb_super_sector_t *super_sect;
        
        
        
        /* Prepare snapshot data for userland. */
        struct snapshot_data_u *snapd;
        snapd = alloc_snapshot_data_u(fd, super_sect);
        clear_snapshot_data_u(fd, snapd);
        initialize_snapshot_data_u(fd, snapd);

        /* Write snapshot data. */
        write_all_sectors_snapshot_data_u(snapd);
        

        /* add several snapshot record */
        
        /* search snapshot record */
        
        /* delete snapshot record */

        /* write all snapshot sectors. */

        
        
        free_snapshot_data_u(snapd);
        close(fd);
}

int main()
{
        test(512, 1000);
        /* test(512, 100000); */
        test(4096, 1000);
        /* test(4096, 100000); */

        return 0;
}

/* end of file. */
