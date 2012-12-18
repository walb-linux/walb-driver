/**
 * test_snapshot.c - Test for snapshot code.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <stdio.h>
#include <unistd.h>

#include "util.h"
#include "walb_util.h"
#include "snapshot.h"

#define DATA_DEV_SIZE (32 * 1024 * 1024)
#define LOG_DEV_SIZE  (16 * 1024 * 1024)

#define LOG_DEV_FILE "tmp/snapshot_test.tmp"

/**
 * @sector_size sector size in bytes.
 * @n_snapshots number of snapshots.
 */
void test(int sector_size, int n_snapshots)
{
	/* Create file as log device. */
	int fd = open(LOG_DEV_FILE, O_RDWR | O_TRUNC |O_CREAT, 0755);
	ASSERT(fd > 0);

	/* Prepare psuedo super sector. */
	struct sector_data *super_sect = sector_alloc(sector_size);
	ASSERT_SECTOR_DATA(super_sect);
	init_super_sector(super_sect,
			512, sector_size,
			DATA_DEV_SIZE / 512, DATA_DEV_SIZE / 512,
			n_snapshots, "test_supersector_name");
	ASSERT_SUPER_SECTOR(super_sect);

	/* now editing */


	/* Prepare snapshot data for userland. */

	/* Write snapshot data. */

	/* add several snapshot record */

	/* search snapshot record */

	/* delete snapshot record */

	/* write all snapshot sectors. */

	/* read all snapshot sectors. */

	/* Check snapshot records are stored correctly. */


	close(fd);
}

int main()
{
	test(512, 1000);
	/* test(512, 100000); */
	test(4096, 10000);
	/* test(4096, 100000); */

	return 0;
}

/* end of file. */
