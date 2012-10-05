/**
 * test_super.c - Test for super sector code.
 *
 * Copyright(C) 2010,2011, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <stdio.h>

#include "util.h"
#include "walb/super.h"

#define DATA_DEV_SIZE (32 * 1024 * 1024)
#define LOG_DEV_SIZE  (16 * 1024 * 1024)

#define LOG_DEV_FILE "tmp/test_super.log_dev"

/**
 *
 * @lbs logical block size.
 * @pbs physical block size.
 * @n_snapshots number of snapshots to manage.
 */
void test(int lbs, int pbs, u64 ddev_lb, u64 ldev_lb,
	int n_snapshots, const char *name)
{
	struct sector_data *super_sect = sector_alloc(pbs);
	ASSERT(super_sect);
	init_super_sector(super_sect,
			lbs, pbs,
			ddev_lb, ldev_lb, n_snapshots,
			name);
	ASSERT_SUPER_SECTOR(super_sect);
	print_super_sector(super_sect);

	int fd = open(LOG_DEV_FILE, O_RDWR | O_CREAT | O_TRUNC, 00775);
	ASSERT(fd > 0);

	bool ret;
	ret = write_super_sector(fd, super_sect);
	ASSERT(ret);
	print_super_sector(super_sect);

	ret = read_super_sector(fd, super_sect);
	ASSERT(ret);
	print_super_sector(super_sect);

	close(fd);
}

int main()
{
	int ddev_lb = DATA_DEV_SIZE / 512;
	int ldev_lb = LOG_DEV_SIZE / 512;
	
	test(512, 512, ddev_lb, ldev_lb, 1000, "");
	test(512, 4096, ddev_lb, ldev_lb, 1000, NULL);
	test(4096, 4096, ddev_lb, ldev_lb, 1000, "");
	test(512, 512, ddev_lb, ldev_lb, 10000, "test_name");

	return 0;
}

/* end of file. */
