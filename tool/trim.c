/**
 * Trim all blocks of a block device that supports
 * discard command.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <stdio.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <errno.h>

#include "walb/common.h"
#include "util.h"

int main(int argc, char *argv[])
{
	if (argc < 2) {
		LOGe("Specify a block device.\n");
		goto error0;
	}
	const char *dev_name = argv[1];

	u64 start_off = 0;
	u64 end_off = (u64)(-1);
	if (argc >= 4) {
		start_off = (u64)atoll(argv[2]);
		start_off *= 512;
		end_off = (u64)atoll(argv[3]);
		end_off *= 512;
	}

	if (!is_valid_bdev(dev_name)) {
		LOGe("Check block device failed %s.\n", dev_name);
		goto error0;
	}
	u64 len = get_bdev_size(dev_name);
	if (len == (u64)(-1)) {
		LOGe("Get device size failed.\n");
		goto error0;
	}
	ASSERT(len % 512 == 0);
	if (end_off > len) {
		end_off = len;
	}

	u64 range[2];
	range[0] = start_off;
	range[1] = end_off;

	int fd = open(dev_name, O_RDWR);
	if (fd < 0) {
		perror("open failed.");
		goto error0;
	}

	/* discard */
#if 1
	int ret = ioctl(fd, BLKDISCARD, &range);
#else
	int ret = ioctl(fd, BLKSECDISCARD, &range);
#endif
	if (ret) {
		LOGe("ioctl() error: %s\n", strerror(errno));
		goto error1;
	}

	if (close(fd)) {
		LOGe("close() error: %s\n", strerror(errno));
		goto error0;
	}
	return 0;

error1:
	close(fd);
error0:
	return 1;
}
