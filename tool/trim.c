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
	char *dev_path;
	u64 start_off = 0;
	u64 end_off = -1;
	u64 offsetAndSize[2];
	int fd, ret;
	struct bdev_info dev_info;

	/* Get command line arguments. */
	if (argc < 2) {
		LOGe("Specify a block device.\n");
		return 1;
	}
	dev_path = argv[1];
	if (argc >= 4) {
		start_off = (u64)atoll(argv[2]);
		start_off *= 512;
		end_off = (u64)atoll(argv[3]);
		end_off *= 512;
	}

	if (!open_bdev_and_get_info(dev_path, &dev_info, &fd, O_RDWR)) {
		return 1;
	}

	ASSERT(dev_info.size % 512 == 0);
	if (end_off > dev_info.size) {
		end_off = dev_info.size;
	}
	if (end_off <= start_off) {
		LOGe("start offset must be < end offset.\n");
		goto error1;
	}

	offsetAndSize[0] = start_off;
	offsetAndSize[1] = end_off - start_off;

	/* discard */
#if 1
	ret = ioctl(fd, BLKDISCARD, &offsetAndSize);
#else
	ret = ioctl(fd, BLKSECDISCARD, &offsetAndSize);
#endif
	if (ret) {
		LOGe("ioctl() error: %s\n", strerror(errno));
		goto error1;
	}

	if (close(fd)) {
		LOGe("close() error: %s\n", strerror(errno));
		return 1;
	}
	return 0;

error1:
	close(fd);
	return 1;
}

/* end of file. */
