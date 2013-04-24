/**
 * General definitions for Walb.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <stdio.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <time.h>
#include <errno.h>

#include "walb/common.h"
#include "walb/block_size.h"
#include "walb/util.h"
#include "random.h"
#include "util.h"

/**
 * hex print
 */
void print_binary_hex(const u8* data, size_t size)
{
	size_t i;
	for (i = 0; i < size; i++) {
		int c = (unsigned char)data[i];

		if (c == 0) {
			printf("__");
		} else {
			printf("%02X", c);
		}
		if (i % 32 == 31) {
			printf("\n");
		}
	}
}

/**
 * Datetime string.
 *
 * String format is like `date +%Y%m%d-%H%M%S` using UTC.
 *
 * @t unix time.
 * @buf result buffer.
 * @n buffer size.
 *
 * RETURN:
 *   true in success, or false.
 */
bool get_datetime_str(time_t t, char* buf, size_t n)
{
	struct tm m;

	if (gmtime_r(&t, &m) == NULL) { return false; }
	return strftime(buf, n, "%Y%m%d-%H%M%S", &m) != 0;
}

/**
 * Open a block device and get its information.
 *
 * @path block device path.
 * @info_p pointer to store block device information.
 * @fd_p pointer to store file descriptor.
 * @flags open flags.
 *
 * RETURN:
 *   true if successfully opened and got its information, or false.
 */
bool open_bdev_and_get_info(
	const char *path, struct bdev_info *info_p, int *fd_p, int flags)
{
	if (!path) {
		LOGe("path is null.\n");
		return false;
	}
	if (*path == '\0') {
		LOGe("path length is zero.\n");
		return false;
	}
	ASSERT(info_p);
	ASSERT(fd_p);

	*fd_p = open(path, flags);
	if (*fd_p < 0) {
		LOGe("open failed\n");
		return false;
	}

	if (fstat(*fd_p, &info_p->sb) == -1) {
		LOGe("fstat failed.\n");
		goto error1;
	}
	if ((info_p->sb.st_mode & S_IFMT) != S_IFBLK) {
		LOGe("%s is not block device.\n", path);
		goto error1;
	}
	info_p->devt = info_p->sb.st_rdev;

	/* size */
	if (ioctl(*fd_p, BLKGETSIZE64, &info_p->size) < 0) {
		LOGe("ioctl BLKGETSIZE64 failed.\n");
		goto error1;
	}
	/* logical sector size */
	if (ioctl(*fd_p, BLKSSZGET, &info_p->lbs) < 0) {
		LOGe("ioctl BLKSSZGET failed.\n");
		goto error1;
	}
	/* physical sector size */
	if (ioctl(*fd_p, BLKPBSZGET, &info_p->pbs) < 0) {
		LOGe("ioctl BLKPSZGET failed.\n");
		goto error1;
	}
	/* Check lbs/pbs. */
	if (info_p->lbs != LOGICAL_BLOCK_SIZE || !is_valid_pbs(info_p->pbs)) {
		LOGe("Wong block size (%u, %u).\n", info_p->lbs, info_p->pbs);
		goto error1;
	}
	/* Do not close the device. */
	return true;

error1:
	close(*fd_p);
	return false;
}

/**
 * Check and get information of a block device.
 */
bool get_bdev_info(const char *path, struct bdev_info *info_p)
{
	int fd;
	if (!open_bdev_and_get_info(path, info_p, &fd, O_RDONLY)) {
		return false;
	}
	ASSERT(0 < fd);
	close(fd);
	return true;
}

/**
 * Check block size equality of two block devices.
 */
bool is_block_size_same(const struct bdev_info *info0, const struct bdev_info *info1)
{
	ASSERT(info0);
	ASSERT(info1);

	if (info0->lbs != info1->lbs) {
		LOGe("Logical block size are not the same %u %u.\n", info0->lbs, info1->lbs);
		return false;
	}
	if (info0->pbs != info1->pbs) {
		LOGe("Physical block size are not the same %u %u.\n", info0->pbs, info1->pbs);
		return false;
	}
	return true;
}

/**
 * Check discard request support by
 * trying to discard the first physical sector.
 *
 * CAUSION:
 *   The first physical sector may be discarded.
 *
 * @fd opened file descriptor.
 *
 * RETURN:
 *   true if the device support discard requests.
 */
bool is_discard_supported(int fd)
{
	unsigned int pbs;
	int ret;
	u64 range[2] = { 0, 0 };

	if (fd < 0) { return false; }

	/* Physical block size [bytes]. */
	ret = ioctl(fd, BLKPBSZGET, &pbs);
	if (ret < 0) { return false; }
	ASSERT_PBS(pbs);
	range[1] = pbs;

	/* Try to discard. */
	ret = ioctl(fd, BLKDISCARD, &range);
	return ret == 0;
}

/**
 * Discard whole area of the block device.
 *
 * @fd opened file descriptor.
 *
 * RETURN:
 *   true if the whole device area has been discarded.
 */
bool discard_whole_area(int fd)
{
	u64 range[2] = { 0, 0 };
	u64 dev_size;
	int ret;

	if (fd < 0) {
		LOGe("fd < 0.\n");
		return false;
	}

	/* Device size [bytes]. */
	ret = ioctl(fd, BLKGETSIZE64, &dev_size);
	if (ret < 0) {
		LOGe("ioctl() failed: %s.\n", strerror(errno));
		return false;
	}
	range[1] = dev_size;

	/* Discard whole area. */
	ret = ioctl(fd, BLKDISCARD, &range);
	if (ret) {
		LOGe("discard failed: %s\n", strerror(errno));
		return false;
	}
	return true;
}

/**
 * Generate uuid
 *
 * @uuid result uuid is stored. it must be u8[UUID_SIZE].
 */
bool generate_uuid(u8* uuid)
{
	return read_urandom(uuid, UUID_SIZE);
}

/**
 * Print uuid.
 *
 * @uuid result uuid is stored. it must be u8[UUID_SIZE].
 */
void print_uuid(const u8* uuid)
{
	size_t i;
	for (i = 0; i < UUID_SIZE; i++) {
		printf("%02x", (unsigned char)uuid[i]);
	}
}

/**
 * Copy uuid.
 *
 * @dst destination.
 * @src source.
 */
void copy_uuid(u8* dst, const u8* src)
{
	memcpy(dst, src, UUID_SIZE);
}

/**
 * Read sector data from the offset.
 *
 * @sector_buf aligned buffer to be filled with read sector data.
 * @sector_size sector size in bytes.
 * @offset offset in sectors [bytes].
 *
 * RETURN:
 *   true in success, or false.
 */
bool read_sector_raw(int fd, u8* sector_buf, u32 sector_size, u64 offset)
{
	return read_sectors_raw(fd, sector_buf, sector_size, offset, 1);
}


/**
 * Read multiple sectors data from the offset.
 *
 * @sectors_buf aligned buffer to be filled with read sectors data.
 * @sector_size sector size in bytes.
 * @offset offset in sectors [bytes].
 * @n number of sectors to read.
 *
 * RETURN:
 *   true in success, or false.
 */
bool read_sectors_raw(
	int fd, u8* sectors_buf, u32 sector_size, u64 offset, int n)
{
	ssize_t r = 0;
	while (r < sector_size * n) {
		ssize_t s = pread(fd, sectors_buf + r,
				sector_size * n - r,
				offset * sector_size + r);
		if (s > 0) {
			r += s;
		} else {
			perror("read sector error.");
			return false;
		}
	}
	ASSERT(r == sector_size * n);
	return true;
}

/**
 * Write sector data to the offset.
 *
 * @fd file descriptor to write.
 * @sector_buf aligned buffer containing sector data.
 * @sector_size sector size in bytes.
 * @offset offset in sectors [bytes].
 *
 * RETURN:
 *   true in success, or false.
 */
bool write_sector_raw(
	int fd, const u8* sector_buf, u32 sector_size, u64 offset)
{
	return write_sectors_raw(fd, sector_buf, sector_size, offset, 1);
}

/**
 * Write multiple sectors data to the offset.
 *
 * @fd file descriptor to write.
 * @sectors_buf aligned buffer containing sectors data.
 * @sector_size sector size in bytes.
 * @offset offset in sectors [bytes].
 * @n number of sectors to be written.
 *
 * RETURN:
 *   true in success, or false.
 */
bool write_sectors_raw(
	int fd, const u8* sectors_buf, u32 sector_size, u64 offset, int n)
{
	ssize_t w = 0;
	while (w < sector_size * n) {
		ssize_t s = pwrite(fd, sectors_buf + w,
				sector_size * n - w,
				offset * sector_size + w);
		if (s > 0) {
			w += s;
		} else {
			perror("write sector error.");
			return false;
		}
	}
	ASSERT(w == sector_size * n);
	return true;
}

/**
 * Read data.
 * This is for stream.
 *
 * @fd file descriptor.
 * @data pointer to store data.
 * @size read size [bytes].
 *
 * RETURN:
 *   true in success, or false.
 */
bool read_data(int fd, u8* data, size_t size)
{
	size_t r = 0;
	ssize_t tmp;

	while (r < size) {
		tmp = read(fd, data + r, size - r);
		if (tmp <= 0) { return false; }
		r += tmp;
	}
	ASSERT(r == size);
	return true;
}

/**
 * Write data.
 * This is for stream.
 *
 * @fd file descriptor.
 * @data data pointer.
 * @size write size [bytes].
 *
 * RETURN:
 *   true in success, or false.
 */
bool write_data(int fd, const u8* data, size_t size)
{
	size_t w = 0;
	ssize_t tmp;

	while (w < size) {
		tmp = write(fd, data + w, size - w);
		if (tmp <= 0) { return false; }
		w += tmp;
	}
	ASSERT(w == size);
	return true;
}

/* end of file */
