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
 * Check block device.
 *
 * RETURN:
 *   true in success, or false.
 */
bool is_valid_bdev(const char* path)
{
	struct stat sb;
	UNUSED dev_t devt;
	UNUSED size_t sector_size;
	UNUSED size_t dev_size;
	UNUSED size_t ssize;
	int fd;
	u64 isize;
	int bs, ss;
	unsigned int pbs;

	if (!path) {
		LOGe("path is null.\n");
		return false;
	}
	if (*path == '\0') {
		LOGe("path length is zero.\n");
		return false;
	}
	if (stat(path, &sb) == -1) {
		LOGe("stat failed: %s.\n", strerror(errno));
		return false;
	}

	if ((sb.st_mode & S_IFMT) != S_IFBLK) {
		LOGe("%s is not block device.\n", path);
		return false;
	}

	devt = sb.st_rdev;
	sector_size = sb.st_blksize;
	dev_size = sb.st_blocks;
	ssize = sb.st_size;

	LOGd("devname: %s\n"
		"device: %d:%d\n"
		"sector_size: %zu\n"
		"device_size: %zu\n"
		"size: %zu\n",
		path,
		MAJOR(devt), MINOR(devt),
		sector_size, dev_size, ssize);

	fd = open(path, O_RDONLY);
	if (fd < 0) {
		LOGe("open failed\n");
		return false;
	}
	/* soft block size */
	if (ioctl(fd, BLKBSZGET, &bs) < 0) {
		goto error1;
	}
	/* logical sector size */
	if (ioctl(fd, BLKSSZGET, &ss) < 0) {
		goto error1;
	}
	/* physical sector size */
	if (ioctl(fd, BLKPBSZGET, &pbs) < 0) {
		goto error1;
	}
	/* size */
	if (ioctl(fd, BLKGETSIZE64, &isize) < 0) {
		goto error1;
	}
	if (close(fd)) {
		LOGe("close failed\n");
		return false;
	}

	LOGd("soft block size: %d\n"
		"logical sector size: %d\n"
		"physical sector size: %u\n"
		"device size: %" PRIu64 "\n",
		bs, ss, pbs, isize);
	return true;

error1:
	close(fd);
	return false;
}

/**
 * open file and confirm it is really block device.
 *
 * @devpath block device path.
 * RETURN:
 *   fd is succeeded, -1 else.
 */
static int open_blk_dev(const char* devpath)
{
	int fd;
	struct stat sb;
	ASSERT(devpath);

	fd = open(devpath, O_RDONLY);
	if (fd < 0) {
		perror("open failed");
		return -1;
	}
	if (fstat(fd, &sb) == -1) {
		perror("fstat failed");
		goto error1;
	}
	if ((sb.st_mode & S_IFMT) != S_IFBLK) {
		LOGe("%s is not block device.\n", devpath);
		goto error1;
	}
	return fd;

error1:
	close(fd);
	return -1;
}


/**
 * Get logical block size of the block device.
 *
 * @devpath device file path.
 * RETURN:
 *   sector size in succeeded, or 0.
 */
unsigned int get_bdev_logical_block_size(const char* devpath)
{
	int fd;
	unsigned int lbs;

	fd = open_blk_dev(devpath);
	if (fd < 0) {
		perror("open failed.");
		return 0;
	}
	if (ioctl(fd, BLKSSZGET, &lbs) < 0) {
		perror("ioctl failed");
		goto error1;
	}
	if (close(fd)) {
		perror("close failed.");
		return 0;
	}
	return lbs;

error1:
	close(fd);
	return 0;
}

/**
 * Get physical block size of the block device.
 *
 * @devpath device file path.
 * RETURN:
 *   sector size in succeeded, or 0.
 */
unsigned int get_bdev_physical_block_size(const char* devpath)
{
	int fd;
	unsigned int pbs;

	fd = open_blk_dev(devpath);
	if (fd < 0) {
		perror("open failed.");
		return 0;
	}
	if (ioctl(fd, BLKPBSZGET, &pbs) < 0) {
		perror("ioctl failed");
		goto error1;
	}
	if (close(fd)) {
		perror("close failed.");
		return 0;
	}
	return pbs;

error1:
	close(fd);
	return 0;
}

/**
 * Get block device size.
 *
 * @devpath device file path.
 * RETURN:
 *   size in bytes in success, or (u64)(-1).
 */
u64 get_bdev_size(const char* devpath)
{
	int fd;
	u64 size;
	const u64 err_size = (u64)(-1);

	fd = open_blk_dev(devpath);
	if (fd < 0) {
		return err_size;
	}
	if (ioctl(fd, BLKGETSIZE64, &size) < 0) {
		goto error1;
	}
	if (close(fd)) {
		return err_size;
	}
	return size;

error1:
	close(fd);
	return err_size;
}

/**
 * Get device id from the device file path.
 *
 * @devpath device file path.
 * RETURN:
 *   device id.
 */
dev_t get_bdev_devt(const char *devpath)
{
	struct stat sb;

	ASSERT(devpath);

	if (stat(devpath, &sb) == -1) {
		LOGe("%s stat failed.\n", devpath);
		return (dev_t)(-1);;
	}
	if ((sb.st_mode & S_IFMT) != S_IFBLK) {
		LOGe("%s is not block device.\n", devpath);
		return (dev_t)(-1);
	}
	return sb.st_rdev;
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
