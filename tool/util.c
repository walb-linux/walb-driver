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

#include "walb/walb.h"
#include "walb/block_size.h"
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
 * @return true in success, or false.
 */
bool get_datetime_str(time_t t, char* buf, size_t n)
{
	struct tm m;
	int ret;
	size_t len;

	if (gmtime_r(&t, &m) == NULL) { return false; }

	ret = snprintf(buf, n, "%d%02d%02d-%02d%02d%02d",
		m.tm_year + 1900,
		m.tm_mon + 1,
		m.tm_mday,
		m.tm_hour,
		m.tm_min,
		m.tm_sec);
	if (ret < 0) { return false; }
	len = (size_t)ret;
	
	ASSERT(len <= n);
	return (len < n ? true : false);
}

/**
 * Check block device.
 *
 * @return 0 in success, or -1.
 */
int check_bdev(const char* path)
{
	struct stat sb;
	dev_t devt;
	size_t sector_size;
	size_t __attribute__((unused)) dev_size;
	size_t __attribute__((unused)) size;
	
	if (path == NULL) {
		LOGe("path is null.\n");
		return -1;
	}
	
	if (stat(path, &sb) == -1) {
		LOGe("stat failed.\n");
		perror("");
		return -1;
	}

	if ((sb.st_mode & S_IFMT) != S_IFBLK) {
		LOGe("%s is not block device.\n", path);
		return -1;
	}

	devt = sb.st_rdev;
	sector_size = sb.st_blksize;
	dev_size = sb.st_blocks;
	size = sb.st_size;

	LOGd("devname: %s\n"
		"device: %d:%d\n"
		"sector_size: %zu\n"
		"device_size: %zu\n"
		"size: %zu\n",
		path,
		MAJOR(devt), MINOR(devt),
		sector_size, dev_size, size);

	{
		int fd;
		u64 size;
		int bs, ss;
		unsigned int pbs;

		fd = open(path, O_RDONLY);
		if (fd < 0) {
			LOGe("open failed\n");
			return -1;
		}
		ioctl(fd, BLKBSZGET, &bs); /* soft block size */
		ioctl(fd, BLKSSZGET, &ss); /* logical sector size */
		ioctl(fd, BLKPBSZGET, &pbs); /* physical sector size */
		ioctl(fd, BLKGETSIZE64, &size); /* size */
		close(fd);

		LOGd("soft block size: %d\n"
			"logical sector size: %d\n"
			"physical sector size: %u\n"
			"device size: %zu\n",
			bs, ss, pbs, (size_t)size);
	}
	
	return 0;
}



/**
 * open file and confirm it is really block device.
 *
 * @devpath block device path.
 * @return fd is succeeded, -1 else.
 */
static int open_blk_dev(const char* devpath)
{
	int fd;
	struct stat sb;
	ASSERT(devpath);
	
	fd = open(devpath, O_RDONLY);
	if (fd < 0) {
		perror("open failed");
		goto error;
	}
	
	if (fstat(fd, &sb) == -1) {
		perror("fstat failed");
		goto close;
	}
	
	if ((sb.st_mode & S_IFMT) != S_IFBLK) {
		LOGe("%s is not block device.\n", devpath);
		goto close;
	}

	return fd;
	
close:
	close(fd);
error:
	return -1;
}


/**
 * Get logical block size of the block device.
 *
 * @devpath device file path.
 * @return sector size in succeeded, or -1.
 */
int get_bdev_logical_block_size(const char* devpath)
{
	int fd;
	unsigned int pbs;

	fd = open_blk_dev(devpath);
	if (fd < 0) { return -1; }
	
	if (ioctl(fd, BLKSSZGET, &pbs) < 0) {
		perror("ioctl failed");
		return -1;
	}
	close(fd);
	return (int)pbs;
}

/**
 * Get physical block size of the block device.
 *
 * @devpath device file path.
 * @return sector size in succeeded, or -1.
 */
int get_bdev_physical_block_size(const char* devpath)
{
	int fd;
	unsigned int pbs;

	fd = open_blk_dev(devpath);
	if (fd < 0) { return -1; }
	
	if (ioctl(fd, BLKPBSZGET, &pbs) < 0) {
		perror("ioctl failed");
		return -1;
	}
	close(fd);
	return (int)pbs;
}

/**
 * Get block device size.
 *
 * @devpath device file path.
 * @return size in bytes.
 */
u64 get_bdev_size(const char* devpath)
{
	int fd;
	u64 size;
	
	fd = open_blk_dev(devpath);
	if (fd < 0 ||
		ioctl(fd, BLKGETSIZE64, &size) < 0) {
		return (u64)(-1); 
	}
	close(fd);

	return size;
}

/**
 * Get device id from the device file path.
 *
 * @devpath device file path.
 * @return device id.
 */
dev_t get_bdev_devt(const char *devpath)
{
	struct stat sb;

	ASSERT(devpath);
	
	if (stat(devpath, &sb) == -1) {
		LOGe("%s stat failed.\n", devpath);
		goto error;
	}

	if ((sb.st_mode & S_IFMT) != S_IFBLK) {
		LOGe("%s is not block device.\n", devpath);
		goto error;
	}

	return sb.st_rdev;

error:
	return (dev_t)(-1);
}

/**
 * Generate uuid
 *
 * @uuid result uuid is stored. it must be u8[16].
 */
void generate_uuid(u8* uuid)
{
	memset_random(uuid, 16);
}

/**
 * Print uuid.
 *
 * @uuid result uuid is stored. it must be u8[16].
 */
void print_uuid(const u8* uuid)
{
	size_t i;
	for (i = 0; i < 16; i++) {
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
	memcpy(dst, src, 16);
}

/**
 * Write sector data to the offset.
 *
 * @fd file descriptor to write.
 * @sector_buf aligned buffer containing sector data.
 * @sector_size sector size in bytes.
 * @offset offset in sectors.
 *
 * @return true in success, or false.
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
 * @offset offset in sectors.
 * @n number of sectors to be written.
 *
 * @return true in success, or false.
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
 * Read sector data from the offset.
 *
 * @fd file descriptor to read.
 * @offset offset in sectors.
 * @sect sectors data to be filled.
 *
 * @return true in success, or false.
 */
bool sector_read(int fd, u64 offset, struct sector_data *sect)
{
	ASSERT(fd > 0);
	ASSERT_SECTOR_DATA(sect);
	int sect_size = sect->size;
	
	ssize_t r = 0;
	while (r < sect_size) {
		ssize_t s = pread(fd, sect->data + r,
				sect_size - r,
				offset * sect_size + r);
		if (s > 0) {
			r += s;
		} else {
			perror("read sector error.");
			return false;
		}
	}
	ASSERT(r == sect_size);
	return true;
}

/**
 * Write sector data to the offset.
 *
 * @fd file descriptor to write.
 * @offset offset in sectors.
 * @sect sectors data to be written.
 *
 * @return true in success, or false.
 */
bool sector_write(int fd, u64 offset, const struct sector_data *sect)
{
	ASSERT(fd > 0);
	ASSERT_SECTOR_DATA(sect);
	int sect_size = sect->size;
	
	ssize_t w = 0;
	while (w < sect_size) {
		ssize_t s = pwrite(fd, sect->data + w,
				sect_size - w,
				offset * sect_size + w);
		if (s > 0) {
			w += s;
		} else {
			perror("write sector error.");
			return false;
		}

	}
	ASSERT(w == sect_size);
	return true;
}

/**
 * Read to sector data in units of logical block.
 * Logical block size is 512 bytes.
 *
 * @fd file descriptor to write.
 * @offset_lb offset [logical block].
 * @sect sector data to be partially read.
 * @idx_lb start index [logical block].
 * @n_lb number of logical blocks to read.
 *
 * RETURN:
 *   true in success, or false.
 */
bool sector_read_lb(
	int fd,  u64 offset_lb, struct sector_data *sect,
	unsigned int idx_lb, unsigned int n_lb)
{
	ASSERT(fd > 0);
	ASSERT_SECTOR_DATA(sect);
	ASSERT(capacity_pb(sect->size, idx_lb + n_lb) == 1);

	ssize_t read_size = n_lb * LOGICAL_BLOCK_SIZE;
	ssize_t off = idx_lb * LOGICAL_BLOCK_SIZE;
	ssize_t r = 0;

	while (r < read_size) {
		ssize_t s = pread(
			fd, sect->data + off + r,
			read_size - r,
			offset_lb * LOGICAL_BLOCK_SIZE + r);
		if (s > 0) {
			r += s;
		} else {
			perror("read sector error.");
			return false;
		}
	}
	ASSERT(r == read_size);
	return true;
}

/**
 * Write from sector data in units of logical block.
 * Logical block size is 512 bytes.
 *
 * @fd file descriptor to write.
 * @offset_lb offset [logical block].
 * @sect sector data to be partially written.
 * @idx_lb start index [logical block].
 * @n_lb number of logical blocks to write.
 *
 * RETURN:
 *   true in success, or false.
 */
bool sector_write_lb(
	int fd,
	u64 offset_lb, const struct sector_data *sect,
	unsigned int idx_lb, unsigned int n_lb)
{
	ASSERT(fd > 0);
	ASSERT_SECTOR_DATA(sect);
	ASSERT(capacity_pb(sect->size, idx_lb + n_lb) == 1);

	ssize_t write_size = n_lb * LOGICAL_BLOCK_SIZE;
	ssize_t off = idx_lb * LOGICAL_BLOCK_SIZE;
	ssize_t w = 0;

	while (w < write_size) {
		ssize_t s = pwrite(
			fd, sect->data + off + w,
			write_size - w,
			offset_lb * LOGICAL_BLOCK_SIZE + w);
		if (s > 0) {
			w += s;
		} else {
			perror("write sector error.");
			return false;
		}
	}
	ASSERT(w == write_size);
	return true;
}

/**
 * Read multiple sectors data at an offset.
 */
bool sector_array_pread(
	int fd, u64 offset,
	struct sector_data_array *sect_ary,
	unsigned int start_idx, unsigned int n_sectors)
{
	ASSERT(fd > 0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(start_idx + n_sectors <= sect_ary->size);

	unsigned int i, idx;
	u64 off;
	bool ret;
	for (i = 0; i < n_sectors; i++) {
		idx = start_idx + i;
		off = offset + i;
		ret = sector_read(fd, off,
				get_sector_data_in_array(sect_ary, idx));
		if (!ret) { LOGe("read failed.\n"); return false; }
	}
	return true;
}

/**
 * Write multiple sectors data at an offset.
 */
bool sector_array_pwrite(
	int fd, u64 offset,
	const struct sector_data_array *sect_ary,
	unsigned int start_idx, unsigned int n_sectors)
{
	ASSERT(fd > 0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(start_idx + n_sectors <= sect_ary->size);

	unsigned int i, idx;
	u64 off;
	bool ret;
	for (i = 0; i < n_sectors; i++) {
		idx = start_idx + i;
		off = offset + i;
		ret = sector_write(
			fd, off,
			get_sector_data_in_array_const(sect_ary, idx));
		if (!ret) { LOGe("write failed.\n"); return false; }
	}
	return true;
}

/**
 * Read multiple logical sectors at an offset.
 * Logical block size is 512 bytes.
 *
 * @fd file descriptor of the target storage device.
 * @offset_lb storage offset [logical blocks].
 * @sect_ary input sector data array.
 * @idx_lb start offset in sect_ary [logical blocks].
 * @n_lb number of sectors to write [logical blocks].
 *
 * RETURN:
 *   true in success, or false.
 */
bool sector_array_pread_lb(
	int fd, u64 offset_lb,
	struct sector_data_array *sect_ary,
	unsigned int idx_lb, unsigned int n_lb)
{
	ASSERT(fd > 0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(n_lb > 0);

	const unsigned int pbs = sect_ary->sector_size;
	unsigned int idx, off_lb, tmp_lb;
	unsigned int r_lb = 0;
	
	while (r_lb < n_lb) {
		idx = addr_pb(pbs, idx_lb + r_lb);
		off_lb = off_in_pb(pbs, idx_lb + r_lb);
		tmp_lb = min(n_lb_in_pb(pbs) - off_lb, n_lb - r_lb);
		if (!sector_read_lb(
				fd, offset_lb + r_lb,
				sect_ary->array[idx],
				off_lb, tmp_lb)) {
			goto error0;
		}
		r_lb += tmp_lb;
	}
	ASSERT(r_lb == n_lb);
	return true;
	
error0:
	return false;
}

/**
 * Write multiple logical sectors at an offset.
 * Logical block size is 512 bytes.
 *
 * @fd file descriptor of the target storage device.
 * @offset_lb storage offset [logical block].
 * @sect_ary input sector data array.
 * @idx_lb start offset in sect_ary [logical block].
 * @n_lb number of sectors to write [logical block].
 *
 * RETURN:
 *   true in success, or false.
 */
bool sector_array_pwrite_lb(
	int fd, u64 offset_lb,
	const struct sector_data_array *sect_ary,
	unsigned int idx_lb, unsigned int n_lb)
{
	ASSERT(fd > 0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(n_lb > 0);

	const unsigned int pbs = sect_ary->sector_size;
	unsigned int idx, off_lb, tmp_lb;
	unsigned int w_lb = 0;
	
	while (w_lb < n_lb) {
		idx = addr_pb(pbs, idx_lb + w_lb);
		off_lb = off_in_pb(pbs, idx_lb + w_lb);
		tmp_lb = min(n_lb_in_pb(pbs) - off_lb, n_lb - w_lb);
		if (!sector_write_lb(
				fd, offset_lb + w_lb,
				sect_ary->array[idx],
				off_lb, tmp_lb)) {
			goto error0;
		}
		w_lb += tmp_lb;
	}
	ASSERT(w_lb == n_lb);
	return true;
	
error0:
	return false;
}

/**
 * Read multiple sectors.
 *
 * @fd file descriptor of the target storage device.
 * @sect_ary sector data array.
 * @start_idx start offset in sect_ary [sectors].
 * @n_sectors number of sectors to read [sectors].
 *
 * RETURN:
 *   true in success, or false.
 */
bool sector_array_read(
	int fd, 
	struct sector_data_array *sect_ary,
	unsigned int start_idx, unsigned int n_sectors)
{
	ASSERT(fd >= 0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(start_idx + n_sectors <= sect_ary->size);

	unsigned int i, idx;
	bool ret;
	for (i = 0; i < n_sectors; i++) {
		idx = start_idx + i;
		ret = read_data(
			fd,
			(u8 *)sect_ary->array[idx]->data,
			sect_ary->sector_size);
		if (!ret) {
			LOGe("read failed.\n");
			goto error0;
		}
	}
	return true;
error0:
	return false;
}

/**
 * Write multiple sectors.
 *
 * @fd file descriptor of the target storage device.
 * @sect_ary sector data array.
 * @start_idx start offset in sect_ary [sectors].
 * @n_sectors number of sectors to write [sectors].
 *
 * RETURN:
 *   true in success, or false.
 */
bool sector_array_write(
	int fd,
	const struct sector_data_array *sect_ary,
	unsigned int start_idx, unsigned int n_sectors)
{
	ASSERT(fd >= 0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(start_idx + n_sectors <= sect_ary->size);

	unsigned int i, idx;
	bool ret;
	for (i = 0; i < n_sectors; i++) {
		idx = start_idx + i;
		ret = write_data(
			fd,
			(u8 *)sect_ary->array[idx]->data,
			sect_ary->sector_size);
		if (!ret) {
			LOGe("read failed.\n");
			goto error0;
		}
	}
	return true;
error0:
	return false;
}

/**
 * Initialize super sector.
 *
 * @super_sect super sector image to initialize.
 * @lbs logical block size.
 * @pbs physical block size.
 * @ddev_lb device size [logical block].
 * @ldev_lb log device size [logical block]
 * @n_snapshots number of snapshots to keep.
 * @name name of the walb device, or NULL.
 */
void init_super_sector_raw(
	struct walb_super_sector* super_sect,
	unsigned int lbs, unsigned int pbs,
	u64 ddev_lb, u64 ldev_lb, int n_snapshots,
	const char *name)
{
	ASSERT(super_sect);
	ASSERT(lbs > 0);
	ASSERT(pbs > 0);
	ASSERT(ddev_lb < (u64)(-1));
	ASSERT(ldev_lb < (u64)(-1));

	ASSERT(sizeof(struct walb_super_sector) <= (size_t)pbs);

	/* Calculate number of snapshot sectors. */
	int n_sectors;
	int t = get_max_n_records_in_snapshot_sector(pbs);
	n_sectors = (n_snapshots + t - 1) / t;

	LOGd("metadata_size: %d\n", n_sectors);

	/* Prepare super sector */
	memset(super_sect, 0, sizeof(super_sect));
	/* Set sector type. */
	super_sect->sector_type = SECTOR_TYPE_SUPER;
	/* Fill parameters. */
	super_sect->logical_bs = lbs;
	super_sect->physical_bs = pbs;
	super_sect->snapshot_metadata_size = n_sectors;
	generate_uuid(super_sect->uuid);
	super_sect->ring_buffer_size =
		ldev_lb / (pbs / lbs)
		- get_ring_buffer_offset(pbs, n_snapshots);
	super_sect->oldest_lsid = 0;
	super_sect->written_lsid = 0;
	super_sect->device_size = ddev_lb;
	char *rname = set_super_sector_name(super_sect, name);
	if (name && strlen(name) != strlen(rname)) {
		printf("name %s is pruned to %s.\n", name, rname);
	}

	ASSERT(is_valid_super_sector_raw(super_sect, pbs));
}

/**
 * Initialize super sector image.
 */
void init_super_sector(struct sector_data *sect,
		unsigned int lbs, unsigned int pbs,
		u64 ddev_lb, u64 ldev_lb, int n_snapshots,
		const char *name)
{
	ASSERT_SECTOR_DATA(sect);
	ASSERT(pbs == sect->size);
	
	init_super_sector_raw(
		sect->data, lbs, pbs, ddev_lb, ldev_lb, n_snapshots, name);
}

/**
 * Print super sector for debug.
 * This will be obsolute. Use print_super_sector() instead.
 */
void print_super_sector_raw(const struct walb_super_sector* super_sect)
{
	ASSERT(super_sect);
	printf("checksum: %08x\n"
		"logical_bs: %u\n"
		"physical_bs: %u\n"
		"snapshot_metadata_size: %u\n",
		super_sect->checksum,
		super_sect->logical_bs,
		super_sect->physical_bs,
		super_sect->snapshot_metadata_size);
	print_uuid(super_sect->uuid);
	printf("\n"
		"name: \"%s\"\n"
		"ring_buffer_size: %lu\n"
		"oldest_lsid: %lu\n"
		"written_lsid: %lu\n"
		"device_size: %lu\n",
		super_sect->name,
		super_sect->ring_buffer_size,
		super_sect->oldest_lsid,
		super_sect->written_lsid,
		super_sect->device_size);
}

/**
 * Print super sector.
 */
void print_super_sector(const struct sector_data *sect)
{
	ASSERT_SUPER_SECTOR(sect);
	print_super_sector_raw((const struct walb_super_sector *)sect->data);
}

/**
 * Write super sector to the log device.
 *
 * @fd file descripter of log device.
 * @super_sect super sector data.
 *
 * @return true in success, or false.
 */
bool write_super_sector_raw(int fd, const struct walb_super_sector* super_sect)
{
	ASSERT(super_sect);
	u32 sect_sz = super_sect->physical_bs;
	
	/* Memory image of sector. */
	u8 *sector_buf;
	if (posix_memalign((void **)&sector_buf, PAGE_SIZE, sect_sz) != 0) {
		goto error0;
	}
	memset(sector_buf, 0, sect_sz);
	memcpy(sector_buf, super_sect, sizeof(*super_sect));

	/* Set sector type. */
	((struct walb_super_sector *)sector_buf)->sector_type = SECTOR_TYPE_SUPER;
	
	/* Calculate checksum. */
	struct walb_super_sector *super_sect_tmp = (struct walb_super_sector *)sector_buf;
	super_sect_tmp->checksum = 0;
	u32 csum = checksum(sector_buf, sect_sz);
	print_binary_hex(sector_buf, sect_sz);/* debug */
	super_sect_tmp->checksum = csum;
	print_binary_hex(sector_buf, sect_sz);/* debug */
	ASSERT(checksum(sector_buf, sect_sz) == 0);
	
	/* Really write sector data. */
	u64 off0 = get_super_sector0_offset_2(super_sect);
	u64 off1 = get_super_sector1_offset_2(super_sect);
	if (!write_sector_raw(fd, sector_buf, sect_sz, off0) ||
		!write_sector_raw(fd, sector_buf, sect_sz, off1)) {
		goto error1;
	}
	free(sector_buf);
	return true;

error1:
	free(sector_buf);
error0:
	return false;
}

/**
 * Write super sector to the log device.
 */
bool write_super_sector(int fd, const struct sector_data *sect)
{
	if (is_valid_super_sector(sect)) {
		return write_super_sector_raw(fd, sect->data);
	} else {
		return false;
	}
}

/**
 * Read sector data from the offset.
 *
 * @sector_buf aligned buffer to be filled with read sector data.
 * @sector_size sector size in bytes.
 * @offset offset in sectors.
 *
 * @return true in success, or false.
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
 * @offset offset in sectors.
 * @n number of sectors to read.
 *
 * @return true in success, or false.
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
 * Read super sector from the log device.
 *
 * This is obsolute. Use read_super_sector() instead.
 *
 * @fd file descripter of log device.
 * @super_sect super sector to be filled.
 * @sector_size sector size in bytes.
 * @n_snapshots number of snapshots to be stored.
 *
 * @return true in success, or false.
 */
bool read_super_sector_raw(
	int fd, struct walb_super_sector* super_sect,
	u32 sector_size, u32 n_snapshots)
{
	/* 1. Read two sectors
	   2. Compare them and choose one having larger written_lsid. */
	ASSERT(super_sect);
	ASSERT(sector_size <= PAGE_SIZE);
	
	/* Memory image of sector. */
	u8 *buf, *buf0, *buf1;
	if (posix_memalign((void **)&buf, PAGE_SIZE, sector_size * 2) != 0) {
		perror("memory allocation failed.");
		goto error0;
	}
	buf0 = buf;
	buf1 = buf + sector_size;

	u64 off0 = get_super_sector0_offset(sector_size);
	u64 off1 = get_super_sector1_offset(sector_size, n_snapshots);

	bool ret0 = read_sector_raw(fd, buf0, sector_size, off0);
	bool ret1 = read_sector_raw(fd, buf1, sector_size, off1);

	if (ret0 && checksum(buf0, sector_size) != 0) {
		ret0 = -1;
	}
	if (ret1 && checksum(buf1, sector_size) != 0) {
		ret1 = -1;
	}
	if (ret0 && ((struct walb_super_sector *)buf0)->sector_type != SECTOR_TYPE_SUPER) {
		ret0 = -1;
	}
	if (ret1 && ((struct walb_super_sector *)buf1)->sector_type != SECTOR_TYPE_SUPER) {
		ret1 = -1;
	}
	if (! ret0 && ! ret1) {
		LOGe("Both superblocks are broken.\n");
		goto error1;
	} else if (ret0 && ret1) {
		u64 lsid0 = ((struct walb_super_sector *)buf0)->written_lsid;
		u64 lsid1 = ((struct walb_super_sector *)buf1)->written_lsid;
		if (lsid0 >= lsid1) {
			memcpy(super_sect, buf0, sizeof(*super_sect));
		} else {
			memcpy(super_sect, buf1, sizeof(*super_sect));
		}
	} else if (ret0) {
		memcpy(super_sect, buf0, sizeof(*super_sect));
	} else {
		ASSERT(ret1);
		memcpy(super_sect, buf1, sizeof(*super_sect));
	}
	
	free(buf);
	return true;

error1:
	free(buf);
error0:
	return false;
}

/**
 * Read super sector.
 *
 * Currently 2nd super sector is not read.
 */
bool read_super_sector(int fd, struct sector_data *sect)
{
	if (!is_valid_sector_data(sect)) {
		LOGe("Sector data is not valid.\n");
		goto error0;
	}

	ASSERT(sect->size <= PAGE_SIZE);
	
	u64 off0 = get_super_sector0_offset(sect->size);
	if (!sector_read(fd, off0, sect)) {
		LOGe("Read sector failed.\n");
		goto error0;
	}
	if (checksum(sect->data, sect->size) != 0) {
		LOGe("Checksum invalid.\n");
		goto error0;
	}
	if (!is_valid_super_sector(sect)) {
		LOGe("Super sector invalid.\n");
		goto error0;
	}
	return true;
error0:
	return false;
}

/**
 * Print bitmap data.
 */
void print_bitmap(const u8* bitmap, size_t size)
{
	size_t i, j;
	for (i = 0; i < size; i++) {
		for (j = 0; j < 8; j++) {
			if (bitmap[i] & (1 << j)) { /* on */
				printf("1");
			} else { /* off */
				printf("0");
			}
		}
	}
}

/**
 * Print bitmap data.
 */
void print_u32bitmap(const u32 bitmap)
{
	u32 i;
	for (i = 0; i < 32; i++) {
		if (bitmap & (1 << i)) { /* on */
			printf("1");
		} else { /* off */
			printf("0");
		}
	}
}

/**
 * Read data.
 *
 * @fd file descriptor.
 * @data pointer to store data.
 * @size read size [bytes].
 *
 * @return true in success, or false.
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
 *
 * @fd file descriptor.
 * @data data pointer.
 * @size write size [bytes].
 *
 * @return true in success, or false.
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

/**
 * Check block size of two devices.
 *
 * @return true when two devices has compatible block sizes, or false.
 */
bool is_same_block_size(const char* devpath1, const char* devpath2)
{
	ASSERT(check_bdev(devpath1) == 0 && check_bdev(devpath2) == 0);

	int lbs1 = get_bdev_logical_block_size(devpath1);
	int lbs2 = get_bdev_logical_block_size(devpath2);
	int pbs1 = get_bdev_physical_block_size(devpath1);
	int pbs2 = get_bdev_physical_block_size(devpath2);

	return (lbs1 == lbs2 && pbs1 == pbs2);
}

/* end of file */
