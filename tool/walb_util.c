/**
 * General definitions for Walb.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include "walb/walb.h"
#include "walb/block_size.h"
#include "util.h"
#include "walb_util.h"
#include "random.h"

/**
 * Read a specified size of data.
 */
static bool pread_all(int fd, void *buf, size_t count, off_t offset)
{
	char *c = (char *)buf;
	size_t r = 0;

	while (r < count) {
		ssize_t s = pread(fd, c + r, count - r, offset + r);
		if (s <= 0) {
			perror("pread error");
			return false;
		}
		r += s;
	}
	ASSERT(r == count);
	return true;
}

/**
 * Write a specified size of data.
 */
static bool pwrite_all(int fd, const void *buf, size_t count, off_t offset)
{
	const char *c = (char *)buf;
	size_t w = 0;

	while (w < count) {
		ssize_t s = pwrite(fd, c + w, count - w, offset + w);
		if (s <= 0) {
			perror("pwrite error.");
			return false;
		}
		w += s;
	}
	ASSERT(w == count);
	return true;
}

/**
 * Read sector data from the offset.
 *
 * @fd file descriptor to read.
 * @offset offset in sectors.
 * @sect sectors data to be filled.
 *
 * RETURN:
 *   true in success, or false.
 */
bool sector_read(int fd, u64 offset, struct sector_data *sect)
{
	ASSERT(fd > 0);
	ASSERT_SECTOR_DATA(sect);

	return pread_all(fd, sect->data, sect->size, offset * sect->size);
}

/**
 * Write sector data to the offset.
 *
 * @fd file descriptor to write.
 * @offset offset in sectors.
 * @sect sectors data to be written.
 *
 * RETURN:
 *   true in success, or false.
 */
bool sector_write(int fd, u64 offset, const struct sector_data *sect)
{
	ASSERT(fd > 0);
	ASSERT_SECTOR_DATA(sect);

	return pwrite_all(fd, sect->data, sect->size, offset * sect->size);
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
	const size_t count = n_lb * LOGICAL_BLOCK_SIZE;
	const off_t buf_off = idx_lb * LOGICAL_BLOCK_SIZE;
	const off_t file_off = offset_lb * LOGICAL_BLOCK_SIZE;

	ASSERT(fd > 0);
	ASSERT_SECTOR_DATA(sect);
	ASSERT(capacity_pb(sect->size, idx_lb + n_lb) == 1);

	return pread_all(fd, sect->data + buf_off, count, file_off);
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
	int fd, u64 offset_lb, const struct sector_data *sect,
	unsigned int idx_lb, unsigned int n_lb)
{
	const ssize_t count = n_lb * LOGICAL_BLOCK_SIZE;
	const off_t buf_off = idx_lb * LOGICAL_BLOCK_SIZE;
	const off_t file_off = offset_lb * LOGICAL_BLOCK_SIZE;

	ASSERT(fd > 0);
	ASSERT_SECTOR_DATA(sect);
	ASSERT(capacity_pb(sect->size, idx_lb + n_lb) == 1);

	return pwrite_all(fd, sect->data + buf_off, count, file_off);
}

/**
 * Read multiple sectors data at an offset.
 *
 * RETURN:
 *   true in success, or false.
 */
bool sector_array_pread(
	int fd, u64 offset,
	struct sector_data_array *sect_ary,
	unsigned int start_idx, unsigned int n_sectors)
{
	unsigned int i;

	ASSERT(fd > 0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(start_idx + n_sectors <= sect_ary->size);

	for (i = 0; i < n_sectors; i++) {
		unsigned int idx = start_idx + i;
		u64 off = offset + i;
		bool ret = sector_read(fd, off,
				get_sector_data_in_array(sect_ary, idx));
		if (!ret) {
			LOGe("read failed.\n");
			return false;
		}
	}
	return true;
}

/**
 * Write multiple sectors data at an offset.
 *
 * RETURN:
 *  true in success, or false.
 */
bool sector_array_pwrite(
	int fd, u64 offset,
	const struct sector_data_array *sect_ary,
	unsigned int start_idx, unsigned int n_sectors)
{
	unsigned int i;

	ASSERT(fd > 0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(start_idx + n_sectors <= sect_ary->size);

	for (i = 0; i < n_sectors; i++) {
		unsigned int idx = start_idx + i;
		u64 off = offset + i;
		bool ret = sector_write(
			fd, off,
			get_sector_data_in_array_const(sect_ary, idx));
		if (!ret) {
			LOGe("write failed.\n");
			return false;
		}
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
	const unsigned int pbs = sect_ary->sector_size;
	unsigned int r_lb = 0;

	ASSERT(fd > 0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(n_lb > 0);

	while (r_lb < n_lb) {
		unsigned int idx = addr_pb(pbs, idx_lb + r_lb);
		unsigned int off_lb = off_in_pb(pbs, idx_lb + r_lb);
		unsigned int tmp_lb =
			get_min_value(n_lb_in_pb(pbs) - off_lb, n_lb - r_lb);
		if (!sector_read_lb(
				fd, offset_lb + r_lb,
				sect_ary->array[idx],
				off_lb, tmp_lb)) {
			return false;
		}
		r_lb += tmp_lb;
	}
	ASSERT(r_lb == n_lb);
	return true;
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
	const unsigned int pbs = sect_ary->sector_size;
	unsigned int w_lb = 0;

	ASSERT(fd > 0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(n_lb > 0);

	while (w_lb < n_lb) {
		unsigned int idx = addr_pb(pbs, idx_lb + w_lb);
		unsigned int off_lb = off_in_pb(pbs, idx_lb + w_lb);
		unsigned int tmp_lb =
			get_min_value(n_lb_in_pb(pbs) - off_lb, n_lb - w_lb);
		if (!sector_write_lb(
				fd, offset_lb + w_lb,
				sect_ary->array[idx],
				off_lb, tmp_lb)) {
			return false;
		}
		w_lb += tmp_lb;
	}
	ASSERT(w_lb == n_lb);
	return true;
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
	unsigned int i;

	ASSERT(fd >= 0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(start_idx + n_sectors <= sect_ary->size);

	for (i = 0; i < n_sectors; i++) {
		unsigned int idx = start_idx + i;
		bool ret = read_data(
			fd,
			(u8 *)sect_ary->array[idx]->data,
			sect_ary->sector_size);
		if (!ret) {
			LOGe("read failed.\n");
			return false;
		}
	}
	return true;
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
	unsigned int i;

	ASSERT(fd >= 0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(start_idx + n_sectors <= sect_ary->size);

	for (i = 0; i < n_sectors; i++) {
		unsigned int idx = start_idx + i;
		bool ret = write_data(
			fd,
			(u8 *)sect_ary->array[idx]->data,
			sect_ary->sector_size);
		if (!ret) {
			LOGe("read failed.\n");
			return false;
		}
	}
	return true;
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
 *
 * RETURN:
 *   true in success.
 */
bool init_super_sector_raw(
	struct walb_super_sector* super_sect,
	unsigned int lbs, unsigned int pbs,
	u64 ddev_lb, u64 ldev_lb, int n_snapshots,
	const char *name)
{
	int n_sectors;
	int t;
	u32 salt;
	char *rname;
	bool ret;

	ASSERT(super_sect);
	ASSERT(0 < lbs);
	ASSERT(0 < pbs);
	ASSERT(0 < ddev_lb);
	ASSERT(0 < ldev_lb);

	ASSERT(sizeof(struct walb_super_sector) <= (size_t)pbs);

	/* Calculate number of snapshot sectors. */
	t = get_max_n_records_in_snapshot_sector(pbs);
	n_sectors = (n_snapshots + t - 1) / t;

	LOGd("metadata_size: %d\n", n_sectors);

	/* Prepare super sector */
	memset(super_sect, 0, sizeof(super_sect));
	/* Set sector type. */
	super_sect->sector_type = SECTOR_TYPE_SUPER;
	/* Fill parameters. */
	super_sect->version = WALB_VERSION;
	super_sect->logical_bs = lbs;
	super_sect->physical_bs = pbs;
	super_sect->snapshot_metadata_size = n_sectors;
	ret = generate_uuid(super_sect->uuid);
	if (!ret) { return false; }
	memset_random((u8 *)&salt, sizeof(salt));
	LOGn("salt: %"PRIu32"\n", salt);
	super_sect->log_checksum_salt = salt;
	super_sect->ring_buffer_size =
		ldev_lb / (pbs / lbs)
		- get_ring_buffer_offset(pbs, n_snapshots);
	super_sect->oldest_lsid = 0;
	super_sect->written_lsid = 0;
	super_sect->device_size = ddev_lb;
	rname = set_super_sector_name(super_sect, name);
	if (name && strlen(name) != strlen(rname)) {
		printf("name %s is pruned to %s.\n", name, rname);
	}

	ASSERT(is_valid_super_sector_raw(super_sect, pbs));
	return true;
}

/**
 * Initialize super sector image.
 */
bool init_super_sector(
	struct sector_data *sect,
	unsigned int lbs, unsigned int pbs,
	u64 ddev_lb, u64 ldev_lb, int n_snapshots,
	const char *name)
{
	ASSERT_SECTOR_DATA(sect);
	ASSERT(pbs == sect->size);

	return init_super_sector_raw(
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
		"snapshot_metadata_size: %u\n"
		"log_checksum_salt: %"PRIu32"\n",
		super_sect->checksum,
		super_sect->logical_bs,
		super_sect->physical_bs,
		super_sect->snapshot_metadata_size,
		super_sect->log_checksum_salt);
	printf("uuid: ");
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
	printf("ring_buffer_offset: %lu\n",
		get_ring_buffer_offset_2(super_sect));
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
 * RETURN:
 *   true in success, or false.
 */
bool write_super_sector_raw(int fd, const struct walb_super_sector* super_sect)
{
	u32 sect_sz;
	u8 *buf;
	struct walb_super_sector *p;
	u32 csum;
	u64 off0, off1;
	bool ret0, ret1;

	ASSERT(super_sect);

	sect_sz = super_sect->physical_bs;

	/* Memory image of sector. */
	if (posix_memalign((void **)&buf, PAGE_SIZE, sect_sz) != 0) {
		return false;
	}
	memset(buf, 0, sect_sz);
	memcpy(buf, super_sect, sizeof(*super_sect));
	p = (struct walb_super_sector *)buf;

	/* Set sector type. */
	p->sector_type = SECTOR_TYPE_SUPER;

	/* Calculate checksum. */
	p->checksum = 0;
	csum = checksum(buf, sect_sz, 0);
	print_binary_hex(buf, sect_sz);/* debug */
	p->checksum = csum;
	print_binary_hex(buf, sect_sz);/* debug */
	ASSERT(checksum(buf, sect_sz, 0) == 0);

	/* Really write sector data. */
	off0 = get_super_sector0_offset_2(super_sect);
	off1 = get_super_sector1_offset_2(super_sect);
	ret0 = write_sector_raw(fd, buf, sect_sz, off0);
	ret1 = write_sector_raw(fd, buf, sect_sz, off1);

	free(buf);
	return ret0 && ret1;
}

/**
 * Write super sector to the log device.
 *
 * RETURN:
 *   true in success, or false.
 */
bool write_super_sector(int fd, const struct sector_data *sect)
{
	if (!is_valid_super_sector(sect)) {
		return false;
	}
	return write_super_sector_raw(fd, sect->data);
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
 * RETURN:
 *   true in success, or false.
 */
bool read_super_sector_raw(
	int fd, struct walb_super_sector* super_sect,
	u32 sector_size, u32 n_snapshots)
{
	u8 *buf, *buf0, *buf1;
	u64 off0, off1;
	bool ret0, ret1, ret;
	struct walb_super_sector *p0, *p1;

	/* 1. Read two sectors
	   2. Compare them and choose one having larger written_lsid. */
	ASSERT(super_sect);
	ASSERT(sector_size <= PAGE_SIZE);

	/* Memory image of sector. */
	if (posix_memalign((void **)&buf, PAGE_SIZE, sector_size * 2) != 0) {
		perror("memory allocation failed.");
		return false;
	}
	buf0 = buf;
	buf1 = buf + sector_size;
	p0 = (struct walb_super_sector *)buf0;
	p1 = (struct walb_super_sector *)buf1;
	off0 = get_super_sector0_offset(sector_size);
	off1 = get_super_sector1_offset(sector_size, n_snapshots);
	ret0 = read_sector_raw(fd, buf0, sector_size, off0);
	ret1 = read_sector_raw(fd, buf1, sector_size, off1);

	if (ret0 && checksum(buf0, sector_size, 0) != 0) { ret0 = false; }
	if (ret1 && checksum(buf1, sector_size, 0) != 0) { ret1 = false; }
	if (ret0 && p0->sector_type != SECTOR_TYPE_SUPER) { ret0 = false; }
	if (ret1 && p1->sector_type != SECTOR_TYPE_SUPER) { ret1 = false; }
	if (!ret0 && !ret1) {
		LOGe("Both superblocks are broken.\n");
		ret = false;
	} else if (ret0 && ret1) {
		u64 lsid0 = p0->written_lsid;
		u64 lsid1 = p1->written_lsid;
		if (lsid0 >= lsid1) {
			memcpy(super_sect, buf0, sizeof(*super_sect));
		} else {
			memcpy(super_sect, buf1, sizeof(*super_sect));
		}
		ret = true;
	} else if (ret0) {
		memcpy(super_sect, buf0, sizeof(*super_sect));
		ret = true;
	} else {
		memcpy(super_sect, buf1, sizeof(*super_sect));
		ret = true;
	}

	free(buf);
	return ret;
}

/**
 * Read super sector.
 *
 * Currently 2nd super sector is not read.
 *
 * RETURN:
 *  true in success, or false.
 */
bool read_super_sector(int fd, struct sector_data *sect)
{
	u64 off0;

	if (!is_valid_sector_data(sect)) {
		LOGe("Sector data is not valid.\n");
		return false;
	}
	ASSERT(sect->size <= PAGE_SIZE);

	off0 = get_super_sector0_offset(sect->size);
	if (!sector_read(fd, off0, sect)) {
		LOGe("Read sector failed.\n");
		return false;
	}
	if (checksum(sect->data, sect->size, 0) != 0) {
		LOGe("Checksum invalid.\n");
		return false;
	}
	if (!is_valid_super_sector(sect)) {
		LOGe("Super sector invalid.\n");
		return false;
	}
	return true;
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

/* end of file. */
