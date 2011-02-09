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

#include "walb.h"
#include "random.h"
#include "util.h"

/**
 * hex print
 */
void print_binary_hex(const u8* data, size_t size)
{
        size_t i;
        for (i = 0; i < size; i ++) {

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
        size_t sector_size, dev_size, size;
        
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
        ASSERT(devpath != NULL);
        
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

        ASSERT(devpath != NULL);
        
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
        for (i = 0; i < 16; i ++) {
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
 * Allocate sector (aligned and can be deallocated with free()).
 *
 * @sector_size sector size.
 *
 * @return pointer to allocated sector in success, or NULL.
 */
u8* alloc_sector(int sector_size)
{
        return alloc_sectors(sector_size, 1);
}

/**
 * Allocate multiple sectors (aligned and can be deallocated with @free()).
 *
 * @sector_size sector size.
 * @n number of sectors.
 *
 * @return pointer to allocated memory in success, or NULL.
 */
u8* alloc_sectors(int sector_size, int n)
{
        u8 *sectors;
        if (posix_memalign((void **)&sectors, sector_size, sector_size * n) != 0) {
                return NULL;
        }

        return sectors;
}

/**
 * Realloc multiple sectors.
 *
 * @memptr pointer to allocated pointer.
 * @sector_size sector size.
 * @n number of sectors.
 *
 * @return true in success, or false.
 *         if false, the memory is freed.
 */
bool realloc_sectors(u8** memptr, int sector_size, int n)
{
        u8* tmp;
        tmp = realloc(*memptr, sector_size * n);

        if (tmp) {
                *memptr = tmp;
                return true;
        } else {
                free(*memptr);
                return false;
        }
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
bool write_sector(int fd, const u8* sector_buf, u32 sector_size, u64 offset)
{
        return write_sectors(fd, sector_buf, sector_size, offset, 1);
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
bool write_sectors(int fd, const u8* sectors_buf, u32 sector_size, u64 offset, int n)
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
 * Print super sector for debug.
 */
void print_super_sector(const walb_super_sector_t* super_sect)
{
        ASSERT(super_sect != NULL);
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
 * Write super sector to the log device.
 *
 * @fd file descripter of log device.
 * @super_sect super sector data.
 *
 * @return true in success, or false.
 */
bool write_super_sector(int fd, const walb_super_sector_t* super_sect)
{
        ASSERT(super_sect != NULL);
        u32 sect_sz = super_sect->physical_bs;
        
        /* Memory image of sector. */
        u8 *sector_buf;
        if (posix_memalign((void **)&sector_buf, PAGE_SIZE, sect_sz) != 0) {
                goto error0;
        }
        memset(sector_buf, 0, sect_sz);
        memcpy(sector_buf, super_sect, sizeof(*super_sect));

        /* Set sector type. */
        ((walb_super_sector_t *)sector_buf)->sector_type = SECTOR_TYPE_SUPER;
        
        /* Calculate checksum. */
        walb_super_sector_t *super_sect_tmp = (walb_super_sector_t *)sector_buf;
        super_sect_tmp->checksum = 0;
        u32 csum = checksum(sector_buf, sect_sz);
        print_binary_hex(sector_buf, sect_sz);/* debug */
        super_sect_tmp->checksum = csum;
        print_binary_hex(sector_buf, sect_sz);/* debug */
        ASSERT(checksum(sector_buf, sect_sz) == 0);
        
        /* Really write sector data. */
        u64 off0 = get_super_sector0_offset_2(super_sect);
        u64 off1 = get_super_sector1_offset_2(super_sect);
        if (! write_sector(fd, sector_buf, sect_sz, off0) ||
            ! write_sector(fd, sector_buf, sect_sz, off1)) {
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
 * Read sector data from the offset.
 *
 * @sector_buf aligned buffer to be filled with read sector data.
 * @sector_size sector size in bytes.
 * @offset offset in sectors.
 *
 * @return true in success, or false.
 */
bool read_sector(int fd, u8* sector_buf, u32 sector_size, u64 offset)
{
        return read_sectors(fd, sector_buf, sector_size, offset, 1);
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
bool read_sectors(int fd, u8* sectors_buf, u32 sector_size, u64 offset, int n)
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
 * @fd file descripter of log device.
 * @super_sect super sector to be filled.
 * @sector_size sector size in bytes.
 * @n_snapshots number of snapshots to be stored.
 *
 * @return true in success, or false.
 */
bool read_super_sector(int fd, walb_super_sector_t* super_sect, u32 sector_size, u32 n_snapshots)
{
        /* 1. Read two sectors
           2. Compare them and choose one having larger written_lsid. */
        ASSERT(super_sect != NULL);
        ASSERT((int)sector_size <= PAGE_SIZE);
        
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

        bool ret0 = read_sector(fd, buf0, sector_size, off0);
        bool ret1 = read_sector(fd, buf1, sector_size, off1);

        if (ret0 && checksum(buf0, sector_size) != 0) {
                ret0 = -1;
        }
        if (ret1 && checksum(buf1, sector_size) != 0) {
                ret1 = -1;
        }
        if (ret0 && ((walb_super_sector_t *)buf0)->sector_type != SECTOR_TYPE_SUPER) {
                ret0 = -1;
        }
        if (ret1 && ((walb_super_sector_t *)buf1)->sector_type != SECTOR_TYPE_SUPER) {
                ret1 = -1;
        }
        if (! ret0 && ! ret1) {
                LOGe("Both superblocks are broken.\n");
                goto error1;
        } else if (ret0 && ret1) {
                u64 lsid0 = ((walb_super_sector_t *)buf0)->written_lsid;
                u64 lsid1 = ((walb_super_sector_t *)buf1)->written_lsid;
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
 * Set super sector name.
 *
 * @super_sect super sector.
 * @name name or NULL.
 *
 * @return pointer to result name.
 */
char* set_super_sector_name(walb_super_sector_t* super_sect, const char *name)
{
        if (name == NULL) {
                super_sect->name[0] = '\0';
        } else {
                strncpy(super_sect->name, name, DISK_NAME_LEN);
        }
        return super_sect->name;
}

/**
 * Check super sector.
 */
bool is_valid_super_sector(const walb_super_sector_t* super, int physical_bs)
{
        /* checksum */
        if (checksum((const u8 *)super, physical_bs) != 0) {
                LOGe("checksum is not valid.\n");
                goto error0;
        }
        /* sector type */
        if (super->sector_type != SECTOR_TYPE_SUPER) {
                LOGe("sector type is not valid %d.\n", super->sector_type);
                goto error0;
        }
        /* physical block size */
        if (super->physical_bs != (u32)physical_bs) {
                LOGe("physical block size is not same %d %d.\n",
                     (int)super->physical_bs, physical_bs);
                goto error0;
        }
        /* physical/logical block size */
        if (! (super->physical_bs >= super->logical_bs &&
               super->physical_bs % super->logical_bs == 0)) {
                LOGe("physical/logical block size is not valid. (physical %u, logical %u)\n",
                     super->physical_bs, super->logical_bs);
                goto error0;
        }
        /* lsid consistency. */
        if (! (super->oldest_lsid <= super->written_lsid &&
               super->written_lsid - super->oldest_lsid <= super->ring_buffer_size)) {
                LOGe("oldest_lsid, written_lsid, ring_buffer_size is not consistent.\n");
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
        for (i = 0; i < size; i ++) {
                for (j = 0; j < 8; j ++) {
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
        for (i = 0; i < 32; i ++) {
                if (bitmap & (1 << i)) { /* on */
                        printf("1");
                } else { /* off */
                        printf("0");
                }
        }
}

/**
 * Print snapshot record for debug.
 */
void print_snapshot_record(const walb_snapshot_record_t* snap_rec)
{
        ASSERT(snap_rec != NULL);
        printf("lsid: %lu, "
               "timestamp: %lu, "
               "name: %s\n",
               snap_rec->lsid, snap_rec->timestamp, snap_rec->name);
}

/**
 * print snapshot sector for debug.
 */
void print_snapshot_sector(const walb_snapshot_sector_t* snap_sect, u32 sector_size)
{
        printf("checksum: %u\n", snap_sect->checksum);

        printf("bitmap: ");
        print_u32bitmap(snap_sect->bitmap);
        printf("\n");

        /* Print continuous snapshot records */
        int i;
        int max = get_max_n_records_in_snapshot_sector(sector_size);
        for (i = 0; i < max; i ++) {
                printf("snapshot record %d: ", i);
                print_snapshot_record(&snap_sect->record[i]);
        }
}

/**
 * Allocate sector and set all zero.
 */
u8* alloc_sector_zero(int sector_size)
{
        return alloc_sectors_zero(sector_size, 1);
}

/**
 * Allocate multiple sectors and set all zero.
 */
u8* alloc_sectors_zero(int sector_size, int n)
{
        u8 *sectors = alloc_sectors(sector_size, n);
        if (sectors == NULL) { return NULL; }
        memset(sectors, 0, sector_size * n);
        return sectors;
}


/**
 * Write snapshot sector.
 *
 * @fd File descriptor of log device.
 * @super_sect super sector data to refer its members.
 * @snap_sect snapshot sector data to be written.
 *            It's allocated size must be really sector size.
 *            Only checksum area will be overwritten.
 * @idx idx'th sector is written. (0 <= idx < snapshot_metadata_size)
 *
 * @return true in success, or false.
 */
bool write_snapshot_sector(int fd, const walb_super_sector_t* super_sect,
                           walb_snapshot_sector_t* snap_sect, u32 idx)
{
        ASSERT(fd >= 0);
        ASSERT(super_sect != NULL);
        ASSERT(snap_sect != NULL);
        
        u32 sect_sz = super_sect->physical_bs;
        u32 meta_sz = super_sect->snapshot_metadata_size;
        if (idx >= meta_sz) {
                LOGe("idx range over. idx: %u meta_sz: %u\n", idx, meta_sz);
                return false;
        }

        /* checksum */
        u8 *sector_buf = (u8*)snap_sect;
        snap_sect->checksum = 0;
        u32 csum = checksum(sector_buf, sect_sz);
        snap_sect->checksum = csum;
        ASSERT(checksum(sector_buf, sect_sz) == 0);

        /* really write sector data. */
        u64 off = get_metadata_offset_2(super_sect) + idx;
        if (! write_sector(fd, sector_buf, sect_sz, off)) {
                return false;
        }
        return true;
}

/**
 * Read snapshot sector.
 *
 * @fd File descriptor of log device.
 * @super_sect super sector data to refer its members.
 * @snap_sect snapshot sector buffer to be read.
 *            It's allocated size must be really sector size.
 * @idx idx'th sector is read. (0 <= idx < snapshot_metadata_size)
 *
 * @return true in success, or false.
 */
bool read_snapshot_sector(int fd, const walb_super_sector_t* super_sect,
                          walb_snapshot_sector_t* snap_sect, u32 idx)
{
        ASSERT(fd >= 0);
        ASSERT(super_sect != NULL);
        ASSERT(snap_sect != NULL);
        
        u32 sect_sz = super_sect->physical_bs;
        u32 meta_sz = super_sect->snapshot_metadata_size;
        if (idx >= meta_sz) {
                LOGe("idx range over. idx: %u meta_sz: %u\n", idx, meta_sz);
                return false;
        }
        
        /* Read sector data.
           Confirm checksum. */
        u8 *sector_buf = (u8 *)snap_sect;
        u64 off = get_metadata_offset_2(super_sect) + idx;
        if (! read_sector(fd, sector_buf, sect_sz, off) ||
            checksum(sector_buf, sect_sz) != 0) {
                return false;
        }
        return true;
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
