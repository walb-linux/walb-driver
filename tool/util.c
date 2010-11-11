/**
 * General definitions for Walb.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license GPLv2 or later.
 */
#include <stdio.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <linux/fs.h>

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


int check_log_dev(const char* path)
{
        struct stat sb;
        dev_t devt;
        size_t sector_size, dev_size, size;
        
        ASSERT(path != NULL);
        if (stat(path, &sb) == -1) {
                LOG("stat failed.\n");
                perror("");
                return -1;
        }

        if ((sb.st_mode & S_IFMT) != S_IFBLK) {
                LOG("%s is not block device.\n", path);
                return -1;
        }

        devt = sb.st_rdev;
        sector_size = sb.st_blksize;
        dev_size = sb.st_blocks;
        size = sb.st_size;

        printf("devname: %s\n"
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
                        LOG("open failed\n");
                        return -1;
                }
                ioctl(fd, BLKBSZGET, &bs); /* soft block size */
                ioctl(fd, BLKSSZGET, &ss); /* logical sector size */
                ioctl(fd, BLKPBSZGET, &pbs); /* physical sector size */
                ioctl(fd, BLKGETSIZE64, &size); /* size */
                close(fd);

                printf("soft block size: %d\n"
                       "logical sector size: %d\n"
                       "physical sector size: %u\n"
                       "device size: %zu\n",
                       bs, ss, pbs, (size_t)size);
        }
        
        /* not yet implemented */
  
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
                LOG("%s is not block device.\n", devpath);
                goto close;
        }

        return fd;
        
close:
        close(fd);
error:
        return -1;
}


/**
 * Get physical block size of the block device.
 *
 * @devpath device file path.
 * @return sector size in succeeded, or -1.
 */
int get_bdev_sector_size(const char* devpath)
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
                LOG("%s stat failed.\n", devpath);
                goto error;
        }

        if ((sb.st_mode & S_IFMT) != S_IFBLK) {
                LOG("%s is not block device.\n", devpath);
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
 * Write sector data to the offset.
 *
 * @sector_buf aligned buffer containing sector data.
 * @sector_size sector size in bytes.
 * @offset offset in sectors.
 * @return true in success, or false.
 */
bool write_sector(int fd, const u8* sector_buf, u32 sector_size, u64 offset)
{
        ssize_t w = 0;
        while (w < sector_size) {
                ssize_t s = pwrite(fd, sector_buf + w,
                                   sector_size - w,
                                   offset * sector_size);
                if (s > 0) {
                        w += s;
                } else {
                        perror("write sector error.");
                        return false;
                }
        }
        ASSERT(w == sector_size);
        return true;
}

/**
 * Print super sector for debug.
 */
void print_super_sector(const walb_super_sector_t* super_sect)
{
        ASSERT(super_sect != NULL);
        printf("checksum: %u\n"
               "sector_size: %u\n"
               "snapshot_metadata_size: %u\n",
               super_sect->checksum,
               super_sect->sector_size,
               super_sect->snapshot_metadata_size);
        print_uuid(super_sect->uuid);
        printf("\n"
               "start_offset: %lu\n"
               "ring_buffer_size: %lu\n"
               "oldest_lsid: %lu\n"
               "written_lsid: %lu\n"
               "device_size: %lu\n",
               super_sect->start_offset,
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
        u32 sect_sz = super_sect->sector_size;
        
        /* Memory image of sector. */
        u8 *sector_buf;
        if (posix_memalign((void **)&sector_buf, PAGE_SIZE, sect_sz) != 0) {
                goto error0;
        }
        memset(sector_buf, 0, sect_sz);
        memcpy(sector_buf, super_sect, sizeof(*super_sect));

        /* Calculate checksum. */
        walb_super_sector_t *super_sect_tmp = (walb_super_sector_t *)sector_buf;
        super_sect_tmp->checksum = 0;
        u32 csum = checksum(sector_buf, sect_sz);
        print_binary_hex(sector_buf, sect_sz);/* debug */
        super_sect_tmp->checksum = ~csum + 1;
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
        ssize_t r = 0;
        while (r < sector_size) {
                ssize_t s = pread(fd, sector_buf + r,
                                  sector_size - r,
                                  offset * sector_size);
                if (s > 0) {
                        r += s;
                } else {
                        perror("read sector error.");
                        return false;
                }
        }
        ASSERT(r == sector_size);
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

        
        /* Memory image of sector. */
        u8 *buf, *buf0, *buf1;
        if (posix_memalign((void **)&buf, PAGE_SIZE, sector_size * 2) != 0) {
                perror("memory allocation failed.");
                goto error0;
        }
        buf0 = buf;
        buf1 = buf + sector_size;

        u32 off0 = get_super_sector0_offset(sector_size);
        u32 off1 = get_super_sector1_offset(sector_size, n_snapshots);

        bool ret0 = read_sector(fd, buf0, sector_size, off0);
        bool ret1 = read_sector(fd, buf1, sector_size, off1);
        
        if (ret0 && checksum(buf0, sector_size) != 0) {
                ret0 = -1;
        }
        if (ret1 && checksum(buf1, sector_size) != 0) {
                ret1 = -1;
        }
        if (! ret0 && ! ret1) {
                LOG("checksum is wrong and both superblocks are broken.\n");
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
 * print snapshot sector for debug.
 */
void print_snapshot_sector(const walb_snapshot_sector_t* snap_sect)
{
        /* not yet implemented */

        printf("checksum: %u\n", snap_sect->checksum);
}


/**
 * Write snapshot sector.
 *
 * @fd File descriptor of log device.
 * @super_sect super sector data to refer its members.
 * @snap_sect snapshot sector data to be written.
 * @idx idx'th sector is written. (0 <= idx < snapshot_metadata_size)
 *
 * @return true in success, or false.
 */
bool write_snapshot_sector(int fd, const walb_super_sector_t* super_sect,
                           const walb_snapshot_sector_t* snap_sect, u32 idx)
{
        ASSERT(fd >= 0);
        ASSERT(super_sect != NULL);
        ASSERT(snap_sect != NULL);
        
        u32 sect_sz = super_sect->sector_size;
        u32 meta_sz = super_sect->snapshot_metadata_size;
        if (idx >= meta_sz) {
                LOG("idx range over. idx: %u meta_sz: %u\n", idx, meta_sz);
                goto error0;
        }

        u8 *sector_buf;
        if (posix_memalign((void **)&sector_buf, PAGE_SIZE, sect_sz) != 0) {
                goto error0;
        }

        memset(sector_buf, 0, sect_sz);
        memcpy(sector_buf, snap_sect, sizeof(*snap_sect));

        /* checksum */
        walb_snapshot_sector_t *snap_sect_tmp = (walb_snapshot_sector_t *)sector_buf;
        
        snap_sect_tmp->checksum = 0;
        u32 csum = checksum(sector_buf, sect_sz);
        snap_sect_tmp->checksum = ~csum + 1;
        ASSERT(checksum(sector_buf, sect_sz) == 0);

        /* really write sector data. */
        u64 off = get_metadata_offset_2(super_sect) + idx;
        if (! write_sector(fd, sector_buf, sect_sz, off)) {
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
 * Read snapshot sector.
 *
 * @fd File descriptor of log device.
 * @super_sect super sector data to refer its members.
 * @snap_sect snapshot sector buffer to be read.
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
        
        u32 sect_sz = super_sect->sector_size;
        u32 meta_sz = super_sect->snapshot_metadata_size;
        if (! idx < meta_sz) {
                LOG("idx range over.\n");
                goto error0;
        }

        u8 *sector_buf;
        if (posix_memalign((void **)&sector_buf, PAGE_SIZE, sect_sz) != 0) {
                goto error0;
        }

        /* Read sector data. */
        u64 off = get_metadata_offset_2(super_sect) + idx;
        if (! read_sector(fd, sector_buf, sect_sz, off)) {
                goto error1;
        }

        /* Confirm checksum. */
        if (checksum(sector_buf, sect_sz) != 0) {
                goto error1;
        }

        memcpy(snap_sect, sector_buf, sizeof(*snap_sect));

        free(sector_buf);
        return true;
                
error1:
        free(sector_buf);
error0:
        return false;
}


/* end of file */
