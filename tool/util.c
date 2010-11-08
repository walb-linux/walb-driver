/**
 * General definitions for Walb.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license GPLv2 or later.
 */
#define _FILE_OFFSET_BITS 64

#include "userland.h"
#include "util.h"
#include "random.h"

#include <stdio.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <fcntl.h>

#include <linux/fs.h>

#define dprintf(fmt, ...)                                               \
        printf("DEBUG(%s,%d):" fmt, __FILE__, __LINE__, ##__VA_ARGS__)   
        

int check_log_dev(char* path)
{
        struct stat sb;
        dev_t devt;
        size_t sector_size, dev_size, size;
        
        assert(path != NULL);
        if (stat(path, &sb) == -1) {
                dprintf("stat failed.\n");
                perror("");
                return -1;
        }

        if ((sb.st_mode & S_IFMT) != S_IFBLK) {
                dprintf("%s is not block device.\n", path);
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
                        dprintf("open failed\n");
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
static int open_blk_dev(char* devpath)
{
        int fd;
        struct stat sb;

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
                dprintf("%s is not block device.\n", devpath);
                goto close;
        }

        return fd;
        
close:
        close(fd);
error:
        return -1;
}


/**
 *
 *
 * @return sector size in succeeded, or -1.
 */
int get_bdev_sector_size(char* devpath)
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

u64 get_bdev_size(char* devpath)
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


void generate_uuid(u8* uuid)
{
        memset_random(uuid, 16);
}

