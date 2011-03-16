/**
 * Logpack functions for walbctl.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <string.h>

#include "util.h"
#include "logpack.h"

/**
 * Check logpack header.
 *
 * @logpack logpack to be checked.
 * @physical_bs physical block size (logpack size).
 *
 * @return true in success, or false.
 */
bool check_logpack_header(const walb_logpack_header_t* lhead,
                          int physical_bs)
{
        /* check others */
        if (lhead->n_records == 0 ||
            lhead->total_io_size == 0 ||
            lhead->sector_type != SECTOR_TYPE_LOGPACK) {
                LOGe("log pack header is invalid "
                     "(n_records: %u total_io_size %u sector_type %u).\n",
                     lhead->n_records, lhead->total_io_size,
                     lhead->sector_type);
                goto error0;
        }
        
        /* confirm checksum */
        if (checksum((const u8 *)lhead, physical_bs) != 0) {
                LOGe("logpack header checksum is invalid (lsid %"PRIu64").\n",
                     lhead->logpack_lsid);
                goto error0;
        }

        return true;
error0:
        return false;
}       


/**
 * Read logpack header sector from log device.
 *
 * @fd log device fd opened.
 * @super_sectp super sector.
 * @lsid logpack lsid to read.
 * @logpack buffer to store logpack header data.
 *          This allocated size must be sector size.
 *
 * @return ture in success, or false.
 */
bool read_logpack_header_from_wldev(int fd,
                                    const walb_super_sector_t* super_sectp,
                                    u64 lsid, walb_logpack_header_t* lhead)
{
        /* calc offset in the ring buffer */
        u64 ring_buffer_offset = get_ring_buffer_offset_2(super_sectp);
        u64 ring_buffer_size = super_sectp->ring_buffer_size;

        u64 off = ring_buffer_offset + lsid % ring_buffer_size;

        /* read sector */
        if (! read_sector(fd, (u8 *)lhead, super_sectp->physical_bs, off)) {
                LOGe("read logpack header (lsid %"PRIu64") failed.\n", lsid);
                goto error0;
        }

        /* check lsid */
        if (lsid != lhead->logpack_lsid) {
                LOGe("lsid (given %"PRIu64" read %"PRIu64") is invalid.\n",
                     lsid, lhead->logpack_lsid);
                goto error0;
        }

        if (! check_logpack_header(lhead, super_sectp->physical_bs)) {
                LOGe("check logpack header failed.\n");
                goto error0;
        }

        return true;
        
error0:
        return false;
}

/**
 * Print logpack header.
 *
 * @logpack log pack data.
 */
void print_logpack_header(const walb_logpack_header_t* lhead)
{
        ASSERT(lhead != NULL);
        int i;
        printf("*****logpack header*****\n"
               "checksum: %08x\n"
               "n_records: %u\n"
               "n_padding: %u\n"
               "total_io_size: %u\n"
               "logpack_lsid: %"PRIu64"\n",
               lhead->checksum,
               lhead->n_records,
               lhead->n_padding,
               lhead->total_io_size,
               lhead->logpack_lsid);
        for (i = 0; i < lhead->n_records; i ++) {
                printf("record %d\n"
                       "  checksum: %08x\n"
                       "  lsid: %"PRIu64"\n"
                       "  lsid_local: %u\n"
                       "  is_padding: %u\n"
                       "  io_size: %u\n"
                       "  is_exist: %u\n"
                       "  offset: %"PRIu64"\n",
                       i,
                       lhead->record[i].checksum,
                       lhead->record[i].lsid,
                       lhead->record[i].lsid_local,
                       lhead->record[i].is_padding,
                       lhead->record[i].io_size,
                       lhead->record[i].is_exist,
                       lhead->record[i].offset);
                printf("logpack lsid: %"PRIu64"\n", 
                       lhead->record[i].lsid - lhead->record[i].lsid_local);
        }
}

/**
 * Write logpack header.
 *
 * @fd file descriptor to write.
 * @physical_bs physical block size.
 * @logpack logpack to be written.
 *
 * @return true in success, or false.
 */
bool write_logpack_header(int fd,
                          int physical_bs,
                          const walb_logpack_header_t* lhead)
{
        return write_data(fd, (const u8 *)lhead, physical_bs);
}

/**
 * Read logpack data.
 * Padding area will be also read.
 *
 * @fd file descriptor of log device.
 * @super_sectp super sector.
 * @logpack logpack header.
 * @buf buffer.
 * @bufsize buffer size in bytes.
 *
 * @return true in success, or false.
 */
bool read_logpack_data_from_wldev(int fd,
                                  const walb_super_sector_t* super_sectp,
                                  const walb_logpack_header_t* lhead,
                                  u8* buf, size_t bufsize)
{
        int logical_bs = super_sectp->logical_bs;
        int physical_bs = super_sectp->physical_bs;
        int n_lb_in_pb = physical_bs / logical_bs;
        ASSERT(physical_bs % logical_bs == 0);

        if (lhead->total_io_size * physical_bs > (ssize_t)bufsize) {
                LOGe("buffer size is not enough.\n");
                return false;
        }

        int i;
        int n_req = lhead->n_records;
        int total_pb;
        u64 log_off;
        u32 log_lb, log_pb;
        u8 *buf_off;
        
        buf_off = 0;
        total_pb = 0;
        for (i = 0; i < n_req; i ++) {

                log_lb = lhead->record[i].io_size;

                /* Calculate num of physical blocks. */
                log_pb = log_lb / n_lb_in_pb;
                if (log_lb % n_lb_in_pb != 0) { log_pb ++; }

                log_off = get_offset_of_lsid_2
                        (super_sectp, lhead->record[i].lsid);
                LOGd("lsid: %"PRIu64" log_off: %"PRIu64"\n",
                     lhead->record[i].lsid,
                     log_off);

                buf_off = buf + (total_pb * physical_bs);
                if (lhead->record[i].is_padding == 0) {

                        /* Read data for the log record. */
                        if (! read_sectors(fd, buf_off, physical_bs, log_off, log_pb)) {
                                LOGe("read sectors failed.\n");
                                goto error0;
                        }

                        /* Confirm checksum */
                        u32 csum = checksum((const u8 *)buf_off, logical_bs * log_lb);
                        if (csum != lhead->record[i].checksum) {
                                LOGe("log header checksum is invalid. %08x %08x\n",
                                     csum, lhead->record[i].checksum);
                                goto error0;
                        }
                } else {
                        /* memset zero instead of read due to padding area. */
                        memset(buf_off, 0, log_pb * physical_bs);
                }
                total_pb += log_pb;
        }

        return true;
        
error0:        
        return false;
}

/**
 * Read logpack header from fd.
 *
 * @fd file descriptor (opened, seeked)
 * @physical_bs physical block size (logpack header size)
 * @logpack logpack to be filled. (allocated size must be physical_bs).
 *
 * @return true in success, or false.
 */
bool read_logpack_header(int fd,
                         int physical_bs,
                         walb_logpack_header_t* lhead)
{
        /* Read */
        if (! read_data(fd, (u8 *)lhead, physical_bs)) {
                return false;
        }

        /* Check */
        if (! check_logpack_header(lhead, physical_bs)) {
                return false;
        }

        return true;
}

/**
 * Read logpack data from ds.
 *
 * @fd file descriptor (opened, seeked)
 * @logical_bs logical block size.
 * @phycial_bs physical block size.
 * @logpack corresponding logpack header.
 * @buf buffer to be filled.
 * @bufsize buffser size.
 *
 * @return true in success, or false.
 */
bool read_logpack_data(int fd,
                       int logical_bs, int physical_bs,
                       const walb_logpack_header_t* lhead,
                       u8* buf, size_t bufsize)
{
        ASSERT(physical_bs % logical_bs == 0);
        const int n_lb_in_pb = physical_bs / logical_bs;

        if (lhead->total_io_size * physical_bs > (ssize_t)bufsize) {
                LOGe("buffer size is not enough.\n");
                goto error0;
        }

        int i;
        const int n_req = lhead->n_records;
        u32 total_pb;

        total_pb = 0;
        for (i = 0; i < n_req; i ++) {

                u32 log_lb = lhead->record[i].io_size;

                u32 log_pb = log_lb / n_lb_in_pb;
                if (log_lb % n_lb_in_pb != 0) { log_pb ++; }

                u8 *buf_off = buf + (total_pb * physical_bs);
                if (lhead->record[i].is_padding == 0) {

                        /* Read data of the log record. */
                        if (! read_data(fd, buf_off, log_pb * physical_bs)) {
                                LOGe("read log data failed.\n");
                                goto error0;
                        }

                        /* Confirm checksum. */
                        u32 csum = checksum((const u8 *)buf_off, log_lb * logical_bs);
                        if (csum != lhead->record[i].checksum) {
                                LOGe("log header checksum in invalid. %08x %08x\n",
                                     csum, lhead->record[i].checksum);
                                goto error0;
                        }
                } else {
                        memset(buf_off, 0, log_pb * physical_bs);
                }
                total_pb += log_pb;
        }
        
        return true;
                
error0:
        return false;
}

/**
 * Redo logpack.
 *
 * @fd file descriptor of data device (opened).
 * @logical_bs logical block size.
 * @physical_bs physical block size.
 * @logpack logpack header to be redo.
 * @buf logpack data. (meaning data size: lhead->total_io_size * physical_bs)
 */
bool redo_logpack(int fd,
                  int logical_bs, int physical_bs,
                  const walb_logpack_header_t* lhead,
                  const u8* buf)
{
        int i;
        int n_req = lhead->n_records;
        
        for (i = 0; i < n_req; i ++) {

                if (lhead->record[i].is_padding != 0) {
                        continue;
                }

                int buf_off = (int)(lhead->record[i].lsid_local - 1) * physical_bs;
                u64 off_lb = lhead->record[i].offset;
                int size_lb = lhead->record[i].io_size;

                if (! write_sectors(fd, buf + buf_off, logical_bs, off_lb, size_lb)) {
                        LOGe("write sectors failed.\n");
                        goto error0;
                }
        }

        return true;
error0:        
        return false;
}

/**
 * Alloate empty logpack data.
 *
 * @logical_bs logical block size.
 * @physical_bs physical block size.
 * @n_sectors initial number of sectors. n_sectors > 0.
 *
 * @return pointer to allocated logpack in success, or NULL.
 */
logpack_t* alloc_logpack(int logical_bs, int physical_bs, int n_sectors)
{
    ASSERT(logical_bs > 0);
    ASSERT(physical_bs >= logical_bs);
    ASSERT(physical_bs % logical_bs == 0);
    ASSERT(n_sectors > 0);

    /* Allocate for itself. */
    logpack_t* logpack = (logpack_t *)malloc(sizeof(logpack_t));
    if (! logpack) { goto error; }
    logpack->logical_bs = logical_bs;
    logpack->physical_bs = physical_bs;

    /* Header sector. */
    logpack->head_sect = sector_alloc_zero(physical_bs);
    if (! logpack->head_sect) { goto error; }

    /* Data sectors. */
    logpack->data_sects = sector_data_array_alloc(physical_bs, n_sectors);
    if (! logpack->data_sects) { goto error; }

    walb_logpack_header_t *lhead = logpack_get_header(logpack);
    ASSERT(lhead);
    lhead->checksum = 0;
    lhead->sector_type = SECTOR_TYPE_LOGPACK;
    lhead->total_io_size = 0;
    lhead->logpack_lsid = INVALID_LSID;
    lhead->n_records = 0;
    lhead->n_padding = 0;
    int i;
    int n_max = max_n_log_record_in_sector(physical_bs);
    for (i = 0; i < n_max; i ++) {
	lhead->record[i].is_exist = 0;
    }
    return logpack;

error:
    free_logpack(logpack);
    return NULL;
}

/**
 * Free logpack.
 */
void free_logpack(logpack_t* logpack)
{
    if (logpack) {
	sector_data_array_free(logpack->data_sects);
	sector_free(logpack->head_sect);
	free(logpack);
    }
}

/**
 * Get pointer to logpack header image.
 */
walb_logpack_header_t* logpack_get_header(logpack_t* logpack)
{
    ASSERT(logpack != NULL);
    ASSERT(logpack->head_sect != NULL);
    ASSERT(logpack->head_sect->data != NULL);

    return (walb_logpack_header_t *)logpack->head_sect->data;
}

/**
 * Add IO request to a logpack.
 * Data will be copied.
 *
 * @logpack logpack to be modified.
 * @data written data by IO.
 * @size IO size in bytes.
 */
bool logpack_add_io_request(logpack_t* logpack, const u8* data, int size)
{
    /* now editing */

    return false;
}

/**
 * Create random logpack data.
 */
walb_logpack_header_t* create_random_logpack(int logical_bs, int physical_bs, const u8* buf)
{
    /* now editing */

    return NULL;
}

/* end of file */
