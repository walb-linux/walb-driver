/**
 * Logpack functions for walbctl.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license GPLv2 or later.
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
bool check_logpack_header(const walb_logpack_header_t* logpack,
                          int physical_bs)
{
        /* check others */
        if (logpack->n_records == 0 ||
            logpack->total_io_size == 0 ||
            logpack->sector_type != SECTOR_TYPE_LOGPACK) {
                LOG("log pack header is invalid "
                    "(n_records: %u total_io_size %u sector_type %u).\n",
                    logpack->n_records, logpack->total_io_size,
                    logpack->sector_type);
                goto error0;
        }
        
        /* confirm checksum */
        if (checksum((const u8 *)logpack, physical_bs) != 0) {
                LOG("logpack header checksum is invalid (lsid %"PRIu64").\n",
                    logpack->logpack_lsid);
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
                                    u64 lsid, walb_logpack_header_t* logpack)
{
        /* calc offset in the ring buffer */
        u64 ring_buffer_offset = get_ring_buffer_offset_2(super_sectp);
        u64 ring_buffer_size = super_sectp->ring_buffer_size;

        u64 off = ring_buffer_offset + lsid % ring_buffer_size;

        /* read sector */
        if (! read_sector(fd, (u8 *)logpack, super_sectp->physical_bs, off)) {
                LOG("read logpack header (lsid %"PRIu64") failed.\n", lsid);
                goto error0;
        }

        /* check lsid */
        if (lsid != logpack->logpack_lsid) {
                LOG("lsid (given %"PRIu64" read %"PRIu64") is invalid.\n",
                    lsid, logpack->logpack_lsid);
                goto error0;
        }

        if (! check_logpack_header(logpack, super_sectp->physical_bs)) {
                LOG("check logpack header failed.\n");
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
void print_logpack_header(const walb_logpack_header_t* logpack)
{
        ASSERT(logpack != NULL);
        int i;
        printf("*****logpack header*****\n"
               "checksum: %08x\n"
               "n_records: %u\n"
               "n_padding: %u\n"
               "total_io_size: %u\n"
               "logpack_lsid: %"PRIu64"\n",
               logpack->checksum,
               logpack->n_records,
               logpack->n_padding,
               logpack->total_io_size,
               logpack->logpack_lsid);
        for (i = 0; i < logpack->n_records; i ++) {
                printf("record %d\n"
                       "  checksum: %08x\n"
                       "  lsid: %"PRIu64"\n"
                       "  lsid_local: %u\n"
                       "  is_padding: %u\n"
                       "  io_size: %u\n"
                       "  is_exist: %u\n"
                       "  offset: %"PRIu64"\n",
                       i,
                       logpack->record[i].checksum,
                       logpack->record[i].lsid,
                       logpack->record[i].lsid_local,
                       logpack->record[i].is_padding,
                       logpack->record[i].io_size,
                       logpack->record[i].is_exist,
                       logpack->record[i].offset);
                printf("logpack lsid: %"PRIu64"\n", 
                       logpack->record[i].lsid - logpack->record[i].lsid_local);
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
                          const walb_logpack_header_t* logpack)
{
        return write_data(fd, (const u8 *)logpack, physical_bs);
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
                                  const walb_logpack_header_t* logpack,
                                  u8* buf, size_t bufsize)
{
        int logical_bs = super_sectp->logical_bs;
        int physical_bs = super_sectp->physical_bs;
        int n_lb_in_pb = physical_bs / logical_bs;
        ASSERT(physical_bs % logical_bs == 0);

        if (logpack->total_io_size * physical_bs > (ssize_t)bufsize) {
                LOG("buffer size is not enough.\n");
                return false;
        }

        int i;
        int n_req = logpack->n_records;
        int total_pb;
        u64 log_off;
        u32 log_lb, log_pb;
        u8 *buf_off;
        
        buf_off = 0;
        total_pb = 0;
        for (i = 0; i < n_req; i ++) {

                log_lb = logpack->record[i].io_size;

                /* Calculate num of physical blocks. */
                log_pb = log_lb / n_lb_in_pb;
                if (log_lb % n_lb_in_pb != 0) { log_pb ++; }

                log_off = get_offset_of_lsid_2
                        (super_sectp, logpack->record[i].lsid);
                LOG("lsid: %"PRIu64" log_off: %"PRIu64"\n",
                    logpack->record[i].lsid,
                    log_off);

                buf_off = buf + (total_pb * physical_bs);
                if (logpack->record[i].is_padding == 0) {

                        /* Read data for the log record. */
                        if (! read_sectors(fd, buf_off, physical_bs, log_off, log_pb)) {
                                LOG("read sectors failed.\n");
                                goto error0;
                        }

                        /* Confirm checksum */
                        u32 csum = checksum((const u8 *)buf_off, logical_bs * log_lb);
                        if (csum != logpack->record[i].checksum) {
                                LOG("log header checksum is invalid. %08x %08x\n",
                                    csum, logpack->record[i].checksum);
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
                         walb_logpack_header_t* logpack)
{
        /* Read */
        if (! read_data(fd, (u8 *)logpack, physical_bs)) {
                return false;
        }

        /* Check */
        if (! check_logpack_header(logpack, physical_bs)) {
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
                       const walb_logpack_header_t* logpack,
                       u8* buf, size_t bufsize)
{
        ASSERT(physical_bs % logical_bs == 0);
        const int n_lb_in_pb = physical_bs / logical_bs;

        if (logpack->total_io_size * physical_bs > (ssize_t)bufsize) {
                LOG("buffer size is not enough.\n");
                goto error0;
        }

        int i;
        const int n_req = logpack->n_records;
        u32 total_pb;

        total_pb = 0;
        for (i = 0; i < n_req; i ++) {

                u32 log_lb = logpack->record[i].io_size;

                u32 log_pb = log_lb / n_lb_in_pb;
                if (log_lb % n_lb_in_pb != 0) { log_pb ++; }

                u8 *buf_off = buf + (total_pb * physical_bs);
                if (logpack->record[i].is_padding == 0) {

                        /* Read data of the log record. */
                        if (! read_data(fd, buf_off, log_pb * physical_bs)) {
                                LOG("read log data failed.\n");
                                goto error0;
                        }

                        /* Confirm checksum. */
                        u32 csum = checksum((const u8 *)buf_off, log_lb * logical_bs);
                        if (csum != logpack->record[i].checksum) {
                                LOG("log header checksum in invalid. %08x %08x\n",
                                    csum, logpack->record[i].checksum);
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
 * @buf logpack data. (meaning data size: logpack->total_io_size * physical_bs)
 */
bool redo_logpack(int fd,
                  int logical_bs, int physical_bs,
                  const walb_logpack_header_t* logpack,
                  const u8* buf)
{
        int i;
        int n_req = logpack->n_records;
        
        for (i = 0; i < n_req; i ++) {

                if (logpack->record[i].is_padding != 0) {
                        continue;
                }

                int buf_off = (int)(logpack->record[i].lsid_local - 1) * physical_bs;
                u64 off_lb = logpack->record[i].offset;
                int size_lb = logpack->record[i].io_size;

                if (! write_sectors(fd, buf + buf_off, logical_bs, off_lb, size_lb)) {
                        LOG("write sectors failed.\n");
                        goto error0;
                }
        }

        return true;
error0:        
        return false;
}


/* end of file */
