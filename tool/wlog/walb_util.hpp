/**
 * @file
 * @brief Utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2012 Cybozu Labs, Inc.
 */
#ifndef WALB_UTIL_HPP
#define WALB_UTIL_HPP

#include <cassert>
#include <memory>
#include <cstdlib>
#include <functional>

#include "util.hpp"
#include "walb/super.h"
#include "walb/log_device.h"
#include "../walblog_format.h"

namespace walb {
namespace util {

/**
 * WalB super sector.
 */
class WalbSuperBlock
{
private:
    BlockDevice& bd_;
    const unsigned int pbs_;
    const u64 offset_;

    struct FreeDeleter {
        void operator()(u8 *p) {
            ::free(p);
        }
    };
    std::unique_ptr<u8, FreeDeleter> data_;

public:
    WalbSuperBlock(BlockDevice& bd)
        : bd_(bd)
        , pbs_(bd.getPhysicalBlockSize())
        , offset_(get1stSuperBlockOffsetStatic(pbs_))
        , data_(allocAlignedBufferStatic(pbs_)) {

        /* Read the superblock. */
#if 0
        ::printf("offset %" PRIu64" pbs %u\n", offset_ * pbs_, pbs_);
#endif
        bd_.read(offset_ * pbs_, pbs_, (char *)data_.get());

#if 0
        print(); //debug
#endif

        /* Check. */
        if (!isValid()) {
            throw RT_ERR("super block is not valid.");
        }
    }

    u16 getSectorType() const { return super()->sector_type; }
    u16 getVersion() const { return super()->version; }
    u32 getChecksum() const { return super()->checksum; }
    u32 getLogicalBlockSize() const { return super()->logical_bs; }
    u32 getPhysicalBlockSize() const { return super()->physical_bs; }
    u32 getMetadataSize() const { return super()->snapshot_metadata_size; }
    u32 getLogChecksumSalt() const { return super()->log_checksum_salt; }
    const u8* getUuid() const { return super()->uuid; }
    const char* getName() const { return super()->name; }
    u64 getRingBufferSize() const { return super()->ring_buffer_size; }
    u64 getOldestLsid() const { return super()->oldest_lsid; }
    u64 getWrittenLsid() const { return super()->written_lsid; }
    u64 getDeviceSize() const { return super()->device_size; }

    /*
     * Offset and size.
     */

    u64 get1stSuperBlockOffset() const {
        return offset_;
    }

    u64 getMetadataOffset() const {
        return ::get_metadata_offset_2(super());
    }

    u64 get2ndSuperBlockOffset() const {
        u64 oft = ::get_super_sector1_offset_2(super());
        assert(oft == getMetadataOffset() + getMetadataSize());
        return ::get_super_sector1_offset_2(super());
    }

    u64 getRingBufferOffset() const {
        u64 oft = ::get_ring_buffer_offset_2(super());
        assert(oft == get2ndSuperBlockOffset() + 1);
        return oft;
    }

    /**
     * Convert lsid to the position in the log device.
     *
     * @lsid target log sequence id.
     *
     * RETURN:
     *   Offset in the log device [physical block].
     */
    u64 getOffsetFromLsid(u64 lsid) const {
        if (lsid == INVALID_LSID) {
            throw RT_ERR("Invalid lsid.");
        }
        u64 s = getRingBufferSize();
        if (s == 0) {
            throw RT_ERR("Ring buffer size must not be 0.");
        }
        return (lsid % s) + getRingBufferOffset();
    }

    void print() const {

        ::printf("sectorType: %u\n"
                 "version: %u\n"
                 "checksum: %u\n"
                 "lbs: %u\n"
                 "pbs: %u\n"
                 "metadataSize: %u\n"
                 "logChecksumSalt: %u\n"
                 "name: %s\n"
                 "ringBufferSize: %" PRIu64 "\n"
                 "oldestLsid: %" PRIu64 "\n"
                 "writtenLsid: %" PRIu64 "\n"
                 "deviceSize: %" PRIu64 "\n"
                 "ringBufferOffset: %" PRIu64 "\n",
                 getSectorType(),
                 getVersion(),
                 getChecksum(),
                 getLogicalBlockSize(),
                 getPhysicalBlockSize(),
                 getMetadataSize(),
                 getLogChecksumSalt(),
                 getName(),
                 getRingBufferSize(),
                 getOldestLsid(),
                 getWrittenLsid(),
                 getDeviceSize(),
                 getRingBufferOffset());

        ::printf("uuid: ");
        for (int i = 0; i < 16; i++) {
            ::printf("%02x", getUuid()[i]);
        }
        ::printf("\n");
    }

private:
    static u64 get1stSuperBlockOffsetStatic(unsigned int pbs) {
        return ::get_super_sector0_offset(pbs);
    }

    static u8* allocAlignedBufferStatic(unsigned int pbs) {

        u8 *p;
        int ret = ::posix_memalign((void **)&p, pbs, pbs);
        if (ret) {
            throw std::bad_alloc();
        }
        return p;
    }

    struct walb_super_sector* super() {
        return (struct walb_super_sector *)data_.get();
    }

    const struct walb_super_sector* super() const {
        return (const struct walb_super_sector *)data_.get();
    }

    bool isValid(bool isChecksum = true) const {
        if (::is_valid_super_sector_raw(super(), pbs_) == 0) {
            return false;
        }
        if (isChecksum) {
            return true;
        } else {
            return ::checksum(data_.get(), pbs_, 0) == 0;
        }
    }
};

/**
 * Logpack header.
 */
class WalbLogpackHeader
{
private:
    std::shared_ptr<u8> ptr_;
    const unsigned int pbs_;
    const u32 salt_;

public:
    WalbLogpackHeader(std::shared_ptr<u8> ptr, unsigned int pbs, u32 salt)
        : ptr_(ptr)
        , pbs_(pbs)
        , salt_(salt) {
        ASSERT_PBS(pbs);
    }

    std::shared_ptr<u8> getBlock() const {
        return ptr_;
    }

    struct walb_logpack_header& header() {
        checkPointer();
        return *(struct walb_logpack_header *)ptr_.get();
    }

    const struct walb_logpack_header& header() const {
        checkPointer();
        return *(struct walb_logpack_header *)ptr_.get();
    }

    unsigned int pbs() const { return pbs_; }

    u32 salt() const { return salt_; }
    unsigned int totalIoSize() const { return header().total_io_size; }
    unsigned int nRecords() const { return header().n_records; }
    unsigned int nPadding() const { return header().n_padding; }
    u64 logpackLsid() const { return header().logpack_lsid; }

    /**
     * N'th log record.
     */
    struct walb_log_record& operator[](size_t pos) { return record(pos); }
    const struct walb_log_record& operator[](size_t pos) const { return record(pos); }

    struct walb_log_record& record(size_t pos) {
        checkIndexRange(pos);
        return header().record[pos];
    }

    const struct walb_log_record& record(size_t pos) const {
        checkIndexRange(pos);
        return header().record[pos];
    }

    bool isValid(bool isChecksum = true) const {
        if (isChecksum) {
            return ::is_valid_logpack_header_with_checksum(
                &header(), pbs(), salt()) != 0;
        } else {
            return ::is_valid_logpack_header(&header()) != 0;
        }
    }

    void printRecord(size_t pos) const {
        const struct walb_log_record &rec = record(pos);
        ::printf("record %zu\n"
                 "  checksum: %08x(%u)\n"
                 "  lsid: %" PRIu64"\n"
                 "  lsid_local: %u\n"
                 "  is_exist: %u\n"
                 "  is_padding: %u\n"
                 "  is_discard: %u\n"
                 "  offset: %" PRIu64"\n"
                 "  io_size: %u\n",
                 pos,
                 rec.checksum, rec.checksum,
                 rec.lsid, rec.lsid_local,
                 ::test_bit_u32(LOG_RECORD_EXIST, &rec.flags),
                 ::test_bit_u32(LOG_RECORD_PADDING, &rec.flags),
                 ::test_bit_u32(LOG_RECORD_DISCARD, &rec.flags),
                 rec.offset, rec.io_size);
    }

    void printHeader() const {
        const struct walb_logpack_header &logh = header();
        ::printf("*****logpack header*****\n"
                 "checksum: %08x(%u)\n"
                 "n_records: %u\n"
                 "n_padding: %u\n"
                 "total_io_size: %u\n"
                 "logpack_lsid: %" PRIu64"\n",
                 logh.checksum, logh.checksum,
                 logh.n_records,
                 logh.n_padding,
                 logh.total_io_size,
                 logh.logpack_lsid);
    }

    void print() const {
        printHeader();
        for (size_t i = 0; i < nRecords(); i++) {
            printRecord(i);
        }
    }

    /**
     * Shrink.
     */
    void shrink(size_t invalidIdx) {
        assert(invalidIdx < nRecords());

        /* Check the last valid record is padding or not. */
        if (invalidIdx > 0 &&
            ::test_bit_u32(LOG_RECORD_PADDING, &record(invalidIdx - 1).flags)) {
            invalidIdx--;
            header().n_padding--;
            assert(header().n_padding == 0);
        }

        /* Invalidate records. */
        for (size_t i = invalidIdx; i < nRecords(); i++) {
            ::log_record_init(&record(i));
        }

        /* Set n_records and total_io_size. */
        header().n_records = invalidIdx;
        header().total_io_size = 0;
        for (size_t i = 0; i < nRecords(); i++) {
            auto &rec = record(i);
            if (!::test_bit_u32(LOG_RECORD_DISCARD, &rec.flags)) {
                header().total_io_size += ::capacity_pb(pbs(), rec.io_size);
            }
        }

        /* Calculate checksum. */
        header().checksum = 0;
        header().checksum = ::checksum(ptr_.get(), pbs(), salt());

        assert(isValid());
    }

    /* Get next logpack lsid. */
    u64 nextLogpackLsid() const {
        if (nRecords() > 0) {
            return logpackLsid() + 1 + totalIoSize();
        } else {
            return logpackLsid();
        }
    }

private:
    void checkPointer() const {
        if (!ptr_.get()) { throw RT_ERR("Header is null."); }
    }

    void checkIndexRange(size_t pos) const {
        if (pos >= nRecords()) { throw RT_ERR("index out of range."); }
    }
};

/**
 * Log data of an IO.
 */
class WalbLogpackData
{
private:
    WalbLogpackHeader& logh_;
    size_t pos_;
    std::vector<std::shared_ptr<u8> > data_;

public:
    WalbLogpackData(WalbLogpackHeader& logh, size_t pos)
        : logh_(logh)
        , pos_(pos)
        , data_() {
        assert(pos < logh.nRecords());
        data_.reserve(logh.nRecords());
    }

    void addBlock(std::shared_ptr<u8> block) {
        data_.push_back(block);
    }

    std::shared_ptr<u8> getBlock(size_t idx) const {
        assert(hasData());
        assert(idx < ioSizePb());
        return data_[idx];
    }

    struct walb_log_record& record() {
        return logh_.record(pos_);
    }

    const struct walb_log_record& record() const {
        return logh_.record(pos_);
    }

    unsigned int pbs() const { return logh_.pbs(); }

    bool isExist() const {
        return test_bit_u32(LOG_RECORD_EXIST, &record().flags);
    }

    bool isPadding() const {
        return test_bit_u32(LOG_RECORD_PADDING, &record().flags);
    }

    bool isDiscard() const {
        return test_bit_u32(LOG_RECORD_DISCARD, &record().flags);
    }

    bool hasData() const {
        return isExist() && !isDiscard();
    }

    bool hasDataForChecksum() const {
        return isExist() && !isDiscard() && !isPadding();
    }

    unsigned int ioSizeLb() const {
        return record().io_size;
    }

    unsigned int ioSizePb() const {
        return capacity_pb(pbs(), ioSizeLb());
    }

    bool isValid(bool isChecksum = true) const {
        const auto &rec = record();
        if (!::is_valid_log_record_const(&rec)) {
            return false;
        }
        if (isChecksum && hasDataForChecksum() &&
            calcIoChecksum() != rec.checksum) {
            return false;
        }
        return true;
    }

private:
    u32 calcIoChecksum() const {
        unsigned int pbs = logh_.pbs();
        const auto &rec = record();
        assert(hasDataForChecksum());
        assert(rec.io_size > 0);
        unsigned int nPb = ioSizePb();
        unsigned int remaining = ioSizeLb() * LOGICAL_BLOCK_SIZE;

        if (nPb != data_.size()) {
            throw RT_ERR("There is not sufficient data block.");
        }

        u32 csum = logh_.salt();
        for (size_t i = 0; i < nPb; i++) {
            if (remaining >= pbs) {
                csum = ::checksum_partial(
                    csum, data_[i].get(), pbs);
                remaining -= pbs;
            } else {
                csum = ::checksum_partial(
                    csum, data_[i].get(), remaining);
                remaining = 0;
            }
        }
        csum = ::checksum_finish(csum);
        //::printf("csum %08x(%u)\n", csum, csum); //debug
        return csum;
    }
};

/**
 * Walb logfile header.
 */
class WalbLogFileHeader
{
private:
    std::vector<u8> data_;

public:
    WalbLogFileHeader()
        : data_(WALBLOG_HEADER_SIZE, 0) {}

    void init(unsigned int pbs, u32 salt, const u8 *uuid, u64 beginLsid, u64 endLsid) {
        ::memset(&data_[0], 0, WALBLOG_HEADER_SIZE);
        header().sector_type = SECTOR_TYPE_WALBLOG_HEADER;
        header().version = WALB_VERSION;
        header().header_size = WALBLOG_HEADER_SIZE;
        header().log_checksum_salt = salt;
        header().logical_bs = LOGICAL_BLOCK_SIZE;
        header().physical_bs = pbs;
        ::memcpy(header().uuid, uuid, 16);
        header().begin_lsid = beginLsid;
        header().end_lsid = endLsid;
    }

    void read(int fd) {
        util::FdReader fdr(fd);
        fdr.read(reinterpret_cast<char *>(&data_[0]), WALBLOG_HEADER_SIZE);
    }

    void write(int fd) {
        header().checksum = 0;
        header().checksum = ::checksum(&data_[0], WALBLOG_HEADER_SIZE, 0);
        util::FdWriter fdw(fd);
        fdw.write(reinterpret_cast<char *>(&data_[0]), WALBLOG_HEADER_SIZE);
    }

    struct walblog_header& header() {
        return *reinterpret_cast<struct walblog_header *>(&data_[0]);
    }

    const struct walblog_header& header() const {
        return *reinterpret_cast<const struct walblog_header *>(&data_[0]);
    }

    bool isValid(bool isChecksum = true) const {
        CHECKd(header().sector_type == SECTOR_TYPE_WALBLOG_HEADER);
        CHECKd(header().version == WALB_VERSION);
        CHECKd(header().begin_lsid < header().end_lsid);
        if (isChecksum) {
            CHECKd(::checksum(&data_[0], WALBLOG_HEADER_SIZE, 0) == 0);
        }
        return true;
      error:
        return false;
    }
};

} //namespace util
} //namespace walb

#endif /* WALB_UTIL_HPP */
