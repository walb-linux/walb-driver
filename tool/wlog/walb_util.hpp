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

#include "util.hpp"
#include "walb/super.h"
#include "walb/log_device.h"

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
    std::unique_ptr<u8> data_;

public:
    WalbSuperBlock(BlockDevice& bd)
        : bd_(bd)
        , pbs_(bd.getPhysicalBlockSize())
        , offset_(get1stSuperBlockOffsetStatic(pbs_))
        , data_(allocAlignedBufferStatic(pbs_)) {

        /* Read the superblock. */
        ::printf("offset %" PRIu64" pbs %u\n", offset_ * pbs_, pbs_);
        bd_.read(offset_ * pbs_, pbs_, (char *)data_.get());

        print(); // debug

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
                 "ringBufferSize: %" PRIu64"\n"
                 "oldestLsid: %" PRIu64"\n"
                 "writtenLsid: %" PRIu64"\n"
                 "deviceSize: %" PRIu64"\n",
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
                 getDeviceSize());

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


    bool isValid() const {
        if (::is_valid_super_sector_raw(super(), pbs_) == 0) {
            return false;
        }
        return ::checksum(data_.get(), pbs_, 0) == 0;
    }
};

class WalbLogpack
{
private:
    unsigned int pbs_; // physical block size.
    u32 salt_; // checksum salt.
    struct walb_logpack_header *logh_;
    std::vector<u8 *> data_;

public:
    WalbLogpack(unsigned int pbs, u32 checksumSalt)
        : pbs_(pbs)
        , salt_(checksumSalt)
        , logh_()
        , data_() {}
    ~WalbLogpack() {}

    WalbLogpack(WalbLogpack&& rhs)
        : pbs_(rhs.pbs_)
        , salt_(rhs.salt_)
        , logh_(rhs.logh_)
        , data_(std::move(rhs.data_)) {

        rhs.logh_ = nullptr;
    }

    void setHeader(struct walb_logpack_header *logh) {
        assert(logh);
        logh_ = logh;
        data_.reserve(logh->total_io_size);
    }

    void addBlock(u8 *block) {
        assert(block);
        data_.push_back(block);
    }

    const struct walb_logpack_header& header() const {
        if (!logh_) {
            throw RT_ERR("logpack header is null.");
        }
        return *logh_;
    }

    /**
     * N'th log record.
     */
    struct walb_log_record& operator[](size_t pos) {
        return record(pos);
    }

    const struct walb_log_record& operator[](size_t pos) const {
        return record(pos);
    }

    struct walb_log_record& record(size_t pos) {
        if (!logh_) { throw RT_ERR("Header is null."); }
        checkIndexRange(pos);
        return logh_->record[pos];
    }

    const struct walb_log_record& record(size_t pos) const {
        if (!logh_) { throw RT_ERR("Header is null"); }
        checkIndexRange(pos);
        return logh_->record[pos];
    }

    unsigned int totalIoSize() const {
        return logh_ ? logh_->total_io_size : 0;
    }

    unsigned int nRecords() const {
        return logh_ ? logh_->n_records : 0;
    }

    bool isHeaderValid() const {
        if (!logh_) { return false; }
        return ::is_valid_logpack_header_with_checksum(logh_, pbs_, salt_) != 0;
    }

    bool isDataValid() const {
        if (!logh_) { return false; }
        for (unsigned int i = 0; i < nRecords(); i++) {
            const auto &rec = record(i);
            if (!::is_valid_log_record_const(&rec)) {
                return false;
            }
            if (rec.io_size == 0) { continue; }
            if (calcIoChecksum(i) != 0) { return false; }
        }
        return true;
    }

    bool isValid() const {
        return isHeaderValid() && isDataValid();
    }

private:

    u32 calcIoChecksum(size_t pos) const {
        const auto &rec = record(pos);
        assert(rec.io_size > 0);
        size_t start = rec.lsid_local - 1;
        unsigned int n_pb = capacity_pb(pbs_, rec.io_size);
        unsigned int remaining =
            static_cast<unsigned int>(rec.io_size) * LOGICAL_BLOCK_SIZE;

        u32 csum = salt_;
        for (size_t i = start; i < start + n_pb; i++) {
            if (remaining >= pbs_) {
                csum = ::checksum_partial(
                    csum, data_[i], pbs_);
                remaining -= pbs_;
            } else {
                csum = ::checksum_partial(
                    csum, data_[i], remaining);

                remaining = 0;
            }
        }
        return ::checksum_finish(csum);
    }

    void checkIndexRange(size_t pos) const {
        assert(logh_);
        if (pos >= logh_->n_records) {
            throw RT_ERR("index out of range.");
        }
    }
};

} //namespace util
} //namespace walb

#endif /* WALB_UTIL_HPP */
