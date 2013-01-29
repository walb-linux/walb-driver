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
#include "walb/log_record.h"
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
        read();
#if 0
        print(); //debug
#endif
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

    void setOldestLsid(u64 oldestLsid) {
        super()->oldest_lsid = oldestLsid;
    }
    void setWrittenLsid(u64 writtenLsid) {
        super()->written_lsid = writtenLsid;
    }
    void setDeviceSize(u64 deviceSize) {
        super()->device_size = deviceSize;
    }
    void setLogChecksumSalt(u32 salt) {
        super()->log_checksum_salt = salt;
    }
    void setUuid(const u8 *uuid) {
        ::memcpy(super()->uuid, uuid, 16);
    }
    void updateChecksum() {
        super()->checksum = 0;
        super()->checksum = ::checksum(data_.get(), pbs_, 0);
    }

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
        UNUSED u64 oft = ::get_super_sector1_offset_2(super());
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

    void read() {
        bd_.read(offset_ * pbs_, pbs_, (char *)data_.get());
        /* Check. */
        if (!isValid()) {
            throw RT_ERR("super block is not valid.");
        }
    }

    void write() {
        updateChecksum();
        bd_.write(offset_ * pbs_, pbs_,
                  reinterpret_cast<char *>(data_.get()));
    }

    void print(FILE *fp) const {
        ::fprintf(fp,
                  "sectorType: %u\n"
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

        ::fprintf(fp, "uuid: ");
        for (int i = 0; i < 16; i++) {
            ::fprintf(fp, "%02x", getUuid()[i]);
        }
        ::fprintf(fp, "\n");
    }

    void print() const {
        print(::stdout);
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

class InvalidLogpackData : public std::exception {
public:
    virtual const char *what() const noexcept {
        return "invalid logpack data.";
    }
};

/**
 * Logpack header.
 */
class WalbLogpackHeader
{
private:
    using Block = std::shared_ptr<u8>;

    Block block_;
    const unsigned int pbs_;
    const u32 salt_;

public:
    WalbLogpackHeader(Block block, unsigned int pbs, u32 salt)
        : block_(block)
        , pbs_(pbs)
        , salt_(salt) {
        ASSERT_PBS(pbs);
    }

    Block getBlock() const {
        return block_;
    }

    const u8* getRawBuffer() const {
        return block_.get();
    }

    struct walb_logpack_header& header() {
        checkBlock();
        return *(struct walb_logpack_header *)block_.get();
    }

    const struct walb_logpack_header& header() const {
        checkBlock();
        return *(struct walb_logpack_header *)block_.get();
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

    struct walb_log_record& recordUnsafe(size_t pos) {
        return header().record[pos];
    }

    const struct walb_log_record& recordUnsafe(size_t pos) const {
        return header().record[pos];
    }

    struct walb_log_record& record(size_t pos) {
        checkIndexRange(pos);
        return recordUnsafe(pos);
    }

    const struct walb_log_record& record(size_t pos) const {
        checkIndexRange(pos);
        return recordUnsafe(pos);
    }

    bool isValid(bool isChecksum = true) const {
        if (isChecksum) {
            return ::is_valid_logpack_header_and_records_with_checksum(
                &header(), pbs(), salt()) != 0;
        } else {
            return ::is_valid_logpack_header_and_records(&header()) != 0;
        }
    }

    void printRecord(FILE *fp, size_t pos) const {
        const struct walb_log_record &rec = record(pos);
        ::fprintf(fp,
                  "record %zu\n"
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

    void printHeader(FILE *fp) const {
        const struct walb_logpack_header &logh = header();
        ::fprintf(fp,
                  "*****logpack header*****\n"
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

    void print(FILE *fp) const {
        printHeader(fp);
        for (size_t i = 0; i < nRecords(); i++) {
            printRecord(fp, i);
        }
    }

    void printRecord(size_t pos) const { printRecord(::stdout, pos); }
    void printHeader() const { printHeader(::stdout); }
    void print() const { print(::stdout); }

    /**
     * Print each IO oneline.
     * logpack_lsid, mode(W, D, or P), offset[lb], io_size[lb].
     */
    void printShort(FILE *fp) const {
        for (size_t i = 0; i < nRecords(); i++) {
            const struct walb_log_record &rec = record(i);
            assert(::test_bit_u32(LOG_RECORD_EXIST, &rec.flags));
            char mode = 'W';
            if (::test_bit_u32(LOG_RECORD_DISCARD, &rec.flags)) {
                mode = 'D';
            }
            if (::test_bit_u32(LOG_RECORD_PADDING, &rec.flags)) {
                mode = 'P';
            }
            ::fprintf(fp,
                      "%" PRIu64 "\t%c\t%" PRIu64 "\t%u\n",
                      header().logpack_lsid,
                      mode, rec.offset, rec.io_size);
        }
    }

    void printShort() const { printShort(::stdout); }

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
        header().checksum = ::checksum(block_.get(), pbs(), salt());

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

    /* Update checksum field. */
    void updateChecksum() {
        header().checksum = 0;
        header().checksum = ::checksum(
            reinterpret_cast<const u8*>(&header()), pbs(), salt());
    }

    /**
     * Write the logpack header block.
     */
    void write(int fd) {
        updateChecksum();
        if (!isValid(true)) {
            throw RT_ERR("logpack header invalid.");
        }
        util::FdWriter fdw(fd);
        fdw.write(reinterpret_cast<const char*>(block_.get()), pbs());
    }

    /**
     * Initialize logpack header block.
     */
    void init(u64 lsid) {
        ::memset(&header(), 0, pbs());
        header().logpack_lsid = lsid;
        header().sector_type = SECTOR_TYPE_LOGPACK;
        /*
          header().total_io_size = 0;
          header().n_records = 0;
          header().n_padding = 0;
          header().checksum = 0;
        */
    }

    /**
     * Add a normal IO.
     *
     * @offset [logical block]
     * @size [logical block]
     * RETURN:
     *   true in success, or false (you must create a new header).
     */
    bool addNormalIo(u64 offset, u16 size) {
        if (header().n_records >= ::max_n_log_record_in_sector(pbs())) {
            return false;
        }
        if (header().total_io_size + capacity_pb(pbs(), size) > 65535) {
            return false;
        }
        if (size == 0) {
            throw RT_ERR("Normal IO can not be zero-sized.");
        }
        size_t pos = header().n_records;
        struct walb_log_record &rec = recordUnsafe(pos);
        rec.flags = 0;
        ::set_bit_u32(LOG_RECORD_EXIST, &rec.flags);
        rec.offset = offset;
        rec.io_size = size;
        rec.lsid_local = header().total_io_size + 1;
        rec.lsid = header().logpack_lsid + rec.lsid_local;
        rec.checksum = 0; /* You must set this lator. */

        header().n_records++;
        header().total_io_size += capacity_pb(pbs(), size);
        assert(::is_valid_logpack_header_and_records(&header()));
        return true;
    }

    /**
     * Add a discard IO.
     *
     * @offset [logical block]
     * @size [logical block]
     * RETURN:
     *   true in success, or false (you must create a new header).
     */
    bool addDiscardIo(u64 offset, u16 size) {
        if (header().n_records >= ::max_n_log_record_in_sector(pbs())) {
            return false;
        }
        if (size == 0) {
            throw RT_ERR("Discard IO can not be zero-sized.");
        }
        size_t pos = header().n_records;
        struct walb_log_record &rec = recordUnsafe(pos);
        rec.flags = 0;
        ::set_bit_u32(LOG_RECORD_EXIST, &rec.flags);
        ::set_bit_u32(LOG_RECORD_DISCARD, &rec.flags);
        rec.offset = offset;
        rec.io_size = size;
        rec.lsid_local = header().total_io_size + 1;
        rec.lsid = header().logpack_lsid + rec.lsid_local;
        rec.checksum = 0; /* Not be used. */

        header().n_records++;
        /* You must not update total_io_size. */
        assert(::is_valid_logpack_header_and_records(&header()));
        return true;
    }

    /**
     * Add a padding.
     *
     * @size [logical block]
     * RETURN:
     *   true in success, or false (you must create a new header).
     */
    bool addPadding(u16 size) {
        if (header().n_records >= ::max_n_log_record_in_sector(pbs())) {
            return false;
        }
        if (header().total_io_size + capacity_pb(pbs(), size) > 65535) {
            return false;
        }
        if (header().n_padding > 0) {
            return false;
        }
        if (size % n_lb_in_pb(pbs()) != 0) {
            throw RT_ERR("Padding size must be pbs-aligned.");
        }

        size_t pos = header().n_records;
        struct walb_log_record &rec = recordUnsafe(pos);
        rec.flags = 0;
        ::set_bit_u32(LOG_RECORD_EXIST, &rec.flags);
        ::set_bit_u32(LOG_RECORD_PADDING, &rec.flags);
        rec.offset = 0; /* not be used. */
        rec.io_size = size;
        rec.lsid_local = header().total_io_size + 1;
        rec.lsid = header().logpack_lsid + rec.lsid_local;
        rec.checksum = 0;  /* not be used. */

        header().n_records++;
        header().total_io_size += capacity_pb(pbs(), size);
        header().n_padding++;
        assert(::is_valid_logpack_header_and_records(&header()));
        return true;
    }

    /**
     * @newLsid new logpack lsid.
     *   If -1, nothing will be changed.
     */
    void updateLsid(u64 newLsid) {
        assert(isValid(false));
        if (newLsid == u64(-1)) {
            return;
        }
        if (header().logpack_lsid == newLsid) {
            return;
        }

        header().logpack_lsid = newLsid;
        for (size_t i = 0; i < header().n_records; i++) {
            struct walb_log_record &rec = record(i);
            rec.lsid = newLsid + rec.lsid_local;
        }
        assert(isValid(false));
    }

private:
    void checkBlock() const {
        if (block_.get() == nullptr) {
            throw RT_ERR("Header is null.");
        }
    }

    void checkIndexRange(size_t pos) const {
        if (pos >= nRecords()) {
            throw RT_ERR("index out of range.");
        }
    }
};

/**
 * Log data of an IO.
 */
class WalbLogpackData
{
private:
    using Block = std::shared_ptr<u8>;

    WalbLogpackHeader& logh_;
    size_t pos_;
    std::vector<Block> data_;

public:
    WalbLogpackData(WalbLogpackHeader& logh, size_t pos)
        : logh_(logh)
        , pos_(pos)
        , data_() {
        assert(pos < logh.nRecords());
        data_.reserve(logh.nRecords());
    }

    void addBlock(Block block) {
        data_.push_back(block);
    }

    Block getBlock(size_t idx) const {
        assert(hasData());
        assert(idx < ioSizePb());
        return data_[idx];
    }

    /**
     * Causion!!!
     * You can access the range of
     * [ret, ret + LOGICAL_BLOCK_SIZE).
     */
    u8* getLogicalBlock(size_t idx) const {
        assert(hasData());
        assert(idx < ioSizeLb());

        Block b = getBlock(::addr_pb(pbs(), idx));
        return b.get() + off_in_pb(pbs(), idx) * LOGICAL_BLOCK_SIZE;
    }

    struct walb_log_record& record() {
        return logh_.record(pos_);
    }

    const struct walb_log_record& record() const {
        return logh_.record(pos_);
    }

    u64 lsid() const { return record().lsid; }
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

    void setPadding() {
        set_bit_u32(LOG_RECORD_PADDING, &record().flags);
    }
    void setExist() {
        set_bit_u32(LOG_RECORD_EXIST, &record().flags);
    }
    void setDiscard() {
        set_bit_u32(LOG_RECORD_DISCARD, &record().flags);
    }
    void clearPadding() {
        clear_bit_u32(LOG_RECORD_PADDING, &record().flags);
    }
    void clearExist() {
        clear_bit_u32(LOG_RECORD_EXIST, &record().flags);
    }
    void clearDiscard() {
        clear_bit_u32(LOG_RECORD_DISCARD, &record().flags);
    }

    unsigned int ioSizeLb() const { return record().io_size; }
    unsigned int ioSizePb() const { return capacity_pb(pbs(), ioSizeLb()); }
    u64 offset() const { return record().offset; }

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

    bool setChecksum() {
        if (!hasDataForChecksum()) {
            return false;
        }
        if (ioSizePb() != data_.size()) {
            return false;
        }
        record().checksum = calcIoChecksum();
        return true;
    }

private:
    u32 calcIoChecksum() const {
        unsigned int pbs = logh_.pbs();
        assert(hasDataForChecksum());
        assert(ioSizeLb() > 0);
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
        FdReader fdr(fd);
        read(fdr);
    }

    void read(FdReader& fdr) {
        fdr.read(reinterpret_cast<char *>(&data_[0]), WALBLOG_HEADER_SIZE);
    }

    void write(int fd) {
        FdWriter fdw(fd);
        write(fdw);
    }

    void write(util::FdWriter& fdw) {
        header().checksum = 0;
        header().checksum = ::checksum(&data_[0], WALBLOG_HEADER_SIZE, 0);
        fdw.write(reinterpret_cast<char *>(&data_[0]), WALBLOG_HEADER_SIZE);
    }

    struct walblog_header& header() {
        return *reinterpret_cast<struct walblog_header *>(&data_[0]);
    }

    const struct walblog_header& header() const {
        return *reinterpret_cast<const struct walblog_header *>(&data_[0]);
    }

    u32 checksum() const { return header().checksum; }
    u32 salt() const { return header().log_checksum_salt; }
    unsigned int lbs() const { return header().logical_bs; }
    unsigned int pbs() const { return header().physical_bs; }
    u64 beginLsid() const { return header().begin_lsid; }
    u64 endLsid() const { return header().end_lsid; }
    const u8* uuid() const { return &header().uuid[0]; }
    u16 sectorType() const { return header().sector_type; }
    u16 headerSize() const { return header().header_size; }
    u16 version() const { return header().version; }

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

    void print(FILE *fp) const {
        ::fprintf(fp,
            "sector_type %d\n"
            "version %u\n"
            "header_size %u\n"
            "log_checksum_salt %" PRIu32 " (%08x)\n"
            "logical_bs %u\n"
            "physical_bs %u\n"
            "uuid %02x%02x%02x%02x"
            "%02x%02x%02x%02x"
            "%02x%02x%02x%02x"
            "%02x%02x%02x%02x\n"
            "begin_lsid %" PRIu64 "\n"
            "end_lsid %" PRIu64 "\n",
            header().sector_type,
            header().version,
            header().header_size,
            header().log_checksum_salt,
            header().log_checksum_salt,
            header().logical_bs,
            header().physical_bs,
            header().uuid[0], header().uuid[1],
            header().uuid[2], header().uuid[3],
            header().uuid[4], header().uuid[5],
            header().uuid[6], header().uuid[7],
            header().uuid[8], header().uuid[9],
            header().uuid[10], header().uuid[11],
            header().uuid[12], header().uuid[13],
            header().uuid[14], header().uuid[15],
            header().begin_lsid,
            header().end_lsid);
    }

    void print() {
        print(::stdout);
    }
};

} //namespace util
} //namespace walb

#endif /* WALB_UTIL_HPP */
