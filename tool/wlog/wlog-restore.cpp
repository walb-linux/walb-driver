/**
 * @file
 * @brief walb log restore tool for test.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <string>
#include <cstdio>
#include <stdexcept>
#include <cstdint>
#include <queue>
#include <memory>
#include <deque>
#include <algorithm>
#include <utility>
#include <set>
#include <limits>

#include <unistd.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <getopt.h>

#include "util.hpp"
#include "memory_buffer.hpp"
#include "walb_log.hpp"

#include "walb/walb.h"

/**
 * Command line configuration.
 */
class Config
{
private:
    std::string ldevPath_; /* Log device to restore wlog. */
    uint64_t beginLsid_; /* start lsid to restore. */
    uint64_t endLsid_; /* end lsid to restore. The range is [start, end). */
    int64_t lsidDiff_; /* 0 means no change. */
    uint64_t invalidLsid_; /* -1 means no invalidation. */
    uint64_t ddevLb_; /* 0 means no clipping. */
    bool isVerify_;
    bool isVerbose_;
    bool isHelp_;
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : ldevPath_()
        , beginLsid_(0)
        , endLsid_(-1)
        , lsidDiff_(0)
        , invalidLsid_(-1)
        , ddevLb_(0)
        , isVerify_(false)
        , isVerbose_(false)
        , isHelp_(false)
        , args_() {
        parse(argc, argv);
    }

    const std::string& ldevPath() const { return ldevPath_; }
    uint64_t beginLsid() const { return beginLsid_; }
    uint64_t endLsid() const { return endLsid_; }
    int64_t lsidDiff() const { return lsidDiff_; }
    uint64_t invalidLsid() const { return invalidLsid_; }
    uint64_t ddevLb() const { return ddevLb_; }
    bool isVerify() const { return isVerify_; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }

    void print() const {
        ::printf("ldevPath: %s\n"
                 "beginLsid: %" PRIu64 "\n"
                 "endLsid: %" PRIu64 "\n"
                 "lsidDiff: %" PRIi64 "\n"
                 "invalidLsid: %" PRIu64 "\n"
                 "ddevLb: %" PRIu64 "\n"
                 "verify: %d\n"
                 "verbose: %d\n"
                 "isHelp: %d\n",
                 ldevPath().c_str(),
                 beginLsid(), endLsid(), lsidDiff(),
                 invalidLsid(), ddevLb(),
                 isVerify(), isVerbose(), isHelp());
        int i = 0;
        for (const auto& s : args_) {
            ::printf("arg%d: %s\n", i++, s.c_str());
        }
    }

    static void printHelp() {
        ::printf("%s", generateHelpString().c_str());
    }

    void check() const {
        if (beginLsid() >= endLsid()) {
            throwError("beginLsid must be < endLsid.");
        }
        if (ldevPath_.empty()) {
            throwError("Specify log device path.");
        }
    }

    class Error : public std::runtime_error {
    public:
        explicit Error(const std::string &msg)
            : std::runtime_error(msg) {}
    };

private:
    /* Option ids. */
    enum Opt {
        BEGIN_LSID = 1,
        END_LSID,
        LSID_DIFF,
        LSID_INVALID,
        DDEV_SIZE,
        VERIFY,
        VERBOSE,
        HELP,
    };

    void throwError(const char *format, ...) const {
        va_list args;
        std::string msg;
        va_start(args, format);
        try {
            msg = cybozu::util::formatStringV(format, args);
        } catch (...) {}
        va_end(args);
        throw Error(msg);
    }

    void parse(int argc, char* argv[]) {
        while (1) {
            const struct option long_options[] = {
                {"beginLsid", 1, 0, Opt::BEGIN_LSID},
                {"endLsid", 1, 0, Opt::END_LSID},
                {"lsidDiff", 1, 0, Opt::LSID_DIFF},
                {"invalidLsid", 1, 0, Opt::LSID_INVALID},
                {"ddevSize", 1, 0, Opt::DDEV_SIZE},
                {"verify", 0, 0, Opt::VERIFY},
                {"verbose", 0, 0, Opt::VERBOSE},
                {"help", 0, 0, Opt::HELP},
                {0, 0, 0, 0}
            };
            int option_index = 0;
            int c = ::getopt_long(argc, argv, "b:e:d:i:s:vh", long_options, &option_index);
            if (c == -1) { break; }

            switch (c) {
            case Opt::BEGIN_LSID:
            case 'b':
                beginLsid_ = ::atoll(optarg);
                break;
            case Opt::END_LSID:
            case 'e':
                endLsid_ = ::atoll(optarg);
                break;
            case Opt::LSID_DIFF:
            case 'd':
                lsidDiff_ = ::atoll(optarg);
                break;
            case Opt::LSID_INVALID:
            case 'i':
                invalidLsid_ = ::atoll(optarg);
                break;
            case Opt::DDEV_SIZE:
            case 's':
                ddevLb_ = cybozu::util::fromUnitIntString(optarg);
                break;
            case Opt::VERIFY:
                isVerify_ = true;
                break;
            case Opt::VERBOSE:
            case 'v':
                isVerbose_ = true;
                break;
            case Opt::HELP:
            case 'h':
                isHelp_ = true;
                break;
            default:
                throwError("Unknown option.");
            }
        }

        while(optind < argc) {
            args_.push_back(std::string(argv[optind++]));
        }
        if (!args_.empty()) {
            ldevPath_ = args_[0];
        }
    }

    static std::string generateHelpString() {
        return cybozu::util::formatString(
            "Wlresotre: restore walb log to a log device.\n"
            "Usage: wlrestore [options] LOG_DEVICE_PATH < WLOG_FILE\n"
            "Options:\n"
            "  -b, --beginLsid LSID:  begin lsid to restore. (default: 0)\n"
            "  -e, --endLsid LSID:    end lsid to restore. (default: -1)\n"
            "  -d, --lsidDiff DIFF:   lsid diff. (default: 0)\n"
            "  -i, --invalidLsid LSID:invalidate lsid after restore. (default: no invalidation)\n"
            "  -s, --ddevSize SIZE:   data device size for clipping. (default: no clipping)\n"
            "      --verify:          verify written logpack (default: no)\n"
            "  -v, --verbose:         verbose messages to stderr.\n"
            "  -h, --help:            show this message.\n");
    }
};

/**
 * WalbLog Generator for test.
 */
class WalbLogRestorer
{
private:
    const Config& config_;
    int64_t lsidDiff_;

    using Block = std::shared_ptr<u8>;
    using BlockA = cybozu::util::BlockAllocator<u8>;
    using BlockDev = cybozu::util::BlockDevice;
    using WlogHeader = walb::log::WalbLogFileHeader;
    using PackHeader = walb::log::WalbLogpackHeader;
    using PackData = walb::log::WalbLogpackData;
    using FdReader = cybozu::util::FdReader;
    using SuperBlock = walb::log::WalbSuperBlock;

public:
    WalbLogRestorer(const Config& config)
        : config_(config)
        , lsidDiff_(config.lsidDiff())
        {}

    /**
     * Restore the log to the device.
     */
    void restore(int fdIn) {
        /* Read walb log file header from stdin. */
        FdReader fdr(fdIn);
        WlogHeader wlHead;
        wlHead.read(fdr);
        if (!wlHead.isValid()) {
            throw RT_ERR("Walb log file header is invalid.");
        }
        const unsigned int pbs = wlHead.pbs();

        /* Open the log device. */
        BlockDev blkdev(config_.ldevPath(), O_RDWR);
        if (!blkdev.isBlockDevice()) {
            ::fprintf(
                ::stderr,
                "Warning: the log device does not seem to be block device.\n");
        }

        /* Load superblock. */
        SuperBlock super(blkdev);

        /* Check physical block size. */
        if (super.getPhysicalBlockSize() != pbs) {
            throw RT_ERR("Physical block size differs.\n");
        }

        /* Allocate buffer for logpacks. */
        const unsigned int BUFFER_SIZE = 16 * 1024 * 1024; /* 16MB */
        BlockA ba(BUFFER_SIZE / pbs, pbs, pbs);

        /* Set lsid range. */
        uint64_t beginLsid = wlHead.beginLsid() + lsidDiff_;
        ::printf("Try to restore lsid range [%" PRIu64 ", %" PRIu64 ")\n",
                 wlHead.beginLsid(), wlHead.endLsid());
        if (lsidDiff_ != 0) {
            ::printf("Lsid map %" PRIu64 " to %" PRIu64 " (diff %" PRIi64 ")\n",
                     wlHead.beginLsid(), beginLsid, lsidDiff_);
        }
        uint64_t restoredLsid = beginLsid;

        /* Read and write each logpack. */
        try {
            while (readLogpackAndRestore(
                       fdr, blkdev, super, ba, wlHead, restoredLsid)) {}
        } catch (cybozu::util::EofError &e) {
            ::printf("Reached input EOF.\n");
        } catch (walb::log::InvalidLogpackData &e) {
            throw RT_ERR("InvalidLogpackData");
        }

        /* Create and write superblock finally. */
        super.setOldestLsid(beginLsid);
        super.setWrittenLsid(beginLsid); /* for redo */
        super.setUuid(wlHead.uuid());
        super.setLogChecksumSalt(wlHead.salt());
        super.write();

        /* Invalidate the last log block. */
        if (beginLsid < restoredLsid) {
            invalidateLsid(blkdev, super, ba, pbs, restoredLsid);
        }
        /* Invalidate the specified block. */
        if (config_.invalidLsid() != uint64_t(-1)) {
            invalidateLsid(blkdev, super, ba, pbs, config_.invalidLsid());
        }

        /* Finalize the log device. */
        blkdev.fdatasync();
        blkdev.close();

        ::printf("Restored lsid range [%" PRIu64 ", %" PRIu64 "].\n",
                 beginLsid, restoredLsid);
    }
private:
    /**
     * Invalidate a specified lsid.
     */
    void invalidateLsid(
        BlockDev &blkdev, SuperBlock &super, BlockA &ba,
        unsigned int pbs, uint64_t lsid) {
        uint64_t off = super.getOffsetFromLsid(lsid);
        Block b = ba.alloc();
        ::memset(b.get(), 0, pbs);
        blkdev.write(off * pbs, pbs, reinterpret_cast<const char *>(b.get()));
    }

    /**
     * Read a block data from a fd reader.
     */
    Block readBlock(FdReader& fdr, BlockA& ba, unsigned int pbs) {
        Block b = ba.alloc();
        if (b.get() == nullptr) {
            throw RT_ERR("allocate failed.");
        }
        fdr.read(reinterpret_cast<char *>(b.get()), pbs);
        return b;
    }

    /**
     * Read a logpack data.
     */
    void readLogpackData(PackData &logd, FdReader &fdr, BlockA &ba) {
        if (!logd.hasData()) { return; }
        //::printf("ioSizePb: %u\n", logd.ioSizePb()); //debug
        for (size_t i = 0; i < logd.ioSizePb(); i++) {
            logd.addBlock(readBlock(fdr, ba, logd.pbs()));
        }
        if (!logd.isValid()) {
            throw walb::log::InvalidLogpackData();
        }
    }

    /**
     * Read a logpack and restore it.
     *
     * @fdr wlog input.
     * @blkdev log block device.
     * @super super block (with the blkdev).
     * @ba block allocator.
     * @wlHead wlog header.
     * @restoresLsid lsid of the next logpack will be set if restored.
     *
     * RETURN:
     *   true in normal termination, or false.
     */
    bool readLogpackAndRestore(
        FdReader &fdr, BlockDev &blkdev,
        SuperBlock &super, BlockA &ba, WlogHeader &wlHead,
        uint64_t &restoredLsid) {

        u32 salt = wlHead.salt();
        unsigned int pbs = wlHead.pbs();

        /* Read logpack header. */
        PackHeader logh(readBlock(fdr, ba, pbs), pbs, salt);
        if (!logh.isValid()) {
            return false;
        }
        if (config_.isVerbose()) {
            logh.printShort();
        }
        const u64 originalLsid = logh.logpackLsid();
        if (config_.endLsid() <= originalLsid) {
            return false;
        }
        /* Update lsid if necessary. */
        if (lsidDiff_ != 0) {
            if (!logh.updateLsid(logh.logpackLsid() + lsidDiff_)) {
                ::fprintf(::stderr, "lsid overflow ocurred.\n");
                return false;
            }
        }

        /* Padding check. */
        u64 offPb = super.getOffsetFromLsid(logh.logpackLsid());
        const u64 endOffPb = super.getRingBufferOffset() +
            super.getRingBufferSize();
        if (endOffPb < offPb + 1 + logh.totalIoSize()) {
            /* Create and write padding logpack. */
            unsigned int paddingPb = endOffPb - offPb;
            assert(0 < paddingPb);
            assert(paddingPb < (1U << 16));
            PackHeader paddingLogh(ba.alloc(), pbs, wlHead.salt());
            paddingLogh.init(logh.logpackLsid());
            paddingLogh.addPadding(paddingPb - 1);
            paddingLogh.updateChecksum();
            assert(paddingLogh.isValid());
            blkdev.write(
                offPb * pbs, pbs, paddingLogh.ptr<char>());

            /* Update logh's lsid information. */
            lsidDiff_ += paddingPb;
            if (!logh.updateLsid(logh.logpackLsid() + paddingPb)) {
                ::fprintf(::stderr, "lsid overflow ocurred.\n");
                return false;
            }
            assert(super.getOffsetFromLsid(logh.logpackLsid())
                   == super.getRingBufferOffset());
            offPb = super.getRingBufferOffset();
        }

        /* Read logpack data. */
        std::vector<Block> blocks;
        blocks.reserve(logh.totalIoSize());
        for (size_t i = 0; i < logh.nRecords(); i++) {
            PackData logd(logh, i);
            readLogpackData(logd, fdr, ba);
            if (logd.hasData()) {
                for (size_t j = 0; j < logd.ioSizePb(); j++) {
                    blocks.push_back(logd.getBlock(j));
                }
            }
            if (0 < config_.ddevLb() &&
                config_.ddevLb() < logd.offset() + logd.ioSizeLb()) {
                /* This IO should be clipped. */
                logd.clearPadding();
                logd.record().offset = 0;
            }
        }
        assert(blocks.size() == logh.totalIoSize());

        if (originalLsid < config_.beginLsid()) {
            /* Skip to restore. */
            return true;
        }

        /* Restore. */
        logh.updateChecksum();
        assert(logh.isValid());
        assert(offPb + 1 + logh.totalIoSize() <= endOffPb);

        if (config_.isVerbose()) {
            ::printf("header %u records\n", logh.nRecords());
            ::printf("offPb %" PRIu64 "\n", offPb);
        }
        blkdev.write(offPb * pbs, pbs, logh.ptr<char>());
        for (size_t i = 0; i < blocks.size(); i++) {
            blkdev.write((offPb + 1 + i) * pbs, pbs,
                         reinterpret_cast<const char *>(blocks[i].get()));
        }

        if (config_.isVerify()) {
            /* Currently only header block will be verified. */
            Block b2 = ba.alloc();
            blkdev.read(
                offPb * pbs, pbs,
                reinterpret_cast<char *>(b2.get()));
            PackHeader logh2(b2, pbs, salt);
            int ret = ::memcmp(logh.ptr<char>(), logh2.ptr<char>(), pbs);
            if (ret) {
                throw RT_ERR("Logpack header verification failed: "
                             "lsid %" PRIu64 " offPb %" PRIu64 ".",
                             logh2.logpackLsid(), offPb);
            }
            if (!logh2.isValid()) {
                throw RT_ERR("Stored logpack header Invalid: "
                             "lsid %" PRIu64 " offPb %" PRIu64 ".",
                             logh2.logpackLsid(), offPb);
            }
        }

        restoredLsid = logh.logpackLsid() + 1 + logh.totalIoSize();
        return true;
    }
};

int main(int argc, char* argv[])
{
    int ret = 0;

    try {
        Config config(argc, argv);
        if (config.isHelp()) {
            Config::printHelp();
            return 0;
        }
        config.check();

        WalbLogRestorer wlRes(config);
        wlRes.restore(0);

    } catch (Config::Error& e) {
        ::printf("Command line error: %s\n\n", e.what());
        Config::printHelp();
        ret = 1;
    } catch (std::runtime_error& e) {
        LOGe("Error: %s\n", e.what());
        ret = 1;
    } catch (std::exception& e) {
        LOGe("Exception: %s\n", e.what());
        ret = 1;
    } catch (...) {
        LOGe("Caught other error.\n");
        ret = 1;
    }

    return ret;
}

/* end of file. */
