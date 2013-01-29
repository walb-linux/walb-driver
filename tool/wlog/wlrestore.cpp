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
#include "walb_util.hpp"

#include "walb/walb.h"

/**
 * Wlredo command configuration.
 */
class Config
{
private:
    std::string ldevPath_; /* Log device to restore wlog. */
    uint64_t beginLsid_; /* start lsid to restore. */
    uint64_t endLsid_; /* end lsid to restore. The range is [start, end). */
    uint64_t newLsid_; /* -1 means default lsid. */
    uint64_t ddevLb_; /* 0 means no clipping. */
    bool isVerbose_;
    bool isHelp_;
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : ldevPath_()
        , beginLsid_(0)
        , endLsid_(-1)
        , newLsid_(-1)
        , ddevLb_(0)
        , isVerbose_(false)
        , isHelp_(false)
        , args_() {
        parse(argc, argv);
    }

    const std::string& ldevPath() const { return ldevPath_; }
    uint64_t beginLsid() const { return beginLsid_; }
    uint64_t endLsid() const { return endLsid_; }
    uint64_t newLsid() const { return newLsid_; }
    uint64_t ddevLb() const { return ddevLb_; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }

    void print() const {
        ::printf("ldevPath: %s\n"
                 "beginLsid: %" PRIu64 "\n"
                 "endLsid: %" PRIu64 "\n"
                 "newLsid: %" PRIu64 "\n"
                 "ddevLb: %" PRIu64 "\n"
                 "verbose: %d\n"
                 "isHelp: %d\n",
                 ldevPath().c_str(),
                 beginLsid(), endLsid(), newLsid(), ddevLb(),
                 isVerbose(), isHelp());
        int i = 0;
        for (const auto& s : args_) {
            ::printf("arg%d: %s\n", i++, s.c_str());
        }
    }

    static void printHelp() {
        ::printf("%s", generateHelpString().c_str());
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
        NEW_LSID,
        DDEV_SIZE,
        VERBOSE,
        HELP,
    };

    void throwError(const char *format, ...) const {
        va_list args;
        std::string msg;
        va_start(args, format);
        try {
            msg = walb::util::formatStringV(format, args);
        } catch (...) {}
        va_end(args);
        throw Error(msg);
    }

    void parse(int argc, char* argv[]) {
        while (1) {
            const struct option long_options[] = {
                {"beginLsid", 1, 0, Opt::BEGIN_LSID},
                {"endLsid", 1, 0, Opt::END_LSID},
                {"newLsid", 1, 0, Opt::NEW_LSID},
                {"ddevSize", 1, 0, Opt::DDEV_SIZE},
                {"verbose", 0, 0, Opt::VERBOSE},
                {"help", 0, 0, Opt::HELP},
                {0, 0, 0, 0}
            };
            int option_index = 0;
            int c = ::getopt_long(argc, argv, "b:e:n:s:vh", long_options, &option_index);
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
            case Opt::NEW_LSID:
            case 'n':
                newLsid_ = ::atoll(optarg);
                break;
            case Opt::DDEV_SIZE:
            case 's':
                ddevLb_ = walb::util::fromUnitIntString(optarg);
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
        check();
    }

    static std::string generateHelpString() {
        return walb::util::formatString(
            "Wlresotre: restore walb log to a log device.\n"
            "Usage: wlrestore [options] LOG_DEVICE_PATH\n"
            "Options:\n"
            "  -b, --beginLsid LSID:  begin lsid to restore. (default: 0)\n"
            "  -e, --endLsid LSID:    end lsid to restore. (default: -1)\n"
            "  -n, --newLsid LSID:    new lsid of the first log. (default: no change)\n"
            "  -s, --ddevSize SIZE:   data device size for clipping. (default: no clipping)\n"
            "  -v, --verbose:         verbose messages to stderr.\n"
            "  -h, --help:            show this message.\n");
    }

    void check() const {
        if (beginLsid() >= endLsid()) {
            throwError("beginLsid must be < endLsid.");
        }
        if (ldevPath_.empty()) {
            throwError("Specify log device path.");
        }
    }
};

/**
 * WalbLog Generator for test.
 */
class WalbLogRestorer
{
private:
    const Config& config_;
    uint64_t lsidDiff_;

    using Block = std::shared_ptr<u8>;
    using BlockA = walb::util::BlockAllocator<u8>;

public:
    WalbLogRestorer(const Config& config)
        : config_(config)
        , lsidDiff_(0)
        {}

    /**
     * Restore the log to the device.
     */
    void restore(int fdIn) {
        /* Read walb log file header from stdin. */
        walb::util::FdReader fdr(fdIn);
        walb::util::WalbLogFileHeader wlHead;
        wlHead.read(fdr);
        if (!wlHead.isValid()) {
            throw RT_ERR("Walb log file header is invalid.");
        }
        const unsigned int pbs = wlHead.pbs();

        /* Set lsidDiff_ if necessary. */
        if (config_.newLsid() != uint64_t(-1)) {
            lsidDiff_ = config_.newLsid() - wlHead.beginLsid();
        }

        /* Open the log device. */
        walb::util::BlockDevice blkdev(config_.ldevPath(), O_RDWR);
        if (!blkdev.isBlockDevice()) {
            ::fprintf(::stderr, "Warning: the log device does not seem to be block device.\n");
        }

        /* Load superblock. */
        walb::util::WalbSuperBlock super(blkdev);

        /* Check physical block size. */
        if (super.getPhysicalBlockSize() != pbs) {
            throw RT_ERR("Physical block size differs.\n");
        }

        /* Allocate buffer for logpacks. */
        const unsigned int BUFFER_SIZE = 16 * 1024 * 1024; /* 16MB */
        BlockA ba(BUFFER_SIZE / pbs, pbs, pbs);

        /* Read and write each logpack. */
        try {
            while (readLogpackAndRestore(fdr, blkdev, super, ba, wlHead)) {}

        } catch (walb::util::EofError &e) {
            ::printf("Reached input EOF.\n");
        } catch (walb::util::InvalidLogpackData &e) {
            throw RT_ERR("InvalidLogpackData");
        }

        /* Create and write superblock finally. */
        uint64_t lsid = uint64_t(-1);
        if (config_.newLsid() == uint64_t(-1)) {
            lsid = wlHead.beginLsid();
        } else {
            lsid = config_.newLsid();
        }
        super.setOldestLsid(lsid);
        super.setWrittenLsid(lsid);
        super.setUuid(wlHead.uuid());
        super.setLogChecksumSalt(wlHead.salt());
        super.write();

        /* Finalize the log device. */
        blkdev.fdatasync();
        blkdev.close();
    }
private:
    /**
     * Read a block data from a fd reader.
     */
    Block readBlock(walb::util::FdReader& fdr, BlockA& ba, unsigned int pbs) {
        Block b = ba.alloc();
        if (b.get() == nullptr) {
            throw RT_ERR("allocate failed.");
        }
        char *p = reinterpret_cast<char *>(b.get());
        fdr.read(p, pbs);
        return b;
    }

    /**
     * Read a logpack data.
     */
    void readLogpackData(
        walb::util::WalbLogpackData &logd, walb::util::FdReader &fdr,
        walb::util::BlockAllocator<u8> &ba) {
        if (!logd.hasData()) { return; }
        //::printf("ioSizePb: %u\n", logd.ioSizePb()); //debug
        for (size_t i = 0; i < logd.ioSizePb(); i++) {
            logd.addBlock(readBlock(fdr, ba, logd.pbs()));
        }
        if (!logd.isValid()) {
            throw walb::util::InvalidLogpackData();
        }
    }

    /**
     * RETURN:
     *   true in normal termination, or false.
     */
    bool readLogpackAndRestore(
        walb::util::FdReader &fdr,
        walb::util::BlockDevice &blkdev,
        walb::util::WalbSuperBlock &super,
        walb::util::BlockAllocator<u8> &ba,
        walb::util::WalbLogFileHeader &wlHead) {

        u32 salt = wlHead.salt();
        unsigned int pbs = wlHead.pbs();

        /* Read logpack header. */
        walb::util::WalbLogpackHeader logh(readBlock(fdr, ba, pbs), pbs, salt);
        if (!logh.isValid()) {
            return false;
        }
#if 0
        logh.printShort(); /* debug */
#endif
        const u64 originalLsid = logh.logpackLsid();
        if (config_.endLsid() <= originalLsid) {
            return true;
        }
        /* Update lsid if necessary. */
        if (lsidDiff_ != 0) {
            logh.updateLsid(logh.logpackLsid() + lsidDiff_);
        }

        /* Padding check. */
        u64 offPb = super.getOffsetFromLsid(logh.logpackLsid());
        const u64 endOffPb = super.getRingBufferOffset() + super.getRingBufferSize();
        if (endOffPb < offPb + 1 + logh.totalIoSize()) {
            /* Create and write padding logpack. */
            unsigned int paddingPb = endOffPb - offPb;
            assert(0 < paddingPb);
            assert(paddingPb < (1U << 16));
            walb::util::WalbLogpackHeader paddingLogh(
                ba.alloc(), pbs, wlHead.salt());
            paddingLogh.init(logh.logpackLsid());
            paddingLogh.addPadding(paddingPb - 1);
            paddingLogh.updateChecksum();
            assert(paddingLogh.isValid());
            blkdev.write(
                offPb * pbs, pbs,
                reinterpret_cast<const char *>(paddingLogh.getRawBuffer()));

            /* Update logh's lsid information. */
            lsidDiff_ += paddingPb;
            logh.updateLsid(logh.logpackLsid() + paddingPb);
            assert(super.getOffsetFromLsid(logh.logpackLsid())
                   == super.getRingBufferOffset());
            offPb = super.getRingBufferOffset();
        }

        /* Read logpack data. */
        std::vector<Block> blocks;
        blocks.reserve(logh.totalIoSize());
        for (size_t i = 0; i < logh.nRecords(); i++) {
            walb::util::WalbLogpackData logd(logh, i);
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
        blkdev.write(
            offPb * pbs, pbs,
            reinterpret_cast<const char *>(logh.getRawBuffer()));
        for (size_t i = 0; i < blocks.size(); i++) {
            blkdev.write((offPb + 1 + i) * pbs, pbs,
                         reinterpret_cast<const char *>(blocks[i].get()));
        }
        return true;
    }
};

int main(int argc, char* argv[])
{
    int ret = 0;

    try {
        Config config(argc, argv);
        /* config.print(); */

        if (config.isHelp()) {
            Config::printHelp();
            return 1;
        }
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
