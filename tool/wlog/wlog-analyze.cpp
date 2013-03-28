/**
 * @file
 * @brief Read walb log and analyze it.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <string>
#include <cstdio>
#include <stdexcept>
#include <queue>
#include <memory>
#include <deque>

#include <unistd.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <getopt.h>

#include "util.hpp"
#include "fileio.hpp"
#include "memory_buffer.hpp"
#include "walb_log.hpp"
#include "aio_util.hpp"

#include "walb/walb.h"

/**
 * Command line configuration.
 */
class Config
{
private:
    bool isFromStdin_;
    unsigned int blockSize_;
    bool isVerbose_;
    bool isHelp_;
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : isFromStdin_(false)
        , blockSize_(LOGICAL_BLOCK_SIZE)
        , isVerbose_(false)
        , isHelp_(false)
        , args_() {
        parse(argc, argv);
    }

    size_t numWlogs() const { return isFromStdin() ? 1 : args_.size(); }
    const std::string& inWlogPath(size_t idx) const { return args_[idx]; }
    bool isFromStdin() const { return isFromStdin_; }
    unsigned int blockSize() const { return blockSize_; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }

    void print() const {
        ::printf("numWlogs: %zu\n"
                 "isFromStdin: %d\n"
                 "blockSize: %u\n"
                 "verbose: %d\n"
                 "isHelp: %d\n",
                 numWlogs(), isFromStdin(), blockSize(),
                 isVerbose(), isHelp());
        int i = 0;
        for (const auto& s : args_) {
            ::printf("arg%d: %s\n", i++, s.c_str());
        }
    }

    static void printHelp() {
        ::printf("%s", generateHelpString().c_str());
    }

    void check() const {
        if (numWlogs() == 0) {
            throwError("Specify input wlog path.");
        }
        if (blockSize() % LOGICAL_BLOCK_SIZE != 0) {
            throwError("Block size must be a multiple of %u.",
                       LOGICAL_BLOCK_SIZE);
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
        BLK_SIZE = 1,
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
                {"blockSize", 1, 0, Opt::BLK_SIZE},
                {"verbose", 0, 0, Opt::VERBOSE},
                {"help", 0, 0, Opt::HELP},
                {0, 0, 0, 0}
            };
            int option_index = 0;
            int c = ::getopt_long(argc, argv, "b:vh", long_options, &option_index);
            if (c == -1) { break; }

            switch (c) {
            case Opt::BLK_SIZE:
            case 'b':
                blockSize_ = cybozu::util::fromUnitIntString(optarg);
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
        if (args_.empty() || args_[0] == "-") {
            isFromStdin_ = true;
        }
    }

    static std::string generateHelpString() {
        return cybozu::util::formatString(
            "Wlanalyze: analyze wlog.\n"
            "Usage: wlanalyze [options] WLOG_PATH [WLOG_PATH...]\n"
            "  WLOG_PATH: walb log path. '-' for stdin. (default: '-')\n"
            "             wlog files must be linkable each other in line.\n"
            "Options:\n"
            "  -b, --blockSize SIZE: block size in bytes. (default: %u)\n"
            "  -v, --verbose:        verbose messages to stderr.\n"
            "  -h, --help:           show this message.\n",
            LOGICAL_BLOCK_SIZE);
    }
};

class WalbLogAnalyzer
{
private:
    const Config &config_;

    /* If bits_[physical block offset] is set,
       the block is written once or more. */
    std::vector<bool> bits_;

    /* Number of written logical blocks. */
    uint64_t writtenLb_;

    using LogpackHeader = walb::log::WalbLogpackHeader;
    using LogpackHeaderPtr = std::shared_ptr<LogpackHeader>;
    using LogpackData = walb::log::WalbLogpackData;
    using Block = std::shared_ptr<u8>;

public:
    WalbLogAnalyzer(const Config &config)
        : config_(config), bits_(), writtenLb_(0) {}

    void analyze() {
        uint64_t lsid = -1;
        u8 uuid[UUID_SIZE];
        if (config_.isFromStdin()) {
            while (true) {
                uint64_t nextLsid = analyzeWlog(0, lsid, uuid);
                if (nextLsid == lsid) { break; }
                lsid = nextLsid;
            }
        } else {
            for (size_t i = 0; i < config_.numWlogs(); i++) {
                cybozu::util::FileOpener fo(config_.inWlogPath(i), O_RDONLY);
                lsid = analyzeWlog(fo.fd(), lsid, uuid);
                fo.close();
            }
        }
        printResult();
    }

private:
    /**
     * Try to read wlog data.
     *
     * @inFd fd for wlog input stream.
     * @beginLsid begin lsid to check continuity of wlog(s).
     *   specify uint64_t(-1) not to check that.
     * @uuid uuid for equality check.
     *   If beginLsid is uint64_t(-1), the uuid will be set.
     *   Else the uuid will be used to check equality of wlog source device.
     *
     * RETURN:
     *   end lsid of the wlog data.
     */
    uint64_t analyzeWlog(
        int inFd, uint64_t beginLsid, u8 *uuid) {

        if (inFd < 0) {
            throw RT_ERR("inFd is not valid");
        }
        cybozu::util::FdReader fdr(inFd);

        walb::log::WalbLogFileHeader wh;
        try {
            wh.read(fdr);
        } catch (cybozu::util::EofError &e) {
            return beginLsid;
        }
        if (!wh.isValid(true)) {
            throw RT_ERR("invalid wlog header.");
        }
        if (config_.isVerbose()) {
            wh.print(::stderr);
        }

        if (beginLsid == uint64_t(-1)) {
            /* First call. */
            ::memcpy(uuid, wh.uuid(), UUID_SIZE);
        } else {
            if (::memcmp(uuid, wh.uuid(), UUID_SIZE) != 0) {
                throw RT_ERR("Not the same wlog uuid.");
            }
        }

        const unsigned int pbs = wh.pbs();
        const unsigned int bufferSize = 16 * 1024 * 1024;
        cybozu::util::BlockAllocator<u8> ba(bufferSize / pbs, pbs, pbs);

        uint64_t lsid = wh.beginLsid();
        if (beginLsid != uint64_t(-1) && lsid != beginLsid) {
            throw RT_ERR("wrong lsid.");
        }
        try {
            while (lsid < wh.endLsid()) {
                LogpackHeaderPtr loghp = readLogpackHeader(fdr, ba, wh.salt());
                LogpackHeader &logh = *loghp;
                if (lsid != logh.logpackLsid()) {
                    throw RT_ERR("wrong lsid.");
                }
                readLogpackData(logh, fdr, ba);
                updateBitmap(logh);
                lsid = logh.nextLogpackLsid();
            }
        } catch (cybozu::util::EofError &e) {
        }
        if (lsid != wh.endLsid()) {
            throw RT_ERR("the wlog lacks logs from %" PRIu64 ". endLsid is %" PRIu64 "",
                         lsid, wh.endLsid());
        }
        return lsid;
    }

    Block readBlock(
        cybozu::util::FdReader &fdr, cybozu::util::BlockAllocator<u8> &ba) {
        Block b = ba.alloc();
        unsigned int bs = ba.blockSize();
        fdr.read(reinterpret_cast<char *>(b.get()), bs);
        return b;
    }

    LogpackHeaderPtr readLogpackHeader(
        cybozu::util::FdReader &fdr, cybozu::util::BlockAllocator<u8> &ba,
        uint32_t salt) {
        Block b = readBlock(fdr, ba);
        return LogpackHeaderPtr(new LogpackHeader(b, ba.blockSize(), salt));
    }

    /**
     * Read, validate, and throw away logpack data.
     */
    void readLogpackData(
        LogpackHeader &logh, cybozu::util::FdReader &fdr,
        cybozu::util::BlockAllocator<u8> &ba) {
        for (size_t i = 0; i < logh.nRecords(); i++) {
            walb::log::WalbLogpackData logd(logh, i);
            if (!logd.hasData()) { continue; }
            for (size_t j = 0; j < logd.ioSizePb(); j++) {
                logd.addBlock(readBlock(fdr, ba));
            }
            if (!logd.isValid()) {
                throw walb::log::InvalidLogpackData();
            }
        }
    }

    /**
     * Update bitmap with a logpack header.
     */
    void updateBitmap(const LogpackHeader &logh) {
        const unsigned int bs = config_.blockSize();
        for (size_t i = 0; i < logh.nRecords(); i++) {
            const struct walb_log_record &rec = logh.record(i);
            if (::test_bit_u32(LOG_RECORD_PADDING, &rec.flags)) {
                continue;
            }
            uint64_t offLb = rec.offset;
            unsigned int sizeLb = rec.io_size;
            uint64_t offPb0 = ::addr_pb(bs, offLb);
            uint64_t offPb1 = ::capacity_pb(bs, offLb + sizeLb);
            setRange(offPb0, offPb1);

            writtenLb_ += sizeLb;
        }
    }

    void resize(size_t size) {
        const size_t s = bits_.size();
        if (size <= s) { return; }
        bits_.resize(size);
        for (size_t i = s; i < size; i++) {
            bits_[i] = false;
        }
    }

    void setRange(size_t off0, size_t off1) {
        resize(off1);
        assert(off0 <= off1);
        for (size_t i = off0; i < off1; i++) {
            bits_[i] = true;
        }
    }

    uint64_t rank(size_t offset) const {
        uint64_t c = 0;
        for (size_t i = 0; i < offset && i < bits_.size(); i++) {
            c += (bits_[i] ? 1 : 0);
        }
        return c;
    }

    uint64_t count() const {
        return rank(bits_.size());
    }

    void printResult() const {
        unsigned int bs = config_.blockSize();

        const uint64_t written = ::capacity_pb(bs, writtenLb_);
        const uint64_t changed = count();
        double rate = 0;
        if (written > 0) {
            rate = static_cast<double>(written - changed)
                / static_cast<double>(written);
        }

        ::printf("block size: %u\n"
                 "number of written blocks: %" PRIu64 "\n"
                 "number of changed blocks: %" PRIu64 "\n"
                 "overwritten rate: %.2f\n",
                 bs, written, changed, rate);
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

        WalbLogAnalyzer wlAnalyzer(config);
        wlAnalyzer.analyze();

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
