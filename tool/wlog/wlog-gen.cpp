/**
 * @file
 * @brief walb log generator for test.
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
    unsigned int pbs_; /* physical block size [byte] */
    uint64_t devSize_; /* [byte]. */
    unsigned int minIoSize_; /* [byte]. */
    unsigned int maxIoSize_; /* [byte]. */
    unsigned int maxPackSize_; /* [byte]. */
    uint64_t outLogSize_; /* Approximately output log size [byte]. */
    uint64_t lsid_; /* start lsid [physical block]. */
    bool isPadding_;
    bool isDiscard_;
    bool isAllZero_;
    bool isVerbose_;
    bool isHelp_;
    std::string outPath_;
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : pbs_(LOGICAL_BLOCK_SIZE)
        , devSize_(16 * 1024 * 1024) /* default 16MB. */
        , minIoSize_(pbs_)
        , maxIoSize_(32 * 1024) /* default 32KB. */
        , maxPackSize_(16 * 1024 * 1024) /* default 16MB. */
        , outLogSize_(1024 * 1024) /* default 1MB. */
        , lsid_(0)
        , isPadding_(true)
        , isDiscard_(true)
        , isAllZero_(true)
        , isVerbose_(false)
        , isHelp_(false)
        , outPath_()
        , args_() {
        parse(argc, argv);
    }

    uint64_t devLb() const { return devSize_ / LOGICAL_BLOCK_SIZE; }
    unsigned int minIoLb() const { return minIoSize_ / LOGICAL_BLOCK_SIZE; }
    unsigned int maxIoLb() const { return maxIoSize_ / LOGICAL_BLOCK_SIZE; }
    unsigned int pbs() const { return pbs_; }
    unsigned int maxPackPb() const { return maxPackSize_ / pbs(); }
    uint64_t outLogPb() const { return outLogSize_ / pbs(); }
    uint64_t lsid() const { return lsid_; }
    bool isPadding() const { return isPadding_; }
    bool isDiscard() const { return isDiscard_; }
    bool isAllZero() const { return isAllZero_; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }
    const std::string& outPath() const { return outPath_; }

    void print() const {
        ::printf("devLb: %" PRIu64 "\n"
                 "minIoLb: %u\n"
                 "maxIoLb: %u\n"
                 "pbs: %u\n"
                 "maxPackPb: %u\n"
                 "outLogPb: %" PRIu64 "\n"
                 "lsid: %" PRIu64 "\n"
                 "outPath: %s\n"
                 "isPadding: %d\n"
                 "isDiscard: %d\n"
                 "verbose: %d\n"
                 "isHelp: %d\n",
                 devLb(), minIoLb(), maxIoLb(),
                 pbs(), maxPackPb(), outLogPb(),
                 lsid(), outPath().c_str(),
                 isPadding(), isDiscard(), isVerbose(), isHelp());
        int i = 0;
        for (const auto& s : args_) {
            ::printf("arg%d: %s\n", i++, s.c_str());
        }
    }

    static void printHelp() {
        ::printf("%s", generateHelpString().c_str());
    }

    void check() const {
        if (!::is_valid_pbs(pbs())) {
            throwError("pbs invalid.");
        }
        if (65535 < minIoLb()) {
            throwError("minSize must be < 512 * 65536 bytes.");
        }
        if (65535 < maxIoLb()) {
            throwError("maxSize must be < 512 * 65536 bytes.");
        }
        if (maxIoLb() < minIoLb()) {
            throwError("minIoSize must be <= maxIoSize.");
        }
        if (maxPackPb() < 1 + ::capacity_pb(pbs(), maxIoLb())) {
            throwError("maxPackSize must be >= pbs + maxIoSize.");
        }
        if (lsid() + outLogPb() < lsid()) {
            throwError("lsid will overflow.");
        }
        if (outPath().size() == 0) {
            throwError("specify outPath.");
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
        DEVSIZE = 1,
        MINIOSIZE,
        MAXIOSIZE,
        PBS,
        MAXPACKSIZE,
        OUTLOGSIZE,
        LSID,
        NOPADDING,
        NODISCARD,
        NOALLZERO,
        OUTPATH,
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

    template <typename IntType>
    IntType str2int(const char *str) const {
        return static_cast<IntType>(cybozu::util::fromUnitIntString(str));
    }

    void parse(int argc, char* argv[]) {
        while (1) {
            const struct option long_options[] = {
                {"devSize", 1, 0, Opt::DEVSIZE},
                {"minIoSize", 1, 0, Opt::MINIOSIZE},
                {"maxIoSize", 1, 0, Opt::MAXIOSIZE},
                {"pbs", 1, 0, Opt::PBS},
                {"maxPackSize", 1, 0, Opt::MAXPACKSIZE},
                {"outLogSize", 1, 0, Opt::OUTLOGSIZE},
                {"lsid", 1, 0, Opt::LSID},
                {"nopadding", 0, 0, Opt::NOPADDING},
                {"nodiscard", 0, 0, Opt::NODISCARD},
                {"noallzero", 0, 0, Opt::NOALLZERO},
                {"outPath", 1, 0, Opt::OUTPATH},
                {"verbose", 0, 0, Opt::VERBOSE},
                {"help", 0, 0, Opt::HELP},
                {0, 0, 0, 0}
            };
            int option_index = 0;
            int c = ::getopt_long(argc, argv, "s:b:o:z:vh", long_options, &option_index);
            if (c == -1) { break; }

            switch (c) {
            case Opt::DEVSIZE:
            case 's':
                devSize_ = cybozu::util::fromUnitIntString(optarg);
                break;
            case Opt::MINIOSIZE:
                minIoSize_ = str2int<unsigned int>(optarg);
                break;
            case Opt::MAXIOSIZE:
                maxIoSize_ = str2int<unsigned int>(optarg);
                break;
            case Opt::PBS:
            case 'b':
                pbs_ = str2int<unsigned int>(optarg);
                break;
            case Opt::MAXPACKSIZE:
                maxPackSize_ = str2int<unsigned int>(optarg);
                break;
            case Opt::OUTLOGSIZE:
            case 'z':
                outLogSize_ = str2int<uint64_t>(optarg);
                break;
            case Opt::LSID:
                lsid_ = str2int<uint64_t>(optarg);
                break;
            case Opt::NOPADDING:
                isPadding_ = false;
                break;
            case Opt::NODISCARD:
                isDiscard_ = false;
                break;
            case Opt::NOALLZERO:
                isAllZero_ = false;
                break;
            case Opt::OUTPATH:
            case 'o':
                outPath_ = std::string(optarg);
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
    }

    static std::string generateHelpString() {
        return cybozu::util::formatString(
            "Wlog-gen: generate walb log randomly.\n"
            "Usage: wlog-gen [options]\n"
            "Options:\n"
            "  -o, --outPath PATH:    output file path or '-' for stdout.\n"
            "  -b, --pbs SIZE:        physical block size [byte]. (default: %u)\n"
            "  -s, --devSize SIZE:    device size [byte]. (default: 16M)\n"
            "  -z, --outLogSize SIZE: total log size to generate [byte]. (default: 1M)\n"
            "  --minIoSize SIZE:      minimum IO size [byte]. (default: pbs)\n"
            "  --maxIoSize SIZE:      maximum IO size [byte]. (default: 32K)\n"
            "  --maxPackSize SIZE:    maximum logpack size [byte]. (default: 16M)\n"
            "  --lsid LSID:           lsid of the first log. (default: 0)\n"
            "  --nopadding:           no padding. (default: randomly inserted)\n"
            "  --nodiscard:           no discard. (default: randomly inserted)\n"
            "  --noallzero:           no all-zero. (default: randomly inserted)\n"
            "  -v, --verbose:         verbose messages to stderr.\n"
            "  -h, --help:            show this message.\n",
            LOGICAL_BLOCK_SIZE);
    }
};

/**
 * WalbLog Generator for test.
 */
class WalbLogGenerator
{
private:
    const Config& config_;
    uint64_t lsid_;

    class Rand
    {
    private:
        std::random_device rd_;
        std::mt19937 gen_;
        std::uniform_int_distribution<uint32_t> dist32_;
        std::uniform_int_distribution<uint64_t> dist64_;
        std::poisson_distribution<uint16_t> distp_;
    public:
        Rand()
            : rd_()
            , gen_(rd_())
            , dist32_(0, UINT32_MAX)
            , dist64_(0, UINT64_MAX)
            , distp_(4) {}

        uint32_t get32() { return dist32_(gen_); }
        uint64_t get64() { return dist64_(gen_); }
        uint16_t getp() { return distp_(gen_); }
    };
public:
    WalbLogGenerator(const Config& config)
        : config_(config)
        , lsid_(config.lsid()) {}

    void generate() {
        if (config_.outPath() == "-") {
            generateAndWrite(1);
        } else {
            cybozu::util::FileOpener f(
                config_.outPath(),
                O_WRONLY | O_CREAT | O_TRUNC, S_IREAD | S_IWRITE);
            generateAndWrite(f.fd());
            f.close();
        }
    }
private:
    using Block = std::shared_ptr<u8>;

    void setUuid(Rand &rand, std::vector<u8> &uuid) {
        const size_t t = sizeof(uint64_t);
        const size_t n = uuid.size() / t;
        const size_t m = uuid.size() % t;
        for (size_t i = 0; i < n; i++) {
            *reinterpret_cast<uint64_t *>(&uuid[i * t]) = rand.get64();
        }
        for (size_t i = 0; i < m; i++) {
            uuid[n * t + i] = static_cast<u8>(rand.get32());
        }
    }

    void generateAndWrite(int fd) {
        Rand rand;
        uint64_t writtenPb = 0;
        walb::log::WalbLogFileHeader wlHead;
        std::vector<u8> uuid(UUID_SIZE);
        setUuid(rand, uuid);

        const uint32_t salt = rand.get32();
        const unsigned int pbs = config_.pbs();
        uint64_t lsid = config_.lsid();
        Block hBlock = cybozu::util::allocateBlocks<u8>(pbs, pbs);
        cybozu::util::BlockAllocator<u8> ba(config_.maxPackPb(), pbs, pbs);

        /* Generate and write walb log header. */
        wlHead.init(pbs, salt, &uuid[0], lsid, uint64_t(-1));
        if (!wlHead.isValid(false)) {
            throw RT_ERR("WalbLogHeader invalid.");
        }
        wlHead.write(fd);
        if (config_.isVerbose()) {
            wlHead.print(::stderr);
        }

        uint64_t nPack = 0;
        while (writtenPb < config_.outLogPb()) {
            walb::log::WalbLogpackHeader logh(hBlock, pbs, salt);
            generateLogpackHeader(rand, logh, lsid);
            assert(::is_valid_logpack_header_and_records(&logh.header()));
            uint64_t tmpLsid = lsid + 1;

            /* Prepare blocks and calc checksum if necessary. */
            std::vector<Block> blocks;
            for (unsigned int i = 0; i < logh.nRecords(); i++) {
                walb::log::WalbLogpackData logd(logh, i);
                if (logd.hasData()) {
                    bool isAllZero = false;
                    if (config_.isAllZero()) {
                        isAllZero = rand.get32() % 100 < 10;
                    }
                    for (unsigned int j = 0; j < logd.ioSizePb(); j++) {
                        Block b = ba.alloc();
                        ::memset(b.get(), 0, pbs);
                        if (!isAllZero) {
                            *reinterpret_cast<uint64_t *>(b.get()) = tmpLsid++;
                        }
                        logd.addBlock(b);
                        blocks.push_back(b);
                    }
                }
                if (logd.hasDataForChecksum()) {
                    UNUSED bool ret = logd.setChecksum();
                    assert(ret);
                    assert(logd.isValid(true));
                }
            }
            assert(blocks.size() == logh.totalIoSize());

            /* Calculate header checksum and write. */
            cybozu::util::FdWriter fdw(fd);
            logh.write(fdw);

            /* Write each IO data. */
            for (Block b : blocks) {
#if 0
                ::printf("block data %" PRIu64 "\n",
                         *reinterpret_cast<uint64_t *>(b.get())); /* debug */
#endif
                fdw.write(reinterpret_cast<const char *>(b.get()), pbs);
            }

            uint64_t w = 1 + logh.totalIoSize();
            assert(tmpLsid == lsid + w);
            writtenPb += w;
            lsid += w;
            nPack++;

            if (config_.isVerbose()) {
                ::fprintf(::stderr, ".");
                if (nPack % 80 == 79) {
                    ::fprintf(::stderr, "\n");
                }
                ::fflush(::stderr);
            }
        }
        if (config_.isVerbose()) {
            ::fprintf(::stderr,
                      "\n"
                      "nPack: %" PRIu64 "\n"
                      "written %" PRIu64 " physical blocks\n",
                      nPack, writtenPb);
        }
    }

    /**
     * Generate logpack header randomly.
     */
    void generateLogpackHeader(
        Rand &rand, walb::log::WalbLogpackHeader &logh, uint64_t lsid) {
        logh.init(lsid);
        const unsigned int pbs = config_.pbs();
        const unsigned int maxNumRecords = ::max_n_log_record_in_sector(pbs);
        const size_t nRecords = (rand.get32() % maxNumRecords) + 1;
        const uint64_t devLb = config_.devLb();

        for (size_t i = 0; i < nRecords; i++) {
            uint64_t offset = rand.get64() % devLb;
            /* Decide io_size. */
            uint16_t ioSize = config_.minIoLb();
            uint16_t range = config_.maxIoLb() - config_.minIoLb();
            if (0 < range) {
                ioSize += rand.get32() % range;
            }
            if (devLb < offset + ioSize) {
                ioSize = devLb - offset; /* clipping. */
            }
            assert(0 < ioSize);
            /* Check total_io_size limitation. */
            if (0 < logh.totalIoSize() && 1 < nRecords &&
                config_.maxPackPb() <
                logh.totalIoSize() + ::capacity_pb(pbs, ioSize)) {
                break;
            }
            /* Decide IO type. */
            unsigned int v = rand.get32() % 100;
            if (config_.isPadding() && v < 10) {
                uint16_t psize = capacity_lb(pbs, capacity_pb(pbs, ioSize));
                if (v < 5) { psize = 0; } /* padding size can be 0. */
                if (!logh.addPadding(psize)) { break; }
                continue;
            }
            if (config_.isDiscard() && v < 30) {
                if (!logh.addDiscardIo(offset, ioSize)) { break; }
                continue;
            }
            if (!logh.addNormalIo(offset, ioSize)) { break; }
        }
        logh.isValid(false);
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
            return 0;
        }
        config.check();

        WalbLogGenerator wlGen(config);
        wlGen.generate();

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
