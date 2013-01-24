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
#include "walb_util.hpp"
#include "aio_util.hpp"

#include "walb/walb.h"

/**
 * Wlredo command configuration.
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
    bool isVerbose_;
    bool isHelp_;
    std::string outPath_;
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : pbs_(512)
        , devSize_(16 * 1024 * 1024) /* default 16MB. */
        , minIoSize_(pbs_)
        , maxIoSize_(1024 * 1024) /* default 1MB. */
        , maxPackSize_(16 * 1024 * 1024) /* default 16MB. */
        , outLogSize_(1024 * 1024) /* default 1MB. */
        , lsid_(0)
        , isPadding_(true)
        , isDiscard_(true)
        , isVerbose_(false)
        , isHelp_(false)
        , outPath_()
        , args_() {
        parse(argc, argv);
    }

    uint64_t devLb() const { return devSize_ / 512; }
    unsigned int minIoLb() const { return minIoSize_ / 512; }
    unsigned int maxIoLb() const { return maxIoSize_ / 512; }
    unsigned int pbs() const { return pbs_; }
    unsigned int maxPackPb() const { return maxPackSize_ / pbs(); }
    uint64_t outLogPb() const { return outLogSize_ / pbs(); }
    uint64_t lsid() const { return lsid_; }
    bool isPadding() const { return isPadding_; }
    bool isDiscard() const { return isDiscard_; }
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
        OUTPATH,
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
                {"devSize", 1, 0, Opt::DEVSIZE},
                {"minIoSize", 1, 0, Opt::MINIOSIZE},
                {"maxIoSize", 1, 0, Opt::MAXIOSIZE},
                {"pbs", 1, 0, Opt::PBS},
                {"maxPackSize", 1, 0, Opt::MAXPACKSIZE},
                {"outLogSize", 1, 0, Opt::OUTLOGSIZE},
                {"lsid", 1, 0, Opt::LSID},
                {"nopadding", 0, 0, Opt::NOPADDING},
                {"nodiscard", 0, 0, Opt::NODISCARD},
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
                devSize_ = walb::util::fromUnitIntString(optarg);
                break;
            case Opt::MINIOSIZE:
                minIoSize_ = static_cast<unsigned int>(walb::util::fromUnitIntString(optarg));
                break;
            case Opt::MAXIOSIZE:
                maxIoSize_ = static_cast<unsigned int>(walb::util::fromUnitIntString(optarg));
                break;
            case Opt::PBS:
            case 'b':
                pbs_ = static_cast<unsigned int>(walb::util::fromUnitIntString(optarg));
                break;
            case Opt::MAXPACKSIZE:
                maxPackSize_ = static_cast<unsigned int>(walb::util::fromUnitIntString(optarg));
                break;
            case Opt::OUTLOGSIZE:
            case 'z':
                outLogSize_ = walb::util::fromUnitIntString(optarg);
                break;
            case Opt::LSID:
                lsid_ = walb::util::fromUnitIntString(optarg);
                break;
            case Opt::NOPADDING:
                isPadding_ = false;
                break;
            case Opt::NODISCARD:
                isDiscard_ = false;
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
        check();
    }

    static std::string generateHelpString() {
        return walb::util::formatString(
            "Wlgen: generate walb log randomly.\n"
            "Usage: wlgen [options]\n"
            "Options:\n"
            "  -o, --outPath PATH:    output file path or '-' for stdout.\n"
            "  -b, --pbs(-b) SIZE:    physical block size [byte]. (default: 512)\n"
            "  -s, --devSize SIZE:    device size [byte]. (default: 16M)\n"
            "  -z, --outLogSize SIZE: total log size to generate [byte]. (default: 1M)\n"
            "  --minIoSize SIZE:      minimum IO size [byte]. (default: pbs)\n"
            "  --maxIoSize SIZE:      maximum IO size [byte]. (default: 1M)\n"
            "  --maxPackSize SIZE:    maximum logpack size [byte]. (default: 16M)\n"
            "  --lsid LSID:           lsid of the first log. (default: 0)\n"
            "  --nopadding:           no padding. (default: randomly inserted)\n"
            "  --nodiscard:           no discard. (default: randomly inserted)\n"
            "  -v, --verbose:         verbose messages to stderr.\n"
            "  -h, --help:            show this message.\n");
    }

    void check() const {
        if (pbs() < 512) {
            throwError("pbs must be 512 or more.");
        }
        if (pbs() % 512 != 0) {
            throwError("pbs must be multiple of 512.");
        }
        if (minIoLb() > 65535) {
            throwError("minSize must be < 512 * 65536 bytes.");
        }
        if (maxIoLb() > 65535) {
            throwError("maxSize must be < 512 * 65536 bytes.");
        }
        if (minIoLb() > maxIoLb()) {
            throwError("minIoSize must be <= maxIoSize.");
        }
        if (maxPackPb() < 1 + capacity_pb(pbs(), maxIoLb())) {
            throwError("maxPackSize must be >= pbs + maxIoSize.");
        }
        if (lsid() + outLogPb() < lsid()) {
            throwError("lsid will overflow.");
        }
        if (outPath().size() == 0) {
            throwError("specify outPath.");
        }
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

        uint32_t get32() {
            return dist32_(gen_);
        }

        uint64_t get64() {
            return dist64_(gen_);
        }

        uint16_t getp() {
            return distp_(gen_);
        }
    };
public:
    WalbLogGenerator(const Config& config)
        : config_(config)
        , lsid_(config.lsid()) {}

    void generate() {
        if (config_.outPath() == "-") {
            generateAndWrite(1);
        } else {
            walb::util::FileOpener f(
                config_.outPath(),
                O_WRONLY | O_CREAT | O_TRUNC, S_IREAD | S_IWRITE);
            generateAndWrite(f.fd());
            f.close();
        }
    }
private:
    using Block = std::shared_ptr<u8>;

    void generateAndWrite(int fd) {
        Rand rand;
        uint64_t written = 0;
        walb::util::WalbLogFileHeader wlHead;
        std::vector<u8> uuid(16);
        for (int i = 0; i < 4; i ++) {
            *reinterpret_cast<uint32_t *>(&uuid[i * 4]) = rand.get32();
        }

        const uint32_t salt = rand.get32();
        const unsigned int pbs = config_.pbs();
        uint64_t lsid = config_.lsid();
        Block hBlock = walb::util::allocateBlock<u8>(pbs, pbs);
        walb::util::BlockAllocator<u8> ba(config_.maxPackPb(), pbs, pbs);

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
        while (written < config_.outLogPb()) {
            walb::util::WalbLogpackHeader logh(hBlock, pbs, salt);
            generateLogpackHeader(rand, logh, lsid);
            assert(::is_valid_logpack_header_and_records(&logh.header()));
            uint64_t tmpLsid = lsid + 1;

            /* Prepare blocks and calc checksum if necessary. */
            std::vector<Block> blocks;
            for (unsigned int i = 0; i < logh.nRecords(); i++) {
                walb::util::WalbLogpackData logd(logh, i);
                if (logd.hasData()) {
                    for (unsigned int j = 0; j < logd.ioSizePb(); j++) {
                        Block b = ba.alloc();
                        memset(b.get(), 0, pbs);
                        *reinterpret_cast<uint64_t *>(b.get()) = tmpLsid++;
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
            logh.write(fd);

            /* Write each IO data. */
            walb::util::FdWriter fdw(fd);
            for (Block b : blocks) {
#if 0
                ::printf("block data %" PRIu64 "\n", *reinterpret_cast<uint64_t *>(b.get())); /* debug */
#endif
                fdw.write(reinterpret_cast<const char *>(b.get()), pbs);
            }

            uint64_t w = 1 + logh.totalIoSize();
            assert(tmpLsid == lsid + w);
            written += w;
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
                      nPack, written);
        }
    }

    /**
     * Generate logpack header randomly.
     */
    void generateLogpackHeader(
        Rand &rand, walb::util::WalbLogpackHeader &logh, uint64_t lsid) {
        logh.init(lsid);
        const unsigned int pbs = config_.pbs();
        const unsigned int maxNumRecords = ::max_n_log_record_in_sector(pbs);
        const size_t nRecords = (rand.get32() % maxNumRecords) + 1;
        const size_t paddingPos = rand.get32() % nRecords;
        const uint64_t devLb = config_.devLb();

        for (size_t i = 0; i < nRecords; i++) {
            uint64_t offset = rand.get64() % devLb;
            uint16_t ioSize = config_.minIoLb();
            uint16_t range = config_.maxIoLb() - config_.minIoLb();
            if (range > 0) {
                ioSize += rand.get32() % range;
            }
            assert(ioSize > 0);
            if (offset + ioSize > devLb) {
                ioSize = devLb - offset; /* clipping. */
            }
            if (logh.totalIoSize() > 0 && nRecords > 1 &&
                logh.totalIoSize() + capacity_pb(pbs, ioSize) > config_.maxPackPb()) {
                break;
            }
            if (config_.isPadding() && i == paddingPos && i != nRecords - 1) {
                uint16_t psize = capacity_lb(pbs, capacity_pb(pbs, ioSize));
                if (!logh.addPadding(psize)) { break; }
                continue;
            }
            bool isDiscard = config_.isDiscard() &&
                (rand.get32() & 0x00000007) == 0;
            if (isDiscard) {
                if (!logh.addDiscardIo(offset, ioSize)) { break; }
            } else {
                if (!logh.addNormalIo(offset, ioSize)) { break; }
            }
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
            return 1;
        }
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
