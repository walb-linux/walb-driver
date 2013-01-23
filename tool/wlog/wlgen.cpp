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
    uint64_t devSize_; /* [byte]. */
    unsigned int minIoSize_; /* [byte]. */
    unsigned int maxIoSize_; /* [byte]. */
    unsigned int pbs_; /* physical block size [byte] */
    unsigned int maxPackSize_; /* [byte]. */
    uint64_t outLogSize_; /* Approximately output log size [byte]. */
    uint64_t lsid_; /* start lsid [physical block]. */
    bool isPadding_;
    bool isDiscard_;
    bool isVerbose_;
    bool isHelp_;
    std::string outPath_;
    const std::string helpString_;
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : devSize_(16 * 1024 * 1024) /* default 16MB. */
        , minIoSize_(1)
        , maxIoSize_(1024 * 1024) /* default 1MB. */
        , pbs_(512)
        , maxPackSize_(16 * 1024 * 1024) /* default 16MB. */
        , outLogSize_(1024 * 1024) /* default 1MB. */
        , lsid_(0)
        , isPadding_(true)
        , isDiscard_(true)
        , isVerbose_(false)
        , isHelp_(false)
        , outPath_()
        , helpString_(generateHelpString(argv[0]))
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

    void printHelp() const {
        ::printf("%s", helpString_.c_str());
    }

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

    uint64_t parseSize(const char* arg) const {
        int shift = 0;
        std::string s(arg);
        const size_t sz = s.size();

        if (sz == 0) { RT_ERR("Invalid argument."); }
        switch (s[sz - 1]) {
        case 'p':
        case 'P':
            shift += 10;
        case 't':
        case 'T':
            shift += 10;
        case 'g':
        case 'G':
            shift += 10;
        case 'm':
        case 'M':
            shift += 10;
        case 'k':
        case 'K':
            shift += 10;
            s.resize(sz - 1);
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            break;
        default:
            RT_ERR("Invalid suffix charactor.");
        }

        for (size_t i = 0; i < sz - 1; i++) {
            if (!('0' <= arg[i] && arg[i] <= '9')) {
                RT_ERR("Not numeric charactor.");
            }
        }
        uint64_t val = atoll(s.c_str());
        if ((val << shift) >> shift != val) {
            RT_ERR("Size overflow.");
        }
        return val << shift;
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
            int c = ::getopt_long(argc, argv, "", long_options, &option_index);
            if (c == -1) { break; }
            switch (c) {
            case Opt::DEVSIZE:
                devSize_ = parseSize(optarg);
                break;
            case Opt::MINIOSIZE:
                minIoSize_ = static_cast<unsigned int>(parseSize(optarg));
                break;
            case Opt::MAXIOSIZE:
                maxIoSize_ = static_cast<unsigned int>(parseSize(optarg));
                break;
            case Opt::PBS:
                pbs_ = static_cast<unsigned int>(parseSize(optarg));
                break;
            case Opt::MAXPACKSIZE:
                maxPackSize_ = static_cast<unsigned int>(parseSize(optarg));
                break;
            case Opt::OUTLOGSIZE:
                outLogSize_ = parseSize(optarg);
                break;
            case Opt::LSID:
                lsid_ = parseSize(optarg);
                break;
            case Opt::NOPADDING:
                isPadding_ = false;
                break;
            case Opt::NODISCARD:
                isDiscard_ = false;
                break;
            case Opt::OUTPATH:
                outPath_ = std::string(optarg);
                break;
            case Opt::VERBOSE:
                isVerbose_ = true;
                break;
            case Opt::HELP:
                isHelp_ = true;
                break;
            default:
                RT_ERR("Unknown option.");
            }
        }

        while(optind < argc) {
            args_.push_back(std::string(argv[optind++]));
        }
        try {
            check();
        } catch (std::runtime_error& e) {
            printHelp();
            throw;
        }
    }

    static std::string generateHelpString(const char *argv0) {
        return walb::util::formatString(
            "Usage: %s [options]\n"
            "Options:\n"
            "  --pbs [size]:        physical block size [byte].\n"
            "  --devSize [size]:    device size [byte].\n"
            "  --outLogSize [size]: total log size to generate [byte].\n"
            "  --minIoSize [size]:  minimum IO size [byte].\n"
            "  --maxIoSize [size]:  maximum IO size [byte].\n"
            "  --maxPackSize [size]: maximum logpack size [byte].\n"
            "  --lsid [lsid]:       lsid of the first log.\n"
            "  --nopadding:         no padding. (default: randomly inserted)\n"
            "  --nodiscard:         no discard. (default: randomly inserted)\n"
            "  --outPath [path]:    output file path or '-' for stdout. (default: stdout)\n"
            "  --verbose:           verbose messages to stderr.\n"
            "  --help:              show this message.\n"
            , argv0);
    }

    void check() const {
        if (pbs() < 512) {
            throw RT_ERR("pbs must be 512 or more.");
        }
        if (pbs() % 512 != 0) {
            throw RT_ERR("pbs must be multiple of 512.");
        }
        if (minIoLb() > 65535) {
            throw RT_ERR("minSize must be < 512 * 65536 bytes.");
        }
        if (maxIoLb() > 65535) {
            throw RT_ERR("maxSize must be < 512 * 65536 bytes.");
        }
        if (minIoLb() > maxIoLb()) {
            throw RT_ERR("minIoSize must be <= maxIoSize.");
        }
        if (maxPackPb() < 1 + capacity_pb(pbs(), maxIoLb())) {
            throw RT_ERR("maxPackSize must be >= pbs + maxIoSize.");
        }
        if (lsid() + outLogPb() < lsid()) {
            throw RT_ERR("lsid will overflow.");
        }
        if (outPath().size() == 0) {
            throw RT_ERR("specify outPath.");
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
                    bool ret = logd.setChecksum();
                    assert(ret);
                }
            }
            assert(blocks.size() == logh.totalIoSize());

            /* Calculate header checksum and write. */
            logh.write(fd);

            /* Write each IO data. */
            walb::util::FdWriter fdw(fd);
            for (Block b : blocks) {
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
            if (logh.totalIoSize() > 0 &&
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
            config.printHelp();
            return 1;
        }
        WalbLogGenerator wlGen(config);
        wlGen.generate();

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
