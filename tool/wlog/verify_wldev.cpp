/**
 * @file
 * @brief Verify logs on a walb log device by comparing with an IO recipe.
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
#include "io_recipe.hpp"
#include "walb/common.h"
#include "walb/block_size.h"

/**
 * Command line configuration.
 */
class Config
{
private:
    uint64_t beginLsid_;
    uint64_t endLsid_;
    bool isVerbose_;
    bool isHelp_;
    std::string recipePath_; /* recipe path or "-" for stdin. */
    std::string wldevPath_; /* walb log devcie path. */
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : beginLsid_(-1)
        , endLsid_(-1)
        , isVerbose_(false)
        , isHelp_(false)
        , recipePath_("-")
        , wldevPath_()
        , args_() {
        parse(argc, argv);
    }

    uint64_t beginLsid() const { return beginLsid_; }
    uint64_t endLsid() const { return endLsid_; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }
    const std::string& recipePath() const { return recipePath_; }
    const std::string& wldevPath() const { return wldevPath_; }

    void print() const {
        FILE *fp = ::stderr;
        ::fprintf(fp,
                  "beginLsid: %" PRIu64 "\n"
                  "endLsid: %" PRIu64 "\n"
                  "verbose: %d\n"
                  "isHelp: %d\n"
                  "recipe: %s\n"
                  "wldev: %s\n",
                  beginLsid(), endLsid(), isVerbose(), isHelp(),
                  recipePath_.c_str(), wldevPath().c_str());
        int i = 0;
        for (const auto& s : args_) {
            ::fprintf(fp, "arg%d: %s\n", i++, s.c_str());
        }
    }

    static void printHelp() {
        ::printf("%s", generateHelpString().c_str());
    }

    void check() const {
        if (wldevPath_.empty()) {
            throwError("Specify walb log device.");
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
        RECIPEPATH,
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
                {"beginLsid", 1, 0, Opt::BEGIN_LSID},
                {"endLsid", 1, 0, Opt::END_LSID},
                {"recipe", 1, 0, Opt::RECIPEPATH},
                {"verbose", 0, 0, Opt::VERBOSE},
                {"help", 0, 0, Opt::HELP},
                {0, 0, 0, 0}
            };
            int option_index = 0;
            int c = ::getopt_long(argc, argv, "b:e:r:vh", long_options, &option_index);
            if (c == -1) { break; }

            switch (c) {
            case Opt::BEGIN_LSID:
            case 'b':
                beginLsid_ = str2int<uint64_t>(optarg);
                break;
            case Opt::END_LSID:
            case 'e':
                endLsid_ = str2int<uint64_t>(optarg);
                break;
            case Opt::RECIPEPATH:
            case 'r':
                recipePath_ = optarg;
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
            wldevPath_ = args_[0];
        }
    }

    static std::string generateHelpString() {
        return cybozu::util::formatString(
            "verify_wldev: verify logs on a walb log device with an IO recipe.\n"
            "Usage: verify_wldev [options] WALB_LOG_DEVICE\n"
            "Options:\n"
            "  -b, --beginLsid LSID: begin lsid. (default: oldest lsid)\n"
            "  -e, --endLsid LSID:   end lsid. (default: written lsid)\n"
            "  -r, --recipe PATH:    recipe file path. '-' for stdin. (default: '-')\n"
            "  -v, --verbose:     verbose messages to stderr.\n"
            "  -h, --help:        show this message.\n");
    }
};

class WldevVerifier
{
private:
    using PackHeader = walb::log::WalbLogpackHeader;
    using PackHeaderPtr = std::shared_ptr<PackHeader>;
    using PackData = walb::log::WalbLogpackData;
    using PackDataPtr = std::shared_ptr<PackData>;

    const Config &config_;
    cybozu::util::BlockDevice wlDev_;
    walb::log::WalbSuperBlock super_;
    const unsigned int pbs_;
    const uint32_t salt_;
    const unsigned int BUFFER_SIZE_;
    cybozu::util::BlockAllocator<uint8_t> ba_;

public:
    WldevVerifier(const Config &config)
        : config_(config)
        , wlDev_(config.wldevPath(), O_RDONLY | O_DIRECT)
        , super_(wlDev_)
        , pbs_(super_.getPhysicalBlockSize())
        , salt_(super_.getLogChecksumSalt())
        , BUFFER_SIZE_(16 << 20) /* 16MB */
        , ba_(BUFFER_SIZE_ / pbs_, pbs_, pbs_) {}

    void run() {
        /* Get IO recipe parser. */
        std::shared_ptr<cybozu::util::FileOpener> rFop;
        if (config_.recipePath() != "-") {
            rFop.reset(new cybozu::util::FileOpener(config_.recipePath(), O_RDONLY));
        }
        int rFd = 0;
        if (rFop) { rFd = rFop->fd(); }
        walb::util::IoRecipeParser recipeParser(rFd);

        /* Decide lsid range to verify. */
        uint64_t beginLsid = config_.beginLsid();
        if (beginLsid == uint64_t(-1)) { beginLsid = super_.getOldestLsid(); }
        uint64_t endLsid = config_.endLsid();
        if (endLsid == uint64_t(-1)) { endLsid = super_.getWrittenLsid(); }
        if (endLsid <= beginLsid) {
            throw RT_ERR("Invalid lsid range.");
        }

        /* Read walb logs and verify them with IO recipes. */
        uint64_t lsid = beginLsid;
        while (lsid < endLsid) {
            PackHeaderPtr loghp = readPackHeader(lsid);
            PackHeader &logh = *loghp;
            if (lsid != logh.logpackLsid()) { throw RT_ERR("wrong lsid"); }
            std::queue<PackDataPtr> q;
            readPackData(logh, q);

            while (!q.empty()) {
                PackDataPtr logdp = q.front();
                q.pop();
                PackData &logd = *logdp;
                if (recipeParser.isEnd()) {
                    throw RT_ERR("Recipe not found.");
                }
                walb::util::IoRecipe recipe = recipeParser.get();
                if (recipe.offsetB() != logd.record().offset) {
                    RT_ERR("offset mismatch.");
                }
                if (recipe.ioSizeB() != logd.record().io_size) {
                    RT_ERR("io_size mismatch.");
                }
                /* Validate the log and confirm checksum equality. */
                const uint32_t csum0 = logd.calcIoChecksum(0);
                const uint32_t csum1 = logd.record().checksum;
                const uint32_t csum2 = logd.calcIoChecksum();
                const bool isValid = logd.isValid(false) &&
                    recipe.csum() == csum0 && csum1 == csum2;

                /* Print result. */
                ::printf("%s\t%s\t%08x\t%08x\t%08x\n", isValid ? "OK" : "NG",
                         recipe.toString().c_str(), csum0, csum1, csum2);
            }
            lsid = logh.nextLogpackLsid();
        }

        if (!recipeParser.isEnd()) {
            throw RT_ERR("There are still remaining recipes.");
        }
    }

private:
    using Block = std::shared_ptr<uint8_t>;

    Block readBlock(uint64_t lsid) {
        Block b = ba_.alloc();
        uint64_t offset = super_.getOffsetFromLsid(lsid);
        wlDev_.read(offset * pbs_, pbs_, reinterpret_cast<char *>(b.get()));
        return b;
    }

    PackHeaderPtr readPackHeader(uint64_t lsid) {
        Block b = readBlock(lsid);
        return PackHeaderPtr(new PackHeader(b, pbs_, salt_));
    }

    void readPackData(PackHeader &logh, std::queue<PackDataPtr> &queue) {
        for (size_t i = 0; i < logh.nRecords(); i++) {
            PackDataPtr logdp(new PackData(logh, i));
            PackData &logd = *logdp;
            if (!logd.hasData()) { continue; }
            for (uint64_t lsid = logd.lsid(); lsid < logd.lsid() + logd.ioSizePb(); lsid++) {
                logd.addBlock(readBlock(lsid));
            }
            if (!logd.hasDataForChecksum()) { continue; }
            /* Only normal IOs will be inserted. */
            queue.push(logdp);
        }
    }
};

int main(int argc, char* argv[])
{
    try {
        Config config(argc, argv);
        /* config.print(); */
        if (config.isHelp()) {
            Config::printHelp();
            return 0;
        }
        config.check();

        WldevVerifier v(config);
        v.run();
        return 0;

    } catch (Config::Error& e) {
        ::printf("Command line error: %s\n\n", e.what());
        Config::printHelp();
        return 1;
    } catch (std::runtime_error& e) {
        LOGe("Error: %s\n", e.what());
        return 1;
    } catch (std::exception& e) {
        LOGe("Exception: %s\n", e.what());
        return 1;
    } catch (...) {
        LOGe("Caught other error.\n");
        return 1;
    }
}

/* end file. */
