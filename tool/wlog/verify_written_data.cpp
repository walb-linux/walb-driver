/**
 * @file
 * @brief verify data written by write_random_data.
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

#include "checksum.hpp"
#include "util.hpp"
#include "fileio.hpp"
#include "memory_buffer.hpp"
#include "io_recipe.hpp"

#include "walb/common.h"
#include "walb/block_size.h"

/**
 * Command line configuration.
 */
class Config
{
private:
    unsigned int bs_; /* block size [byte] */
    bool isVerbose_;
    bool isHelp_;
    std::string recipePath_; /* recipe file path. */
    std::string targetPath_; /* device or file path. */
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : bs_(LOGICAL_BLOCK_SIZE)
        , isVerbose_(false)
        , isHelp_(false)
        , recipePath_("-")
        , targetPath_()
        , args_() {
        parse(argc, argv);
    }

    unsigned int blockSize() const { return bs_; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }
    const std::string& targetPath() const { return targetPath_; }
    const std::string& recipePath() const { return recipePath_; }

    bool isDirect() const {
#if 0
        return false;
#else
        return blockSize() % LOGICAL_BLOCK_SIZE == 0;
#endif
    }

    void print() const {
        FILE *fp = ::stderr;
        ::fprintf(fp, "blockSize: %u\n"
                  "verbose: %d\n"
                  "isHelp: %d\n"
                  "recipe: %s\n"
                  "targetPath: %s\n",
                  blockSize(), isVerbose(), isHelp(),
                  recipePath_.c_str(), targetPath().c_str());
        int i = 0;
        for (const auto& s : args_) {
            ::fprintf(fp, "arg%d: %s\n", i++, s.c_str());
        }
    }

    static void printHelp() {
        ::printf("%s", generateHelpString().c_str());
    }

    void check() const {
        if (blockSize() == 0) {
            throwError("blockSize must be non-zero.");
        }
        if (targetPath().size() == 0) {
            throwError("specify target device or file.");
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
        BLOCKSIZE = 1,
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
                {"blockSize", 1, 0, Opt::BLOCKSIZE},
                {"recipe", 1, 0, Opt::RECIPEPATH},
                {"verbose", 0, 0, Opt::VERBOSE},
                {"help", 0, 0, Opt::HELP},
                {0, 0, 0, 0}
            };
            int option_index = 0;
            int c = ::getopt_long(argc, argv, "b:i:vh", long_options, &option_index);
            if (c == -1) { break; }

            switch (c) {
            case Opt::BLOCKSIZE:
            case 'b':
                bs_ = str2int<unsigned int>(optarg);
                break;
            case Opt::RECIPEPATH:
            case 'i':
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
            targetPath_ = args_[0];
        }
    }

    static std::string generateHelpString() {
        return cybozu::util::formatString(
            "verify_written_data: verify data written by write_random_data.\n"
            "Usage: verify_written_data [options] [DEVICE|FILE]\n"
            "Options:\n"
            "  -b, --blockSize SIZE:  block size [byte]. (default: %u)\n"
            "  -i, --recipe PATH:     recipe file path. '-' for stdin. (default: '-')\n"
            "  -v, --verbose:         verbose messages to stderr.\n"
            "  -h, --help:            show this message.\n",
            LOGICAL_BLOCK_SIZE);
    }
};

class IoDataVerifier
{
private:
    const Config &config_;
    cybozu::util::BlockDevice bd_;
    size_t bufSizeB_; /* buffer size [block]. */
    std::shared_ptr<char> buf_;

public:
    IoDataVerifier(const Config &config)
        : config_(config)
        , bd_(config.targetPath(), O_RDONLY | (config.isDirect() ? O_DIRECT : 0))
        , bufSizeB_(1024 * 1024 / config.blockSize()) /* 1MB */
        , buf_(getBufferStatic(config.blockSize(), bufSizeB_, config.isDirect())) {
        assert(buf_);
    }

    void run() {
        const unsigned int bs = config_.blockSize();

        /* Get IO recipe parser. */
        std::shared_ptr<cybozu::util::FileOpener> fop;
        if (config_.recipePath() != "-") {
            fop.reset(new cybozu::util::FileOpener(config_.recipePath(), O_RDONLY));
        }
        int fd = 0;
        if (fop) { fd = fop->fd(); }
        walb::util::IoRecipeParser recipeParser(fd);

        /* Read and verify for each IO recipe. */
        while (!recipeParser.isEnd()) {
            walb::util::IoRecipe r = recipeParser.get();
            resizeBufferIfneed(r.ioSizeB());
            bd_.read(r.offsetB() * bs, r.ioSizeB() * bs, buf_.get());
            uint32_t csum = cybozu::util::calcChecksum(buf_.get(), r.ioSizeB() * bs, 0);
            ::printf("%s\t%s\t%08x\n",
                     (csum == r.csum() ? "OK" : "NG"), r.toString().c_str(), csum);
        }
    }

private:
    static std::shared_ptr<char> getBufferStatic(unsigned int blockSize, unsigned sizeB, bool isDirect) {
        assert(0 < blockSize);
        assert(0 < sizeB);
        if (isDirect) {
            return cybozu::util::allocateBlocks<char>(blockSize, blockSize * sizeB);
        } else {
            return std::shared_ptr<char>(reinterpret_cast<char *>(::malloc(blockSize * sizeB)));
        }
    }

    void resizeBufferIfneed(unsigned int newSizeB) {
        if (newSizeB <= bufSizeB_) { return; }
        const unsigned int bs = config_.blockSize();
        if (config_.isDirect()) {
            buf_ = cybozu::util::allocateBlocks<char>(bs, bs * newSizeB);
        } else {
            buf_.reset(reinterpret_cast<char *>(::malloc(bs * newSizeB)));
        }
        if (!buf_) { throw std::bad_alloc(); }
        bufSizeB_ = newSizeB;
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

        IoDataVerifier v(config);
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
