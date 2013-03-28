/**
 * @file
 * @brief write random data for test.
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
#include "io_recipe.hpp"
#include "memory_buffer.hpp"
#include "walb/common.h"
#include "walb/block_size.h"

/**
 * Command line configuration.
 */
class Config
{
private:
    unsigned int bs_; /* block size [byte] */
    uint64_t offsetB_; /* [block]. */
    uint64_t sizeB_; /* [block]. */
    unsigned int minIoB_; /* [block]. */
    unsigned int maxIoB_; /* [block]. */
    bool isVerbose_;
    bool isHelp_;
    std::string targetPath_; /* device or file path. */
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : bs_(LOGICAL_BLOCK_SIZE)
        , offsetB_(0)
        , sizeB_(0)
        , minIoB_(1)
        , maxIoB_(64)
        , isVerbose_(false)
        , isHelp_(false)
        , targetPath_()
        , args_() {
        parse(argc, argv);
    }

    unsigned int blockSize() const { return bs_; }
    uint64_t offsetB() const { return offsetB_; }
    uint64_t sizeB() const { return sizeB_; }
    unsigned int minIoB() const { return minIoB_; }
    unsigned int maxIoB() const { return maxIoB_; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }
    const std::string& targetPath() const { return targetPath_; }

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
                  "offsetB: %" PRIu64 "\n"
                  "sizeB: %" PRIu64 "\n"
                  "minIoB: %u\n"
                  "maxIoB: %u\n"
                  "verbose: %d\n"
                  "isHelp: %d\n"
                  "targetPath: %s\n",
                  blockSize(), offsetB(), sizeB(), minIoB(), maxIoB(),
                  isVerbose(), isHelp(), targetPath().c_str());
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
        if (minIoB() == 0) {
            throwError("minIoSize must be > 0.");
        }
        if (maxIoB() == 0) {
            throwError("maxIoSize must be > 0.");
        }
        if (maxIoB() < minIoB()) {
            throwError("minIoSize must be <= maxIoSize.");
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
        OFFSET,
        SIZE,
        MINIOSIZE,
        MAXIOSIZE,
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
                {"offset", 1, 0, Opt::OFFSET},
                {"size", 1, 0, Opt::SIZE},
                {"minIoSize", 1, 0, Opt::MINIOSIZE},
                {"maxIoSize", 1, 0, Opt::MAXIOSIZE},
                {"verbose", 0, 0, Opt::VERBOSE},
                {"help", 0, 0, Opt::HELP},
                {0, 0, 0, 0}
            };
            int option_index = 0;
            int c = ::getopt_long(argc, argv, "b:o:s:n:x:vh", long_options, &option_index);
            if (c == -1) { break; }

            switch (c) {
            case Opt::BLOCKSIZE:
            case 'b':
                bs_ = str2int<unsigned int>(optarg);
                break;
            case Opt::OFFSET:
            case 'o':
                offsetB_ = str2int<uint64_t>(optarg);
                break;
            case Opt::SIZE:
            case 's':
                sizeB_ = str2int<uint64_t>(optarg);
                break;
            case Opt::MINIOSIZE:
            case 'n':
                minIoB_ = str2int<unsigned int>(optarg);
                break;
            case Opt::MAXIOSIZE:
            case 'x':
                maxIoB_ = str2int<unsigned int>(optarg);
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
            "write_random_data: generate random data and write them.\n"
            "Usage: write_random_data [options] [DEVICE|FILE]\n"
            "Options:\n"
            "  -b, --blockSize SIZE:  block size [byte]. (default: %u)\n"
            "  -o, --offset OFFSET:   start offset [block]. (default: 0)\n"
            "  -s, --size SIZE:       written size [block]. (default: device size)\n"
            "  -n, --minIoSize SIZE:  minimum IO size [block]. (default: 1)\n"
            "  -x, --maxIoSize SIZE:  maximum IO size [block]. (default: 64)\n"
            "  -v, --verbose:         verbose messages to stderr.\n"
            "  -h, --help:            show this message.\n",
            LOGICAL_BLOCK_SIZE);
    }
};

class RandomDataWriter
{
private:
    const Config &config_;
    cybozu::util::BlockDevice bd_;
    cybozu::util::Rand<unsigned int> randUint_;
    std::shared_ptr<char> buf_;

public:
    RandomDataWriter(const Config &config)
        : config_(config)
        , bd_(config.targetPath(), O_RDWR | (config.isDirect() ? O_DIRECT : 0))
        , randUint_()
        , buf_(getBufferStatic(config.blockSize(), config.maxIoB(), config.isDirect())) {
        assert(buf_);
    }

    void run() {
        uint64_t totalSize = decideSize();
        uint64_t offset = config_.offsetB();
        uint64_t written = 0;

        while (written < totalSize) {
            const unsigned int bs = config_.blockSize();
            unsigned int ioSize = decideIoSize(totalSize - written);
            fillBufferRandomly(ioSize);
            uint32_t csum = cybozu::util::calcChecksum(buf_.get(), bs * ioSize, 0);
            bd_.write(offset * bs, bs * ioSize, buf_.get());
            walb::util::IoRecipe r(offset, ioSize, csum);
            r.print();

            offset += ioSize;
            written += ioSize;
        }
        assert(written == totalSize);

        config_.offsetB();
        bd_.fdatasync();
    }
private:
    uint64_t decideSize() {
        uint64_t size = config_.sizeB();
        if (size == 0) {
            size = bd_.getDeviceSize() / config_.blockSize();
        }
        if (size == 0) {
            throw RT_ERR("device or file size is 0.");
        }
        return size;
    }

    unsigned int decideIoSize(uint64_t maxSize) {
        unsigned int min = config_.minIoB();
        unsigned int max = config_.maxIoB();
        if (maxSize < max) { max = maxSize; }
        if (max < min) { min = max; }
        return randomUInt(min, max);
    }

    unsigned int randomUInt(unsigned int min, unsigned int max) {
        assert(min <= max);
        if (min == max) { return min; }
        return randUint_.get() % (max - min) + min;
    }

    static std::shared_ptr<char> getBufferStatic(
        unsigned int blockSize, unsigned int maxIoB, bool isDirect) {
        assert(0 < blockSize);
        assert(0 < maxIoB);
        if (isDirect) {
            return cybozu::util::allocateBlocks<char>(blockSize, blockSize * maxIoB);
        } else {
            return std::shared_ptr<char>(reinterpret_cast<char *>(::malloc(blockSize * maxIoB)));
        }
    }

    void fillBufferRandomly(unsigned int sizeB) {
        assert(0 < sizeB);
        size_t offset = 0;
        size_t remaining = config_.blockSize() * sizeB;
        unsigned int r;
        assert(0 < remaining);
        while (sizeof(r) <= remaining) {
            r = randUint_.get();
            *reinterpret_cast<unsigned int *>(buf_.get() + offset) = r;
            offset += sizeof(r);
            remaining -= sizeof(r);
        }
        while (0 < remaining) {
            r = randUint_.get();
            *reinterpret_cast<unsigned int *>(buf_.get() + offset) = static_cast<char>(r);
            offset++;
            remaining--;
        }
        assert(offset == config_.blockSize() * sizeB);
        assert(remaining == 0);
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

        RandomDataWriter rdw(config);
        rdw.run();
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
