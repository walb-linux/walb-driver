/**
 * @file
 * @brief Simple binary diff.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <string>
#include <vector>
#include <stdexcept>
#include <getopt.h>

#include "util.hpp"
#include "fileio.hpp"

/**
 * Command line configuration.
 */
class Config
{
private:
    unsigned int blockSize_;
    bool isVerbose_;
    bool isHelp_;
    std::vector<std::string> args_;
public:
    Config(int argc, char* argv[])
        : blockSize_(512)
        , isVerbose_(false)
        , isHelp_(false)
        , args_() {
        parse(argc, argv);
    }

    const std::string& filePath1() const { return args_[0]; }
    const std::string& filePath2() const { return args_[1]; }
    unsigned int blockSize() const { return blockSize_; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }

    static void printHelp() {
        ::printf("%s", generateHelpString().c_str());
    }

    void check() const {
        if (args_.size() < 2) {
            throwError("Specify two files.");
        }
        if (blockSize_ == 0) {
            throwError("Block size must be positive integer.");
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
                blockSize_ = ::atoll(optarg);
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
            "bdiff: Show block diff.\n"
            "Usage: bdiff [options] FILE1 FILE2\n"
            "Options:\n"
            "  -b, --blockSize SIZE:  block size in bytes (default: 512)\n"
            "  -v, --verbose:         verbose messages to stderr.\n"
            "  -h, --help:            show this message.\n");
    }
};

/**
 * RETURN:
 *   Number of different blocks.
 */
uint64_t checkBlockDiff(Config& config)
{
    cybozu::util::FileOpener f1(config.filePath1(), O_RDONLY);
    cybozu::util::FileOpener f2(config.filePath2(), O_RDONLY);
    cybozu::util::FdReader fdr1(f1.fd());
    cybozu::util::FdReader fdr2(f2.fd());

    const unsigned int bs = config.blockSize();
    std::unique_ptr<char> p1(new char[bs]);
    std::unique_ptr<char> p2(new char[bs]);
#if 0
    ::printf("%d\n%d\n", f1.fd(), f2.fd());
#endif

    uint64_t nDiffer = 0;
    uint64_t nChecked = 0;
    try {
        while (true) {
            fdr1.read(p1.get(), bs);
            fdr2.read(p2.get(), bs);
            if (::memcmp(p1.get(), p2.get(), bs) != 0) {
                nDiffer++;
                if (config.isVerbose()) {
                    ::printf("block %" PRIu64 " differ\n", nChecked);
                    cybozu::util::printByteArray(p1.get(), bs);
                    cybozu::util::printByteArray(p2.get(), bs);
                }
            }
            nChecked++;
        }
    } catch (cybozu::util::EofError& e) {
    }

    f1.close();
    f2.close();
    ::printf("%" PRIu64 "/%" PRIu64 " differs\n",
             nDiffer, nChecked);

    return nDiffer;
}

int main(int argc, char* argv[])
{
    int ret = 1;

    try {
        Config config(argc, argv);
        if (config.isHelp()) {
            Config::printHelp();
            return 0;
        }
        config.check();

        if (checkBlockDiff(config) == 0) {
            ret = 0;
        }
    } catch (Config::Error& e) {
        ::fprintf(::stderr, "Command line error: %s\n\n", e.what());
        Config::printHelp();
        ret = 1;
    } catch (std::runtime_error& e) {
        ::fprintf(::stderr, "Error: %s\n", e.what());
        ret = 1;
    } catch (std::exception& e) {
        ::fprintf(::stderr, "Exception: %s\n", e.what());
        ret = 1;
    } catch (...) {
        ::fprintf(::stderr, "Caught other error.\n");
        ret = 1;
    }
    return ret;
}
