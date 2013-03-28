/**
 * @file
 * @brief Update walb log header.
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
#include "walb_util.hpp"
#include "aio_util.hpp"

#include "walb/walb.h"

/**
 * Command line configuration.
 */
class Config
{
private:
    bool isBeginLsid_;
    bool isEndLsid_;
    bool isSalt_;
    bool isUuid_;
    uint64_t beginLsid_;
    uint64_t endLsid_;
    uint32_t salt_;
    std::vector<u8> uuid_;
    bool isVerbose_;
    bool isHelp_;
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : isBeginLsid_(false)
        , isEndLsid_(false)
        , isSalt_(false)
        , isUuid_(false)
        , beginLsid_(0)
        , endLsid_(-1)
        , salt_(0)
        , uuid_(UUID_SIZE)
        , isVerbose_(false)
        , isHelp_(false)
        , args_() {
        parse(argc, argv);
    }

    const std::string& inWlogPath() const { return args_[0]; }
    bool isBeginLsid() const { return isBeginLsid_; }
    bool isEndLsid() const { return isEndLsid_; }
    bool isSalt() const { return isSalt_; }
    bool isUuid() const { return isUuid_; }
    uint64_t beginLsid() const { return beginLsid_; }
    uint64_t endLsid() const { return endLsid_; }
    uint32_t salt() const { return salt_; }
    const std::vector<u8>& uuid() const { return uuid_; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }

    void print(::FILE *fp) const {
        ::fprintf(fp,
                  "beginLsid: %" PRIu64 "\n"
                  "endLsid: %" PRIu64 "\n"
                  "salt: %u\n"
                  "uuid: ",
                  beginLsid(), endLsid(), salt());

        for (size_t i = 0; i < UUID_SIZE; i++) {
            ::fprintf(fp, "%02x", uuid()[i]);
        }
        ::fprintf(fp,
                  "\n"
                  "verbose: %d\n"
                  "isHelp: %d\n",
                  isVerbose(), isHelp());
        int i = 0;
        for (const auto& s : args_) {
            ::printf("arg%d: %s\n", i++, s.c_str());
        }
    }

    void print() const { print(::stdout); }

    static void printHelp() {
        ::printf("%s", generateHelpString().c_str());
    }

    void check() const {
        if (args_.empty()) {
            throwError("Specify input wlog path.");
        }
    }

    class Error : public std::runtime_error {
    public:
        explicit Error(const std::string &msg)
            : std::runtime_error(msg) {}
    };

private:
    uint8_t hexchar2uint8(u8 c) const {
        if ('0' <= c && c <= '9') {
            return c - '0';
        }
        if ('a' <= c && c <= 'f') {
            return c - 'a' + 10;
        }
        if ('A' <= c && c <= 'F') {
            return c - 'A' + 10;
        }
        throwError("wrong UUID charactor: %c.", c);
        return 0;
    }

    void setUuid(const std::string &uuidStr) {
        if (uuidStr.size() != 32) {
            throwError("Invalid UUID string.");
        }
        for (size_t i = 0; i < UUID_SIZE; i++) {
            /* ex. "ff" -> 255 */
            uuid_[i] = hexchar2uint8(uuidStr[i * 2]) * 16 +
                hexchar2uint8(uuidStr[i * 2 + 1]);
        }
    }

    /* Option ids. */
    enum Opt {
        BEGIN_LSID = 1,
        END_LSID,
        SALT,
        UUID,
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
                {"uuid", 1, 0, Opt::UUID},
                {"salt", 1, 0, Opt::SALT},
                {"verbose", 0, 0, Opt::VERBOSE},
                {"help", 0, 0, Opt::HELP},
                {0, 0, 0, 0}
            };
            int option_index = 0;
            int c = ::getopt_long(argc, argv, "b:e:u:s:vh", long_options, &option_index);
            if (c == -1) { break; }

            switch (c) {
            case Opt::BEGIN_LSID:
            case 'b':
                isBeginLsid_ = true;
                beginLsid_ = ::atoll(optarg);
                break;
            case Opt::END_LSID:
            case 'e':
                isEndLsid_ = true;
                endLsid_ = ::atoll(optarg);
                break;
            case Opt::SALT:
            case 's':
                isSalt_ = true;
                salt_ = ::atoll(optarg);
                break;
            case Opt::UUID:
            case 'u':
                isUuid_ = true;
                setUuid(optarg);
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
            "Wlupdate: update wlog file header.\n"
            "Usage: wlupdate [options] WLOG_PATH\n"
            "  WLOG_PATH: walb log path. must be seekable.\n"
            "Options:\n"
            "  -b, --beginLsid LSID: begin lsid.\n"
            "  -e, --endLsid LSID:   end lsid.\n"
            "  -s, --salt SALT:      logpack salt.\n"
            "  -u, --uuid UUID:      uuid in hex string.\n"
            "  -v, --verbose:        verbose messages to stderr.\n"
            "  -h, --help:           show this message.\n");
    }
};

class WalbLogUpdater
{
private:
    const Config &config_;

    using LogpackHeader = walb::util::WalbLogpackHeader;
    using LogpackHeaderPtr = std::shared_ptr<LogpackHeader>;
    using LogpackData = walb::util::WalbLogpackData;
    using Block = std::shared_ptr<u8>;

public:
    WalbLogUpdater(const Config &config)
        : config_(config) {}

    void update() {
        cybozu::util::FileOpener fo(config_.inWlogPath(), O_RDWR);
        walb::util::WalbLogFileHeader wh;

        /* Read header. */
        cybozu::util::FdReader fdr(fo.fd());
        fdr.lseek(0, SEEK_SET);
        wh.read(fdr);
        if (!wh.isValid(true)) {
            throw RT_ERR("invalid wlog header.");
        }
        wh.print(::stderr); /* debug */

        /* Update */
        bool updated = false;
        if (config_.isBeginLsid()) {
            updated = true;
            wh.header().begin_lsid = config_.beginLsid();
        }
        if (config_.isEndLsid()) {
            updated = true;
            wh.header().end_lsid = config_.endLsid();
        }
        if (config_.isSalt()) {
            updated = true;
            wh.header().log_checksum_salt = config_.salt();
        }
        if (config_.isUuid()) {
            updated = true;
            ::memcpy(wh.header().uuid, &config_.uuid()[0], UUID_SIZE);
        }

        /* Write header if necessary. */
        if (updated) {
            if (!wh.isValid(false)) {
                throw RT_ERR("Updated header is invalid.");
            }
            cybozu::util::FdWriter fdw(fo.fd());
            fdw.lseek(0, SEEK_SET);
            wh.write(fdw);
            fo.close();
            wh.print(::stderr); /* debug */
        } else {
            ::fprintf(::stderr, "Not updated.\n");
        }
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

        WalbLogUpdater wlUpdater(config);
        wlUpdater.update();

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
