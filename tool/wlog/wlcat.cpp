/**
 * @file
 * @brief Cat walb log device.
 * @author HOSHINO Takashi
 *
 * (C) 2012 Cybozu Labs, Inc.
 */
#include <string>
#include <cstdio>
#include <stdexcept>

#include <unistd.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

#include "aio_util.hpp"

#include "walb/walb.h"
// #include "util.h"

#if 0
#include <cstdint>
typedef uint64_t u64;
typedef uint32_t u32;
typedef uint16_t u16;
typedef uint8_t u8;
#endif


class Config
{
private:
    std::string deviceName_;
    u64 lsid0_;
    u64 lsid1_;

public:
    Config(int argc, char* argv[]) {

        if (argc != 4) {
            throw std::runtime_error("Specify just 3 values.");
        }
        deviceName_ = std::string(argv[1]);
        lsid0_ = static_cast<u64>(::atoll(argv[2]));
        lsid1_ = static_cast<u64>(::atoll(argv[3]));

        if (lsid0_ > lsid1_) {
            throw std::runtime_error("Invalid lsid range.");
        }
    }

    const char* deviceName() const { return deviceName_.c_str(); }
    u64 lsid0() const { return lsid0_; }
    u64 lsid1() const { return lsid1_; }
};

class WalblogCat
{
    const Config& config_;
    const int fd_;
    const size_t blockSize_;
    const size_t bufferSize_; // [block].

public:
    WalblogCat(const Config& config, size_t bufferSize)
        : config_(config)
        , fd_(openDevice(config.deviceName()))
        , blockSize_(getBlockSize(fd_))
        , bufferSize_(bufferSize) {

        ::printf("blockSize %zu\n", blockSize_);
    }

    ~WalblogCat() {

        if (fd_ > 0) {
            int ret = ::close(fd_);
            if (ret) {
                ::fprintf(::stderr, "Close failed.\n");
            }
        }
    }

private:
    static int openDevice(const char* deviceName) {

        int fd = ::open(deviceName, O_RDONLY | O_DIRECT);
        if (fd < 0) {
            throw std::runtime_error("Open failed.");
        }

        /* Check the file is the block device. */

        /* now editing */

        return fd;
    }

    static unsigned int getBlockSize(int fd) {

        unsigned int pbs;

        assert(fd > 0);

        if (::ioctl(fd, BLKPBSZGET, &pbs) < 0) {
            throw std::runtime_error("Getting physical block size failed.");
        }
        return pbs;
    }
};

int main(int argc, char* argv[])
{
    int ret = 0;

    try {
        Config config(argc, argv);
        WalblogCat wlCat(config, 32);

    } catch (std::runtime_error& e) {
        ::printf("Error: %s\n", e.what());

    } catch (...) {
        ::printf("Caught other error.\n");
        ret = 1;
    }

    return ret;
}

/* end of file. */
