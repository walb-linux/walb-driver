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

#include "util.hpp"
#include "aio_util.hpp"

#include "walb/walb.h"

/**
 * Wlcat command configuration.
 */
class Config
{
private:
    std::string deviceName_;
    u64 lsid0_;
    u64 lsid1_;

public:
    Config(int argc, char* argv[]) {

        if (argc != 4) {
            throw RT_ERR("Specify just 3 values.");
        }
        deviceName_ = std::string(argv[1]);
        lsid0_ = static_cast<u64>(::atoll(argv[2]));
        lsid1_ = static_cast<u64>(::atoll(argv[3]));

        if (lsid0_ > lsid1_) {
            throw RT_ERR("Invalid lsid range.");
        }
    }

    const char* deviceName() const { return deviceName_.c_str(); }
    u64 lsid0() const { return lsid0_; }
    u64 lsid1() const { return lsid1_; }
};

/**
 * To read WalB log.
 */
class WalbLogRead
{
private:
    const Config& config_;
    walb::util::BlockDevice bd_;
    const size_t blockSize_;
    const size_t queueSize_;
    walb::aio::Aio aio_;
    walb::util::BlockBuffer bb_;
    u64 lsid_;

    class LsidToOffset {
        /* now editing */
    };

public:
    WalbLogRead(const Config& config, size_t bufferSize)
        : config_(config)
        , bd_(config.deviceName(), O_RDONLY | O_DIRECT)
        , blockSize_(getBlockSize(bd_.getFd()))
        , queueSize_(getQueueSize(bufferSize, blockSize_))
        , aio_(bd_.getFd(), queueSize_)
        , bb_(queueSize_, blockSize_)
        , lsid_(config.lsid0()) {

        LOGn("blockSize %zu\n", blockSize_); //debug
    }

    bool read(int outFd) {

        if (outFd <= 0) {
            throw RT_ERR("outFd is not valid.");
        }

        //readLogpack(lsid_);

        /* now editing */

    }



private:

    bool readLogpack() {





        /* now editing */
        return false;
    }

    static int openDevice(const char* deviceName) {

        int fd = ::open(deviceName, O_RDONLY | O_DIRECT);
        if (fd < 0) {
            throw std::runtime_error("Open failed.");
        }

        /* Check the file is the block device. */
        struct stat sb;
        if (::stat(deviceName, &sb) < 0) {
            std::string msg("stat failed: ");
            msg += strerror(errno);
            throw std::runtime_error(msg);
        }

        if ((sb.st_mode & S_IFMT) != S_IFBLK) {
            throw RT_ERR("%s is not a block device.", deviceName);
        }

        return fd;
    }

    static unsigned int getBlockSize(int fd) {

        unsigned int pbs;

        assert(fd > 0);

        if (::ioctl(fd, BLKPBSZGET, &pbs) < 0) {
            throw RT_ERR("Getting physical block size failed.");
        }
        assert(pbs > 0);

        return pbs;
    }

    static size_t getQueueSize(size_t bufferSize, size_t blockSize) {

        size_t qs = bufferSize / blockSize;
        if (qs == 0) {
            throw RT_ERR("Queue size is must be positive.");
        }
        return qs;
    }

};

const size_t KILO = 1024;
const size_t MEGA = KILO * 1024;
const size_t BUFFER_SIZE = 16 * MEGA;

int main(int argc, char* argv[])
{
    int ret = 0;

    try {
        Config config(argc, argv);
        WalbLogRead wlRead(config, BUFFER_SIZE);

    } catch (std::runtime_error& e) {
        LOGe("Error: %s\n", e.what());
        ret = 1;

    } catch (...) {
        LOGe("Caught other error.\n");
        ret = 1;
    }

    return ret;
}

/* end of file. */
