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
#include <queue>
#include <memory>

#include <unistd.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

#include "util.hpp"
#include "walb_util.hpp"
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
    walb::util::WalbSuperBlock super_;
    const size_t blockSize_;
    const size_t queueSize_;
    walb::aio::Aio aio_;
    //walb::util::BlockBuffer bb_;
    walb::util::BlockAllocator ba_;
    std::queue<std::shared_ptr<char> > blkPtrQueue_;
    unsigned int nPending_;
    u64 lsid_;
    u64 aheadLsid_;

public:
    WalbLogRead(const Config& config, size_t bufferSize)
        : config_(config)
        , bd_(config.deviceName(), O_RDONLY | O_DIRECT)
        , super_(bd_)
        , blockSize_(bd_.getPhysicalBlockSize())
        , queueSize_(getQueueSizeStatic(bufferSize, blockSize_))
        , aio_(bd_.getFd(), queueSize_)
          //, bb_(queueSize_ * 2, blockSize_)
        , ba_(blockSize_, blockSize_)
        , blkPtrQueue_()
        , nPending_(0)
        , lsid_(config.lsid0())
        , aheadLsid_(lsid_) {

        LOGn("blockSize %zu\n", blockSize_); //debug
    }

    ~WalbLogRead() {
        while (nPending_ > 0) {
            aio_.waitOne();
            nPending_--;
        }
    }

    bool read(int outFd) {
        if (outFd <= 0) {
            throw RT_ERR("outFd is not valid.");
        }

        while (lsid_ < config_.lsid1()) {
            walb::util::WalbLogpack pack = readLogpack();
            //bb_.free(pack.totalIoSize() + 1);

            // for (size_t i = 0; i < pack.totalIoSize(); i++) {
            //     blkPtrQueue_.pop();
            // }

            //::printf("freeblocks: %zu\n", bb_.getNumFreeBlocks());
            ::printf("%" PRIu64"\n", pack.header().logpack_lsid);
        }

        return false;
        /* now editing */
    }

private:

    walb::util::WalbLogpack readLogpack() {

        walb::util::WalbLogpack pack(blockSize_, super_.getLogChecksumSalt());

        std::queue<walb::aio::AioData> aioDataQ;
        u64 lsid = readBlocks(1, aioDataQ);
        walb::aio::AioData &aiod = aioDataQ.front();
        struct walb_logpack_header *logh =
            (struct walb_logpack_header *)aiod.buf;
        pack.setHeader(logh);
        aioDataQ.pop();

        if (!pack.isHeaderValid()) {
            throw RT_ERR("invalid logpack header.");
        }
        if (pack.header().logpack_lsid != lsid) {
            throw RT_ERR("logpack %" PRIu64" is not the expected one %" PRIu64".",
                         pack.header().logpack_lsid, lsid);
        }

        int nBlocks = pack.totalIoSize();
        readBlocks(nBlocks, aioDataQ);
        for (int i = 0; i < nBlocks; i++) {
            walb::aio::AioData &aiod = aioDataQ.front();
            pack.addBlock((u8 *)aiod.buf);
            aioDataQ.pop();
        }
        assert(aioDataQ.empty());

#if 1
        pack.print(); // debug
#endif

        if (!pack.isDataValid()) {
            throw RT_ERR("invalid logpack data.");
        }
        return std::move(pack);
    }

    u64 readBlocks(size_t nr, std::queue<walb::aio::AioData>& aioDataQueue) {
        size_t beforeSize = aioDataQueue.size();
        if (nr > queueSize_) {
            throw RT_ERR("Number of requests exceeds the buffer size.");
        }
        readAhead();
        aio_.wait(nr, aioDataQueue);
        size_t afterSize = aioDataQueue.size();
        assert(afterSize - beforeSize == nr);
        nPending_ -= nr;
        readAhead();

        u64 ret = lsid_;
        lsid_ += nr;
        return ret;
    }

    void readAhead() {
        size_t nio = 0;
        while (nPending_ < queueSize_) {
            off_t oft = super_.getOffsetFromLsid(aheadLsid_) * blockSize_;
            //char *buf = bb_.alloc();
            std::shared_ptr<char> p = ba_.alloc();
            //::printf("nFreeBlocks: %zu\n", bb_.getNumFreeBlocks()); //debug
            bool ret = aio_.prepareRead(oft, blockSize_, p.get());
            assert(ret);
            blkPtrQueue_.push(p);
            nio++;
            aheadLsid_++;
            nPending_++;
        }
        if (nio > 0) {
            aio_.submit();
        }
    }

    static size_t getQueueSizeStatic(size_t bufferSize, size_t blockSize) {
        size_t qs = bufferSize / blockSize;
        if (qs == 0) {
            throw RT_ERR("Queue size is must be positive.");
        }
        return qs;
    }
};

const size_t KILO = 1024;
const size_t MEGA = KILO * 1024;
const size_t BUFFER_SIZE = 32 * MEGA;

int main(int argc, char* argv[])
{
    int ret = 0;

    try {
        Config config(argc, argv);
        WalbLogRead wlRead(config, BUFFER_SIZE);
        wlRead.read(1);

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
