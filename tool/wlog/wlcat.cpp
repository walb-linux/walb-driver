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
    walb::util::BlockAllocator<u8> ba_;

    struct Block {
        u64 lsid;
        std::shared_ptr<u8> ptr;
        Block(u64 lsid, std::shared_ptr<u8> ptr)
            : lsid(lsid), ptr(ptr) {}
    };

    std::queue<Block> ioQ_;
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
        , ioQ_()
        , aheadLsid_(config.lsid0()) {

        LOGn("blockSize %zu\n", blockSize_); //debug
    }

    ~WalbLogRead() {
        while (!ioQ_.empty()) {
            Block &b = ioQ_.front();
            try {
                aio_.waitFor(reinterpret_cast<char *>(b.ptr.get()));
            } catch (...) {}
            ioQ_.pop();
        }
    }

    bool read(int outFd) {
        if (outFd <= 0) {
            throw RT_ERR("outFd is not valid.");
        }

        u64 lsid = config_.lsid0();

        while (lsid < config_.lsid1()) {

            readAhead();

            walb::util::WalbLogpackHeader logh = readLogpackHeader();
            //logh.print(); //debug

            for (size_t i = 0; i < logh.nRecords(); i++) {
                walb::util::WalbLogpackData logd(logh, i);
                readLogpackData(logd);
            }

            //bb_.free(pack.totalIoSize() + 1);

            // for (size_t i = 0; i < pack.totalIoSize(); i++) {
            //     blkPtrQueue_.pop();
            // }

            //::printf("freeblocks: %zu\n", bb_.getNumFreeBlocks());
            ::printf("%" PRIu64"\n", logh.header().logpack_lsid);
        }

        return false;
        /* now editing */
    }

private:

    walb::util::WalbLogpackHeader readLogpackHeader() {
        auto block = readBlock();
        walb::util::WalbLogpackHeader logh(
            block.ptr, super_.getPhysicalBlockSize(), super_.getLogChecksumSalt());

        if (!logh.isValid()) {
            throw RT_ERR("invalid logpack header.");
        }
        if (logh.header().logpack_lsid != block.lsid) {
            throw RT_ERR("logpack %" PRIu64" is not the expected one %" PRIu64".",
                         logh.header().logpack_lsid, block.lsid);
        }

        /* now editing */
        //return std::move(logh);
        return logh;
    }

    void readLogpackData(walb::util::WalbLogpackData& logd) {
        if (!logd.hasData()) { return; }

        std::queue<walb::aio::AioData> aioDataQ;
        //::printf("ioSizePb: %u\n", logd.ioSizePb()); //debug

        for (size_t i = 0; i < logd.ioSizePb(); i++) {
            auto block = readBlock();
            logd.addBlock(block.ptr);
        }

        if (!logd.isValid()) {
            throw RT_ERR("invalid logpack data.");
        }
    }

    /**
     * Read next block.
     */
    Block readBlock() {
        auto b = ioQ_.front();
        char *p = reinterpret_cast<char *>(b.ptr.get());
        auto *aiod = aio_.waitFor(p);
        assert(aiod->buf == p);
        ioQ_.pop();
        return b;
    }

    void readAhead() {
        size_t nio = 0;
        while (ioQ_.size() < queueSize_) {
            off_t oft = super_.getOffsetFromLsid(aheadLsid_) * blockSize_;
            //char *buf = bb_.alloc();
            Block b(aheadLsid_, ba_.alloc());
            bool ret = aio_.prepareRead(
                oft, blockSize_,
                reinterpret_cast<char *>(b.ptr.get()));
            assert(ret);
            ioQ_.push(b);
            nio++;
            aheadLsid_++;
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
const size_t BUFFER_SIZE = 1 * MEGA;

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
