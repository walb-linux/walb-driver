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
        walb::aio::AioDataPtr aiod;
        Block(u64 lsid, std::shared_ptr<u8> ptr)
            : lsid(lsid), ptr(ptr), aiod() {}
    };

    std::queue<Block> ioQ_;
    u64 aheadLsid_;

    class InvalidLogpackData : public std::exception
    {
    public:
        virtual const char *what() const noexcept {
            return "invalid logpack data.";
        }
    };

public:
    WalbLogRead(const Config& config, size_t bufferSize)
        : config_(config)
        , bd_(config.deviceName(), O_RDONLY | O_DIRECT)
        , super_(bd_)
        , blockSize_(bd_.getPhysicalBlockSize())
        , queueSize_(getQueueSizeStatic(bufferSize, blockSize_))
        , aio_(bd_.getFd(), queueSize_)
        , ba_(queueSize_ * 2, blockSize_, blockSize_)
        , ioQ_()
        , aheadLsid_(config.lsid0()) {

        LOGn("blockSize %zu\n", blockSize_); //debug
    }

    ~WalbLogRead() {
        //::printf("~WalbLogRead\n"); //debug
        while (!ioQ_.empty()) {
            Block &b = ioQ_.front();
            try {
                aio_.waitFor(b.aiod->key);
            } catch (...) {}
            ioQ_.pop();
        }
    }

    void read(int outFd) {
        if (outFd <= 0) {
            throw RT_ERR("outFd is not valid.");
        }

        walb::util::FdWriter writer(outFd);

        /* Write walblog header. */
        walb::util::WalbLogFileHeader wh;
        wh.init(super_.getPhysicalBlockSize(), super_.getLogChecksumSalt(),
                super_.getUuid(), config_.lsid0(), config_.lsid1());
#if 1
        wh.write(outFd);
#endif

        u64 lsid = config_.lsid0();
        while (lsid < config_.lsid1()) {
            bool isEnd = false;

            readAhead();
            walb::util::WalbLogpackHeader logh = readLogpackHeader();
            //logh.print(); //debug

            std::queue<std::shared_ptr<walb::util::WalbLogpackData> > q;

            /* Read a logpack */
            for (size_t i = 0; i < logh.nRecords(); i++) {
                readAhead();
                std::shared_ptr<walb::util::WalbLogpackData> p(
                    new walb::util::WalbLogpackData(logh, i));
                try {
                    readLogpackData(*p);
                    q.push(p);
                } catch (InvalidLogpackData& e) {
                    logh.shrink(i);
                    isEnd = true;
                    break;
                }
            }

            /* Write the logpack. */
            if (logh.nRecords() > 0) {
                /* Write the header. */
                auto p = logh.getBlock();
#if 1
                writer.write(reinterpret_cast<char *>(p.get()), logh.pbs());
#endif
                /* Write the IO data. */
                while (!q.empty()) {
                    auto logd = q.front();
                    q.pop();
                    if (!logd->hasData()) {
                        continue;
                    }
                    for (size_t i = 0; i < logd->ioSizePb(); i++) {
                        auto p = logd->getBlock(i);
#if 1
                        writer.write(reinterpret_cast<char *>(p.get()), logh.pbs());
#endif
                    }
                }
            }

            //::printf("%" PRIu64"\n", logh.header().logpack_lsid);

            if (isEnd) {
                break;
            }
            lsid = logh.nextLogpackLsid();
        }
#if 1
        writer.fdatasync();
#endif
    }

private:
    /**
     * Read a logpack header.
     */
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
        return logh;
    }

    /**
     * Read a logpack data.
     */
    void readLogpackData(walb::util::WalbLogpackData& logd) {
        if (!logd.hasData()) { return; }
        //::printf("ioSizePb: %u\n", logd.ioSizePb()); //debug
        for (size_t i = 0; i < logd.ioSizePb(); i++) {
            auto block = readBlock();
            logd.addBlock(block.ptr);
        }
        if (!logd.isValid()) {
            throw InvalidLogpackData();
        }
    }

    /**
     * Read next block.
     */
    Block readBlock() {
        if (ioQ_.empty()) {
            throw RT_ERR("ioQ empty.");
        }
        auto b = ioQ_.front();
        //::printf("aioKey: %u\n", b.aioKey); //debug
        auto aiod = aio_.waitFor(b.aiod->key);
        assert(aiod->key == b.aiod->key);
        assert(aiod->buf == reinterpret_cast<char *>(b.ptr.get()));
        assert(aiod->done);
        ioQ_.pop();
        return b;
    }

    void readAhead() {
        size_t nio = 0;
        while (ioQ_.size() < queueSize_) {
            off_t oft = super_.getOffsetFromLsid(aheadLsid_) * blockSize_;
            //char *buf = bb_.alloc();
            Block b(aheadLsid_, ba_.alloc());
            walb::aio::AioDataPtr p = aio_.prepareRead(
                oft, blockSize_,
                reinterpret_cast<char *>(b.ptr.get()));
            if (p.get() == nullptr) {
                throw RT_ERR("allocate failed.");
            }
            b.aiod = p;
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

constexpr size_t KILO = 1024;
constexpr size_t MEGA = KILO * 1024;
constexpr size_t BUFFER_SIZE = 4 * MEGA;

int main(int argc, char* argv[])
{
    int ret = 0;

    try {
#if 0
        walb::aio::testAioDataAllocator();
#endif
        Config config(argc, argv);
        WalbLogRead wlRead(config, BUFFER_SIZE);
        wlRead.read(1);

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
