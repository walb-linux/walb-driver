/**
 * @file
 * @brief Read walb log device and archive it.
 * @author HOSHINO Takashi
 *
 * (C) 2012 Cybozu Labs, Inc.
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
class WalbLogReader
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

        Block(const Block &rhs)
            : lsid(rhs.lsid), ptr(rhs.ptr) {}

        void print(::FILE *p) const {
            ::fprintf(p, "Block lsid %" PRIu64" ptr %p",
                      lsid, ptr.get());
        }
    };

    struct Io {
        off_t offset; // [bytes].
        size_t size; // [bytes].
        unsigned int aioKey;
        bool done;
        std::deque<Block> blocks;

        Io(off_t offset, size_t size)
            : offset(offset), size(size)
            , aioKey(0), done(false), blocks() {}

        std::shared_ptr<u8> ptr() {
            return blocks.front().ptr;
        }

        void print(::FILE *p) const {
            ::fprintf(p, "IO offset: %zu size: %zu aioKey: %u done: %d\n",
                      offset, size, aioKey, done ? 1 : 0);
            for (auto &b : blocks) {
                ::fprintf(p, "  ");
                b.print(p);
                ::fprintf(p, "\n");
            }
        }
    };

    typedef std::shared_ptr<Io> IoPtr;

    class IoQueue {
    private:
        std::deque<IoPtr> ioQ_;
        const walb::util::WalbSuperBlock& super_;
        const size_t blockSize_;
        static constexpr size_t maxIoSize_ = 1024 * 1024;

    public:
        explicit IoQueue(const walb::util::WalbSuperBlock& super,
                         size_t blockSize)
            : ioQ_(), super_(super), blockSize_(blockSize) {}

        void addBlock(const Block& block) {
            IoPtr iop = createIo(block);
            if (ioQ_.empty()) {
                ioQ_.push_back(iop);
                return;
            }
            if (tryMerge(ioQ_.back(), iop)) {
                //::fprintf(::stderr, "merged %zu\n", ioQ_.back()->size); //debug
                return;
            }
            ioQ_.push_back(iop);
        }

        IoPtr pop() {
            IoPtr p = ioQ_.front();
            ioQ_.pop_front();
            return p;
        }

        bool empty() const {
            return ioQ_.empty();
        }

        std::shared_ptr<u8> ptr() {
            return ioQ_.front()->ptr();
        }

    private:
        IoPtr createIo(const Block& block) {
            off_t offset = super_.getOffsetFromLsid(block.lsid) * blockSize_;
            IoPtr p(new Io(offset, blockSize_));
            p->blocks.push_back(block);
            return p;
        }

        bool tryMerge(IoPtr io0, IoPtr io1) {

            //io0->print(::stderr); //debug
            //io1->print(::stderr); //debug

            assert(!io1->blocks.empty());
            if (io0->blocks.empty()) {
                io0 = std::move(io1);
                return true;
            }

            /* Check mas io size. */
            if (io0->size + io1->size > maxIoSize_) {
                return false;
            }

            /* Check Io targets and buffers are adjacent. */
            if (io0->offset + static_cast<off_t>(io0->size) != io1->offset) {
                //::fprintf(::stderr, "offset mismatch\n"); //debug
                return false;
            }
            u8 *p0 = io0->blocks.back().ptr.get();
            u8 *p1 = io1->blocks.front().ptr.get();
            if (p0 + blockSize_ != p1) {
                //::fprintf(::stderr, "buffer mismatch\n"); //debug
                return false;
            }

            /* Merge. */
            io0->size += io1->size;
            while (!io1->blocks.empty()) {
                Block &b = io1->blocks.front();
                io0->blocks.push_back(b);
                io1->blocks.pop_front();
            }
            return true;
        }
    };

    std::queue<IoPtr> ioQ_;
    size_t nPendingBlocks_;
    u64 aheadLsid_;

    class InvalidLogpackData : public std::exception
    {
    public:
        virtual const char *what() const noexcept {
            return "invalid logpack data.";
        }
    };

public:
    WalbLogReader(const Config& config, size_t bufferSize)
        : config_(config)
        , bd_(config.deviceName(), O_RDONLY | O_DIRECT)
        , super_(bd_)
        , blockSize_(bd_.getPhysicalBlockSize())
        , queueSize_(getQueueSizeStatic(bufferSize, blockSize_))
        , aio_(bd_.getFd(), queueSize_)
        , ba_(queueSize_ * 2, blockSize_, blockSize_)
        , ioQ_()
        , nPendingBlocks_(0)
        , aheadLsid_(config.lsid0()) {

        //LOGn("blockSize %zu\n", blockSize_); //debug
    }

    ~WalbLogReader() {
        //::printf("~WalbLogReader\n"); //debug
        while (!ioQ_.empty()) {
            IoPtr p = ioQ_.front();
            try {
                aio_.waitFor(p->aioKey);
            } catch (...) {}
            ioQ_.pop();
        }
    }

    /**
     * Read walb log from the device and write to outFd with wl header.
     */
    void catLog(int outFd) {
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
        IoPtr iop = ioQ_.front();
        if (!iop->done) {
            aio_.waitFor(iop->aioKey);
            iop->done = true;
        }
        Block b = iop->blocks.front();
        iop->blocks.pop_front();
        if (iop->blocks.empty()) {
            ioQ_.pop();
        }
        nPendingBlocks_--;
        return b;
    }

    void readAhead() {
        /* Prepare blocks and IOs. */
        IoQueue ioQ(super_, blockSize_);
        while (nPendingBlocks_ < queueSize_) {
            Block b(aheadLsid_, ba_.alloc());
            if (b.ptr.get() == nullptr) {
                throw RT_ERR("allocate failed.");
            }
            ioQ.addBlock(b);
            aheadLsid_++;
            nPendingBlocks_++;
        }

        /* Prepare aio. */
        size_t nio = 0;
        while (!ioQ.empty()) {
            IoPtr iop = ioQ.pop();
            unsigned int key = aio_.prepareRead(
                iop->offset, iop->size,
                reinterpret_cast<char *>(iop->ptr().get()));
            if (key == 0) {
                throw RT_ERR("prpeareRead failed.");
            }
            iop->aioKey = key;
            nio++;
            ioQ_.push(iop);
        }

        /* Submit. */
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
        WalbLogReader wlReader(config, BUFFER_SIZE);
        wlReader.catLog(1);

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
