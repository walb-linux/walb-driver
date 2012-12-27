/**
 * @file
 * @brief Redo walb log.
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
#include <multimap>

#include <unistd.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

#include "util.hpp"
#include "walb_util.hpp"
#include "aio_util.hpp"

#include "walb/walb.h"

/**
 * Wlredo command configuration.
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
 * Block data.
 */
struct Block
{
    u64 lsid;
    std::shared_ptr<u8> ptr;

    Block(u64 lsid, std::shared_ptr<u8> ptr)
        : lsid(lsid), ptr(ptr) {}

    explicit Block(std::shared_ptr<u8> ptr)
        : lsid(0), ptr(ptr) {}

    explicit Block(const Block &rhs)
        : lsid(rhs.lsid), ptr(rhs.ptr) {}

    void print(::FILE *p) const {
        ::fprintf(p, "Block lsid %" PRIu64" ptr %p",
                  lsid, ptr.get());
    }
};

/**
 * Io data.
 */
struct Io
{
    off_t offset; // [bytes].
    size_t size; // [bytes].
    unsigned int aioKey;
    bool done;
    std::deque<std::shared_ptr<u8> > blocks;
    unsigned int nOverlapping; // To serialize overlapping IOs.

    Io(off_t offset, size_t size)
        : offset(offset), size(size)
        , aioKey(0), done(false), blocks()
        , nOverlapping(0) {}

    std::shared_ptr<u8> ptr() {
        return blocks.front();
    }

    template<typename T>
    T* firstPtr() {
        return reinterpret_cast<T *>(blocks.front().get());
    }


    bool empty() const {
        return blocks.empty();
    }

    void print(::FILE *p) const {
        ::fprintf(p, "IO offset: %zu size: %zu aioKey: %u done: %d\n",
                  offset, size, aioKey, done ? 1 : 0);
        for (auto &b : blocks) {
            ::fprintf(p, " block %p\n", b.get());
        }
    }
};

typedef std::shared_ptr<Io> IoPtr;

/**
 * This class can merge the last IO in the queue
 * and the added IO into a large IO.
 */
class IoQueue {
private:
    std::deque<IoPtr> ioQ_;
    const size_t blockSize_;
    static constexpr size_t maxIoSize_ = 1024 * 1024;

public:
    explicit IoQueue(size_t blockSize)
        : ioQ_(), blockSize_(blockSize) {}

    void add(IoPtr iop) {
        if (iop.get() == nullptr) {
            /* Do nothing. */
            return;
        }
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
    bool tryMerge(IoPtr io0, IoPtr io1) {

        //io0->print(::stderr); //debug
        //io1->print(::stderr); //debug

        assert(!io1->empty());
        if (io0->empty()) {
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
        u8 *p0 = io0->blocks.back().get();
        u8 *p1 = io1->blocks.front().get();
        if (p0 + blockSize_ != p1) {
            //::fprintf(::stderr, "buffer mismatch\n"); //debug
            return false;
        }

        /* Merge. */
        io0->size += io1->size;
        while (!io1->empty()) {
            std::shared_ptr<u8> p = io1->blocks.front();
            io0->blocks.push_back(p);
            io1->blocks.pop_front();
        }
        return true;
    }
};

/**
 * IO(s) must be FIFO.
 */
class OverlappingData
{
private:
    std::multimap<off_t, IoPtr> mmap_;
    size_t maxSize_;

public:
    OverlappingData()
        : mmap_(), maxSize_(0) {}

    void ins(IoPtr iop) {

        std::multimap::iterator i =
             mmap_.lower_bound(iop->offset - maxSize_)




    }

    void del(IoPtr iop, std::queue<IoPtr> &ioQ) {






    }
};

/**
 * To apply walb log.
 */
class WalbLogWrite
{
private:
    const Config& config_;
    walb::util::BlockDevice bd_;
    const size_t blockSize_;
    const size_t queueSize_;
    walb::aio::Aio aio_;
    walb::util::BlockAllocator<u8> ba_;
    walb::util::WalbLogFileHeader wh_;
    const bool isDiscardSupport_;

    std::queue<IoPtr> ioQ_;
    size_t nPendingBlocks_;

    class InvalidLogpackData : public std::exception {
    public:
        virtual const char *what() const noexcept {
            return "invalid logpack data.";
        }
    };

    typedef std::shared_ptr<walb::util::WalbLogpackData> LogDataPtr;

public:
    WalbLogWrite(const Config& config, size_t bufferSize, bool isDiscardSupport = false)
        : config_(config)
        , bd_(config.deviceName(), O_RDWR | O_DIRECT)
        , blockSize_(bd_.getPhysicalBlockSize())
        , queueSize_(getQueueSizeStatic(bufferSize, blockSize_))
        , aio_(bd_.getFd(), queueSize_)
        , ba_(queueSize_ * 2, blockSize_, blockSize_)
        , wh_()
        , isDiscardSupport_(isDiscardSupport)
        , ioQ_()
        , nPendingBlocks_(0) {
        //LOGn("blockSize %zu\n", blockSize_); //debug
    }

    ~WalbLogWrite() {
        while (!ioQ_.empty()) {
            IoPtr p = ioQ_.front();
            ioQ_.pop();
            try {
                aio_.waitFor(p->aioKey);
            } catch (...) {}
        }
    }

    /**
     * Read logs from inFd and apply them to the device.
     */
    void readAndApply(int inFd) {
        if (inFd < 0) {
            throw RT_ERR("inFd is not valid.");
        }

        walb::util::FdReader fdr(inFd);

        /* Read walblog header. */
        wh_.read(inFd);
        if (!wh_.isValid()) {
            throw RT_ERR("WalbLog header invalid.");
        }

        if (!canApply()) {
            throw RT_ERR("This walblog can not be applied to the device.");
        }

        /* now editing */
        while (true) {
            try {
                Block b = readBlock(fdr);
                walb::util::WalbLogpackHeader logh(b.ptr, blockSize_, salt());
                if (!logh.isValid()) {
                    break;
                }
                for (size_t i = 0; i < logh.nRecords(); i++) {
                    LogDataPtr logd = allocLogData(logh, i);
                    readLogpackData(*logd);
                    createIoAndSubmit(*logd);
                }

            } catch (walb::util::EofError &e) {
                break;
            } catch (InvalidLogpackData &e) {
                break;
            }
        } // while (true)

        /* Wait for all pending IOs. */
        waitForAllPendingIos();

        /* Sync device. */
        bd_.fdatasync();
    }

private:
    bool canApply() const {
        const struct walblog_header &h = wh_.header();
        if (h.physical_bs < blockSize_ || h.physical_bs % blockSize_ != 0) {
            LOGe("Physical block size differ %u %u.\n", h.physical_bs);
            return false;
        }
        return true;
    }

    u32 salt() const {
        return wh_.header().log_checksum_salt;
    }

    LogDataPtr allocLogData(walb::util::WalbLogpackHeader &logh, size_t i) {
        return LogDataPtr(new walb::util::WalbLogpackData(logh, i));
    }

    /**
     * Read a logpack data.
     */
    void readLogpackData(walb::util::WalbLogpackData &logd, FdReader &fdr) {
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
     * Read a block data from a fd reader.
     */
    Block readBlock(FdReader& fdr) {
        Block b(0, bs_.alloc());
        if (b.ptr.get() == nullptr) {
            throw RT_ERR("allocate failed.");
        }
        char *p = reinterpret_cast<char *>(b.ptr.get());
        fdr.read(p, blockSize_);
        return b;
    }

    /**
     * Create an IO.
     */
    IoPtr createIo(off_t offset, size_t size, std::shared_ptr<u8> block) {
        IoPtr p(new Io(offset, size));
        p->blocks.push_back(block);
        return p;
    }

    void executeDiscard(walb::util::WalbLogpackData &logd) {

        /* Wait for all IO done. */

        /* Issue the corresponding discard IO. */

        /* now editing */
    }

    /**
     *
     */
    void insertToOverlappingData(IoPtr iop) {

        /* now editing */
    }

    /**
     * @iop iop to be deleted.
     * @ioQ All iop(s) will be added where iop->nOverlapping became 0.
     */
    void deleteFromOverlappingData(IoPtr iop, std::queue<IoPtr> ioQ) {



        /* now editing */
    }

    /**
     * Wait for IOs in order to submit an IO with 'nr' blocks.
     * @nr <= queueSize_.
     *
     * You must keep nPendingBlocks_ <= queueSize_ always.
     */
    void waitForBlocks(unsigned int nr) {
        assert(nr <= queueSize_);
        std::queue<IoPtr> ioQ;

        while ((nPendingBlocks_ + nr > queueSize_) || !ioQ.empty()) {
            /* Wait for a IO. */
            if (!ioQ_.empty()) {
                IoPtr iop = ioQ_.front();
                size_t nBlocks = bytesToPb(iop->size);
                ioQ_.pop();
                assert(iop->nOverlapping == 0);
                assert(iop->aioKey != 0);
                aio_.waitFor(iop->aioKey);
                deleteFromOverlappingData(iop, ioQ);
                nDone += nBlocks;
                nPendingBlocks_ -= nBlocks;
            }
            /* Submit overlapping IOs. */
            size_t nIo = 0;
            while (!ioQ.empty()) {
                IoPtr iop = ioQ.front();
                size_t nBlocks = bytesToPb(iop->size);
                if (nPendingBlocks_ + nBlocks > queueSize_) {
                    break;
                }
                ioQ.pop();
                assert(iop->nOverlapping == 0);
                iop->aioKey = aio_.prepareWrite(iop->offset, iop->size, iop->firstPtr<char>());
                ioQ_.push(iop);
                nIo++;
                nDone -= nBlocks;
                nPendingBlocks_ += nBlocks;
            }
            if (nIo > 0) {
                aio_.submit();
            }
        }

        /* Final properties. */
        assert(ioQ.empty());
        assert(nPendingBlocks_ + nr <= queueSize_);
    }

    void waitForAllPendingIos() {
        while (!ioQ_.empty()) {
            waitForBlocks(queueSize_);
        }
    }

    unsigned int bytesToPb(unsigned int bytes) const {
        assert(bytes % LOGICAL_BLOCK_SIZE == 0);
        unsigned int lb = bytes / LOGICAL_BLOCK_SIZE;
        return ::capacity_pb(blockSize_, lb);
    }

    /**
     * Create related IOs and submit it.
     */
    void createIoAndSubmit(walb::util::WalbLogpackData &logd) {
        assert(logd.isExist());
        if (logd.isPadding()) {
            /* Do nothing. */
            return;
        }

        if (logd.isDiscard()) {
            if (isDiscardSupport_) {
                executeDiscard(logd);
            }
            return;
        }

        /* Wait for IOs completed while the queue will overflow. */
        while (nPendingBlocks_ + logd.ioSizePb() > queueSize_) {
            assert(!ioQ_.empty());
            IoPtr iop = ioQ_.front();
            ioQ_.pop();
            aio_.waitFor(iop->aioKey);
            nPendingBlocks_ -= bytesToPb(iop->size);
        }

        /* Create IOs. */
        IoQueue ioQ(blockSize_);
        size_t remaining = logd.ioSizeLb() * LOGICAL_BLOCK_SIZE;
        off_t off = static_cast<off_t>(logd.offset());
        size_t nBlocks = 0;
        for (size_t i = 0; i < logd.ioSizePb(); i++) {
            std::shared_ptr<u8> p = logd.getBlock(i);
            nBlocks++;
            if (remaining >= blockSize_) {
                IoPtr iop = createIo(off, blockSize_, p);
                ioQ.add(iop);
                off += blockSize_;
                remaining -= blockSize_;
            } else {
                IoPtr iop = createIo(off, remaining, p);
                ioQ.add(iop);
                off += remaining;
                remaining = 0;
            }
        }
        assert(remaining == 0);
        assert(nBlocks > 0);
        nPendingBlocks_ += nBlocks;
        assert(nPendingBlock_ <= queueSize_);

        /* Submit IOs. */
        size_t nIo = 0;
        while (!ioQ.empty()) {
            IoPtr iop = ioQ.front();
            ioQ.pop();
            insertToOverlappingData(iop);
            if (iop->nOverlapping == 0) {
                iop->aioKey = aio_.prepareWrite(iop->offset, iop->size, iop->firstPtr<char>());
                assert(iop->aioKey > 0);
                ioQ_.push(iop);
                nIo++;
            }
        }
        if (nIo > 0) {
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
        Config config(argc, argv);
        WalbLogApplyer wlApp(config, BUFFER_SIZE);
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
