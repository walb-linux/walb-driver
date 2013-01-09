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
#include <map>
#include <algorithm>

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

public:
    Config(int argc, char* argv[]) {
        if (argc != 2) {
            throw RT_ERR("Specify just a value.");
        }
        deviceName_ = std::string(argv[1]);
    }

    const char* deviceName() const { return deviceName_.c_str(); }
};

using Block = std::shared_ptr<u8>;

class SimpleSequenceIdGenerator
{
private:
    u64 id_;

public:
    SimpleSequenceIdGenerator() : id_(0) {}
    SimpleSequenceIdGenerator(const SimpleSequenceIdGenerator &rhs) = delete;
    SimpleSequenceIdGenerator(SimpleSequenceIdGenerator &&rhs) = delete;
    ~SimpleSequenceIdGenerator() = default;

    u64 operator()() {
        return id_++;
    }
} DefaultSequenceIdGenerator;

/**
 * Io data.
 */
class Io
{
private:
    off_t offset_; // [bytes].
    size_t size_; // [bytes].
    unsigned int aioKey_;
    bool isSubmitted_;
    bool isCompleted_;
    std::deque<Block> blocks_;
    unsigned int nOverlapped_; // To serialize overlapped IOs.
    u64 sequenceId_;

    using IoPtr = std::shared_ptr<Io>;

public:
    Io(off_t offset, size_t size)
        : offset_(offset), size_(size), aioKey_(0)
        , isSubmitted_(false), isCompleted_(false)
        , blocks_(), nOverlapped_(0)
        , sequenceId_(DefaultSequenceIdGenerator()) {}

    Io(off_t offset, size_t size, Block block)
        : Io(offset, size) {
        setBlock(block);
    }

    Io(const Io &rhs) = delete;
    Io(Io &&rhs) = delete;
    Io& operator=(const Io &rhs) = delete;

    Io& operator=(Io &&rhs) {
        offset_ = rhs.offset_;
        size_ = rhs.size_;
        aioKey_ = rhs.aioKey_;
        isSubmitted_ = rhs.isSubmitted_;
        isCompleted_ = rhs.isCompleted_;
        blocks_ = std::move(rhs.blocks_);
        nOverlapped_ = rhs.nOverlapped_;
        sequenceId_ = rhs.sequenceId_;
        return *this;
    }

    off_t offset() const { return offset_; }
    size_t size() const { return size_; }
    bool isSubmitted() const { return isSubmitted_; }
    bool isCompleted() const { return isCompleted_; }
    const std::deque<std::shared_ptr<u8> >& blocks() const { return blocks_; }
    unsigned int& nOverlapped() { return nOverlapped_; }
    unsigned int& aioKey() { return aioKey_; }
    std::shared_ptr<u8> ptr() { return blocks_.front(); }
    Block block() { return ptr(); } /* This is just alias of ptr(). */
    bool empty() const { return blocks().empty(); }
    u64 sequenceId() const { return sequenceId_; }

    template<typename T>
    T* rawPtr() { return reinterpret_cast<T*>(ptr().get()); }
    u8* rawPtr() { return ptr().get(); }

    void setBlock(Block b) {
        assert(blocks_.empty());
        blocks_.push_back(b);
    }

    void submit() {
        assert(!isSubmitted());
        isSubmitted_ = true;
    }

    void complete() {
        assert(!isCompleted());
        isCompleted_ = true;
    }

    void print(::FILE *p) const {
        ::fprintf(p, "IO offset: %zu size: %zu aioKey: %u "
                  "submitted: %d completed: %d\n",
                  offset_, size_, aioKey_,
                  isSubmitted_ ? 1 : 0, isCompleted_ ? 1 : 0);
        for (auto &b : blocks_) {
            ::fprintf(p, " block %p\n", b.get());
        }
    }

    /**
     * Can an IO be merged to this.
     */
    bool canMerge(const IoPtr rhs) const {
        assert(rhs.get() != nullptr);

        /* They must have data buffers. */
        if (blocks_.empty() || rhs->blocks_.empty()) {
            return false;
        }

        /* Check Io targets and buffers are adjacent. */
        if (offset_ + static_cast<off_t>(size_) != rhs->offset_) {
            //::fprintf(::stderr, "offset mismatch\n"); //debug
            return false;
        }

        /* Check buffers are contiguous. */
        u8 *p0 = blocks_.front().get();
        u8 *p1 = rhs->blocks_.front().get();
        return p0 + size_ == p1;
    }

    /**
     * Try merge an IO.
     *
     * RETURN:
     *   true if merged, or false.
     */
    bool tryMerge(IoPtr rhs) {
        if (!canMerge(rhs)) {
            return false;
        }
        size_ += rhs->size_;
        while (!rhs->empty()) {
            std::shared_ptr<u8> p = rhs->blocks_.front();
            blocks_.push_back(p);
            rhs->blocks_.pop_front();
        }
        return true;
    }

    /**
     * RETURN:
     *   true if overlapped.
     */
    bool isOverlapped(const IoPtr rhs) const {
        assert(rhs.get() != nullptr);
        return offset_ + static_cast<off_t>(size_) > rhs->offset_ &&
            rhs->offset_ + static_cast<off_t>(rhs->size_) > offset_;
    }
};

using IoPtr = std::shared_ptr<Io>;

/**
 * This class can merge the last IO in the queue
 * and the added IO into a large IO.
 */
class IoQueue {
private:
    std::deque<IoPtr> ioQ_;
    static const size_t maxIoSize_ = 1024 * 1024; //1MB.

public:
    IoQueue() : ioQ_() {}
    ~IoQueue() = default;

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

    /**
     * Do not call this while empty() == true.
     */
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
    /**
     * MUST REFACTOR.
     * bool canMerge(IoPtr rhs);
     */
    bool tryMerge(IoPtr io0, IoPtr io1) {
        //io0->print(::stderr); //debug
        //io1->print(::stderr); //debug

        /* Replace if empty. */
        assert(!io1->empty());
        if (io0->empty()) {
            *io0 = std::move(*io1);
            return true;
        }

        /* Check mas io size. */
        if (io0->size() + io1->size() > maxIoSize_) {
            return false;
        }

        /* Try merge. */
        return io0->tryMerge(io1);
    }
};

/**
 * In order to serialize overlapped IOs execution.
 * IOs must be FIFO.
 */
class OverlappedData
{
private:
    /*
     * Key: IO offset, Value: Io pointer.
     */
    std::multimap<off_t, IoPtr> mmap_;
    size_t maxSize_;

public:
    OverlappedData()
        : mmap_(), maxSize_(0) {}

    ~OverlappedData() = default;
    OverlappedData(const OverlappedData& rhs) = delete;
    OverlappedData& operator=(const OverlappedData& rhs) = delete;

    /**
     * Insert to the overlapped data.
     *
     * (1) count overlapped IOs.
     * (2) set iop->nOverlapped to the number of overlapped IOs.
     */
    void ins(IoPtr iop) {
        assert(iop.get() != nullptr);

        /* Get search range. */
        off_t key0 = 0;
        if (iop->offset() > static_cast<off_t>(maxSize_)) {
            key0 = iop->offset() - static_cast<off_t>(maxSize_);
        }
        off_t key1 = iop->offset() + iop->size();

        /* Count overlapped IOs. */
        iop->nOverlapped() = 0;
        auto it = mmap_.lower_bound(key0);
        int c = 0;
        while (it != mmap_.end() && it->first < key1) {
            IoPtr p = it->second;
            if (p->isOverlapped(iop)) {
                iop->nOverlapped()++;
            }
            it++;
            c++;
        }
        //::printf("mmap.size %zu c %d\n", mmap_.size(), c); //debug

        /* Insert iop. */
        mmap_.insert(std::make_pair(iop->offset(), iop));

        /* Update maxSize_. */
        if (maxSize_ < iop->size()) {
            maxSize_ = iop->size();
        }
    }

    /**
     * Delete from the overlapped data.
     *
     * (1) Delete from the overlapping data.
     * (2) Decrement the overlapping IOs in the data.
     * (3) IOs where iop->nOverlapped became 0 will be added to the ioQ.
     */
    void del(IoPtr iop, std::queue<IoPtr> &ioQ) {
        assert(iop.get() != nullptr);
        assert(iop->nOverlapped() == 0);

        /* Delete iop. */
        deleteFromMap(iop);

        /* Reset maxSize_ if empty. */
        if (mmap_.empty()) {
            maxSize_ = 0;
        }

        /* Get search range. */
        off_t key0 = 0;
        if (iop->offset() > static_cast<off_t>(maxSize_)) {
            key0 = iop->offset() - static_cast<off_t>(maxSize_);
        }
        off_t key1 = iop->offset() + iop->size();

        /* Decrement nOverlapped of overlapped IOs. */
        auto it = mmap_.lower_bound(key0);
        while (it != mmap_.end() && it->first < key1) {
            IoPtr p = it->second;
            if (p->isOverlapped(iop)) {
                p->nOverlapped()--;
                if (p->nOverlapped() == 0) {
#if 0
                    ::printf("nOverlapped0\t%" PRIu64 "\t%zu\n",
                             static_cast<u64>(p->offset()) >> 9,
                             p->size() >> 9); /* debug */
#endif
                    ioQ.push(p);
                }
            }
            it++;
        }
    }

    bool empty() const {
        return mmap_.empty();
    }

private:
    /**
     * Delete an IoPtr from the map.
     */
    void deleteFromMap(IoPtr iop) {
        auto pair = mmap_.equal_range(iop->offset());
        auto it = pair.first;
        auto it1 = pair.second;
        bool isDeleted = false;
        while (it != mmap_.end() && it != it1) {
            assert(it->first == iop->offset());
            IoPtr p = it->second;
            if (p == iop) {
                mmap_.erase(it);
                isDeleted = true;
                break;
            }
            it++;
        }
        assert(isDeleted);
    }
};

/**
 * To apply walb log.
 */
class WalbLogApplyer
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

    std::queue<IoPtr> ioQ_; /* serialized. */
    std::deque<IoPtr> submitIoQ_; /* ready to submit. */
    /* Number of blocks where the corresponding IO
       is submitted, but not completed. */
    size_t nPendingBlocks_;
    OverlappedData olData_;

    class InvalidLogpackData : public std::exception {
    public:
        virtual const char *what() const noexcept {
            return "invalid logpack data.";
        }
    };

    using LogDataPtr = std::shared_ptr<walb::util::WalbLogpackData>;

public:
    WalbLogApplyer(
        const Config& config, size_t bufferSize, bool isDiscardSupport = false)
        : config_(config)
        , bd_(config.deviceName(), O_RDWR | O_DIRECT)
        , blockSize_(bd_.getPhysicalBlockSize())
        , queueSize_(getQueueSizeStatic(bufferSize, blockSize_))
        , aio_(bd_.getFd(), queueSize_)
        , ba_(queueSize_ * 2, blockSize_, blockSize_)
        , wh_()
        , isDiscardSupport_(isDiscardSupport)
        , ioQ_()
        , submitIoQ_()
        , nPendingBlocks_(0)
        , olData_() {}

    ~WalbLogApplyer() {
        while (!ioQ_.empty()) {
            IoPtr p = ioQ_.front();
            ioQ_.pop();
            if (p->isSubmitted()) {
                try {
                    aio_.waitFor(p->aioKey());
                } catch (...) {}
            }
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
                walb::util::WalbLogpackHeader logh(b, blockSize_, salt());
                if (!logh.isValid()) {
                    break;
                }

                /* debug */
#if 0
                logh.printShort();
#endif

                for (size_t i = 0; i < logh.nRecords(); i++) {
                    LogDataPtr logd = allocLogData(logh, i);
                    readLogpackData(*logd, fdr);
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
        bool ret = h.physical_bs >= blockSize_ &&
            h.physical_bs % blockSize_ == 0;
        if (!ret) {
            LOGe("Physical block size does not match %u %zu.\n",
                 h.physical_bs, blockSize_);
        }
        return ret;
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
    void readLogpackData(walb::util::WalbLogpackData &logd, walb::util::FdReader &fdr) {
        if (!logd.hasData()) { return; }
        //::printf("ioSizePb: %u\n", logd.ioSizePb()); //debug
        for (size_t i = 0; i < logd.ioSizePb(); i++) {
            auto block = readBlock(fdr);
            logd.addBlock(block);
        }
        if (!logd.isValid()) {
            throw InvalidLogpackData();
        }
    }

    /**
     * Read a block data from a fd reader.
     */
    Block readBlock(walb::util::FdReader& fdr) {
        Block b = ba_.alloc();
        if (b.get() == nullptr) {
            throw RT_ERR("allocate failed.");
        }
        char *p = reinterpret_cast<char *>(b.get());
        fdr.read(p, blockSize_);
        return b;
    }

    /**
     * Create an IO.
     */
    IoPtr createIo(off_t offset, size_t size, Block block) {
        return IoPtr(new Io(offset, size, block));
    }

    void executeDiscard(walb::util::WalbLogpackData &logd) {

        /* Wait for all IO done. */
        waitForAllPendingIos();

        /* Issue the corresponding discard IOs. */

        /* now editing */
        ::printf("discard is not supported now.\n");
    }

    /**
     * Insert to overlapped data.
     */
    void insertToOverlappedData(IoPtr iop) {
        olData_.ins(iop);
    }

    /**
     * @iop iop to be deleted.
     * @ioQ All iop(s) will be added where iop->nOverlapped became 0.
     */
    void deleteFromOverlappedData(IoPtr iop, std::queue<IoPtr> &ioQ) {
        olData_.del(iop, ioQ);
    }

    /**
     * There is no pending IO after returned this method.
     */
    void waitForAllPendingIos() {
        while (!ioQ_.empty() || !submitIoQ_.empty()) {
            submitIos();
            waitForAnIoCompletion();
        }
        assert(olData_.empty());
    }

    unsigned int bytesToPb(unsigned int bytes) const {
        assert(bytes % LOGICAL_BLOCK_SIZE == 0);
        unsigned int lb = bytes / LOGICAL_BLOCK_SIZE;
        return ::capacity_pb(blockSize_, lb);
    }

    /**
     * Wait for an IO completion.
     */
    void waitForAnIoCompletion() {
        //::printf("waitForAnIoCompletion\n"); //debug
        assert(!ioQ_.empty());
        IoPtr iop = ioQ_.front();
        ioQ_.pop();

        /* debug */
        if (!iop->isSubmitted()) {
            ::printf("NOT_SUBMITTED\t%" PRIu64 "\t%zu\n",
                     static_cast<u64>(iop->offset()) >> 9,
                     iop->size() >> 9);
            ::printf("submit queue size: %zu\n", submitIoQ_.size());
            for (auto p : submitIoQ_) {
                ::printf("SUBMIT_Q\t%" PRIu64 "\t%zu\n",
                         static_cast<u64>(p->offset()) >> 9,
                         p->size() >> 9);
            }
        }

        assert(iop->isSubmitted());
        assert(!iop->isCompleted());
        assert(iop->aioKey() > 0);
        aio_.waitFor(iop->aioKey());
        nPendingBlocks_ -= bytesToPb(iop->size());
        iop->complete();
        std::queue<IoPtr> tmpIoQ;
        deleteFromOverlappedData(iop, tmpIoQ);

        /* Sort and insert to the head of submitIoQ_. */
        std::deque<IoPtr> sortedQ;
        while (!tmpIoQ.empty()) {
            IoPtr p = tmpIoQ.front();
            tmpIoQ.pop();
            auto it = std::lower_bound(
                sortedQ.begin(), sortedQ.end(),
                p,
                [](const IoPtr &p0, const IoPtr &p1) {
                    return p0->sequenceId() < p1->sequenceId();
                });
            sortedQ.insert(it, p);
        }
#ifdef DEBUG
        u64 prev = 0;
        for (auto p : sortedQ) {
            assert(prev <= p->sequenceId());
            prev = p->sequenceId();
        }
#endif
        while (!sortedQ.empty()) {
            IoPtr p = sortedQ.back();
#if 0
            ::printf("READY\t\t%" PRIu64 "\t%zu\n",
                     static_cast<u64>(p->offset()) >> 9,
                     p->size() >> 9);
#endif
            submitIoQ_.push_front(p);
            sortedQ.pop_back();
        }

        /* debug */
#if 0
        ::printf("COMPLETE\t\t%" PRIu64 "\t%zu\t%zu\n",
                 (u64)iop->offset() >> 9, iop->size() >> 9,
                 nPendingBlocks_);
#endif

        /* all IOs added to the submitIoQ_ must satisfy
           their nOverlapped == 0. */

        /* You must handle errors here. */
    }

    /**
     * Submit IOs.
     */
    void submitIos() {
        assert(nPendingBlocks_ <= queueSize_);
        while (!submitIoQ_.empty()) {
            IoPtr iop = submitIoQ_.front();
            submitIoQ_.pop_front();

            /* Prepare aio. */
            assert(iop->nOverlapped() == 0);
            iop->aioKey() = aio_.prepareWrite(
                iop->offset(), iop->size(), iop->rawPtr<char>());
            assert(iop->aioKey() > 0);
            iop->submit();
            assert(nPendingBlocks_ <= queueSize_);

            /* debug */
#if 0
            ::printf("SUBMIT\t\t%" PRIu64 "\t%zu\t%zu\n",
                     (u64)iop->offset() >> 9, iop->size() >> 9, nPendingBlocks_);
#endif

            /* Really submit. */
            aio_.submit();
        }
        assert(submitIoQ_.empty());
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

        /* Create IOs. */
        IoQueue tmpIoQ;
        size_t remaining = logd.ioSizeLb() * LOGICAL_BLOCK_SIZE;
        off_t off = static_cast<off_t>(logd.offset()) * LOGICAL_BLOCK_SIZE;
        size_t nBlocks = 0;
        for (size_t i = 0; i < logd.ioSizePb(); i++) {
            Block block = logd.getBlock(i);
            nBlocks++;
            if (remaining >= blockSize_) {
                IoPtr iop = createIo(off, blockSize_, block);
                tmpIoQ.add(iop);
                off += blockSize_;
                remaining -= blockSize_;
            } else {
                IoPtr iop = createIo(off, remaining, block);
                tmpIoQ.add(iop);
                off += remaining;
                remaining = 0;
            }
        }
        assert(remaining == 0);
        assert(!tmpIoQ.empty());
        assert(nBlocks > 0);
        nPendingBlocks_ += nBlocks;

        /* Wait for IOs if there are too much pending IOs. */
        while (!ioQ_.empty() && nPendingBlocks_ + nBlocks > queueSize_) {
            waitForAnIoCompletion();
        }

        /* debug */
#if 0
        ::printf("CREATE\t\t%" PRIu64 "\t%u\n",
                 logd.offset(), logd.ioSizeLb());
#endif

        /* Enqueue IOs. */
        while (!tmpIoQ.empty()) {
            IoPtr iop = tmpIoQ.pop();

            /* Clipping IO that is out of range of the device. */
            /* now editing */

            insertToOverlappedData(iop);
            if (iop->nOverlapped() == 0) {
                /* Ready to submit. */
                submitIoQ_.push_back(iop);
            } else {
                /* debug */
#if 0
                ::printf("OVERLAP\t\t%" PRIu64 "\t%zu\t%u\n",
                         static_cast<u64>(iop->offset()) >> 9,
                         iop->offset() >> 9, iop->nOverlapped());
#endif
            }
            ioQ_.push(iop);
        }

        /* Really submit. */
        submitIos();
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
const size_t BUFFER_SIZE = 4 * MEGA;

int main(int argc, char* argv[])
{
    int ret = 0;

    try {
        Config config(argc, argv);
        WalbLogApplyer wlApp(config, BUFFER_SIZE);
        wlApp.readAndApply(0);

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
