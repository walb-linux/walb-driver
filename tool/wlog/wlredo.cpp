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
#include <algorithm>
#include <utility>
#include <set>

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
    std::string ddevPath_;
    std::string inWlogPath_;
    bool isVerbose_;
    bool isHelp_;
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : ddevPath_()
        , inWlogPath_("-")
        , isVerbose_(false)
        , isHelp_(false)
        , args_() {
        parse(argc, argv);
    }

    const std::string& ddevPath() const { return ddevPath_; }
    const std::string& inWlogPath() const { return inWlogPath_; }
    bool isFromStdin() const { return inWlogPath_ == "-"; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }

    void print() const {
        ::printf("ddevPath: %s\n"
                 "inWlogPath: %s\n"
                 "verbose: %d\n"
                 "isHelp: %d\n",
                 ddevPath().c_str(), inWlogPath().c_str(),
                 isVerbose(), isHelp());
        int i = 0;
        for (const auto& s : args_) {
            ::printf("arg%d: %s\n", i++, s.c_str());
        }
    }

    static void printHelp() {
        ::printf("%s", generateHelpString().c_str());
    }

    void check() const {
        if (ddevPath_.empty()) {
            throwError("Specify device path.");
        }
        if (inWlogPath_.empty()) {
            throwError("Specify input wlog path.");
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
        IN_WLOG_PATH = 1,
        VERBOSE,
        HELP,
    };

    void throwError(const char *format, ...) const {
        va_list args;
        std::string msg;
        va_start(args, format);
        try {
            msg = walb::util::formatStringV(format, args);
        } catch (...) {}
        va_end(args);
        throw Error(msg);
    }

    void parse(int argc, char* argv[]) {
        while (1) {
            const struct option long_options[] = {
                {"inWlogPath", 1, 0, Opt::IN_WLOG_PATH},
                {"verbose", 0, 0, Opt::VERBOSE},
                {"help", 0, 0, Opt::HELP},
                {0, 0, 0, 0}
            };
            int option_index = 0;
            int c = ::getopt_long(argc, argv, "i:vh", long_options, &option_index);
            if (c == -1) { break; }

            switch (c) {
            case Opt::IN_WLOG_PATH:
            case 'i':
                inWlogPath_ = std::string(optarg);
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
        if (!args_.empty()) {
            ddevPath_ = args_[0];
        }
    }

    static std::string generateHelpString() {
        return walb::util::formatString(
            "Wlredo: redo wlog on a block device.\n"
            "Usage: wlcat [options] DEVICE_PATH\n"
            "Options:\n"
            "  -i, --inWlogPath PATH: input wlog path. '-' for stdin. (default: '-')\n"
            "  -v, --verbose:         verbose messages to stderr.\n"
            "  -h, --help:            show this message.\n");
    }
};

using Block = std::shared_ptr<u8>;

/**
 * Sequence id generator.
 */
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
    bool isOverwritten_;
    std::deque<Block> blocks_;
    unsigned int nOverlapped_; // To serialize overlapped IOs.
    u64 sequenceId_;

    using IoPtr = std::shared_ptr<Io>;

public:
    Io(off_t offset, size_t size)
        : offset_(offset), size_(size), aioKey_(0)
        , isSubmitted_(false), isCompleted_(false)
        , isOverwritten_(false)
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
    bool isOverwritten() const { return isOverwritten_; }
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

    void overwritten() {
        if (!isOverwritten_) {
            isOverwritten_ = true;
            if (!isSubmitted()) {
                blocks_.clear(); /* No more required. */
            }
        }
    }

    void markSubmitted() {
        assert(!isSubmitted());
        isSubmitted_ = true;
    }

    void markCompleted() {
        assert(!isCompleted());
        isCompleted_ = true;
    }

    void print(::FILE *p) const {
        ::fprintf(p, "IO offset: %zu size: %zu aioKey: %u "
                  "submitted: %d completed: %d\n",
                  offset_, size_, aioKey_,
                  isSubmitted_, isCompleted_);
        for (auto &b : blocks_) {
            ::fprintf(p, "  block %p\n", b.get());
        }
    }

    void print() const { print(::stdout); }

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
        const off_t off0 = offset_;
        const off_t off1 = rhs->offset_;
        const off_t size0 = static_cast<const off_t>(size_);
        const off_t size1 = static_cast<const off_t>(rhs->size_);
        return off0 < off1 + size1 && off1 < off0 + size0;
    }

    /**
     * RETURN:
     *   true if the IO is fully overwritten by rhs.
     */
    bool isOverwrittenBy(const IoPtr rhs) const {
        assert(rhs.get() != nullptr);
        const off_t off0 = offset_;
        const off_t off1 = rhs->offset_;
        const off_t size0 = static_cast<off_t>(size_);
        const off_t size1 = static_cast<off_t>(rhs->size_);
        return off1 <= off0 && off0 + size0 <= off1 + size1;
    }
};

using IoPtr = std::shared_ptr<Io>;

/**
 * This class can merge the last IO in the queue
 * in order to reduce number of IOs.
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
private:
    /**
     * Try to merge io1 to io0.
     */
    bool tryMerge(IoPtr io0, IoPtr io1) {
        /* Check max io size. */
        if (maxIoSize_ < io0->size() + io1->size()) {
            return false;
        }
        /* Try merge. */
        return io0->tryMerge(io1);
    }
};

/**
 * In order to serialize overlapped IOs execution.
 * IOs must be FIFO. (ins() and del()).
 */
class OverlappedData
{
private:
    using IoSet = std::set<std::pair<off_t, IoPtr> >;
    IoSet set_;
    size_t maxSize_;

public:
    OverlappedData()
        : set_(), maxSize_(0) {}

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
        if (static_cast<off_t>(maxSize_) < iop->offset()) {
            key0 = iop->offset() - static_cast<off_t>(maxSize_);
        }
        off_t key1 = iop->offset() + iop->size();

        /* Count overlapped IOs. */
        iop->nOverlapped() = 0;
        std::pair<off_t, IoPtr> k0 = std::make_pair(key0, IoPtr());
        IoSet::iterator it = set_.lower_bound(k0);
        while (it != set_.end() && it->first < key1) {
            IoPtr p = it->second;
            if (p->isOverlapped(iop)) {
                iop->nOverlapped()++;
                if (p->isOverwrittenBy(iop)) {
                    p->overwritten();
                }
            }
            it++;
        }

        /* Insert iop. */
        set_.insert(std::make_pair(iop->offset(), iop));

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
     *     You can submit them just after returned.
     */
    void del(IoPtr iop, std::queue<IoPtr> &ioQ) {
        assert(iop.get() != nullptr);
        assert(iop->nOverlapped() == 0);

        /* Delete iop. */
        deleteFromSet(iop);

        /* Reset maxSize_ if empty. */
        if (set_.empty()) {
            maxSize_ = 0;
        }

        /* Get search range. */
        off_t key0 = 0;
        if (iop->offset() > static_cast<off_t>(maxSize_)) {
            key0 = iop->offset() - static_cast<off_t>(maxSize_);
        }
        off_t key1 = iop->offset() + iop->size();

        /* Decrement nOverlapped of overlapped IOs. */
        std::pair<off_t, IoPtr> k0 = std::make_pair(key0, IoPtr());
        IoSet::iterator it = set_.lower_bound(k0);
        while (it != set_.end() && it->first < key1) {
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
        return set_.empty();
    }

private:
    /**
     * Delete an IoPtr from the map.
     */
    void deleteFromSet(IoPtr iop) {
        UNUSED size_t n = set_.erase(std::make_pair(iop->offset(), iop));
        assert(n == 1);
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

    std::queue<IoPtr> ioQ_; /* serialized by lsid. */
    std::deque<IoPtr> readyIoQ_; /* ready to submit. */
    std::deque<IoPtr> submitIoQ_; /* submitted, but not completed. */

    /* Number of blocks where the corresponding IO
       is submitted, but not completed. */
    size_t nPendingBlocks_;

    /* Number of prepared but not submitted IOs. */
    size_t nPreparedIos_;

    OverlappedData olData_;

    using PackHeader = walb::util::WalbLogpackHeader;
    using PackData = walb::util::WalbLogpackData;
    using PackDataPtr = std::shared_ptr<PackData>;

public:
    WalbLogApplyer(
        const Config& config, size_t bufferSize, bool isDiscardSupport = false)
        : config_(config)
        , bd_(config.ddevPath().c_str(), O_RDWR | O_DIRECT)
        , blockSize_(bd_.getPhysicalBlockSize())
        , queueSize_(getQueueSizeStatic(bufferSize, blockSize_))
        , aio_(bd_.getFd(), queueSize_)
        , ba_(queueSize_ * 2, blockSize_, blockSize_)
        , wh_()
        , isDiscardSupport_(isDiscardSupport)
        , ioQ_()
        , readyIoQ_()
        , nPendingBlocks_(0)
        , nPreparedIos_(0)
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

        uint64_t beginLsid = wh_.beginLsid();
        uint64_t redoLsid = beginLsid;
        try {
            while (true) {
                Block b = readBlock(fdr);
                PackHeader logh(b, blockSize_, salt());
                if (!logh.isValid()) {
                    break;
                }
                if (config_.isVerbose()) {
                    logh.printShort();
                }
                for (size_t i = 0; i < logh.nRecords(); i++) {
                    PackData logd(logh, i);
                    readLogpackData(logd, fdr);
                    createIoAndPrepare(logd);
                    redoLsid = logd.lsid();
                }
            } // while (true)
            submitIos();

        } catch (walb::util::EofError &e) {
            ::printf("Reach input EOF.\n");
        } catch (walb::util::InvalidLogpackData &e) {
            throw RT_ERR("InalidLogpackData");
        }

        /* Wait for all pending IOs. */
        submitIos();
        waitForAllPendingIos();

        /* Sync device. */
        bd_.fdatasync();

        ::printf("Applied lsid range [%" PRIu64 ", %" PRIu64 ")\n",
                 beginLsid, redoLsid);
    }

private:
    bool canApply() const {
        const struct walblog_header &h = wh_.header();
        bool ret = blockSize_ <= h.physical_bs &&
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

    /**
     * Read a logpack data.
     */
    void readLogpackData(PackData &logd, walb::util::FdReader &fdr) {
        if (!logd.hasData()) { return; }
        //::printf("ioSizePb: %u\n", logd.ioSizePb()); //debug
        for (size_t i = 0; i < logd.ioSizePb(); i++) {
            logd.addBlock(readBlock(fdr));
        }
        if (!logd.isValid()) {
            throw walb::util::InvalidLogpackData();
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
        fdr.read(reinterpret_cast<char *>(b.get()), blockSize_);
        return b;
    }

    /**
     * Create an IO.
     */
    IoPtr createIo(off_t offset, size_t size, Block block) {
        return IoPtr(new Io(offset, size, block));
    }

    /**
     * Discard.
     *
     * Three types:
     * (1) Ignore discard log.
     * (2) Issue discard.
     * (3) Issue zero-data IO.
     */
    void executeDiscard(UNUSED PackData &logd) {

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
        while (!ioQ_.empty() || !readyIoQ_.empty()) {
            waitForAnIoCompletion();
        }
        assert(olData_.empty());
    }

    /**
     * Convert [byte] to [physical block].
     */
    unsigned int bytesToPb(unsigned int bytes) const {
        assert(bytes % LOGICAL_BLOCK_SIZE == 0);
        unsigned int lb = bytes / LOGICAL_BLOCK_SIZE;
        return ::capacity_pb(blockSize_, lb);
    }

    /**
     * Wait for an IO completion.
     * If not submitted, submit before waiting.
     */
    void waitForAnIoCompletion() {
        assert(!ioQ_.empty());
        IoPtr iop = ioQ_.front();
        ioQ_.pop();

        if (!iop->isSubmitted() && !iop->isOverwritten()) {
            /* The IO is not still submitted. */
            prepareIos();
            submitIos();
        }

        if (iop->isSubmitted()) {
            assert(!iop->isCompleted());
            assert(iop->aioKey() > 0);
            aio_.waitFor(iop->aioKey());
            iop->markCompleted();
        } else {
            assert(iop->isOverwritten());
        }
        nPendingBlocks_ -= bytesToPb(iop->size());
        std::queue<IoPtr> tmpIoQ;
        deleteFromOverlappedData(iop, tmpIoQ);

        /* Insert to the head of readyIoQ_. */
        while (!tmpIoQ.empty()) {
            IoPtr p = tmpIoQ.front();
            tmpIoQ.pop();
            if (p->isOverwritten()) {
                /* No need to execute the IO. */
                continue;
            }
            assert(p->nOverlapped() == 0);
            readyIoQ_.push_front(p);
        }

        if (config_.isVerbose()) {
            ::printf("COMPLETE\t\t%" PRIu64 "\t%zu\t%zu\n",
                     (u64)iop->offset() >> 9, iop->size() >> 9,
                     nPendingBlocks_);
        }
    }

    /**
     * Move IOs from readyIoQ_ to submitIoQ_.
     */
    void prepareIos() {
        assert(readyIoQ_.size() <= queueSize_);
        while (!readyIoQ_.empty()) {
            IoPtr iop = readyIoQ_.front();
            readyIoQ_.pop_front();
            if (iop->isOverwritten()) {
                /* No need to execute the IO. */
                continue;
            }
#if 1
            /* Insert to the submit queue (sorted by offset). */
            const auto cmp = [](const IoPtr &p0, const IoPtr &p1) {
                return p0->offset() < p1->offset();
            };
            submitIoQ_.insert(
                std::lower_bound(submitIoQ_.begin(), submitIoQ_.end(),
                                 iop, std::ref(cmp)),
                iop);
#else
            /* Insert to the submit queue. */
            submitIoQ_.push_back(iop);
#endif

            /* now editing */
            if (queueSize_ <= submitIoQ_.size()) {
                submitIos();
            }
        }
        assert(readyIoQ_.empty());
    }

    /**
     * Submit IOs in the submitIoQ_.
     */
    void submitIos() {
        if (submitIoQ_.empty()) {
            return;
        }
        assert(submitIoQ_.size() <= queueSize_);
        size_t nBulk = 0;
        while (!submitIoQ_.empty()) {
            IoPtr iop = submitIoQ_.front();
            submitIoQ_.pop_front();
            if (iop->isOverwritten()) {
                continue;
            }

            /* Prepare aio. */
            assert(iop->nOverlapped() == 0);
            iop->aioKey() = aio_.prepareWrite(
                iop->offset(), iop->size(), iop->rawPtr<char>());
            assert(iop->aioKey() > 0);
            iop->markSubmitted();
            nBulk++;
            if (config_.isVerbose()) {
                ::printf("SUBMIT\t\t%" PRIu64 "\t%zu\t%zu\n",
                         (u64)iop->offset() >> 9, iop->size() >> 9, nPendingBlocks_);
            }
        }
        if (nBulk > 0) {
            aio_.submit();
            if (config_.isVerbose()) {
                ::printf("nBulk: %zu\n", nBulk);
            }
        }
    }

    /**
     * Create related IOs and prepare them.
     */
    void createIoAndPrepare(PackData &logd) {
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
            if (blockSize_ <= remaining) {
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
        while (!ioQ_.empty() && queueSize_ < nPendingBlocks_ + nBlocks) {
            waitForAnIoCompletion();
        }

        if (config_.isVerbose()) {
            ::printf("CREATE\t\t%" PRIu64 "\t%u\n",
                     logd.offset(), logd.ioSizeLb());
        }

        /* Enqueue IOs. */
        while (!tmpIoQ.empty()) {
            IoPtr iop = tmpIoQ.pop();

            /* Clipping IO that is out of range of the device. */
            /* now editing */

            insertToOverlappedData(iop);
            if (iop->nOverlapped() == 0) {
                /* Ready to submit. */
                readyIoQ_.push_back(iop);
            } else {
                /* Will be submitted later. */
                if (config_.isVerbose()) {
                    ::printf("OVERLAP\t\t%" PRIu64 "\t%zu\t%u\n",
                             static_cast<u64>(iop->offset()) >> 9,
                             iop->offset() >> 9, iop->nOverlapped());
                }
            }
            ioQ_.push(iop);
        }
        prepareIos();
    }

    static size_t getQueueSizeStatic(size_t bufferSize, size_t blockSize) {
        if (bufferSize <= blockSize) {
            throw RT_ERR("Buffer size must be > blockSize.");
        }
        size_t qs = bufferSize / blockSize;
        if (qs == 0) {
            throw RT_ERR("Queue size is must be positive.");
        }
        return qs;
    }
};

int main(int argc, char* argv[])
{
    int ret = 0;
    const size_t BUFFER_SIZE = 4 * 1024 * 1024; /* 4MB. */

    try {
        Config config(argc, argv);
        if (config.isHelp()) {
            Config::printHelp();
            return 0;
        }
        config.check();

        WalbLogApplyer wlApp(config, BUFFER_SIZE, false);
        if (config.isFromStdin()) {
            wlApp.readAndApply(0);
        } else {
            walb::util::FileOpener fo(config.inWlogPath(), O_RDONLY);
            wlApp.readAndApply(fo.fd());
            fo.close();
        }
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
