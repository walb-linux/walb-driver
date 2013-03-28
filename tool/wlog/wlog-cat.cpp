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
#include <getopt.h>

#include "util.hpp"
#include "walb_log.hpp"
#include "aio_util.hpp"
#include "memory_buffer.hpp"

#include "walb/walb.h"

/**
 * Wlcat configuration.
 */
class Config
{
private:
    std::string ldevPath_;
    std::string outPath_;
    uint64_t beginLsid_;
    uint64_t endLsid_;
    bool isVerbose_;
    bool isHelp_;
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : ldevPath_()
        , outPath_("-")
        , beginLsid_(0)
        , endLsid_(-1)
        , isVerbose_(false)
        , isHelp_(false)
        , args_() {
        parse(argc, argv);
    }

    const std::string& ldevPath() const { return ldevPath_; }
    uint64_t beginLsid() const { return beginLsid_; }
    uint64_t endLsid() const { return endLsid_; }
    const std::string& outPath() const { return outPath_; }
    bool isOutStdout() const { return outPath_ == "-"; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }

    void print() const {
        ::printf("ldevPath: %s\n"
                 "outPath: %s\n"
                 "beginLsid: %" PRIu64 "\n"
                 "endLsid: %" PRIu64 "\n"
                 "verbose: %d\n"
                 "isHelp: %d\n",
                 ldevPath().c_str(), outPath().c_str(),
                 beginLsid(), endLsid(),
                 isVerbose(), isHelp());
        int i = 0;
        for (const auto &s : args_) {
            ::printf("arg%d: %s\n", i++, s.c_str());
        }
    }

    static void printHelp() {
        ::printf("%s", generateHelpString().c_str());
    }

    void check() const {
        if (beginLsid() >= endLsid()) {
            throwError("beginLsid must be < endLsid.");
        }
        if (ldevPath_.empty()) {
            throwError("Specify log device path.");
        }
        if (outPath_.empty()) {
            throwError("Specify output wlog path.");
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
        OUT_PATH = 1,
        BEGIN_LSID,
        END_LSID,
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
                {"outPath", 1, 0, Opt::OUT_PATH},
                {"beginLsid", 1, 0, Opt::BEGIN_LSID},
                {"endLsid", 1, 0, Opt::END_LSID},
                {"verbose", 0, 0, Opt::VERBOSE},
                {"help", 0, 0, Opt::HELP},
                {0, 0, 0, 0}
            };
            int option_index = 0;
            int c = ::getopt_long(argc, argv, "o:b:e:vh", long_options, &option_index);
            if (c == -1) { break; }

            switch (c) {
            case Opt::OUT_PATH:
            case 'o':
                outPath_ = std::string(optarg);
                break;
            case Opt::BEGIN_LSID:
            case 'b':
                beginLsid_ = ::atoll(optarg);
                break;
            case Opt::END_LSID:
            case 'e':
                endLsid_ = ::atoll(optarg);
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
            ldevPath_ = args_[0];
        }
    }

    static std::string generateHelpString() {
        return cybozu::util::formatString(
            "Wlcat: extract wlog from a log device.\n"
            "Usage: wlcat [options] LOG_DEVICE_PATH\n"
            "Options:\n"
            "  -o, --outPath PATH:   output wlog path. '-' for stdout. (default: '-')\n"
            "  -b, --beginLsid LSID: begin lsid to restore. (default: 0)\n"
            "  -e, --endLsid LSID:   end lsid to restore. (default: -1)\n"
            "  -v, --verbose:        verbose messages to stderr.\n"
            "  -h, --help:           show this message.\n");
    }
};

/**
 * To read walb log from a log device and create wlog file.
 */
class WalbLogReader
{
private:
    const Config& config_;
    cybozu::util::BlockDevice bd_;
    walb::log::WalbSuperBlock super_;
    const size_t blockSize_;
    const size_t queueSize_;
    cybozu::aio::Aio aio_;
    cybozu::util::BlockAllocator<u8> ba_;

    struct Block {
        const u64 lsid;
        std::shared_ptr<u8> ptr;
        const size_t size; // [byte]

        Block(u64 lsid, std::shared_ptr<u8> ptr, size_t size)
            : lsid(lsid), ptr(ptr), size(size) {}

        Block(const Block &rhs)
            : lsid(rhs.lsid), ptr(rhs.ptr), size(rhs.size) {}

        void print(::FILE *p) const {
            ::fprintf(p, "Block lsid %" PRIu64 " ptr %p size %zu",
                      lsid, ptr.get(), size);
        }

        void print() const { print(::stdout); }
    };

    /**
     * Blocks must be contiguous memories.
     */
    struct Io {
        off_t offset; // [byte]
        size_t size; // [byte]
        unsigned int aioKey;
        bool done;
        std::deque<Block> blocks;

        Io(off_t offset, size_t size)
            : offset(offset), size(size)
            , aioKey(0), done(false), blocks() {}

        std::shared_ptr<u8> ptr() {
            return blocks.front().ptr;
        }

        bool isValidSize() const {
            size_t s = 0;
            for (auto &b : blocks) { s += b.size; }
            return size == s;
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

        void print() const { print(::stdout); }
    };

    using IoPtr = std::shared_ptr<Io>;

    class IoQueue {
    private:
        std::deque<IoPtr> ioQ_;
        const walb::log::WalbSuperBlock& super_;
        const size_t blockSize_;
        static const size_t maxIoSize_ = 1024 * 1024;

    public:
        explicit IoQueue(const walb::log::WalbSuperBlock& super,
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

        bool empty() const { return ioQ_.empty(); }
        std::shared_ptr<u8> ptr() { return ioQ_.front()->ptr(); }

    private:
        IoPtr createIo(const Block& block) {
#if 0
            ::fprintf(::stderr, "offPb %" PRIu64 "\n",
                      super_.getOffsetFromLsid(block.lsid));
#endif
            assert(block.size == blockSize_);
            off_t offset = super_.getOffsetFromLsid(block.lsid) * blockSize_;
            IoPtr p(new Io(offset, blockSize_));
            p->blocks.push_back(block);
            assert(p->isValidSize());
            return p;
        }

        bool tryMerge(IoPtr io0, IoPtr io1) {
            assert(io0->isValidSize());
            assert(io1->isValidSize());

            /* Check mas io size. */
            if (io0->size + io1->size > maxIoSize_) {
                return false;
            }

            /* Check Io is contiguous. */
            if (io0->offset + static_cast<off_t>(io0->size) != io1->offset) {
                return false;
            }
            /* Buffers are adjacent. */
            const u8 *p0 = io0->blocks.back().ptr.get();
            const u8 *p1 = io1->blocks.front().ptr.get();
            if (p0 + blockSize_ != p1) {
                return false;
            }

            /* Merge. */
            io0->size += io1->size;
            while (!io1->blocks.empty()) {
                Block &b = io1->blocks.front();
                io0->blocks.push_back(b);
                io1->blocks.pop_front();
            }
            assert(io0->isValidSize());
            return true;
        }
    };

    std::queue<IoPtr> ioQ_;
    size_t nPendingBlocks_;
    u64 aheadLsid_;

    class InvalidLogpackHeader : public std::exception {
    private:
        std::string msg_;
    public:
        InvalidLogpackHeader()
            : msg_("invalid logpack header.") {}
        explicit InvalidLogpackHeader(const std::string& msg)
            : msg_(msg) {}

        virtual const char *what() const noexcept {
            return msg_.c_str();
        }
    };

    class InvalidLogpackData : public std::exception {
    public:
        virtual const char *what() const noexcept {
            return "invalid logpack data.";
        }
    };

    using PackHeader = walb::log::WalbLogpackHeader;
    using PackData = walb::log::WalbLogpackData;
    using PackDataPtr = std::shared_ptr<PackData>;

public:
    WalbLogReader(const Config& config, size_t bufferSize)
        : config_(config)
        , bd_(config.ldevPath().c_str(), O_RDONLY | O_DIRECT)
        , super_(bd_)
        , blockSize_(bd_.getPhysicalBlockSize())
        , queueSize_(getQueueSizeStatic(bufferSize, blockSize_))
        , aio_(bd_.getFd(), queueSize_)
        , ba_(queueSize_ * 2, blockSize_, blockSize_)
        , ioQ_()
        , nPendingBlocks_(0)
        , aheadLsid_(0) {}

    ~WalbLogReader() {
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
        cybozu::util::FdWriter fdw(outFd);

        /* Set lsids. */
        u64 beginLsid = config_.beginLsid();
        if (beginLsid < super_.getOldestLsid()) {
            beginLsid = super_.getOldestLsid();
        }
        aheadLsid_ = beginLsid;

        /* Create and write walblog header. */
        walb::log::WalbLogFileHeader wh;
        wh.init(super_.getPhysicalBlockSize(), super_.getLogChecksumSalt(),
                super_.getUuid(), beginLsid, config_.endLsid());
        wh.write(outFd);

        /* Read and write each logpack. */
        if (config_.isVerbose()) {
            ::fprintf(::stderr, "beginLsid: %" PRIu64 "\n", beginLsid);
        }
        u64 lsid = beginLsid;
        u64 totalPaddingPb = 0;
        u64 nPacks = 0;
        while (lsid < config_.endLsid()) {
            bool isEnd = false;
            readAhead();
            std::unique_ptr<PackHeader> loghP;
            try {
                loghP = std::move(readLogpackHeader());
            } catch (InvalidLogpackHeader& e) {
                if (config_.isVerbose()) {
                    ::fprintf(::stderr, "Caught invalid logpack header error.\n");
                }
                isEnd = true;
                break;
            }
            PackHeader &logh = *loghP;
            std::queue<PackDataPtr> q;
            isEnd = readAllLogpackData(logh, q);
            writeLogpack(fdw, logh, q);
            lsid = logh.nextLogpackLsid();
            totalPaddingPb += logh.totalPaddingPb();
            nPacks++;
            if (isEnd) { break; }
        }
        if (config_.isVerbose()) {
            ::fprintf(::stderr, "endLsid: %" PRIu64 "\n"
                      "lackOfLogPb: %" PRIu64 "\n"
                      "totalPaddingPb: %" PRIu64 "\n"
                      "nPacks: %" PRIu64 "\n",
                      lsid, config_.endLsid() - lsid, totalPaddingPb, nPacks);
        }
    }

private:
    /**
     * Read all IOs data of a logpack.
     *
     * RETURN:
     *   true if logpack has shrinked and should end.
     */
    bool readAllLogpackData(PackHeader &logh, std::queue<PackDataPtr> &q) {
        bool isEnd = false;
        for (size_t i = 0; i < logh.nRecords(); i++) {
            readAhead();
            PackDataPtr p(new PackData(logh, i));
            try {
                readLogpackData(*p);
                q.push(p);
            } catch (InvalidLogpackData& e) {
                if (config_.isVerbose()) { logh.print(::stderr); }
                uint64_t prevLsid = logh.nextLogpackLsid();
                logh.shrink(i);
                uint64_t currentLsid = logh.nextLogpackLsid();
                if (config_.isVerbose()) { logh.print(::stderr); }
                isEnd = true;
                if (config_.isVerbose()) {
                    ::fprintf(::stderr, "Logpack shrink from %" PRIu64 " to %" PRIu64 "\n",
                              prevLsid, currentLsid);
                }
                break;
            }
        }
        return isEnd;
    }

    /**
     * Write a logpack.
     */
    void writeLogpack(
        cybozu::util::FdWriter &fdw, PackHeader &logh,
        std::queue<PackDataPtr> &q) {
        if (logh.nRecords() == 0) {
            return;
        }
        /* Write the header. */
        fdw.write(logh.ptr<char>(), logh.pbs());
        /* Write the IO data. */
        size_t nWritten = 0;
        while (!q.empty()) {
            PackDataPtr logd = q.front();
            q.pop();
            if (!logd->hasData()) { continue; }
            for (size_t i = 0; i < logd->ioSizePb(); i++) {
                fdw.write(logd->ptr<char>(i), logh.pbs());
                nWritten++;
            }
        }
        assert(nWritten == logh.totalIoSize());
    }

    /**
     * Read a logpack header.
     */
    std::unique_ptr<walb::log::WalbLogpackHeader> readLogpackHeader() {
        Block block = readBlock();
        std::unique_ptr<walb::log::WalbLogpackHeader> logh(
            new walb::log::WalbLogpackHeader(
                block.ptr, super_.getPhysicalBlockSize(),
                super_.getLogChecksumSalt()));
#if 0
        logh->print(::stderr);
#endif
        if (!logh->isValid()) {
            throw InvalidLogpackHeader();
        }
        if (logh->header().logpack_lsid != block.lsid) {
            throw InvalidLogpackHeader(
                cybozu::util::formatString(
                    "logpack %" PRIu64 " is not the expected one %" PRIu64 ".",
                    logh->header().logpack_lsid, block.lsid));
        }
        return std::move(logh);
    }

    /**
     * Read a logpack data.
     */
    void readLogpackData(walb::log::WalbLogpackData& logd) {
        if (!logd.hasData()) { return; }
        //::printf("ioSizePb: %u\n", logd.ioSizePb()); //debug
        for (size_t i = 0; i < logd.ioSizePb(); i++) {
            Block block = readBlock();
            logd.addBlock(block.ptr);
        }
        if (!logd.isValid()) {
            if (config_.isVerbose()) {
                logd.print(::stderr);
            }
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
            Block b(aheadLsid_, ba_.alloc(), blockSize_);
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

int main(int argc, char* argv[])
{
    int ret = 0;
    const size_t BUFFER_SIZE = 4 * 1024 * 1024; /* 4MB */

    try {
        Config config(argc, argv);
        if (config.isHelp()) {
            Config::printHelp();
            return 0;
        }
        config.check();

        WalbLogReader wlReader(config, BUFFER_SIZE);
        if (config.isOutStdout()) {
            wlReader.catLog(1);
        } else {
            cybozu::util::FileOpener fo(
                config.outPath(),
                O_WRONLY | O_CREAT | O_TRUNC,
                S_IRWXU | S_IRGRP | S_IROTH);
            wlReader.catLog(fo.fd());
            cybozu::util::FdWriter(fo.fd()).fdatasync();
            fo.close();
        }
    } catch (Config::Error& e) {
        LOGe("Command line error: %s\n\n", e.what());
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
