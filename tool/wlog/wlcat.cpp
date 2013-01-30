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
#include "walb_util.hpp"
#include "aio_util.hpp"

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
        for (const auto& s : args_) {
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
            msg = walb::util::formatStringV(format, args);
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
        return walb::util::formatString(
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
        static const size_t maxIoSize_ = 1024 * 1024;

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
#if 0
            ::fprintf(::stderr, "offPb %" PRIu64 "\n",
                      super_.getOffsetFromLsid(block.lsid));
#endif
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
        , aheadLsid_(config.beginLsid()) {

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
                super_.getUuid(), config_.beginLsid(), config_.endLsid());
#if 1
        wh.write(outFd);
#endif

        if (config_.isVerbose()) {
            ::printf("beginLsid: %" PRIu64 "\n", config_.beginLsid());
        }
        u64 lsid = config_.beginLsid();
        while (lsid < config_.endLsid()) {
            bool isEnd = false;

            readAhead();
            std::unique_ptr<walb::util::WalbLogpackHeader> loghP;
            try {
                loghP = std::move(readLogpackHeader());
                //loghP->print(); //debug
            } catch (InvalidLogpackHeader& e) {
                isEnd = true;
                break;
            }
            walb::util::WalbLogpackHeader &logh = *loghP;

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
                    lsid = logh.nextLogpackLsid();
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
        if (config_.isVerbose()) {
            ::printf("endLsid: %" PRIu64 "\n", lsid);
        }
    }

private:
    /**
     * Read a logpack header.
     */
    std::unique_ptr<walb::util::WalbLogpackHeader> readLogpackHeader() {
        auto block = readBlock();
        std::unique_ptr<walb::util::WalbLogpackHeader> logh(
            new walb::util::WalbLogpackHeader(
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
                walb::util::formatString(
                    "logpack %" PRIu64 " is not the expected one %" PRIu64 ".",
                    logh->header().logpack_lsid, block.lsid));
        }
        return std::move(logh);
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

const size_t BUFFER_SIZE = 4 * 1024 * 1024; /* 4MB */

int main(int argc, char* argv[])
{
    int ret = 0;

    try {
#if 0
        walb::aio::testAioDataAllocator();
#endif
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
            walb::util::FileOpener fo(
                config.outPath(),
                O_WRONLY | O_CREAT | O_TRUNC,
                S_IRWXU | S_IRGRP | S_IROTH);
            wlReader.catLog(fo.fd());
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
