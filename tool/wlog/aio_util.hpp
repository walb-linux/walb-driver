/**
 * @file
 * @brief Aio Utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2012 Cybozu Labs, Inc.
 */
#ifndef AIO_UTIL_HPP
#define AIO_UTIL_HPP

#define _FILE_OFFSET_BITS 64

#include <vector>
#include <queue>
#include <unordered_map>
#include <map>
#include <string>
// #include <algorithm>
#include <exception>
#include <cerrno>
#include <cstdio>
#include <cassert>
#include <memory>

#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <libaio.h>

#include "util.hpp"

namespace walb {
namespace aio {

enum IoType
{
    IOTYPE_READ = 0,
    IOTYPE_WRITE = 1,
    IOTYPE_FLUSH = 2,
};

/**
 * An aio data.
 */
struct AioData
{
    IoType type;
    struct iocb iocb;
    off_t oft;
    size_t size;
    char *buf;
    double beginTime;
    double endTime;
};

/**
 * Pointer to AioData.
 */
typedef std::shared_ptr<AioData> AioDataPtr;

/**
 * Asynchronous IO wrapper.
 *
 * (1) call prepareXXX() once or more.
 * (2) call submit() to submit all prepared IOs.
 * (3) call waitOne() or wait().
 *
 * You can issue up to 'queueSize' IOs concurrently.
 */
class Aio
{
private:
    const int fd_;
    const size_t queueSize_;
    io_context_t ctx_;
    std::queue<AioData *> aioQueue_;

    class AioDataBuffer
    {
    private:
        const size_t size_;
        size_t idx_;
        std::vector<AioData> aioVec_;

    public:
        AioDataBuffer(size_t size)
            : size_(size)
            , idx_(0)
            , aioVec_(size) {}

        AioData* next() {

            AioData *ret = &aioVec_[idx_];
            idx_ = (idx_ + 1) % size_;
            return ret;
        }
    };

    AioDataBuffer aioDataBuf_;
    std::vector<struct iocb *> iocbs_; /* temporal use for submit. */
    std::vector<struct io_event> ioEvents_; /* temporal use for wait. */

public:
    /**
     * @fd Opened file descripter.
     * @queueSize queue size for aio.
     */
    Aio(int fd, size_t queueSize)
        : fd_(fd)
        , queueSize_(queueSize)
        , aioDataBuf_(queueSize * 2)
        , iocbs_(queueSize)
        , ioEvents_(queueSize) {

        assert(fd_ >= 0);
        ::io_queue_init(queueSize_, &ctx_);
    }

    ~Aio() {
        ::io_queue_release(ctx_);
    }

    class EofError : public std::exception {};

    /**
     * Prepare a read IO.
     */
    bool prepareRead(off_t oft, size_t size, char* buf) {

        if (aioQueue_.size() > queueSize_) {
            return false;
        }

        auto* ptr = aioDataBuf_.next();
        aioQueue_.push(ptr);
        ptr->type = IOTYPE_READ;
        ptr->oft = oft;
        ptr->size = size;
        ptr->buf = buf;
        ptr->beginTime = 0.0;
        ptr->endTime = 0.0;
        ::io_prep_pread(&ptr->iocb, fd_, buf, size, oft);
        ptr->iocb.data = ptr;
        return true;
    }

    /**
     * Prepare a write IO.
     */
    bool prepareWrite(off_t oft, size_t size, char* buf) {

        if (aioQueue_.size() > queueSize_) {
            return false;
        }

        auto* ptr = aioDataBuf_.next();
        aioQueue_.push(ptr);
        ptr->type = IOTYPE_WRITE;
        ptr->oft = oft;
        ptr->size = size;
        ptr->buf = buf;
        ptr->beginTime = 0.0;
        ptr->endTime = 0.0;
        ::io_prep_pwrite(&ptr->iocb, fd_, buf, size, oft);
        ptr->iocb.data = ptr;
        return true;
    }

    /**
     * Prepare a flush IO.
     *
     * Currently aio flush is not supported
     * by almost all filesystems and block devices.
     */
    bool prepareFlush() {

        if (aioQueue_.size() > queueSize_) {
            return false;
        }

        auto* ptr = aioDataBuf_.next();
        aioQueue_.push(ptr);
        ptr->type = IOTYPE_FLUSH;
        ptr->oft = 0;
        ptr->size = 0;
        ptr->buf = NULL;
        ptr->beginTime = 0.0;
        ptr->endTime = 0.0;
        ::io_prep_fdsync(&ptr->iocb, fd_);
        ptr->iocb.data = ptr;
        return true;
    }

    /**
     * Submit all prepared IO(s).
     */
    void submit() {

        size_t nr = aioQueue_.size();
        if (nr == 0) {
            return;
        }
        assert(iocbs_.size() >= nr);
        double beginTime = util::getTime();
        for (size_t i = 0; i < nr; i++) {
            auto* ptr = aioQueue_.front();
            aioQueue_.pop();
            iocbs_[i] = &ptr->iocb;
            ptr->beginTime = beginTime;
        }
        assert(aioQueue_.empty());
        int err = ::io_submit(ctx_, nr, &iocbs_[0]);
        if (err != static_cast<int>(nr)) {
            /* ::printf("submit error %d.\n", err); */
            throw EofError();
        }
    }

    /**
     * Wait several IO(s) completed.
     *
     * @nr number of waiting IO(s). nr >= 0.
     * @events event array for temporary use.
     * @aio Queue AioDataPtr of completed IO will be pushed into it.
     */
    void wait(size_t nr, std::queue<AioData>& aioDataQueue) {

        size_t done = 0;
        bool isError = false;
        while (done < nr) {
            int tmpNr = ::io_getevents(ctx_, 1, nr - done, &ioEvents_[done], NULL);
            if (tmpNr < 1) {
                throw std::runtime_error("io_getevents failed.");
            }
            double endTime = util::getTime();
            for (size_t i = done; i < done + tmpNr; i++) {
                auto* iocb = static_cast<struct iocb *>(ioEvents_[i].obj);
                auto* ptr = static_cast<AioData *>(iocb->data);
                if (ioEvents_[i].res != ptr->iocb.u.c.nbytes) {
                    isError = true;
                }
                ptr->endTime = endTime;
                aioDataQueue.push(*ptr);
            }
            done += tmpNr;
        }
        if (isError) {
            // ::printf("wait error.\n");
            throw EofError();
        }
    }

    /**
     * Wait just one IO completed.
     *
     * @return aio data pointer.
     *   This data is available at least before calling
     *   queueSize_ times of prepareWrite/prepareRead.
     */
    AioData* waitOne() {

        auto& event = ioEvents_[0];
        int err = ::io_getevents(ctx_, 1, 1, &event, NULL);
        double endTime = util::getTime();
        if (err != 1) {
            throw std::runtime_error("io_getevents failed.");
        }
        auto* iocb = static_cast<struct iocb *>(event.obj);
        auto* ptr = static_cast<AioData *>(iocb->data);
        if (event.res != ptr->iocb.u.c.nbytes) {
            // ::printf("waitOne error %lu\n", event.res);
            throw EofError();
        }
        ptr->endTime = endTime;
        return ptr;
    }
};

} // namespace aio
} // namespace walb

#endif /* AIO_UTIL_HPP */
