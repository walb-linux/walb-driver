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
#include "../random.h"

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
    unsigned int key;
    IoType type;
    struct iocb iocb;
    off_t oft;
    size_t size;
    char *buf;
    double beginTime;
    double endTime;
    bool done;
};

/**
 * Pointer to AioData.
 */
using AioDataPtr = std::shared_ptr<AioData>;

/**
 * Simple allocator of AioData.
 */
#if 0
class AioDataAllocator
{
private:
    unsigned int key_;

public:
    AioDataAllocator()
        : key_(0) {}

    AioDataPtr alloc() {
        AioData *p = new AioData();
        p->key = key_++;
        return AioDataPtr(p);
    }
};
#else
class AioDataAllocator
{
private:
    unsigned int key_;
    walb::util::DataBuffer<AioData> buf_;

public:
    AioDataAllocator()
        : key_(1)
        , buf_(16384) {}

    explicit AioDataAllocator(size_t size)
        : key_(1)
        , buf_(size) {}

    AioDataPtr alloc() {
        AioData *p = buf_.alloc();
        if (p != nullptr) {
            p->key = getKey();
            return AioDataPtr(p, [&](AioData *p) {
                    buf_.free(p);
                });
        } else {
            AioDataPtr p(new AioData());
            p->key = getKey();
            return p;
        }
    }
private:
    unsigned int getKey() {
        unsigned int ret = key_;
        if (key_ == static_cast<unsigned int>(-1)) {
            key_ += 2;
        } else {
            key_++;
        }
        return ret;
    }
};
#endif

UNUSED
static void testAioDataAllocator()
{
    AioDataAllocator allocator;
    std::queue<AioDataPtr> queue;
    const size_t nTrials = 1000000;

    while (queue.size() < 64) {
        AioDataPtr p = allocator.alloc();
        queue.push(p);
        //::printf("add %u\n", p->key);
    }

    double bTime = util::getTime();
    for (size_t i = 0; i < nTrials; i++) {
        int nr = ::get_random(10);
        for (int j = 0; j < nr; j++) {
            AioDataPtr p = queue.front();
            queue.pop();
            queue.push(p);
        }
        AioDataPtr p = queue.front();
        queue.pop();
        //::printf("del %u\n", p->key);

        p = allocator.alloc();
        queue.push(p);
        //::printf("add %u\n", p->key);
    }
    double eTime = util::getTime();

    while (!queue.empty()) {
        AioDataPtr p = queue.front();
        queue.pop();
        //::printf("del %u\n", p->key);
    }
    ::printf("%.06f sec. %.f /sec.\n",
             (eTime - bTime),
             (double)nTrials / (eTime - bTime));
}

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

    AioDataAllocator allocator_;

    /* Prepared but not submitted. */
    std::queue<AioDataPtr> submitQueue_;

    /*
     * Submitted but not returned.
     * Key: AioData.key, value: aiodata.
     */
    std::unordered_map<unsigned int, AioDataPtr> pendingIOs_;

    /*
     * Completed IOs.
     * Each ptr->done must be true, and it must exist in the pendingIOs_.
     */
    std::queue<AioDataPtr> completedIOs_;

    /* temporal use for submit. */
    std::vector<struct iocb *> iocbs_;

    /* temporal use for wait. */
    std::vector<struct io_event> ioEvents_;

public:
    /**
     * @fd Opened file descripter.
     * @queueSize queue size for aio.
     */
    Aio(int fd, size_t queueSize)
        : fd_(fd)
        , queueSize_(queueSize)
        , allocator_(queueSize * 2)
        , submitQueue_()
        , pendingIOs_()
        , completedIOs_()
        , iocbs_(queueSize)
        , ioEvents_(queueSize) {
        assert(fd_ >= 0);
        ::io_queue_init(queueSize_, &ctx_);
    }

    ~Aio() {
        //::printf("~Aio()\n"); //debug
        ::io_queue_release(ctx_);
    }

    class EofError : public std::exception {
        virtual const char *what() const noexcept {
            return "eof error";
        }
    };

    /**
     * Prepare a read IO.
     * RETURN:
     *   Unique key (non-zero) to identify the IO in success, or 0.
     */
    unsigned int prepareRead(off_t oft, size_t size, char* buf) {
        if (submitQueue_.size() >= queueSize_) {
            return 0;
        }

        AioDataPtr ptr = allocator_.alloc();
        assert(ptr->key != 0);
        submitQueue_.push(ptr);
        ptr->type = IOTYPE_READ;
        ptr->oft = oft;
        ptr->size = size;
        ptr->buf = buf;
        ptr->beginTime = 0.0;
        ptr->endTime = 0.0;
        ptr->done = false;
        ::io_prep_pread(&ptr->iocb, fd_, buf, size, oft);
        ptr->iocb.data = reinterpret_cast<void *>(ptr->key);
        return ptr->key;
    }

    /**
     * Prepare a write IO.
     */
    unsigned int prepareWrite(off_t oft, size_t size, char* buf) {
        if (submitQueue_.size() >= queueSize_) {
            return 0;
        }

        AioDataPtr ptr = allocator_.alloc();
        assert(ptr->key != 0);
        submitQueue_.push(ptr);
        ptr->type = IOTYPE_WRITE;
        ptr->oft = oft;
        ptr->size = size;
        ptr->buf = buf;
        ptr->beginTime = 0.0;
        ptr->endTime = 0.0;
        ptr->done = false;
        ::io_prep_pwrite(&ptr->iocb, fd_, buf, size, oft);
        ptr->iocb.data = reinterpret_cast<void *>(ptr->key);
        return ptr->key;
    }

    /**
     * Prepare a flush IO.
     *
     * Currently aio flush is not supported
     * by almost all filesystems and block devices.
     */
    unsigned int prepareFlush() {
        if (submitQueue_.size() >= queueSize_) {
            return 0;
        }

        AioDataPtr ptr = allocator_.alloc();
        assert(ptr->key != 0);
        submitQueue_.push(ptr);
        ptr->type = IOTYPE_FLUSH;
        ptr->oft = 0;
        ptr->size = 0;
        ptr->buf = nullptr;
        ptr->beginTime = 0.0;
        ptr->endTime = 0.0;
        ptr->done = false;
        ::io_prep_fdsync(&ptr->iocb, fd_);
        ptr->iocb.data = reinterpret_cast<void *>(ptr->key);
        return ptr->key;
    }

    /**
     * Submit all prepared IO(s).
     */
    void submit() {
        size_t nr = submitQueue_.size();
        if (nr == 0) {
            return;
        }
        assert(iocbs_.size() >= nr);
        double beginTime = util::getTime();
        for (size_t i = 0; i < nr; i++) {
            AioDataPtr ptr = submitQueue_.front();
            submitQueue_.pop();
            iocbs_[i] = &ptr->iocb;
            ptr->beginTime = beginTime;
            assert(pendingIOs_.find(ptr->key) == pendingIOs_.end());
            pendingIOs_.insert(std::make_pair(ptr->key, ptr));
            //::printf("submit %u %p\n", ptr->key, ptr->buf); //debug
        }
        assert(submitQueue_.empty());

        size_t done = 0;
        while (done < nr) {
            //::printf("try to submit %zu\n", nr - done); //debug
            int err = ::io_submit(ctx_, nr - done, &iocbs_[done]);
            if (err < 0) {
                ::printf("submit %zu error %d\n", nr - done, err); //debug
                throw EofError();
            } else {
                //::printf("submitted %d\n", err); //debug
                done += err;
            }
        }
    }

    /**
     * Wait for an IO.
     *
     * Do not use wait()/waitOne() and waitFor() concurrently.
     */
    void waitFor(unsigned int key) {
        //::printf("waitFor %u\n", key); //debug
        auto &m = pendingIOs_;
        if (m.find(key) == m.end()) {
            throw RT_ERR("Aio with key %u is not found.\n");
        }
        AioDataPtr p0 = m[key];
        while (!p0->done) {
            AioDataPtr p1 = waitOne_(false);
            p1->done = true;
            if (p0 != p1) {
                completedIOs_.push(p1);
            } else {
                assert(p1->done);
            }
        }
        m.erase(key);
    }

    /**
     * Check a given IO has been completed or not.
     *
     * RETURN:
     *   true if the IO has been completed.
     */
    bool isCompleted(unsigned int key) const {
        const auto &m = pendingIOs_;
        if (m.find(key) == m.end()) {
            throw RT_ERR("Aio with key %u is not found.\n");
        }
        AioDataPtr p0 = m.at(key);
        return p0->done;
    }

    /**
     * Wait several IO(s) completed.
     *
     * @nr number of waiting IO(s). nr >= 0.
     * @queue completed key(s) will be inserted to.
     */
    void wait(size_t nr, std::queue<unsigned int>& queue) {
        while (nr > 0 && !completedIOs_.empty()) {
            AioDataPtr p = completedIOs_.front();
            completedIOs_.pop();
            assert(pendingIOs_.find(p->key) != pendingIOs_.end());
            pendingIOs_.erase(p->key);
            queue.push(p->key);
            nr--;
        }
        if (nr > 0) {
            std::queue<AioDataPtr> q;
            wait_(nr, q, true);
            while (!q.empty()) {
                queue.push(q.front()->key);
                q.pop();
            }
        }
    }

    /**
     * Wait just one IO completed.
     *
     * RETURN: completed key.
     */
    unsigned int waitOne() {
        if (completedIOs_.empty()) {
            AioDataPtr p = waitOne_(true);
            return p->key;
        } else {
            AioDataPtr p = completedIOs_.front();
            completedIOs_.pop();
            assert(pendingIOs_.find(p->key) != pendingIOs_.end());
            pendingIOs_.erase(p->key);
            return p->key;
        }
    }

private:
    /**
     * Wait several IO(s) completed.
     *
     * @nr number of waiting IO(s). nr >= 0.
     * @queue completed key(s) will be inserted to.
     * @isDelete AioDataPtr will be deleted from pendingIOs_ if true.
     */
    void wait_(size_t nr, std::queue<AioDataPtr>& queue, bool isDelete) {
        size_t done = 0;
        bool isError = false;
        while (done < nr) {
            int tmpNr = ::io_getevents(ctx_, 1, nr - done, &ioEvents_[done], NULL);
            if (tmpNr < 1) {
                throw std::runtime_error("io_getevents failed.");
            }
            double endTime = util::getTime();
            for (size_t i = done; i < done + tmpNr; i++) {
                struct iocb* iocb = static_cast<struct iocb *>(ioEvents_[i].obj);
                unsigned int key =
                    static_cast<unsigned int>(
                        reinterpret_cast<uintptr_t>(iocb->data));
                if (ioEvents_[i].res != iocb->u.c.nbytes) {
                    isError = true;
                }
                assert(pendingIOs_.find(key) != pendingIOs_.end());
                AioDataPtr ptr = pendingIOs_[key];
                assert(!ptr->done);
                ptr->done = true;
                ptr->endTime = endTime;
                queue.push(ptr);
                if (isDelete) {
                    pendingIOs_.erase(key);
                }
            }
            done += tmpNr;
        }
        if (isError) {
            ::printf("wait_ error.\n"); //debug
            throw EofError();
        }
    }

    /**
     * Wait just one IO completed.
     *
     * @isDelete AioDataPtr will be deleted from pendingIOs_ if true.
     *
     * RETURN:
     *   AioDataPtr.
     */
    AioDataPtr waitOne_(bool isDelete) {
        auto& event = ioEvents_[0];
        int err = ::io_getevents(ctx_, 1, 1, &event, NULL);
        double endTime = util::getTime();
        if (err != 1) {
            throw std::runtime_error("io_getevents failed.");
        }
        struct iocb *iocb = static_cast<struct iocb *>(event.obj);
        unsigned int key =
            static_cast<unsigned int>(
                reinterpret_cast<uintptr_t>(iocb->data));
        if (event.res != iocb->u.c.nbytes) {
            ::printf("waitOne_ error %lu\n", event.res); //debug
            throw EofError();
        }
        assert(pendingIOs_.find(key) != pendingIOs_.end());
        AioDataPtr ptr = pendingIOs_[key];
        ptr->endTime = endTime;
        assert(!ptr->done);
        ptr->done = true;
        if (isDelete) {
            pendingIOs_.erase(key);
        }
        return ptr;
    }
};

} // namespace aio
} // namespace walb

#endif /* AIO_UTIL_HPP */
