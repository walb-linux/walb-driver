/**
 * @file
 * @brief Utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2012 Cybozu Labs, Inc.
 */
#ifndef UTIL_HPP
#define UTIL_HPP

#include <cassert>
#include <stdexcept>
#include <sstream>
#include <cstdarg>
#include <algorithm>
#include <vector>
#include <queue>
#include <mutex>
#include <cstring>

#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

namespace walb {
namespace util {

/**
 * Create a std::string using printf() like formatting.
 */
std::string formatString(const char * format, ...)
{
    std::string st;
    va_list args;
    int ret;

    st.resize(256);

    for (int i = 0; i < 2; i++) {
        va_start(args, format);
        ret = ::vsnprintf(&st[0], st.size(), format, args);
        va_end(args);
        if (ret < 0) {
            throw std::runtime_error("vsnprintf failed.");
        }
        if (static_cast<size_t>(ret) >= st.size()) {
            st.resize(ret + 1);
            continue;
        }
        if (static_cast<size_t>(ret) + 1 < st.size()) {
            st.resize(ret + 1);
        }
        assert(ret + 1 == st.size());
        break;
    }
    return st;
}

#if 0
/**
 * Each IO log.
 */
struct IoLog
{
    const unsigned int threadId;
    const IoType type;
    const size_t blockId;
    const double startTime; /* unix time [second] */
    const double response; /* [second] */

    IoLog(unsigned int threadId_, IoType type_, size_t blockId_,
          double startTime_, double response_)
        : threadId(threadId_)
        , type(type_)
        , blockId(blockId_)
        , startTime(startTime_)
        , response(response_) {}

    IoLog(const IoLog& log)
        : threadId(log.threadId)
        , type(log.type)
        , blockId(log.blockId)
        , startTime(log.startTime)
        , response(log.response) {}

    void print() {
        ::printf("threadId %d type %d blockId %10zu startTime %.06f response %.06f\n",
                 threadId, (int)type, blockId, startTime, response);
    }
};
#endif

static inline double getTime()
{
    struct timeval tv;
    double t;

    ::gettimeofday(&tv, NULL);

    t = static_cast<double>(tv.tv_sec) +
        static_cast<double>(tv.tv_usec) / 1000000.0;
    return t;
}

enum Mode
{
    READ_MODE, WRITE_MODE, MIX_MODE
};

class BlockDevice
{
private:
    std::string name_;
    Mode mode_;
    int fd_;
    size_t deviceSize_;

    std::once_flag close_flag_;

public:
    BlockDevice(const std::string& name, const Mode mode, bool isDirect)
        : name_(name)
        , mode_(mode)
        , fd_(openDevice(name, mode, isDirect))
        , deviceSize_(getDeviceSizeFirst(fd_)) {
#if 0
        ::printf("device %s size %zu mode %d isDirect %d\n",
                 name_.c_str(), size_, mode_, isDirect_);
#endif
    }
    explicit BlockDevice(BlockDevice&& rhs)
        : name_(std::move(rhs.name_))
        , mode_(rhs.mode_)
        , fd_(rhs.fd_)
        , deviceSize_(rhs.deviceSize_) {

        rhs.fd_ = -1;
    }
    BlockDevice& operator=(BlockDevice&& rhs) {

        name_ = std::move(rhs.name_);
        mode_ = rhs.mode_;
        fd_ = rhs.fd_; rhs.fd_ = -1;
        deviceSize_= rhs.deviceSize_;
        return *this;
    }

    ~BlockDevice() {
        close();
    }

    void close() {
        std::call_once(close_flag_, [&]() {
                if (fd_ > 0) {
                    if (::close(fd_) < 0) {
                        ::fprintf(::stderr, "close() failed.\n");
                    }
                    fd_ = -1;
                }
            });
    }

    /**
     * Get device size [byte].
     */
    size_t getDeviceSize() const {

        return deviceSize_;
    }

    class EofError : public std::exception {};

    /**
     * Read data and fill a buffer.
     */
    void read(off_t oft, size_t size, char* buf) {

        if (deviceSize_ < oft + size) { throw EofError(); }
        ::lseek(fd_, oft, SEEK_SET);
        size_t s = 0;
        while (s < size) {
            ssize_t ret = ::read(fd_, &buf[s], size - s);
            if (ret < 0) {
                throw std::runtime_error(
                    formatString("read failed: %s.", ::strerror(errno)));
            }
            s += ret;
        }
    }

    /**
     * Write data of a buffer.
     */
    void write(off_t oft, size_t size, char* buf) {

        if (deviceSize_ < oft + size) { throw EofError(); }
        if (mode_ == READ_MODE) { throw std::runtime_error("write is not permitted."); }
        ::lseek(fd_, oft, SEEK_SET);
        size_t s = 0;
        while (s < size) {
            ssize_t ret = ::write(fd_, &buf[s], size - s);
            if (ret < 0) {
                throw std::runtime_error(
                    formatString("write failed: %s.", ::strerror(errno)));
            }
            s += ret;
        }
    }

    /**
     * fdatasync.
     */
    void fdatasync() {

        int ret = ::fdatasync(fd_);
        if (ret) {
            throw std::runtime_error(
                formatString("fdsync failed: %s.", ::strerror(errno)));
        }
    }

    /**
     * fsync.
     */
    void fsync() {

        int ret = ::fsync(fd_);
        if (ret) {
            throw std::runtime_error(
                formatString("fsync failed: %s.", ::strerror(errno)));
        }
    }

    Mode getMode() const { return mode_; }
    int getFd() const { return fd_; }

private:

    /**
     * Helper function for constructor.
     */
    static int openDevice(const std::string& name, Mode mode, bool isDirect) {

        int fd;
        int flags = 0;
        switch (mode) {
        case READ_MODE:  flags = O_RDONLY; break;
        case WRITE_MODE: flags = O_WRONLY; break;
        case MIX_MODE:   flags = O_RDWR;   break;
        }
        if (isDirect) { flags |= O_DIRECT; }

        fd = ::open(name.c_str(), flags);
        if (fd < 0) {
            throw std::runtime_error(
                formatString("open %s failed: %s.",
                             name.c_str(), ::strerror(errno)));
        }
        return fd;
    }

    /**
     * Helper function for constructor.
     * Get device size in bytes.
     */
    static size_t getDeviceSizeFirst(int fd) {

        size_t ret;
        struct stat s;
        if (::fstat(fd, &s) < 0) {
            std::string msg(formatString("fstat failed: %s.", ::strerror(errno)));
            throw std::runtime_error(msg);
        }
        if ((s.st_mode & S_IFMT) == S_IFBLK) {
            size_t size;
            if (::ioctl(fd, BLKGETSIZE64, &size) < 0) {
                std::string msg(formatString("ioctl failed: %s.", ::strerror(errno)));
                throw std::runtime_error(msg);
            }
            ret = size;
        } else {
            ret = s.st_size;
        }
#if 0
        std::cout << "devicesize: " << ret << std::endl; //debug
#endif
        return ret;
    }
};

/**
 * Calculate access range.
 */
static inline size_t calcAccessRange(
    size_t accessRange, size_t blockSize, const BlockDevice& dev) {

    return (accessRange == 0) ? (dev.getDeviceSize() / blockSize) : accessRange;
}


class PerformanceStatistics
{
private:
    double total_;
    double max_;
    double min_;
    size_t count_;

public:
    PerformanceStatistics()
        : total_(0), max_(-1.0), min_(-1.0), count_(0) {}
    PerformanceStatistics(double total, double max, double min, size_t count)
        : total_(total), max_(max), min_(min), count_(count) {}

    void updateRt(double rt) {

        if (max_ < 0 || min_ < 0) {
            max_ = rt; min_ = rt;
        } else if (max_ < rt) {
            max_ = rt;
        } else if (min_ > rt) {
            min_ = rt;
        }
        total_ += rt;
        count_++;
    }

    double getMax() const { return max_; }
    double getMin() const { return min_; }
    double getTotal() const { return total_; }
    size_t getCount() const { return count_; }

    double getAverage() const { return total_ / (double)count_; }

    void print() const {
        ::printf("total %.06f count %zu avg %.06f max %.06f min %.06f\n",
                 getTotal(), getCount(), getAverage(),
                 getMax(), getMin());
    }
};

template<typename T> //T is iterator type of PerformanceStatistics.
static inline PerformanceStatistics mergeStats(const T begin, const T end)
{
    double total = 0;
    double max = -1.0;
    double min = -1.0;
    size_t count = 0;

    std::for_each(begin, end, [&](PerformanceStatistics& stat) {

            total += stat.getTotal();
            if (max < 0 || max < stat.getMax()) { max = stat.getMax(); }
            if (min < 0 || min > stat.getMin()) { min = stat.getMin(); }
            count += stat.getCount();
        });

    return PerformanceStatistics(total, max, min, count);
}

/**
 * Convert throughput data to string.
 */
static inline
std::string getDataThroughputString(double throughput)
{
    const double GIGA = static_cast<double>(1000ULL * 1000ULL * 1000ULL);
    const double MEGA = static_cast<double>(1000ULL * 1000ULL);
    const double KILO = static_cast<double>(1000ULL);

    std::stringstream ss;
    if (throughput > GIGA) {
        throughput /= GIGA;
        ss << throughput << " GB/sec";
    } else if (throughput > MEGA) {
        throughput /= MEGA;
        ss << throughput << " MB/sec";
    } else if (throughput > KILO) {
        throughput /= KILO;
        ss << throughput << " KB/sec";
    } else {
        ss << throughput << " B/sec";
    }

    return ss.str();
}

/**
 * Print throughput data.
 * @blockSize block size [bytes].
 * @nio Number of IO executed.
 * @periodInSec Elapsed time [second].
 */
static inline
void printThroughput(size_t blockSize, size_t nio, double periodInSec)
{
    double throughput = static_cast<double>(blockSize * nio) / periodInSec;
    double iops = static_cast<double>(nio) / periodInSec;
    ::printf("Throughput: %.3f B/s %s %.3f iops.\n",
             throughput, getDataThroughputString(throughput).c_str(), iops);
}

/**
 * Ring buffer for block data.
 */
class BlockBuffer
{
private:
    const size_t nr_;
    std::vector<char *> bufArray_;
    size_t idx_;

public:
    BlockBuffer(size_t nr, size_t blockSize)
        : nr_(nr)
        , bufArray_(nr)
        , idx_(0) {

        assert(blockSize % 512 == 0);
        for (size_t i = 0; i < nr; i++) {
            char *p = nullptr;
            int ret = ::posix_memalign((void **)&p, 512, blockSize);
            assert(ret == 0);
            assert(p != nullptr);
            bufArray_[i] = p;
        }
    }

    ~BlockBuffer() {

        for (size_t i = 0; i < nr_; i++) {
            ::free(bufArray_[i]);
        }
    }

    char* next() {

        char *ret = bufArray_[idx_];
        idx_ = (idx_ + 1) % nr_;
        return ret;
    }
};

} //namespace util
} //namespace walb

#endif /* UTIL_HPP */
