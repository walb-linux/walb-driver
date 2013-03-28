/**
 * @file
 * @brief File IO utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2012 Cybozu Labs, Inc.
 */
#include <mutex>
#include <stdexcept>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "util.hpp"

#ifndef FILEIO_HPP
#define FILEIO_HPP

namespace cybozu {
namespace util {

/**
 * Eof error for IO.
 */
class EofError : public std::exception {
public:
    virtual const char *what() const noexcept {
        return "eof error";
    }
};

/**
 * File descriptor operations wrapper.
 */
class FdOperator
{
private:
    int fd_;
public:
    FdOperator(int fd)
        : fd_(fd) {}

    /**
     * read.
     */
    void read(char *buf, size_t size) {
        size_t s = 0;
        while (s < size) {
            ssize_t ret = ::read(fd_, &buf[s], size - s);
            if (ret < 0) {
                throw LibcError(errno, "read failed: ");
            }
            if (ret == 0) {
                throw EofError();
            }
            s += ret;
        }
    }

    /**
     * write.
     */
    void write(const char *buf, size_t size) {
        size_t s = 0;
        while (s < size) {
            ssize_t ret = ::write(fd_, &buf[s], size - s);
            if (ret < 0) {
                throw LibcError(errno, "write failed: ");
            }
            if (ret == 0) {
                throw EofError();
            }
            s += ret;
        }
    }

    /**
     * lseek.
     */
    void lseek(off_t oft, int whence) {
        off_t ret = ::lseek(fd_, oft, whence);
        if (ret == -1) {
            throw LibcError(errno, "lseek failed: ");
        }
    }

    /**
     * fdatasync.
     */
    void fdatasync() {
        int ret = ::fdatasync(fd_);
        if (ret) {
            throw LibcError(errno, "fdsync failed: ");
        }
    }

    /**
     * fsync.
     */
    void fsync() {
        int ret = ::fsync(fd_);
        if (ret) {
            throw LibcError(errno, "fsync failed: ");
        }
    }
};

/**
 * File descriptor wrapper. Read only.
 */
class FdReader : public FdOperator
{
public:
    FdReader(int fd) : FdOperator(fd) {}
    void write(const char *buf, size_t size) = delete;
    void fdatasync() = delete;
    void fsync() = delete;
};

/**
 * File descriptor wrapper. Write only.
 */
class FdWriter : public FdOperator
{
public:
    FdWriter(int fd) : FdOperator(fd) {}
    virtual void read(char *buf, size_t size) = delete;
};

/**
 * A simple file opener.
 * close() will be called in the destructor when you forget to call it.
 */
class FileOpener
{
private:
    int fd_;
    std::once_flag closeFlag_;
public:
    FileOpener(const std::string& filePath, int flags)
        : fd_(staticOpen(filePath, flags))
        , closeFlag_() {}

    FileOpener(const std::string& filePath, int flags, int mode)
        : fd_(staticOpen(filePath, flags, mode))
        , closeFlag_() {}

    ~FileOpener() {
        try {
            close();
        } catch (...) {
        }
    }

    int fd() const {
        if (fd_ < 0) {
            throw RT_ERR("fd < 0.");
        }
        return fd_;
    }

    void close() {
        std::call_once(closeFlag_, [&]() {
                if (::close(fd_)) {
                    throw LibcError(errno, "close failed: ");
                }
                fd_ = -1;
            });
    }
private:
    static int staticOpen(const std::string& filePath, int flags) {
        int fd = ::open(filePath.c_str(), flags);
        if (fd < 0) {
            throw LibcError(errno, "open failed: ");
        }
        return fd;
    }
    static int staticOpen(const std::string& filePath, int flags, int mode) {
        int fd = ::open(filePath.c_str(), flags, mode);
        if (fd < 0) {
            throw LibcError(errno, "open failed: ");
        }
        return fd;
    }
};

/**
 * Block device manager.
 *
 * You can do write/read or other operations on the device.
 * You can deal with normal file as a block device also.
 */
class BlockDevice
{
private:
    std::string name_;
    int openFlags_;
    int fd_;
    bool isBlockDevice_;
    uint64_t deviceSize_; // [bytes].
    unsigned int lbs_; // logical block size [bytes].
    unsigned int pbs_; // physical block size [bytes].

    std::once_flag closeFlag_;

public:
    BlockDevice(const std::string& name, int flags)
        : name_(name)
        , openFlags_(flags)
        , fd_(openDevice(name, flags))
        , isBlockDevice_(isBlockDeviceStatic(fd_))
        , deviceSize_(getDeviceSizeStatic(fd_))
        , lbs_(getLogicalBlockSizeStatic(fd_))
        , pbs_(getPhysicalBlockSizeStatic(fd_)) {
#if 0
        ::printf("device %s size %zu isWrite %d isDirect %d isBlockDevice %d "
                 "lbs %u pbs %u\n",
                 name_.c_str(), deviceSize_,
                 (openFlags_ & O_RDWR) != 0, (openFlags_ & O_DIRECT) != 0,
                 isBlockDevice_, lbs_, pbs_);
#endif
    }

    explicit BlockDevice(BlockDevice&& rhs)
        : name_(std::move(rhs.name_))
        , openFlags_(rhs.openFlags_)
        , fd_(rhs.fd_)
        , isBlockDevice_(rhs.isBlockDevice_)
        , deviceSize_(rhs.deviceSize_)
        , lbs_(rhs.lbs_)
        , pbs_(rhs.pbs_) {

        rhs.fd_ = -1;
    }

    BlockDevice& operator=(BlockDevice&& rhs) {
        name_ = std::move(rhs.name_);
        openFlags_ = rhs.openFlags_;
        fd_ = rhs.fd_; rhs.fd_ = -1;
        isBlockDevice_ = rhs.isBlockDevice_;
        deviceSize_= rhs.deviceSize_;
        lbs_ = rhs.lbs_;
        pbs_ = rhs.pbs_;
        return *this;
    }

    ~BlockDevice() {
        try {
            close();
        } catch (...) {
        }
    }

    void close() {
        std::call_once(closeFlag_, [&]() {
                if (fd_ > 0) {
                    if (::close(fd_) < 0) {
                        throw LibcError(errno, "close failed: ");
                    }
                    fd_ = -1;
                }
            });
    }

    /**
     * Read data and fill a buffer.
     */
    void read(off_t oft, size_t size, char* buf) {
        if (deviceSize_ < oft + size) { throw EofError(); }
        ::lseek(fd_, oft, SEEK_SET);
        size_t s = 0;
        while (s < size) {
#if 0
            ::fprintf(::stderr, "read %d %p &buf[%zu], %zu\n", fd_, &buf[s], s, size - s);
#endif
            ssize_t ret = ::read(fd_, &buf[s], size - s);
            if (ret < 0) {
                throw LibcError(errno, "read failed: ");
            }
            if (ret == 0) {
                throw EofError();
            }
            s += ret;
        }
    }

    /**
     * Write data of a buffer.
     */
    void write(off_t oft, size_t size, const char* buf) {
        if (deviceSize_ < oft + size) { throw EofError(); }
        ::lseek(fd_, oft, SEEK_SET);
        size_t s = 0;
        while (s < size) {
#if 0
            ::fprintf(::stderr, "write %d %p &buf[%zu], %zu\n", fd_, &buf[s], s, size - s);
#endif
            ssize_t ret = ::write(fd_, &buf[s], size - s);
            if (ret < 0) {
                throw LibcError(errno, "write failed: ");
            }
            if (ret == 0) {
                throw EofError();
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
            throw LibcError(errno, "fdatasync failed: ");
        }
    }

    /**
     * fsync.
     */
    void fsync() {
        int ret = ::fsync(fd_);
        if (ret) {
            throw LibcError(errno, "fsync failed: ");
        }
    }

    /**
     * Get device size [byte].
     */
    uint64_t getDeviceSize() const { return deviceSize_; }

    /**
     * Open flags.
     */
    int getFlags() const { return openFlags_; }

    /**
     * File descriptor.
     */
    int getFd() const { return fd_; }

    /**
     * RETURN:
     *   True if the descriptor is of a block device file,
     *   or false.
     */
    bool isBlockDevice() const { return isBlockDevice_; }

    unsigned int getPhysicalBlockSize() const { return pbs_; }
    unsigned int getLogicalBlockSize() const { return lbs_; }

private:
    /**
     * Helper function for constructor.
     */
    static int openDevice(const std::string& name, int flags) {
        int fd = ::open(name.c_str(), flags);
        if (fd < 0) {
            throw LibcError(
                errno, formatString("open %s failed: ", name.c_str()).c_str());
        }
        return fd;
    }

    static void statStatic(int fd, struct stat *s) {
        assert(fd >= 0);
        assert(s);
        if (::fstat(fd, s) < 0) {
            throw LibcError(errno, "fstat failed: ");
        }
    }

    static unsigned int getPhysicalBlockSizeStatic(int fd) {
        assert(fd >= 0);

        if (!isBlockDeviceStatic(fd)) {
            return 512;
        }

        unsigned int pbs;
        if (::ioctl(fd, BLKPBSZGET, &pbs) < 0) {
            throw LibcError(errno, "Getting physical block size failed: ");
        }
        assert(pbs > 0);
        return pbs;
    }

    static unsigned int getLogicalBlockSizeStatic(int fd) {
        assert(fd >= 0);

        if (!isBlockDeviceStatic(fd)) {
            return 512;
        }

        unsigned int lbs;
        if (::ioctl(fd, BLKSSZGET, &lbs) < 0) {
            throw LibcError(errno, "Geting logical block size failed: ");
        }
        assert(lbs > 0);
        return lbs;
    }

    static bool isBlockDeviceStatic(int fd) {
        assert(fd >= 0);

        struct stat s;
        statStatic(fd, &s);
        return (s.st_mode & S_IFMT) == S_IFBLK;
    }

    /**
     * Helper function for constructor.
     * Get device size in bytes.
     *
     * RETURN:
     *   device size [bytes].
     * EXCEPTION:
     *   std::runtime_error.
     */
    static uint64_t getDeviceSizeStatic(int fd) {

        if (isBlockDeviceStatic(fd)) {
            uint64_t size;
            if (::ioctl(fd, BLKGETSIZE64, &size) < 0) {
                throw LibcError(errno, "ioctl failed: ");
            }
            return size;
        } else {
            struct stat s;
            statStatic(fd, &s);
            return static_cast<uint64_t>(s.st_size);
        }
    }
};

}} //namespace cybozu::util

#endif /* FILEIO_HPP */
