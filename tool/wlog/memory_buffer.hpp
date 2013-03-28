/**
 * @file
 * @brief Memory buffer utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2012 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <vector>
#include <set>

#include "util.hpp"

#ifndef MEMORY_BUFFER_HPP
#define MEMORY_BUFFER_HPP

/**
 * Block (aligned memory) allocators using posix_memalign().
 *   allocateBlocks()
 *   class BlockAllocator
 *   class BlockMultiAllocator (This is not much fast.)
 *
 * Memory allocator using malloc().
 *   allocateMemory()
 *
 * For internal use:
 *   class AllocationManager
 *   class BlockBuffer
 *   class BlockMultiBuffer
 */
namespace cybozu {
namespace util {

/**
 * Manager of allocated ranges.
 */
class AllocationManager
{
private:
    /*
     * Entry: (offset, size)
     *
     * Overlapped ranges exist only in the range
     * off0 - maxItemSize_ <= offset < off0 + size0.
     */
    using Entry = std::pair<size_t, size_t>;
    using Set = std::set<Entry>;
    using SetIterator = Set::iterator;
    using SetConstIterator = Set::const_iterator;

    size_t maxItemSize_;
    size_t allocated_;
    Set set_;

public:
    AllocationManager() : maxItemSize_(0), allocated_(0), set_() {}

    /**
     * RETURN:
     *   true if [off, off + size) is not allocated,
     *   or false.
     */
    bool canAlloc(size_t off, size_t size) {
        size_t off0 = 0;
        if (maxItemSize_ < off) { off0 = off - maxItemSize_; }
        size_t off1 = off + size;

        SetIterator it =
            set_.lower_bound(std::make_pair(off0, 0));
        while (it != set_.end() && it->first < off1) {
            if (isOverlapped(it->first, it->second, off, size)) {
                return false;
            }
            ++it;
        }
        return true;
    }

    /**
     * Make a range allocated.
     */
    void setAllocated(size_t off, size_t size) {
        assert(0 < size);
        UNUSED std::pair<SetIterator, bool> p0 =
            set_.insert(std::make_pair(off, size));
        assert(p0.second);
        allocated_ += size;
        if (maxItemSize_ < size) { maxItemSize_ = size; }
    }

    /**
     * Make a range de-allocated.
     */
    void unsetAllocated(size_t off) {
        SetIterator it =
            set_.lower_bound(std::make_pair(off, 0));
        assert(it != set_.end());
        assert(it->first == off);
        size_t size = it->second;
        set_.erase(it);
        allocated_ -= size;
        if (set_.empty()) { maxItemSize_ = 0; }
    }

    size_t getAllocatedSize() const {
        return allocated_;
    }

    /**
     * For debug.
     */
    void printSet() const {
        SetConstIterator it = set_.cbegin();
        while (it != set_.cend()) {
            ::printf("%zu %zu\n", it->first, it->second);
            ++it;
        }
    }
private:
    bool isOverlapped(size_t off0, size_t size0,
                      size_t off1, size_t size1) const {
        return off0 < off1 + size1 && off1 < off0 + size0;
    }
};

/**
 * Allocate aligned memory blocks.
 *
 * @alignment alighment size [byte].
 * @size block size [byte].
 * @nr number of blocks.
 */
template<typename T>
static inline
std::shared_ptr<T> allocateBlocks(size_t alignment, size_t size, size_t nr = 1)
{
    assert(0 < nr);
    T *p = nullptr;
    int ret = ::posix_memalign(reinterpret_cast<void **>(&p), alignment, size * nr);
    if (ret) { throw std::bad_alloc(); }
    assert(p != nullptr);
    //::printf("allocated %p\n", p);
    return std::shared_ptr<T>(p, ::free);
}

/**
 * Ring buffer for block data.
 * Each block memory is aligned.
 *
 * typename T must be one-byte type like char.
 */
template <typename T>
class BlockBuffer /* final */
{
private:
    const size_t nr_;
    const size_t blockSize_;
    T *array_;
    std::vector<bool> bmp_;
    size_t idx_;
    size_t allocated_;

public:
    BlockBuffer(size_t nr, size_t alignment, size_t blockSize)
        : nr_(nr), blockSize_(blockSize)
        , array_(nullptr), bmp_(nr), idx_(0), allocated_(0) {
        if (nr == 0) { return; }
        int ret = ::posix_memalign(
            reinterpret_cast<void **>(&array_), alignment, blockSize * nr);
        if (ret != 0) { throw std::bad_alloc(); }
        assert(array_ != nullptr);
    }
    ~BlockBuffer() noexcept { ::free(array_); }
    DISABLE_COPY_AND_ASSIGN(BlockBuffer);

    T *alloc() {
        if (nr_ == 0 || bmp_[idx_]) { return nullptr; }
        bmp_[idx_] = true;
        uintptr_t pu = reinterpret_cast<uintptr_t>(array_);
        pu += idx_ * blockSize_;
        T *p = reinterpret_cast<T *>(pu);
        idx_ = (idx_ + 1) % nr_;
        allocated_++;
        return p;
    }

    void free(T *p) {
        size_t idx = ptrToIdx(p);
        assert(bmp_[idx]);
        bmp_[idx] = false;
        allocated_--;
    }

private:
    size_t ptrToIdx(T *p) const {
        assert(array_ <= p);
        uintptr_t s =
            reinterpret_cast<uint8_t *>(p) -
            reinterpret_cast<uint8_t *>(array_);
        assert(s % blockSize_ == 0);
        return s / blockSize_;
    }
};

/**
 * Fast aligned memory block allocator
 * with pre-allocated ring buffer.
 */
template <typename T>
class BlockAllocator
{
private:
    const size_t alignment_;
    const size_t blockSize_;
    BlockBuffer<T> bb_;
    size_t totalPre_;
    size_t totalNew_;

public:
    /**
     * @nr number of pre-allocated blocks.
     * @alignment alignment bytes for posix_memalign().
     * @blockSize block size in bytes.
     */
    BlockAllocator(size_t nr, size_t alignment, size_t blockSize)
        : alignment_(alignment), blockSize_(blockSize)
        , bb_(nr, alignment, blockSize)
        , totalPre_(0), totalNew_(0) {}
    ~BlockAllocator() noexcept = default;
    DISABLE_COPY_AND_ASSIGN(BlockAllocator);

    /**
     * Allocate contiguous blocks.
     * If allocation failed from ring buffer,
     * this will allocate memory directly.
     */
    std::shared_ptr<T> alloc() {
        T *p = bb_.alloc();
        if (p == nullptr) {
            totalNew_++;
            return allocateBlocks<T>(alignment_, blockSize_, 1);
        }
        totalPre_++;
        return std::shared_ptr<T>(p, [&](T *p) { bb_.free(p); });
    }

    size_t blockSize() const { return blockSize_; }
    size_t getTotalPre() const { return totalPre_; }
    size_t getTotalNew() const { return totalNew_; }
};

/**
 * Contiguous blocks buffer.
 */
class BlockMultiBuffer /* final */
{
private:
    const size_t nr_;
    const size_t blockSize_;
    size_t idxHead_;
    size_t idxTail_;
    std::vector<size_t> sizeVec_;
    void *buf_;

public:
    /**
     * @nr: Number of items.
     * @alignment: alignment size [byte].
     * @blockSize: [byte].
     */
    BlockMultiBuffer(size_t nr, size_t alignment, size_t blockSize)
        : nr_(nr), blockSize_(blockSize), idxHead_(0), idxTail_(0)
        , sizeVec_(nr, 0), buf_(nullptr) {
        if (nr == 0) { return; }
        int ret = ::posix_memalign(&buf_, alignment, blockSize * nr);
        if (ret != 0) { throw std::bad_alloc(); }
        assert(buf_ != nullptr);
    }

    ~BlockMultiBuffer() noexcept { ::free(buf_); }
    DISABLE_COPY_AND_ASSIGN(BlockMultiBuffer);

    /**
     * Allocate memory
     */
    void *alloc(size_t nr) {
        assert(getContiguousFreeCapacity() <= getFreeCapacity());
        if (getFreeCapacity() <= nr) { return nullptr; }
        if (getContiguousFreeCapacity() <= nr) {
            idxHead_ = 0;
            if (getFreeCapacity() <= nr) { return nullptr; }
        }

        assert(sizeVec_[idxHead_] == 0);
        sizeVec_[idxHead_] = nr;
        uintptr_t pu = reinterpret_cast<uintptr_t>(buf_);
        pu += idxHead_ * blockSize_;
        idxHead_ += nr;
        if (idxHead_ == nr_) { idxHead_ = 0; }
        return reinterpret_cast<void *>(pu);
    }

    void *alloc() { return alloc(1); }

    /**
     * Free a memory.
     */
    void free(void *p) {
        if (p == nullptr) { return; }
        uintptr_t pu0 = reinterpret_cast<uintptr_t>(buf_);
        uintptr_t pu1 = reinterpret_cast<uintptr_t>(p);
        assert(pu0 <= pu1);
        assert((pu1 - pu0) % blockSize_ == 0);
        size_t idx = (pu1 - pu0) / blockSize_;
        assert(0 < sizeVec_[idx]);
        sizeVec_[idx] = 0;

        idx = idxTail_;
        while (idx != idxHead_ && sizeVec_[idx] == 0) {
            idx++;
            if (idx == nr_) { idx = 0; }
        }
        idxTail_ = idx;
    }

    void printVec() const {
        for (size_t s : sizeVec_) {
            ::printf("%zu ", s);
        }
        ::printf("(%zu, %zu)\n", idxTail_, idxHead_);
    }
private:
    size_t getFreeCapacity() const {
        if (idxTail_ <= idxHead_) {
            return nr_ - (idxHead_ - idxTail_);
        } else {
            return idxTail_ - idxHead_;
        }
    }

    size_t getContiguousFreeCapacity() const {
        if (idxHead_ < idxTail_) {
            return idxTail_ - idxHead_;
        } else {
            return nr_ - idxHead_;
        }
    }
};

/**
 * Block allocator for arbitrary number of blocks
 * with pre-allocated ring buffer.
 *
 * T: char type.
 */
template <typename T>
class BlockMultiAllocator /* final */
{
private:
    const size_t alignment_;
    const size_t blockSize_;
    BlockMultiBuffer bb_;
    size_t totalPre_;
    size_t totalNew_;

public:
    /**
     * @nr: number of pre-allocated items.
     * @blockSize: [byte].
     */
    BlockMultiAllocator(size_t nr, size_t alignment, size_t blockSize)
        : alignment_(alignment), blockSize_(blockSize), bb_(nr, alignment, blockSize)
        , totalPre_(0), totalNew_(0) {}

    ~BlockMultiAllocator() noexcept {}
    DISABLE_COPY_AND_ASSIGN(BlockMultiAllocator);

    /**
     * Allocated memory will be freed automatically.
     */
    std::shared_ptr<T> alloc(size_t nr) {
        void *p = bb_.alloc(nr);
        if (p == nullptr) {
            totalNew_ += nr;
            return allocateBlocks<T>(alignment_, blockSize_, nr);
        }
        totalPre_ += nr;
        return std::shared_ptr<T>(reinterpret_cast<T *>(p), [&](T *p) {
                bb_.free(p);
            });
    }

    std::shared_ptr<T> alloc() { return alloc(1); }
    size_t getTotalPre() const { return totalPre_; }
    size_t getTotalNew() const { return totalNew_; }

    void printVec() const { bb_.printVec(); }
};

/**
 * Allocate a contiguous memory.
 *
 * T: any type.
 * @size: memory size in bytes to allocate.
 */
template <typename T>
static inline
std::shared_ptr<T> allocateMemory(size_t size)
{
    T *p = nullptr;
    p = reinterpret_cast<T *>(::malloc(size));
    if (p == nullptr) {
        throw std::bad_alloc();
    }
    return std::shared_ptr<T>(p, ::free);
}

}} //namespace cybozu::util

#endif /* MEMORY_BUFFER_HPP */
