#include <map>
#include <queue>
#include "util.hpp"
#include "memory_buffer.hpp"

struct A
{
    size_t a;
    size_t b;
    size_t c;
    size_t d;
    size_t e;
};

template <typename T>
class SequentialQueue
{
private:
    std::queue<std::shared_ptr<T> > q_;
public:
    SequentialQueue() : q_() {}
    DISABLE_COPY_AND_ASSIGN(SequentialQueue);

    void push(std::shared_ptr<T> ptr) {
        q_.push(ptr);
    }

    void pop() {
        if (empty()) { return; }
        q_.pop();
    }

    void clear() {
        while (!empty()) { pop(); }
    }

    size_t size() const { return q_.size(); }
    bool empty() const { return q_.empty(); }
};

/**
 * Queue where items will be pushed at random position.
 */
template <typename T>
class RandomQueue
{
private:
    using Map = std::multimap<size_t, std::shared_ptr<T> >;

    Map m_;
    cybozu::util::Rand<size_t> rand_;

public:
    RandomQueue() : m_(), rand_() {}
    DISABLE_COPY_AND_ASSIGN(RandomQueue);

    void push(std::shared_ptr<T> ptr) {
        m_.insert(std::make_pair(rand_.get(), ptr));
    }

    void pop() {
        typename Map::iterator it = m_.begin();
        if (it != m_.end()) {
            m_.erase(it);
        }
    }

    void clear() {
        typename Map::iterator it = m_.begin();
        while (it != m_.end()) {
            it = m_.erase(it);
        }
    }

    size_t size() const { return m_.size(); }
    bool empty() const { return m_.empty(); }
};

/**
 * Run block benchmark.
 *
 * Queue: RandomQueue or SequentialQueue.
 * @sizeB: [byte].
 * @initFillB: [byte].
 * @numIter: Number of iterations.
 */
template <typename Queue>
void runBlockBenchmark(
    size_t sizeB, size_t initFillB, size_t numIter)
{
    double t0 = cybozu::util::getTime();
    const size_t LBS = 512;
    cybozu::util::BlockAllocator<char> ba(sizeB / LBS, LBS, LBS);
    Queue q;

    size_t total = 0;
    while (total < initFillB / LBS) {
        std::shared_ptr<char> b = ba.alloc();
        ::memset(b.get(), 0, LBS);
        q.push(b);
        total++;
    }
    for (size_t i = 0; i < numIter; i++) {
        q.pop();
        std::shared_ptr<char> b = ba.alloc();
        ::memset(b.get(), 0, LBS);
        q.push(b);
    }
    q.clear();
    double t1 = cybozu::util::getTime();
    ::printf("execution period: %.06f sec.\n", t1 - t0);
    ::printf("totalPre: %zu\n"
             "totalNew: %zu\n"
             , ba.getTotalPre(), ba.getTotalNew());
}

/**
 * Run block-multi benchmark.
 *
 * Queue: RandomQueue or SequentialQueue.
 * @sizeB: [byte].
 * @initFillB: [byte].
 * @numIter: Number of iterations.
 * @numAlloc: Number of allocation [block].
 */
template <typename Queue>
void runBlockMultiBenchmark(
    size_t sizeB, size_t initFillB, size_t numIter, size_t numAlloc)
{
    double t0 = cybozu::util::getTime();
    const size_t LBS = 512;
    cybozu::util::BlockMultiAllocator<char> ba(sizeB / LBS, LBS, LBS);
    Queue q;

    size_t total = 0;
    while (total < initFillB / LBS) {
        std::shared_ptr<char> b = ba.alloc(numAlloc);
        ::memset(b.get(), 0, LBS * numAlloc);
        q.push(b);
        total += numAlloc;
    }
    for (size_t i = 0; i < numIter; i++) {
        q.pop();
        std::shared_ptr<char> b = ba.alloc(numAlloc);
        ::memset(b.get(), 0, LBS * numAlloc);
        q.push(b);
    }
    q.clear();
    double t1 = cybozu::util::getTime();
    ::printf("execution period: %.06f sec.\n", t1 - t0);
    ::printf("totalPre: %zu\n"
             "totalNew: %zu\n"
             , ba.getTotalPre(), ba.getTotalNew());
}

void runAllocateManagerBenchmark(size_t initFillB, size_t numIter, size_t allocB)
{
    double t0 = cybozu::util::getTime();
    cybozu::util::AllocationManager mgr;
    std::queue<size_t> q;
    size_t off = 0;
    size_t n = initFillB / allocB;

    for (size_t i = 0; i < n; i++) {
        mgr.setAllocated(off, allocB);
        q.push(off);
        off = (off + allocB) % initFillB;
    }
    for (size_t i = n; i < numIter; i++) {
        mgr.unsetAllocated(q.front());
        q.pop();

        mgr.setAllocated(off, allocB);
        q.push(off);
        off = (off + allocB) % initFillB;
    }
    while (!q.empty()) {
        mgr.unsetAllocated(q.front());
        q.pop();
    }
    double t1 = cybozu::util::getTime();

    ::printf("execution period: %.06f sec.\n"
             "number of oiterations: %zu\n"
             , t1 - t0, numIter);
}

int main()
{
    const size_t KB = 1024;
    const size_t MB = 1024 * KB;
    const size_t LBS = 512;

    /* BlockAllocator test. */
    {
        size_t nr = MB / LBS;
        cybozu::util::BlockAllocator<char> ba(nr, LBS, LBS);
        RandomQueue<char> q;
        for (size_t i = 0; i < nr / 2; i++) {
            q.push(ba.alloc());
        }
        for (size_t i = 0; i < 100000; i++) {
            q.pop();
            q.push(ba.alloc());
        }
        q.clear();
    }

    /* BlockMultiAllocator test. */
    {
        size_t nr = MB / LBS;
        cybozu::util::BlockMultiAllocator<char> ba(nr, LBS, LBS);
        RandomQueue<char> q;
        cybozu::util::Rand<size_t> rand;
        size_t total = 0;
        while (total  < nr / 2) {
            size_t s = rand.get() % 8 + 1;
            q.push(ba.alloc(s));
            total += s;
        }
        for (size_t i = 0; i < 100000; i++) {
            q.pop();
            size_t s = rand.get() % 8 + 1;
            q.push(ba.alloc(s));
        }
        q.clear();
    }

    using SQ = SequentialQueue<char>;
    using RQ = RandomQueue<char>;

    size_t preallocated = 32U * MB;
    size_t initFilled = 256 * KB;
    size_t nIter = 10000000;
    size_t nAlloc = 32;
    size_t bs = 4096;

    runAllocateManagerBenchmark(initFilled, nIter, bs);

    runBlockBenchmark<SQ>(preallocated, initFilled, nIter);
    runBlockBenchmark<SQ>(0, initFilled, nIter);

    runBlockBenchmark<RQ>(preallocated, initFilled, nIter);
    runBlockBenchmark<RQ>(0, initFilled, nIter);

    runBlockMultiBenchmark<SQ>(preallocated, initFilled, nIter, nAlloc);
    runBlockMultiBenchmark<SQ>(0, initFilled, nIter, nAlloc);

    runBlockMultiBenchmark<RQ>(preallocated, initFilled, nIter, nAlloc);
    runBlockMultiBenchmark<RQ>(0, initFilled, nIter, nAlloc);

    return 0;
}
