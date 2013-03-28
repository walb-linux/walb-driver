/**
 * @file
 * @brief IO recipe.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#ifndef IO_RECIPE_HPP
#define IO_RECIPE_HPP

#include "util.hpp"

namespace walb {
namespace util {

struct IoRecipe
{
private:
    const uint64_t offsetB_;
    const unsigned int ioSizeB_;
    const uint32_t csum_;
public:
    IoRecipe(uint64_t offsetB, unsigned int ioSizeB, uint32_t csum)
        : offsetB_(offsetB), ioSizeB_(ioSizeB), csum_(csum) {}
    ~IoRecipe() noexcept = default;

    uint64_t offsetB() const { return offsetB_; }
    unsigned int ioSizeB() const { return ioSizeB_; }
    uint32_t csum() const { return csum_; }

    std::string toString() const {
        return cybozu::util::formatString(
            "%" PRIu64 "\t%u\t%08x", offsetB(), ioSizeB(), csum());
    }

    void print(::FILE *fp) const {
        ::fprintf(fp, "%s\n", toString().c_str());
    }
    void print() const {
        print(::stdout);
    }

    static IoRecipe parse(const std::string& line) {
        if (line.empty()) { throw RT_ERR("IoRecipe parse error."); }

        size_t pos1 = line.find('\t');
        if (pos1 == std::string::npos) { throw RT_ERR("IoRecipe parse error."); }
        uint64_t offsetB = std::stoull(line.substr(0, pos1));

        std::string line1(line, pos1 + 1);
        size_t pos2 = line1.find('\t');
        if (pos2 == std::string::npos) { throw RT_ERR("IoRecipe parse error."); }
        unsigned int ioSizeB = std::stoul(line1.substr(0, pos2));

        std::string line2(line1, pos2 + 1);
        uint32_t csum = std::stoul(line2, nullptr, 16);

        return IoRecipe(offsetB, ioSizeB, csum);

    }
};

/**
 * Input data.
 */
class IoRecipeParser
{
private:
    ::FILE *fp_;
    std::queue<IoRecipe> queue_;
    bool isEnd_;
public:
    explicit IoRecipeParser(int fd) : fp_(::fdopen(fd, "r")), queue_(), isEnd_(false) {
        if (fp_ == nullptr) {
            throw RT_ERR("bad file descriptor.");
        }
    }

    IoRecipeParser(const IoRecipeParser &rhs) = delete;

    bool isEnd() const {
        return isEnd_ && queue_.empty();
    }

    IoRecipe get() {
        readAhead();
        if (queue_.empty()) {
            throw RT_ERR("No more input data.");
        }
        IoRecipe recipe = queue_.front();
        queue_.pop();
        return recipe;
    }
private:
    void readAhead() {
        if (isEnd_) { return; }

        while (queue_.size() < 16) {
            char buf[1024];
            if (::fgets(buf, 1024, fp_) == nullptr) {
                isEnd_ = true;
                return;
            }
            queue_.push(IoRecipe::parse(buf));
        }
    }
};

}} //namespace walb::util

#endif /* IO_RECIPE_HPP */
