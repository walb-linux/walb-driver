/**
 * @file
 * @brief Checksum utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2012 Cybozu Labs, Inc.
 */
#include <cstring>

#ifndef CHECKSUM_HPP
#define CHECKSUM_HPP

namespace cybozu {
namespace util {

/**
 * Calculate checksum partially.
 * You must call this several time and finally call checksumFinish() to get csum.
 *
 * @data pointer to data.
 * @size data size.
 * @csum result of previous call, or salt.
 */
uint32_t checksumPartial(const void *data, size_t size, uint32_t csum)
{
    const char *p = reinterpret_cast<const char *>(data);
    while (sizeof(uint32_t) <= size) {
        csum += *reinterpret_cast<const uint32_t *>(p);
        size -= sizeof(uint32_t);
        p += sizeof(uint32_t);
    }
    if (0 < size) {
        uint32_t padding = 0;
        ::memcpy(&padding, p, size);
        csum += padding;
    }
    return csum;
}

/**
 * Finish checksum calculation.
 */
uint32_t checksumFinish(uint32_t csum)
{
    return ~csum + 1;
}

/**
 * Get checksum of a byte array.
 */
uint32_t calcChecksum(const void *data, size_t size, uint32_t salt)
{
    return checksumFinish(checksumPartial(data, size, salt));
}

}} //namespace cybozu::util

#endif /* CHECKSUM_HPP */
