/**
 * Utility functions for walbctl.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_RANDOM_USER_H
#define WALB_RANDOM_USER_H

#include "check_userland.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

#include "walb/userland.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Read /dev/urandom and fill a buffer.
 *
 * @data data buffer.
 * @size filling size in bytes.
 *
 * RETURN:
 *   true in success.
 */
static inline bool read_urandom(void *data, size_t size)
{
	int fd;
	ssize_t read_size;
	fd = open("/dev/urandom", O_RDONLY);
	if (fd < 0) {
		perror("open /dev/urandom failed.");
		return false;
	}
	read_size = 0;
	while (read_size < (ssize_t)size) {
		int r = read(fd, (u8 *)data + read_size, size - read_size);
		if (r <= 0) {
			perror("read /dev/urandom failed.");
			return false;
		}
		read_size += r;
	}
	if (close(fd)) {
		perror("close failed.");
		return false;
	}
	return true;
}

/**
 * Initialize random seed.
 */
static inline void init_random(void)
{
	srand(time(0));
}

/**
 * Get random value in a range.
 * RETURN:
 *   min <= val < max
 */
static inline int get_random_range(int min, int max)
{
	return min + (int)(rand() * (max - min + 0.0) / (RAND_MAX + 1.0));
}

/**
 * Get random value.
 * RETURN:
 *   0 <= val < max.
 */
static inline int get_random(int max)
{
	return get_random_range(0, max);
}

/**
 * Randomly set the buffer.
 */
static inline void memset_random(u8 *data, size_t size)
{
	size_t i;
	for (i = 0; i < size; i++) {
		data[i] = (u8)rand();
	}
}

/**
 * Random generator test.
 */
static inline void test_random()
{
	int i;
	for (i = 0; i < 100000; i++) {
		printf("%d\n", get_random(10));
	}
}

#ifdef __cplusplus
}
#endif

#endif /* WALB_RANDOM_USER_H */
