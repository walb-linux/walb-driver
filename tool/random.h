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
 * Read /dev/urandom to generate random value.
 *
 * @return random value in success, or 0.
 */
static inline u32 read_urandom()
{
	int fd, val;
	fd = open("/dev/urandom", O_RDONLY);
	if (fd < 0) {
		perror("open /dev/urandom failed\n");
		goto error;
	}
	if (read(fd, (void *)&val, sizeof(u32)) != sizeof(u32)) {
		printf("read /dev/urandom failed\n");
		goto error;
	}
	close(fd);
	return val;
error:
	return 0;
}

/**
 * Initialize random seed.
 */
static inline void init_random()
{
	u32 r;
	r = read_urandom();
	if (r == 0) {
		srand(time(0));
	} else {
		srand(r);
	}
}


/**
 * Get random value.
 * @return 0 <= val < max
 */
static inline int get_random(int max)
{
	int min = 0;
	return min + (int)(rand() * (max - min + 0.0) / (RAND_MAX + 1.0));
}

/**
 * Randomly set the buffer.
 */
static inline void memset_random(u8 *data, size_t size)
{
	size_t i;
	for (i = 0; i < size; i++) {
		data[i] = (u8)get_random(255);
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
