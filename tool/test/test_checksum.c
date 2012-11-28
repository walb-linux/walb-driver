/**
 * Checksum test code.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <string.h>

#include "walb/checksum.h"
#include "random.h"

double time_double(struct timeval *tv)
{
	return (double)tv->tv_sec + tv->tv_usec * 0.000001;
}

/**
 * Allocate buffer.
 */
u8* alloc_buf(size_t size)
{
	return (u8*)malloc(size);
}

/**
 *
 */
void free_buf(u8* data)
{
	if (data != NULL)
		free(data);
}

static int comp(const void *p1, const void *p2)
{
	size_t d1 = *(u32*)(p1);
	size_t d2 = *(u32*)(p2);

	if (d1 < d2) return -1;
	if (d1 == d2) return 0;
	return 1;
}

static void make_sorted_random_array_index(
	size_t *ary, size_t size, size_t max_value, size_t align_size)
{
	size_t i, j;
	for (i = 0; i < size; i++) {
		int retry = 1;
		while (retry) {
			ary[i] = 1 + get_random(max_value/align_size - 1);

			retry = 0;
			for (j = 0; j < i; j++) {
				if (ary[j] == ary[i]) {
					retry = 1;
					break;
				}
			}
		}
	}
	qsort(ary, size, sizeof(size_t), comp);
	for (i = 0; i < size; i++) {
		ary[i] *= align_size;
		/* printf("%zu\n", ary[i]); */
	}
	ary[0] = 0;
	ary[size - 1] = max_value;

	for (i = 0; i < size; i++) {
		printf("idx: %zu\n", ary[i]);
	}
}

#define MID_SIZE 16

int main()
{
	size_t i;
	u8 *buf;
	size_t size = 1024 * 1024;
	u32 csum2tmp, csum3tmp;
	u32 csum1, csum2, csum3;
	size_t mid[MID_SIZE];
	struct timeval tv;
	double t1, t2, t3, t4;
	size_t s1, s2, s3;

	init_random();

	printf("making sorted_random_array_index...\n");
	make_sorted_random_array_index(mid, MID_SIZE, size, sizeof(u32));

	printf("making random array...\n");
	buf = alloc_buf(size);
	memset_random(buf, size);

	gettimeofday(&tv, 0); t1 = time_double(&tv);

	csum1 = checksum(buf, size);
	s1 = size;
	gettimeofday(&tv, 0); t2 = time_double(&tv);

	csum2tmp = 0;
	s2 = 0;
	for (i = 0; i < MID_SIZE - 1; i++) {
		size_t tmp_size = mid[i + 1] - mid[i];
		s2 += tmp_size;
		printf("idx: %zu size: %zu\n", mid[i], tmp_size);
		csum2tmp = checksum_partial(csum2tmp, buf + mid[i], tmp_size);
	}
	csum2 = checksum_finish(csum2tmp);
	gettimeofday(&tv, 0); t3 = time_double(&tv);

	csum3tmp = 0;
	csum3tmp = checksum_partial(csum3tmp, buf, size);
	csum3 = checksum_finish(csum3tmp);
	s3 = size;
	gettimeofday(&tv, 0); t4 = time_double(&tv);

	printf("%u (%zu bytes %f sec)\n"
		"%u (%zu bytes %f sec)\n"
		"%u (%zu bytes %f sec)\n",
		csum1, s1, t2 - t1,
		csum2, s2, t3 - t2,
		csum3, s3, t4 - t3);

	ASSERT(csum1 == csum2);
	ASSERT(csum1 == csum3);

#if 0
	printf("copying...\n");
	u8 *buf2 = alloc_buf(size);
	gettimeofday(&tv, 0); t1 = time_double(&tv);
	memcpy(buf2, buf, size);
	gettimeofday(&tv, 0); t2 = time_double(&tv);
	printf("copy %zu bytes takes %f sec\n",
		size, t2 - t1);
	free_buf(buf2);
#endif

	free_buf(buf);
	return 0;
}
