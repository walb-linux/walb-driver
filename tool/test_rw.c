/**
 * Read written block check.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <stdlib.h>
#include <assert.h>

#include "random.h"
#include "util.h"

#if 1
#define BLOCK_SIZE 512
#elif 1
#define BLOCK_SIZE 4096
#else
#define BLOCK_SIZE 32768
#endif

/**
 * Dump memory image for debug.
 */
void dump_memory(u8 *data, size_t size)
{
	size_t i;
	for (i = 0; i < size; i ++) {
		printf("%02X ", data[i]);
		if (i % 32 == 32 - 1) {
			printf("\n");
		}
	}
}

int main(int argc, char* argv[])
{
	int i;
	
	if (argc != 3) {
		printf("usage: test_rw [walb device] [num of blocks]\n");
		exit(1);
	}
	const char *walb_dev = argv[1];
	const char *nblocks_str = argv[2];

	init_random();
	const int n_blocks = 3;
	u8 *block[n_blocks];
	int ret;
	for (i = 0; i < n_blocks; i++) {
		ret = posix_memalign((void **)&block[i], 512, BLOCK_SIZE);
		if (ret != 0) {
			printf("malloc error\n");
			exit(1);
		}
	}

	int fd = open(walb_dev, O_RDWR | O_DIRECT);
	if (fd < 0) {
		printf("open error\n");
		exit(1);
	}

	int num = atoi(nblocks_str);
	for (i = 0; i < num; i ++) {
		memset_random(block[0], BLOCK_SIZE);
		memcpy(block[2], block[0], BLOCK_SIZE);
		memset(block[1], 0, BLOCK_SIZE);
		
		write_sector(fd, block[0], BLOCK_SIZE, i);
#if 1
		memset_random(block[0], BLOCK_SIZE);
#endif
		read_sector(fd, block[1], BLOCK_SIZE, i);

		int c = memcmp(block[1], block[2], BLOCK_SIZE);
		printf("%d %s\n", i, (c == 0 ? "OK" : "NG"));
#if 0
		if (c) {
			dump_memory(block[0], BLOCK_SIZE);
			dump_memory(block[1], BLOCK_SIZE);
			dump_memory(block[2], BLOCK_SIZE);
		}
#endif
	}

	close(fd);
	for (i = 0; i < 3; i ++) {
		free(block[i]);
	}	 
	return 0;
}
