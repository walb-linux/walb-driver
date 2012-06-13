/**
 * Read written block check.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <stdlib.h>

#include "random.h"
#include "util.h"

/* #define BLOCK_SIZE 4096 */
#define BLOCK_SIZE 512
#define DEV_SIZE (1024 * 1024 * 1024 / BLOCK_SIZE) /* num of blocks */

int main(int argc, char* argv[])
{
        if (argc != 3) {
                printf("usage: test_rw [walb device] [num of blocks]\n");
                exit(1);
        }
        const char *walb_dev = argv[1];
        const char *nblocks_str = argv[2];

        init_random();
        u8 *block0;
        u8 *block1;

        if (posix_memalign((void **)&block0, BLOCK_SIZE, BLOCK_SIZE) != 0 ||
            posix_memalign((void **)&block1, BLOCK_SIZE, BLOCK_SIZE) != 0) {

                printf("malloc error\n");
                exit(1);
        }

        int fd = open(walb_dev, O_RDWR | O_DIRECT);
        if (fd < 0) {
                printf("open error\n");
                exit(1);
        }

        int num = atoi(nblocks_str);
        int i, j;
        for (i = 0; i < num; i ++) {
                memset_random(block0, BLOCK_SIZE);
		memset(block1, 0, BLOCK_SIZE);

                write_sector(fd, block0, BLOCK_SIZE, i);
                read_sector(fd, block1, BLOCK_SIZE, i);

                int c = memcmp(block0, block1, BLOCK_SIZE);
                printf("%d %s\n", i, (c == 0 ? "OK" : "NG"));
#if 0
		if (c) {
			for (j = 0; j < BLOCK_SIZE; j ++) {
				printf("%d", block0[j] == block1[j]);
				if (j % 64 == 64 - 1) {
					printf("\n");
				}
			}
			for (j = 0; j < BLOCK_SIZE; j ++) {
				printf("%02X", block0[j]);
				if (j % 32 == 32 - 1) {
					printf("\n");
				}
			}
			printf("\n");
			for (j = 0; j < BLOCK_SIZE; j ++) {
				printf("%02X", block1[j]);
				if (j % 32 == 32 - 1) {
					printf("\n");
				}
			}
			printf("\n");
			
		}
#endif
        }

        close(fd);
        free(block0);
        free(block1);
        
        return 0;
}
