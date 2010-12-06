/**
 * Read written block check.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */

#include <stdlib.h>

#include "random.h"
#include "util.h"

#define BLOCK_SIZE 4096
#define DEV_SIZE (1024 * 1024 * 1024 / BLOCK_SIZE) /* num of blocks */


int main(int argc, char* argv[])
{
        if (argc != 3) {
                printf("usage: test_rw [walb device] [num of blocks]\n");
                exit(1);
        }

        init_random();
        u8 *block0;
        u8 *block1;

        if (posix_memalign((void **)&block0, BLOCK_SIZE, BLOCK_SIZE) != 0 ||
            posix_memalign((void **)&block1, BLOCK_SIZE, BLOCK_SIZE) != 0) {

                printf("malloc error\n");
        }

        int fd = open(argv[1], O_RDWR | O_DIRECT);
        if (fd < 0) {
                printf("open error\n");
                exit(1);
        }

        int num = atoi(argv[2]);
        int i;
        for (i = 0; i < num; i ++) {
                memset_random(block0, BLOCK_SIZE);

                write_sector(fd, block0, BLOCK_SIZE, i);
                read_sector(fd, block1, BLOCK_SIZE, i);
                
                printf("%d %d\n", i, memcmp(block0, block1, BLOCK_SIZE));
        }

        close(fd);
        free(block0);
        free(block1);
        
        return 0;
}
