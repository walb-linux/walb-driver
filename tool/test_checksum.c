/**
 * Checksum test code.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <string.h>

#include "../include/walb.h"

double time_double(struct timeval *tv)
{
        return (double)tv->tv_sec + tv->tv_usec * 0.000001;
}

/**
 * Read /dev/urandom to generate random value.
 */
u32 read_urandom()
{
        int fd, val;
        fd = open("/dev/urandom", O_RDONLY);
        if (fd < 0) {
                perror("open /dev/urandom failed\n");
                exit(1);
        }
        if (read(fd, (void *)&val, sizeof(u32)) != sizeof(u32)) {
                printf("read /dev/urandom failed\n");
                exit(1);
        }
        close(fd);
        return val;
}

/**
 * Get random value.
 * @return 0 <= val < max
 */
int get_random(int max)
{
        int min = 0;
        return min + (int)(rand() * (max - min + 0.0) / (RAND_MAX + 1.0));
}


/**
 * Random generator test.
 */
void test_random()
{
        int i;
        for (i = 0; i < 100000; i ++) {
                printf("%d\n", get_random(10));
        }
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

/**
 * Randomly set the buffer.
 */
void memset_random(u8 *data, size_t size)
{
        size_t i;
        for (i = 0; i < size; i ++) {
                data[i] = (u8)get_random(255);
        }
}

void checksum4()
{
        
}

static int comp(const void *p1, const void *p2)
{
        size_t d1 = *(u32*)(p1);
        size_t d2 = *(u32*)(p2);

        if (d1 < d2) return -1;
        if (d1 == d2) return 0;
        return 1;
}


static void make_sorted_random_array(size_t *ary, size_t size, size_t max_value,
                                     size_t align_size)
{
        size_t i, j;
        for (i = 0; i < size; i ++) {
                int retry = 1;
                while (retry) {
                        ary[i] = 1 + get_random(max_value/align_size - 1);

                        retry = 0;
                        for (j = 0; j < i; j ++) {
                                if (ary[j] == ary[i]) {
                                        retry = 1;
                                        break;
                                }
                        }
                }
        }
        qsort(ary, size, sizeof(size_t), comp);
        for (i = 0; i < size; i ++) {
                ary[i] *= align_size;
                /* printf("%zu\n", ary[i]); */
        }
        ary[0] = 0;
        ary[size - 1] = max_value;
}


int main()
{
        size_t i;
        u8 *buf, *buf2;
        size_t size = 1024 * 1024 * 1024;
        u32 csum1, csum2;
        size_t mid[16];
        struct timeval tv;
        double t1, t2, t3;

        srand(read_urandom());
        make_sorted_random_array(mid, 16, size, sizeof(u32));

        printf("making random array...\n");
        buf = alloc_buf(size);
        memset_random(buf, size);

        gettimeofday(&tv, 0); t1 = time_double(&tv);
        
        csum1 = checksum(buf, size);
        gettimeofday(&tv, 0); t2 = time_double(&tv);

        csum2 = 0;
        for (i = 0; i < 16 - 1; i ++) {
                csum2 += checksum(buf + mid[i], mid[i + 1] - mid[i]);
        }
        gettimeofday(&tv, 0); t3 = time_double(&tv);
        
        printf("%u (%f sec)\n"
               "%u (%f sec)\n",
               csum1, t2 - t1,
               csum2, t3 - t2);

        printf("copying...\n");
        buf2 = alloc_buf(size);
        gettimeofday(&tv, 0); t1 = time_double(&tv);
        memcpy(buf2, buf, size);
        gettimeofday(&tv, 0); t2 = time_double(&tv);
        printf("copy %zu bytes takes %f sec\n",
               size, t2 - t1);

        
        free_buf(buf);
        free_buf(buf2);
        return 0;
}
