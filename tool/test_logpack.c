/**
 * test_logpack.c - Test for logpack code.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <stdio.h>
#include <unistd.h>

#include "util.h"
#include "logpack.h"

#define DATA_DEV_SIZE (32 * 1024 * 1024)
#define LOG_DEV_SIZE  (16 * 1024 * 1024)

#define LOG_DEV_FILE "tmp/logpack_test.tmp"

/**
 * TEST of lb_to_pb().
 */
void TEST_lb_to_pb()
{
    ASSERT(lb_to_pb(512, 512, 0) == 0);
    ASSERT(lb_to_pb(512, 4096, 0) == 0);
    ASSERT(lb_to_pb(512, 512, 3) == 3);
    ASSERT(lb_to_pb(512, 512, 4) == 4);
    ASSERT(lb_to_pb(512, 512, 5) == 5);
    ASSERT(lb_to_pb(512, 4096, 23) == 3);
    ASSERT(lb_to_pb(512, 4096, 24) == 3);
    ASSERT(lb_to_pb(512, 4096, 25) == 4);
}

/**
 * Snapshot logpack.
 *
 * @lbs logical block size.
 * @pbs physical block size.
 */
void test(int lbs, int pbs)
{
    int fd = open(LOG_DEV_FILE, O_RDWR | O_TRUNC | O_CREAT, 0755);
    ASSERT(fd > 0);

    logpack_t *logpack = alloc_logpack(lbs, pbs, 1);
    ASSERT(logpack);

    

    
    /* now editing */
}

int main()
{
    TEST_lb_to_pb();
    
    test(512, 512);
    test(512, 4096);
    test(4096, 4096);
    return 0;
}
