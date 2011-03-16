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
 * Snapshot logpack.
 */
void test()
{
    int fd = open(LOG_DEV_FILE, O_RDWR | O_TRUNC | O_CREAT, 0755);
    ASSERT(fd > 0);

    /* now editing */
}

int main()
{
    test();
    return 0;
}
