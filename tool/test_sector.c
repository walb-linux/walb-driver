/**
 * test_sector.c - Test for sector code.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <stdio.h>
#include <unistd.h>

#include "random.h"
#include "walb_sector.h"

/*******************************************************************************
 * Static functions.
 *******************************************************************************/

static bool is_sector_zero(const struct sector_data *sect)
{
        int i;
        ASSERT_SECTOR_DATA(sect);
        
        for (i = 0; i < sect->size; i ++) {
                if (((u8 *)sect->data)[i] != 0) { return false; }
        }
        return true;
}

static void memset_sector_random(struct sector_data *sect)
{
        ASSERT_SECTOR_DATA(sect);
        memset_random((u8 *)sect->data, sect->size);
}

/*******************************************************************************
 * Test case.
 *******************************************************************************/

void test_single_sector(int sect_size)
{
        struct sector_data *sect0, *sect1;

        sect0 = sector_alloc(sect_size);
        sect1 = sector_alloc_zero(sect_size);
        ASSERT_SECTOR_DATA(sect0);
        ASSERT_SECTOR_DATA(sect1);
        ASSERT(is_same_size_sector(sect0, sect1));
        ASSERT(is_sector_zero(sect1));

        memset_sector_random(sect0);
        sector_copy(sect1, sect0);
        ASSERT(sector_compare(sect0, sect1) == 0);

        sector_zeroclear(sect0);
        ASSERT(is_sector_zero(sect0));

        sector_free(sect0);
        sector_free(sect1);
}

void test_sector_array(int sect_size, int n_sectors)
{
        u8 *raw;
        int raw_size;
        int i;
        struct sector_data_array *sect_ary0, *sect_ary1;
        struct sector_data *sect0, *sect1;

        raw_size = sect_size * (n_sectors + 3) * sizeof(u8);
        raw = (u8 *)malloc(raw_size);
        ASSERT(raw);
        
        sect_ary0 = sector_data_array_alloc(n_sectors, sect_size);
        sect_ary1 = sector_data_array_alloc(n_sectors + 3, sect_size);
        ASSERT_SECTOR_DATA_ARRAY(sect_ary0);
        ASSERT_SECTOR_DATA_ARRAY(sect_ary1);
        ASSERT(sect_ary0->size == n_sectors);
        ASSERT(sect_ary1->size == n_sectors + 3);

        /* Prepare raw data and sect_ary1. */
        memset_random(raw, raw_size);
        for (i = 0; i < n_sectors + 3; i ++) {
                sect1 = get_sector_data_in_array(sect_ary1, i);
                memcpy(sect1->data, raw + i * sect_size, sect_size);
                ASSERT(memcmp(sect1->data, raw + i * sect_size, sect_size) == 0);
        }

        for (i = 0; i < n_sectors; i ++) {
                sect0 = get_sector_data_in_array(sect_ary0, i);
                sect1 = get_sector_data_in_array(sect_ary1, i);
                ASSERT(memcmp(sect1->data, raw + i * sect_size, sect_size) == 0);
                sector_copy(sect0, sect1);
                ASSERT(sector_compare(sect0, sect1) == 0);
        }

        /* Realloc with the same size. */
        sector_data_array_realloc(sect_ary0, n_sectors);
        ASSERT(sect_ary0->size == n_sectors);

        /* Grow the array. */
        sector_data_array_realloc(sect_ary0, n_sectors + 3);
        ASSERT(sect_ary0->size == n_sectors + 3);
        ASSERT_SECTOR_DATA_ARRAY(sect_ary0);
        for (i = 0; i < n_sectors + 3; i ++) {
                sect0 = get_sector_data_in_array(sect_ary0, i);
                sect1 = get_sector_data_in_array(sect_ary1, i);
                if (i >= n_sectors) {
                        memcpy(sect0->data, raw + i * sect_size, sect_size);
                        ASSERT(memcmp(sect0->data, raw + i * sect_size, sect_size) == 0);
                }
                ASSERT(sector_compare(sect0, sect1) == 0);
        }

        /* Shrink the array. */
        sector_data_array_realloc(sect_ary0, n_sectors - 3);
        ASSERT(sect_ary0->size == n_sectors - 3);
        ASSERT_SECTOR_DATA_ARRAY(sect_ary0);
        for (i = 0; i < n_sectors - 3; i ++) {
                sect0 = get_sector_data_in_array(sect_ary0, i);
                sect1 = get_sector_data_in_array(sect_ary1, i);
                ASSERT(sector_compare(sect1, sect0) == 0);
        }
        
        sector_data_array_free(sect_ary0);
        sector_data_array_free(sect_ary1);
        free(raw);
}

int main()
{
        init_random();

        test_single_sector(512);
        test_single_sector(4096);

        test_sector_array(512, 10);
        test_sector_array(4096, 10);
        
        return 0;
}

/* end of file. */
