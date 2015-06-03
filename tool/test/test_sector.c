/**
 * test_sector.c - Test for sector code.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <stdio.h>
#include <unistd.h>

#include "linux/walb/sector.h"
#include "random.h"
#include "util.h"
#include "walb_util.h"
#include "linux/walb/print.h"

#define TEST_FILE "tmp/test_sector_data"

/*******************************************************************************
 * Static functions.
 *******************************************************************************/

UNUSED static bool is_sector_zero(const struct sector_data *sect)
{
	unsigned int i;
	ASSERT_SECTOR_DATA(sect);

	for (i = 0; i < sect->size; i++) {
		if (((u8 *)sect->data)[i] != 0) { return false; }
	}
	return true;
}

UNUSED static void memset_sector_random(struct sector_data *sect)
{
	ASSERT_SECTOR_DATA(sect);
	memset_random((u8 *)sect->data, sect->size);
}

/**
 * Test copy of sector_data_array.
 *
 * @sect_size sector size.
 * @n_sectors number of sectors.
 * @offset start offset in bytes.
 * @copy_size range to copy in bytes.
 */
static void test_sector_array_copy_detail(
	unsigned int sect_size, unsigned int n_sectors,
	unsigned int offset, unsigned int copy_size)
{
	u8 *raw0, *raw1;
	unsigned int raw_size;
	char *str;
	int str_size;
	struct sector_data_array *sect_ary0, *sect_ary1;

	raw_size = sect_size * n_sectors;
	str_size = raw_size * 3 + 1;
	ASSERT(offset < raw_size);
	ASSERT(offset + copy_size <= raw_size);
	ASSERT(copy_size > 0);

	/* Allocate memories. */
	str = (char *)malloc(str_size); ASSERT(str);
	raw0 = (u8 *)malloc(raw_size);
	raw1 = (u8 *)malloc(raw_size);
	ASSERT(raw0);
	ASSERT(raw1);

	sect_ary0 = sector_array_alloc(sect_size, n_sectors);
	sect_ary1 = sector_array_alloc(sect_size, n_sectors);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary1);
	ASSERT(sect_ary0->size == n_sectors);
	ASSERT(sect_ary1->size == n_sectors);

	PRINT_D("sect_size %d n_sectors %d offset %d copy_size %d\n",
		sect_size, n_sectors, offset, copy_size); /* debug */

	/* Initialize */
	memset_random(raw0, raw_size);
	sector_array_copy_from(sect_ary0, 0, raw0, raw_size);
	sector_array_copy_from(sect_ary1, 0, raw0, raw_size);
	ASSERT(sector_array_compare(sect_ary0, sect_ary1) == 0);

	/* Partial copy */
	memset_random(raw1, raw_size);
	sector_array_copy_from(sect_ary0, offset, raw1 + offset, copy_size);
	sector_array_copy_to(sect_ary0, offset, raw0 + offset, copy_size);
	ASSERT(memcmp(raw0 + offset, raw1 + offset, copy_size) == 0);
	sector_array_copy_from(sect_ary1, offset, raw0 + offset, copy_size);
	ASSERT(memcmp(raw0 + offset, raw1 + offset, copy_size) == 0);
	sector_array_copy_to(sect_ary1, 0, raw1, raw_size);
	ASSERT(memcmp(raw0 + offset, raw1 + offset, copy_size) == 0);

	/* Check */
	ASSERT(sector_array_compare(sect_ary0, sect_ary1) == 0);
	ASSERT(memcmp(raw0, raw1, raw_size) == 0);

	/* Deallocate memories. */
	sector_array_free(sect_ary0);
	sector_array_free(sect_ary1);
	free(raw0);
	free(raw1);
	free(str);
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
	ASSERT(is_same_sector(sect0, sect1));

	sector_zeroclear(sect0);
	ASSERT(is_sector_zero(sect0));

	sector_free(sect0);
	sector_free(sect1);
}

void test_sector_array(unsigned int sect_size, unsigned int n_sectors)
{
	u8 *raw;
	unsigned int raw_size;
	unsigned int i;
	struct sector_data_array *sect_ary0, *sect_ary1;
	struct sector_data *sect0, *sect1;
	UNUSED bool retb;

	raw_size = sect_size * (n_sectors + 3) * sizeof(u8);
	raw = (u8 *)malloc(raw_size);
	ASSERT(raw);

	sect_ary0 = sector_array_alloc(sect_size, n_sectors);
	sect_ary1 = sector_array_alloc(sect_size, n_sectors + 3);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary1);
	ASSERT(sect_ary0->size == n_sectors);
	ASSERT(sect_ary1->size == n_sectors + 3);

	/* Prepare raw data and sect_ary1. */
	memset_random(raw, raw_size);
	for (i = 0; i < n_sectors + 3; i++) {
		sect1 = get_sector_data_in_array(sect_ary1, i);
		memcpy(sect1->data, raw + i * sect_size, sect_size);
		ASSERT(memcmp(sect1->data, raw + i * sect_size, sect_size) == 0);
	}

	for (i = 0; i < n_sectors; i++) {
		sect0 = get_sector_data_in_array(sect_ary0, i);
		sect1 = get_sector_data_in_array(sect_ary1, i);
		ASSERT(memcmp(sect1->data, raw + i * sect_size, sect_size) == 0);
		sector_copy(sect0, sect1);
		ASSERT(is_same_sector(sect0, sect1));
	}

	/* Realloc with the same size. */
	retb = sector_array_realloc(sect_ary0, n_sectors);
	ASSERT(retb);
	ASSERT(sect_ary0->size == n_sectors);

	/* Grow the array. */
	retb = sector_array_realloc(sect_ary0, n_sectors + 3);
	ASSERT(retb);
	ASSERT(sect_ary0->size == n_sectors + 3);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary0);
	for (i = 0; i < n_sectors + 3; i++) {
		sect0 = get_sector_data_in_array(sect_ary0, i);
		sect1 = get_sector_data_in_array(sect_ary1, i);
		if (i >= n_sectors) {
			memcpy(sect0->data, raw + i * sect_size, sect_size);
			ASSERT(memcmp(sect0->data, raw + i * sect_size, sect_size) == 0);
		}
		ASSERT(is_same_sector(sect0, sect1));
	}

	/* Shrink the array. */
	sector_array_realloc(sect_ary0, n_sectors - 3);
	ASSERT(sect_ary0->size == n_sectors - 3);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary0);
	for (i = 0; i < n_sectors - 3; i++) {
		sect0 = get_sector_data_in_array(sect_ary0, i);
		sect1 = get_sector_data_in_array(sect_ary1, i);
		ASSERT(is_same_sector(sect1, sect0));
	}

	sector_array_free(sect_ary0);
	sector_array_free(sect_ary1);
	free(raw);
}

void test_sector_array_copy(int sect_size, int n_sectors)
{
	int i;
	int offset, copy_size;
	int n_offset, n_copy_size;

	/* All */
	offset = 0;
	copy_size = n_sectors;
	test_sector_array_copy_detail(sect_size, n_sectors, offset, copy_size);

	/* Randomly */
	for (i = 0; i < 10; i++) {
		offset = get_random(sect_size * n_sectors);
		copy_size = get_random(sect_size * n_sectors - offset - 1) + 1;
		test_sector_array_copy_detail(sect_size, n_sectors, offset, copy_size);
	}

	/* Aligned */
	for (i = 0; i < 10; i++) {
		n_offset = get_random(n_sectors);
		offset = sect_size * n_offset;
		n_copy_size = get_random(n_sectors - n_offset - 1) + 1;
		copy_size = sect_size * n_copy_size;
		test_sector_array_copy_detail(sect_size, n_sectors, offset, copy_size);
	}
}

void test_sector_io(unsigned int sect_size, unsigned int n_sectors)
{
	struct sector_data_array *sect_ary0, *sect_ary1;
	struct sector_data *sect0, UNUSED *sect1;
	UNUSED bool ret;
	UNUSED off_t off;

	/* prepare */
	sect_ary0 = sector_array_alloc(sect_size, n_sectors);
	sect_ary1 = sector_array_alloc(sect_size, n_sectors);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary1);
	ASSERT(sect_ary0->size == n_sectors);
	ASSERT(sect_ary1->size == n_sectors);

	/* Fill random data. */
	unsigned int i;
	for (i = 0; i < n_sectors; i++) {
		sect0 = get_sector_data_in_array(sect_ary0, i);
		memset_sector_random(sect0);
	}

	/* write */
	int fd = open(TEST_FILE, O_RDWR | O_TRUNC |O_CREAT, 00755);
	ASSERT(fd > 0);

	/* seek */
	ret = sector_array_write(fd, sect_ary0, 0, n_sectors);
	ASSERT(ret);

	off = lseek(fd, 0, SEEK_SET);
	ASSERT(off == 0);

	/* read */
	ret = sector_array_read(fd, sect_ary1, 0, n_sectors);
	ASSERT(ret);

	close(fd);

	/* check */
	for (i = 0; i < n_sectors; i++) {
		sect0 = get_sector_data_in_array(sect_ary0, i);
		sect1 = get_sector_data_in_array(sect_ary1, i);

		ASSERT(is_same_sector(sect0, sect1));
	}

	/* free */
	sector_array_free(sect_ary0);
	sector_array_free(sect_ary1);
}

int main()
{
	init_random();

	test_single_sector(512);
	test_single_sector(4096);

	test_sector_array(512, 10);
	test_sector_array(4096, 10);

	test_sector_array_copy(512, 10);
	test_sector_array_copy(4096, 10);

	test_sector_io(512, 10);
	test_sector_io(4096, 10);

	printf("test passed.\n");

	return 0;
}

/* end of file. */
