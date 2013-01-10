/**
 * size_list.c - List of size with unit suffix.
 *
 * Copyright(C) 2012, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include "check_kernel.h"
#include "walb/common.h"
#include "size_list.h"

/*******************************************************************************
 * Global functions definition.
 *******************************************************************************/

/**
 * Get number of entries in devices_str.
 * It returns 3 for "1g,2g,3g".
 * @sizlist_str size list string.
 * RETURN:
 * Length of the list.
 */
unsigned int sizlist_length(const char* sizlist_str)
{
	unsigned int n;
	unsigned int i;
	unsigned int len = strlen(sizlist_str);

	if (len == 0) {
		return 0; /* No data. */
	}
	n = 1;
	for (i = 0; i < len; i++) {
		if (sizlist_str[i] == ',' &&
			sizlist_str[i + 1] != '\0') {
			n++;
		}
	}
	ASSERT(n > 0);
	return n;
}

/**
 * Get size data of an index in the list.
 * It returns 4096 for sizlist_nth_size("1m,2g,4k,8t", 2).
 *
 * @sizlist_str size list string.
 * @n Specify index in the list. 0 <= n < sizlist_length(sizelist_str).
 * RETURN:
 * Size without suffix.
 */
u64 sizlist_nth_size(const char* sizlist_str, unsigned int n)
{
	unsigned int i;
	const char *p = sizlist_str;
	char *p_next;
	int len;
	u64 capacity;

	/* Skip ',' for n times. */
	for (i = 0; i < n; i++) {
		char *q = strchr(p, ',');
		ASSERT(q);
		p = q + 1;
	}

	/* Get length of the entry string. */
	p_next = strchr(p, ',');
	if (p_next) {
		len = p_next - p;
	} else {
		len = strlen(p);
	}

	/* Parse number (with suffix). */
	capacity = 0;
	for (i = 0; i < len; i++) {

		if ('0' <= p[i] && p[i] <= '9') {
			capacity *= 10;
			capacity += (u64)(p[i] - '0');
		} else {
			switch (p[i]) {
			case 't':
				capacity *= 1024;
			case 'g':
				capacity *= 1024;
			case 'm':
				capacity *= 1024;
			case 'k':
				capacity *= 1024;
				break;
			default:
				BUG();
			}
		}
	}
	return capacity;
}

/**
 * Test code.
 */
void test_sizlist(void)
{
	ASSERT(sizlist_length("") == 0);
	ASSERT(sizlist_length("1") == 1);
	ASSERT(sizlist_length("1,2,3") == 3);
	ASSERT(sizlist_length("11,2,33,4,555") == 5);

	ASSERT(sizlist_nth_size("2k", 0) == 2048);
	ASSERT(sizlist_nth_size("1m", 0) == 1048576);
	ASSERT(sizlist_nth_size("1,1m,16k", 1) == 1048576);
	ASSERT(sizlist_nth_size("1,1m,16k", 2) == 16384);
}

/* end of file */
