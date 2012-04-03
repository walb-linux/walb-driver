/**
 * size_list.h - List of size with unit suffix.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_SIZE_LIST_H_KERNEL
#define WALB_SIZE_LIST_H_KERNEL

#include "check_kernel.h"

/* Get length of a list string. */
unsigned int sizlist_length(const char* sizlist_str);
/* Get size of the nth item in a list. */
u64 sizlist_nth_size(const char* sizlist_str, unsigned int n);

/* Test */
UNUSED
void test_sizlist(void);

#endif /* _WALB_SIZE_LIST_H_KERNEL */
