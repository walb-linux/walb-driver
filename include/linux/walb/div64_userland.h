#ifndef WALB_DIV64_USERLAND_H
#define WALB_DIV64_USERLAND_H

#include <assert.h>

static inline u64 div_u64(u64 a, u32 b)
{
	assert(b > 0);
	return a / b;
}

static inline u64 div64_u64_rem(u64 a, u64 b, u64 *rem)
{
	assert(rem);
	*rem = a % b;
	return a / b;
}

#endif
