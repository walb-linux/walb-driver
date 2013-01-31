/**
 * Definitions for block size.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_BLOCK_SIZE_H
#define WALB_BLOCK_SIZE_H

#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

/*******************************************************************************
 * Macros.
 *******************************************************************************/

/**
 * Logical block size is fixed.
 */
#define LOGICAL_BLOCK_SIZE (1U << 9)

/**
 * Assertion of logical/physical block size.
 */
#define ASSERT_LBS_PBS(lbs, pbs) ASSERT(is_valid_lbs_pbs(lbs, pbs))
#define ASSERT_PBS(pbs) ASSERT(is_valid_pbs(pbs))

/*******************************************************************************
 * Prototype of static inline functions.
 *******************************************************************************/

static inline int is_valid_lbs_pbs(unsigned int lbs, unsigned int pbs);
static inline int is_valid_pbs(unsigned int pbs);

static inline unsigned int n_lb_in_pb(unsigned int pbs);

static inline u64 capacity_pb(unsigned int pbs, u64 capacity_lb);
static inline u64 addr_pb(unsigned int pbs, u64 addr_lb);
static inline u64 off_in_pb(unsigned int pbs, u64 addr_lb);
static inline u64 addr_lb(unsigned int pbs, u64 addr_pb);
static inline u64 capacity_lb(unsigned int pbs, u64 capacity_pb);

/*******************************************************************************
 * Definition of static inline functions.
 *******************************************************************************/

/**
 * Logical-physical block size validation.
 */
static inline int is_valid_lbs_pbs(unsigned int lbs, unsigned int pbs)
{
	return (lbs > 0 && pbs >= lbs && pbs % lbs == 0);
}

/**
 * Physical block size validation.
 */
static inline int is_valid_pbs(unsigned int pbs)
{
	return is_valid_lbs_pbs(LOGICAL_BLOCK_SIZE, pbs);
}

/**
 * Get number of logical blocks in a physical block.
 */
static inline unsigned int n_lb_in_pb(unsigned int pbs)
{
	unsigned int ret;
	ASSERT_PBS(pbs);
	ret = pbs / LOGICAL_BLOCK_SIZE;
	ASSERT(ret > 0);
	return ret;
}

/**
 * Capacity conversion (logical to physial).
 *
 * @pbs physical block size in bytes.
 * @capacity_lb number of logical blocks.
 *
 * @return number of physical blocks required to store
 *   capacity_lb logical blocks.
 */
static inline u64 capacity_pb(unsigned int pbs, u64 capacity_lb)
{
	unsigned int n_lb;
	ASSERT_PBS(pbs);
	n_lb = n_lb_in_pb(pbs);
	return ((capacity_lb + n_lb - 1) / n_lb);
}

/**
 * Address conversion (logical to physical).
 */
static inline u64 addr_pb(unsigned int pbs, u64 addr_lb)
{
	ASSERT_PBS(pbs);
	return (addr_lb / (u64)n_lb_in_pb(pbs));
}

/**
 * Get offset in the physical block.
 */
static inline u64 off_in_pb(unsigned int pbs, u64 addr_lb)
{
	ASSERT_PBS(pbs);
	return (addr_lb % (u64)n_lb_in_pb(pbs));
}

/**
 * Address conversion (physical to logical).
 */
static inline u64 addr_lb(unsigned int pbs, u64 addr_pb)
{
	ASSERT_PBS(pbs);
	return (addr_pb * (u64)n_lb_in_pb(pbs));
}

/**
 * Capacity conversion (physial to logical).
 *
 * @pbs physical block size in bytes.
 * @capacity_pb number of physical blocks.
 *
 * @return number of logical blocks.
 */
static inline u64 capacity_lb(unsigned int pbs, u64 capacity_pb)
{
	return addr_lb(pbs, capacity_pb);
}

#ifdef __cplusplus
}
#endif

#endif /* WALB_BLOCK_SIZE_H */
