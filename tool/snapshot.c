/**
 * Snapshot functions for walbctl.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 * @license 3-clause BSD, GPL version 2 or later.
 */
#include <string.h>

#include "util.h"
#include "snapshot.h"

/*******************************************************************************
 * Prototypes for struct snapshot_data_u.
 *******************************************************************************/

/**
 *
 */
struct snapshot_data_u* initialize_snapshot_data_u(
        int fd, const walb_super_sector_t* super_sectp)
{
        /* not yet implemented. */
        return NULL;
}

/**
 *
 */
bool finalize_snapshot_data_u(struct snapshot_data_u* snapd)
{
        /* not yet implemented. */
        return false;
}
        
/**
 *
 */
bool is_valid_snaphsot_data_u(struct snapshot_data_u* snapd)
{
        /* not yet implemented. */
        return false;
}

/*******************************************************************************
 * Functions snapshot manipulation.
 *******************************************************************************/

/**
 *
 */
bool snapshot_add(struct snapshot_data_u *snapd,
                  const char *name, u64 lsid, u64 timestamp)
{
        /* not yet implemented. */
        return false;
}

/**
 *
 */
bool snapshot_del(struct snapshot_data_u *snapd, const char *name)
{
        /* not yet implemented. */
        return false;
}

/**
 *
 */
bool snapshot_del_range(struct snapshot_data_u *snapd, u64 lsid0, u64 lsid1)
{
        /* not yet implemented. */
        return false;
}

/**
 *
 */
struct walb_snapshot_record* snapshot_get(
        struct snapshot_data_u *snapd, const char *name)
{
        /* not yet implemented */
        return NULL;
}

/**
 *
 */
int snapshot_n_records_range(const struct snapshot_data_u *snapd,
                             u64 lsid0, u64 lsid1)
{
        /* not yet implemented */
        return -1;
}
        
/**
 *
 */
int snapshot_n_records(const struct snapshot_data_u *snapd)
{
        /* not yet implemented */
        return -1;
}

/**
 *
 */
int snapshot_list_range(const struct snapshot_data_u *snapd,
                        struct walb_snapshot_record **rec_ary_p, size_t ary_size,
                        u64 lsid0, u64 lsid1)
{
        /* not yet implemented */
        return -1;
}

/**
 *
 */
int snapshot_list(const struct snapshot_data_u *snapd,
                  struct walb_snapshot_record **rec_ary_p, size_t ary_size)
{
        /* not yet implemented */
        return -1;
}

/* end of file */
