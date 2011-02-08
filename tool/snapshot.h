/**
 * Snapshot functions for walbctl.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_SNAPSHOT_H
#define _WALB_SNAPSHOT_H

#include "walb.h"
#include "walb_log_record.h"
#include "walb_log_device.h"

/**
 * Snapshot data structure in the userland.
 * It's just array of sector images.
 */
struct snapshot_data_u
{
        int fd; /* file descriptor of the log device. */
        const walb_super_sector_t *super; /* super sector */
        int n_sectors; /* number of snapshot sectors. */
        u32 next_snapshot_id; /* next snapshot id. */
        walb_snapshot_sector_t *sector; /* array of sector images. */
};

/*
 * Prototypes for struct snapshot_data_u.
 */
struct snapshot_data_u* initialize_snapshot_data_u(
        int fd, const walb_super_sector_t* super_sectp);
bool finalize_snapshot_data_u(struct snapshot_data_u* snapd);
bool is_valid_snaphsot_data_u(struct snapshot_data_u* snapd);

/*
 * Prototypes for snapshot manipulation.
 */
bool snapshot_add(struct snapshot_data_u *snapd,
                  const char *name, u64 lsid, u64 timestamp);
bool snapshot_del(struct snapshot_data_u *snapd, const char *name);
bool snapshot_del_range(struct snapshot_data_u *snapd, u64 lsid0, u64 lsid1);

struct walb_snapshot_record* snapshot_get(
        struct snapshot_data_u *snapd, const char *name);

int snapshot_n_records_range(const struct snapshot_data_u *snapd,
                             u64 lsid0, u64 lsid1);
int snapshot_n_records(const struct snapshot_data_u *snapd);

int snapshot_list_range(const struct snapshot_data_u *snapd,
                        struct walb_snapshot_record **rec_ary_p, size_t ary_size,
                        u64 lsid0, u64 lsid1);
int snapshot_list(const struct snapshot_data_u *snapd,
                  struct walb_snapshot_record **rec_ary_p, size_t ary_size);


#endif /* _WALB_SNAPSHOT_H */
