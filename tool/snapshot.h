/**
 * Snapshot functions for walbctl.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_SNAPSHOT_USER_H
#define _WALB_SNAPSHOT_USER_H

#include "walb.h"
#include "walb_log_record.h"
#include "walb_log_device.h"
#include "walb_snapshot.h"

/**
 * Snapshot data structure in the userland.
 * It's just array of sector images.
 */
struct snapshot_data_u
{
        int fd; /* file descriptor of the log device. */
        const walb_super_sector_t *super; /* super sector */
        u32 next_snapshot_id; /* next snapshot id. */
        
        struct sector_data_array *sect_ary;
};

/**
 * Macros.
 */
#define ASSERT_SNAPSHOT_SECTOR_DATA_U(snapd)    \
        ASSERT(is_valid_snaphsot_data_u(snapd))

/**
 * Iterative over snapshot sectors.
 *
 * @i int sector index.
 * @sect pointer to sector_data.
 * @snapd pointer to struct snapshot_data_u.
 */
#define for_each_snapshot_sector(i, sect, snapd)                        \
        for (i = 0;                                                     \
             i < get_n_sectors(snapd) && (                              \
                     { sect = get_sector(snapd, i); 1; });              \
             i ++)

/**
 * Iterative over snapshot records in the whole snapshot_data_u.
 *
 * @rec_i record index inside sector.
 * @rec pointer to snapshot_record_t.
 * @sect_i sector index.
 * @sect pointer to sector_data.
 * @snapd pointer to snapshot_data_u.
 */
#define for_each_snapshot_record_in_snapd(rec_i, rec, sect_i, sect, snapd) \
        for_each_snapshot_sector(sect_i, sect, snapd)                   \
        for_each_snapshot_record(rec_i, rec, sect)

/*
 * Prototypes for struct snapshot_data_u.
 */
struct snapshot_data_u* alloc_snapshot_data_u(
        int fd, const walb_super_sector_t* super_sectp);
void free_snapshot_data_u(struct snapshot_data_u* snapd);
bool initialize_snapshot_data_u(struct snapshot_data_u* snapd);
bool finalize_snapshot_data_u(struct snapshot_data_u* snapd);
bool is_valid_snaphsot_data_u(struct snapshot_data_u* snapd);

/*
 * Prototypes for snapshot sector IO.
 */
bool read_sector_snapshot_data_u(
        struct snapshot_data_u *snapd, int idx);

bool read_all_sectors_snapshot_data_u(struct snapshot_data_u *snapd);
bool write_sector_snapshot_data_u(
        struct snapshot_data_u *snapd, int idx);
bool write_all_sectors_snapshot_data_u(struct snapshot_data_u *snapd);

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


#endif /* _WALB_SNAPSHOT_USER_H */
