/**
 * Snapshot functions for walbctl.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_SNAPSHOT_USER_H
#define WALB_SNAPSHOT_USER_H

#include "check_userland.h"

#include "walb/walb.h"
#include "walb/log_record.h"
#include "walb/log_device.h"
#include "walb/snapshot.h"

/**
 * Snapshot data structure in the userland.
 * It's just array of sector images.
 */
struct snapshot_data_u
{
        int fd; /* file descriptor of the log device. */
        const struct walb_super_sector *super; /* super sector */
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
 * @rec pointer to struct walb_snapshot_record.
 * @sect_i sector index.
 * @sect pointer to sector_data.
 * @snapd pointer to snapshot_data_u.
 */
#define for_each_snapshot_record_in_snapd(rec_i, rec, sect_i, sect, snapd) \
        for_each_snapshot_sector(sect_i, sect, snapd)                   \
        for_each_snapshot_record(rec_i, rec, sect)


/*
 * Prototypes for utility functions.
 */
void print_snapshot_record(const struct walb_snapshot_record* snap_rec);
void print_snapshot_sector(const struct walb_snapshot_sector* snap_sect, u32 sector_size);
bool write_snapshot_sector(int fd, const struct walb_super_sector* super_sect,
			struct walb_snapshot_sector* snap_sect, u32 idx);
bool read_snapshot_sector(int fd, const struct walb_super_sector* super_sect,
			struct walb_snapshot_sector* snap_sect, u32 idx);

/*
 * Prototypes for struct snapshot_data_u.
 */
struct snapshot_data_u* alloc_snapshot_data_u(
        int fd, const struct walb_super_sector* super_sectp);
void free_snapshot_data_u(struct snapshot_data_u* snapd);
void initialize_snapshot_data_u(struct snapshot_data_u* snapd);
void clear_snapshot_data_u(struct snapshot_data_u *snapd);
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


#endif /* WALB_SNAPSHOT_USER_H */
