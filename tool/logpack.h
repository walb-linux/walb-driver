/**
 * Logpack functions for walbctl.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_LOGPACK_USER_H
#define WALB_LOGPACK_USER_H

#include "check_userland.h"

#include "walb/walb.h"
#include "walb/log_record.h"
#include "walb/log_device.h"

#ifdef __cplusplus
extern "C" {
#endif

bool read_logpack_header_from_wldev(
	int fd,
	const struct walb_super_sector* super_sectp,
	u64 lsid, u32 salt, struct sector_data *logh_sect);
unsigned int read_logpack_data_from_wldev(
	int fd,
	const struct walb_super_sector* super,
	const struct walb_logpack_header* logh, u32 salt,
	struct sector_data_array *sect_ary);

void print_logpack_header(const struct walb_logpack_header* logh);

bool read_logpack_header(
	int fd, unsigned int pbs, u32 salt,
	struct walb_logpack_header* logh);
bool read_logpack_data(
	int fd,
	const struct walb_logpack_header* logh, u32 salt,
	struct sector_data_array *sect_ary);
bool write_logpack_header(
	int fd, unsigned int pbs,
	const struct walb_logpack_header* logh);

bool redo_logpack(
	int fd,
	const struct walb_logpack_header* logh,
	const struct sector_data_array *sect_ary);

bool write_invalid_logpack_header(
	int fd, const struct sector_data *super_sect, u64 lsid);

void shrink_logpack_header(
	struct walb_logpack_header *logh, unsigned int invalid_idx,
	unsigned int pbs, u32 salt);


/*******************************************************************************
 * Helper data structure for logpack data.
 *******************************************************************************/

struct logpack
{
	/* for logpack header. */
	struct sector_data *sectd;

	/* for logpack data. */
	struct sector_data_array *sectd_ary;

	/* pointer to header sector data. */
	struct walb_logpack_header *header;

	unsigned int pbs;
};

struct logpack *alloc_logpack(unsigned int pbs, unsigned int n_sectors);
void free_logpack(struct logpack *pack);
bool resize_logpack_if_necessary(struct logpack *pack, unsigned int n_sectors);

#ifdef __cplusplus
}
#endif

#endif /* WALB_LOGPACK_USER_H */
