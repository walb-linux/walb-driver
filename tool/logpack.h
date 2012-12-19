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
	u64 lsid, u32 salt, struct sector_data *lhead_sect);
bool read_logpack_data_from_wldev(
	int fd,
	const struct walb_super_sector* super,
	const struct walb_logpack_header* lhead, u32 salt,
	struct sector_data_array *sect_ary);

void print_logpack_header(const struct walb_logpack_header* lhead);

bool read_logpack_header(
	int fd, unsigned int pbs, u32 salt, struct walb_logpack_header* lhead);
bool read_logpack_data_raw(
	int fd, unsigned int pbs, u32 salt,
	const struct walb_logpack_header* lhead,
	u8* buf, size_t bufsize);
bool read_logpack_data(
	int fd,
	const struct walb_logpack_header* lhead, u32 salt,
	struct sector_data_array *sect_ary);
bool write_logpack_header(int fd, int physical_bs,
			const struct walb_logpack_header* lhead);

bool redo_logpack(
	int fd,
	const struct walb_logpack_header* lhead,
	const struct sector_data_array *sect_ary);

bool write_invalid_logpack_header(
	int fd, const struct sector_data *super_sect, u64 lsid);

/*******************************************************************************
 * New logpack interface.
 *******************************************************************************/

#include "walb/sector.h"

/**
 * Logpack
 */
struct logpack
{
	struct sector_data *head_sect;
	struct sector_data_array* data_sects;

	unsigned int logical_bs;
	unsigned int physical_bs;
};

/**
 * Assertion for logpack.
 */
#define ASSERT_LOGPACK_CHECKSUM(logpack, salt)	\
	ASSERT(is_valid_logpack(logpack, false, salt))
#define ASSERT_LOGPACK(logpack) \
	ASSERT(is_valid_logpack(logpack, false, 0))

struct logpack* alloc_logpack(unsigned int physical_bs, unsigned int n_sectors);
void free_logpack(struct logpack* logpack);
bool realloc_logpack(struct logpack* logpack, int n_sectors);

bool is_valid_logpack(struct logpack* logpack, bool is_checksum, u32 salt);

static inline struct walb_logpack_header* logpack_get_header(struct logpack* logpack);
static inline u64 logpack_get_lsid(struct logpack* logpack);

/*
 * Not yet implemented.
 */
struct walb_logpack_header* create_random_logpack(
	unsigned int lbs, unsigned int pbs, const u8* buf);
bool logpack_add_io_request(
	struct logpack* logpack,
	u64 offset, const u8* data, int size,
	bool is_padding);


/**
 * Get pointer to logpack header image.
 */
static inline struct walb_logpack_header* logpack_get_header(struct logpack* logpack)
{
	ASSERT(logpack != NULL);
	ASSERT(logpack->head_sect != NULL);
	ASSERT(logpack->head_sect->data != NULL);

	return (struct walb_logpack_header *)logpack->head_sect->data;
}

/**
 * Get lsid of logpack.
 */
static inline u64 logpack_get_lsid(struct logpack* logpack)
{
	ASSERT_LOGPACK(logpack);
	return logpack_get_header(logpack)->logpack_lsid;
}

#ifdef __cplusplus
}
#endif

#endif /* WALB_LOGPACK_USER_H */
