/**
 * sector_io.h - Sector IO operations.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_SECTOR_IO_H_KERNEL
#define WALB_SECTOR_IO_H_KERNEL

#include "check_kernel.h"
#include <linux/list.h>
#include <linux/completion.h>
#include <linux/bio.h>
#include <linux/blkdev.h>
#include "walb/walb.h"
#include "walb/sector.h"

/**
 * BIO wrapper flag.
 */
#define WALB_BIO_INIT    0
#define WALB_BIO_END     1
#define WALB_BIO_ERROR   2

/**
 * Bio wrapper with completion.
 */
struct walb_bio_with_completion
{
        struct bio *bio;
        struct completion wait;
        int status;
        struct list_head list;
};

/*******************************************************************************
 * Function prototypes.
 *******************************************************************************/

/* End IO callback for struct walb_bio_with_completion. */
void walb_end_io_with_completion(struct bio *bio, int error);

/* Sector IO function. */
bool sector_io(int rw, struct block_device *bdev,
	u64 off, struct sector_data *sect);

/* Super sector functions. */
static void walb_print_super_sector(struct walb_super_sector *lsuper0);
static bool walb_read_super_sector(
	struct block_device *ldev, struct sector_data *lsuper);
static bool walb_write_super_sector(
	struct block_device *ldev, struct sector_data *lsuper);

/*******************************************************************************
 * Static inline functions.
 *******************************************************************************/

/**
 * Get super sector pointer.
 */
static inline struct walb_super_sector*
get_super_sector(struct sector_data *sect)
{
        ASSERT_SECTOR_DATA(sect);
        return (struct walb_super_sector *)(sect->data);
}

/**
 * Get logpack header pointer.
 */
static inline struct walb_logpack_header*
get_logpack_header(struct sector_data *sect)
{
        ASSERT_SECTOR_DATA(sect);
        return (struct walb_logpack_header *)(sect->data);
}

#endif /* WALB_SECTOR_IO_H_KERNEL */
