/**
 * alldevs.h - Multiple devices management.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_ALLDEVS_H_KERNEL
#define WALB_ALLDEVS_H_KERNEL

#include "check_kernel.h"
#include <linux/rwsem.h>
#include "kern.h"
#include "sector_io.h"

/**
 * Prototypes.
 *
 * Before call the following functions,
 * you must lock all_wdevs_lock_ semaphore.
 */

/* Init/Exit */
int alldevs_init(void);
void alldevs_exit(void);

/* Add/Del */
int alldevs_add(struct walb_dev* wdev);
void alldevs_del(struct walb_dev* wdev);

/* Pop */
struct walb_dev* alldevs_pop(void);

/* Search */
struct walb_dev* search_wdev_with_minor(unsigned int minor);
struct walb_dev* search_wdev_with_name(const char* name);
struct walb_dev* search_wdev_with_uuid(const u8* uuid);

/* Get free minor. */
unsigned int get_free_minor(void);

/* Lock wrapper */
void alldevs_read_lock(void);
void alldevs_read_unlock(void);
void alldevs_write_lock(void);
void alldevs_write_unlock(void);

#endif /* WALB_ALLDEVS_H_KERNEL */
