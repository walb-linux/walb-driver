/**
 * alldevs.h - Multiple devices management.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_ALLDEVS_H_KERNEL
#define WALB_ALLDEVS_H_KERNEL

#include "check_kernel.h"
#include "kern.h"
#include "sector_io.h"

/**
 * Prototypes.
 *
 * Before call the following functions,
 * you must lock all_wdevs_lock_ mutex.
 */

/* Init/Exit */
int alldevs_init(void);
void alldevs_exit(void);

/* Add/Del */
int alldevs_add(struct walb_dev* wdev);
void alldevs_del(struct walb_dev* wdev);

/* Pop */
struct walb_dev* alldevs_pop(void);

/* Update uuid. */
int alldevs_update_uuid(const u8 *old_uuid, const u8 *new_uuid);

/* Search */
struct walb_dev* search_wdev_with_minor(unsigned int minor);
struct walb_dev* search_wdev_with_name(const char* name);
struct walb_dev* search_wdev_with_uuid(const u8* uuid);

/* Listing and counting. */
int get_wdev_list_range(
	struct walb_disk_data *ddata_k,
	struct walb_disk_data __user *ddata_u,
	size_t n,
	unsigned int minor0, unsigned int minor1);

/* Get number of walb devices. */
unsigned int get_n_devices(void);

/* Get free minor. */
unsigned int get_free_minor(void);

/* Check already used or not. */
bool alldevs_is_already_used(dev_t);

/* Lock wrapper */
void alldevs_lock(void);
void alldevs_unlock(void);

#endif /* WALB_ALLDEVS_H_KERNEL */
