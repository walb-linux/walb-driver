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

/* Init/Exit */
int alldevs_init(void);
void alldevs_exit(void);

/* Lock wrappers */
void alldevs_lock(void);
void alldevs_unlock(void);

/* Get number of walb devices. */
uint get_n_devices(void);

/**
 * Before calling the following functions,
 * you must lock/unlock the resource
 * with alldevs_lock()/alldevs_unlock().
 */

/* Add/Del/Pop/Search */
void alldevs_add(struct walb_dev* wdev);
void alldevs_del(struct walb_dev* wdev);
struct walb_dev* alldevs_pop(void);
struct walb_dev* search_wdev_with_minor(uint minor);

/* Listing and counting. */
int get_wdev_list_range(
	struct walb_disk_data *ddata_k,
	struct walb_disk_data __user *ddata_u,
	size_t n,
	uint minor0, uint minor1);

/* Alloc/free minor id. */
uint alloc_any_minor(void);
uint alloc_specific_minor(uint minor);
void free_minor(uint minor);

/* Check already used or not. */
bool alldevs_is_already_used(dev_t);

#endif /* WALB_ALLDEVS_H_KERNEL */
