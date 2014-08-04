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
 * Before call the following functions,
 * you must lock all_wdevs_lock_ semaphore.
 */

/* Init/Exit */
int alldevs_init(void);
void alldevs_exit(void);

/* Add/Del/Pop/Search */
void alldevs_add(struct walb_dev* wdev);
void alldevs_del(struct walb_dev* wdev);
struct walb_dev* alldevs_pop(void);
struct walb_dev* search_wdev_with_minor(unsigned int minor);

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

/* Lock wrappers */
void alldevs_read_lock(void);
void alldevs_read_unlock(void);
void alldevs_write_lock(void);
void alldevs_write_unlock(void);

#endif /* WALB_ALLDEVS_H_KERNEL */
