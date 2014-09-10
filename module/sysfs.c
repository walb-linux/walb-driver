/**
 * sysfs.c - sysfs related data and functions.
 *
 * (C) 2013, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/sysfs.h>
#include <linux/spinlock.h>
#include "kern.h"
#include "io.h"
#include "wdev_util.h"

/*******************************************************************************
 * Utiltities.
 *******************************************************************************/

static inline struct walb_dev *get_wdev_from_kobj(struct kobject *kobj)
{
	if (!kobj) { return NULL; }
	return container_of(kobj, struct walb_dev, kobj);
}

/*******************************************************************************
 * Funtions to show attributes.
 *******************************************************************************/

static ssize_t walb_attr_show_ldev(struct walb_dev *wdev, char *buf)
{
	dev_t ldevt = wdev->ldev->bd_dev;
	return sprintf(buf, "%u:%u\n", MAJOR(ldevt), MINOR(ldevt));
}

static ssize_t walb_attr_show_ddev(struct walb_dev *wdev, char *buf)
{
	dev_t ddevt = wdev->ddev->bd_dev;
	return sprintf(buf, "%u:%u\n", MAJOR(ddevt), MINOR(ddevt));
}

static ssize_t walb_attr_show_lsids(struct walb_dev *wdev, char *buf)
{
	struct lsid_set lsids;

	spin_lock(&wdev->lsid_lock);
	lsids = wdev->lsids;
	spin_unlock(&wdev->lsid_lock);
	return sprintf(buf,
		"latest       %" PRIu64 "\n"
		"flush        %" PRIu64 "\n"
		"completed    %" PRIu64 "\n"
		"permanent    %" PRIu64 "\n"
		"written      %" PRIu64 "\n"
		"prev_written %" PRIu64 "\n"
		"oldest       %" PRIu64 "\n"
		, lsids.latest
		, lsids.flush
#ifdef WALB_FAST_ALGORITHM
		, lsids.completed
#else
		, lsids.written
#endif
		, lsids.permanent
		, lsids.written
		, lsids.prev_written
		, lsids.oldest);
}

static ssize_t walb_attr_show_name(struct walb_dev *wdev, char *buf)
{
	int len = 0;
	ASSERT(DISK_NAME_LEN <= PAGE_SIZE);
	spin_lock(&wdev->lsuper0_lock);
	if (wdev->lsuper0) {
		len = sprintf(buf, "%s\n",
			get_super_sector_const(wdev->lsuper0)->name);
	}
	spin_unlock(&wdev->lsuper0_lock);
	return len;
}

static ssize_t walb_attr_show_uuid(struct walb_dev *wdev, char *buf)
{
	int len = 0;
	spin_lock(&wdev->lsuper0_lock);
	if (wdev->lsuper0) {
		len = sprint_uuid(buf, UUID_STR_SIZE, get_super_sector_const(wdev->lsuper0)->uuid);
		if (len > 0) {
			strcat(buf, "\n");
			len++;
		}
	}
	spin_unlock(&wdev->lsuper0_lock);
	return len;
}

static ssize_t walb_attr_show_log_capacity(struct walb_dev *wdev, char *buf)
{
	return sprintf(buf, "%" PRIu64 "\n", walb_get_log_capacity(wdev));
}

static ssize_t walb_attr_show_log_usage(struct walb_dev *wdev, char *buf)
{
	return sprintf(buf, "%" PRIu64 "\n", walb_get_log_usage(wdev));
}

static ssize_t walb_attr_show_status(struct walb_dev *wdev, char *buf)
{
	struct iocore_data *iocored = get_iocored_from_wdev(wdev);
	unsigned long flagsW, flagsC;

	if (!iocored)
		return 0;

	flagsW = wdev->flags;
	flagsC = iocored->flags;

	return snprintf(buf, PAGE_SIZE,
		"read_only                %u\n"
		"log_overflow             %u\n"
		"finalize                 %u\n"
		"submit_log_task_working  %u\n"
		"wait_log_task_working    %u\n"
		"submit_data_task_working %u\n"
		"wait_data_task_working   %u\n"
		, test_bit(WALB_STATE_READ_ONLY, &flagsW)
		, test_bit(WALB_STATE_OVERFLOW, &flagsW)
		, test_bit(WALB_STATE_FINALIZE, &flagsW)
		, test_bit(IOCORE_STATE_SUBMIT_LOG_TASK_WORKING, &flagsC)
		, test_bit(IOCORE_STATE_WAIT_LOG_TASK_WORKING, &flagsC)
		, test_bit(IOCORE_STATE_SUBMIT_DATA_TASK_WORKING, &flagsC)
		, test_bit(IOCORE_STATE_WAIT_DATA_TASK_WORKING, &flagsC));
}

/*******************************************************************************
 * Ops and attributes definition.
 *******************************************************************************/

struct walb_sysfs_attr {
	struct attribute attr;
	ssize_t (*show)(struct walb_dev *, char *);
	ssize_t (*store)(struct walb_dev *, char *);
};

static ssize_t walb_attr_show(
	struct kobject *kobj, struct attribute *attr, char *buf)
{
	struct walb_sysfs_attr *wattr = container_of(attr, struct walb_sysfs_attr, attr);
	struct walb_dev *wdev = get_wdev_from_kobj(kobj);

	if (!wdev) {
		return -EINVAL;
	}
	return wattr->show(wdev, buf);
}

static const struct sysfs_ops walb_sysfs_ops = {
	.show = walb_attr_show,
};

#define DECLARE_WALB_SYSFS_ATTR(name)					\
	struct walb_sysfs_attr walb_attr_##name =				\
		__ATTR(name, S_IRUGO, walb_attr_show_##name, NULL)

static DECLARE_WALB_SYSFS_ATTR(ldev);
static DECLARE_WALB_SYSFS_ATTR(ddev);
static DECLARE_WALB_SYSFS_ATTR(lsids);
static DECLARE_WALB_SYSFS_ATTR(name);
static DECLARE_WALB_SYSFS_ATTR(uuid);
static DECLARE_WALB_SYSFS_ATTR(log_capacity);
static DECLARE_WALB_SYSFS_ATTR(log_usage);
static DECLARE_WALB_SYSFS_ATTR(status);

static struct attribute *walb_attrs[] = {
	&walb_attr_ldev.attr,
	&walb_attr_ddev.attr,
	&walb_attr_lsids.attr,
	&walb_attr_name.attr,
	&walb_attr_uuid.attr,
	&walb_attr_log_capacity.attr,
	&walb_attr_log_usage.attr,
	&walb_attr_status.attr,
	NULL,
};

static struct kobj_type walb_ktype = {
	.sysfs_ops = &walb_sysfs_ops,
	.default_attrs = walb_attrs,
};

/*******************************************************************************
 * Global functions.
 *******************************************************************************/

int walb_sysfs_init(struct walb_dev *wdev)
{
	LOGn("walb_sysfs_init\n");
	memset(&wdev->kobj, 0, sizeof(struct kobject));
	return kobject_init_and_add(&wdev->kobj, &walb_ktype,
				&disk_to_dev(wdev->gd)->kobj,
				"%s", "walb");
}

void walb_sysfs_exit(struct walb_dev *wdev)
{
	LOGn("walb_sysfs_exit\n");
	kobject_put(&wdev->kobj);
}

/**
 * Notify userland processes that are polling a sysfs entry with a attribute name.
 */
void walb_sysfs_notify(struct walb_dev *wdev, const char *attr_name)
{
	if (wdev && attr_name) {
		sysfs_notify(&wdev->kobj, NULL, attr_name);
	}
}

/* end of file */
