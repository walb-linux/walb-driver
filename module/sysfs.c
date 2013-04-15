/**
 * sysfs.c - sysfs related data and functions.
 *
 * (C) 2013, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/sysfs.h>
#include <linux/spinlock.h>
#include "kern.h"

struct walb_sysfs_attr {
	struct attribute attr;
	ssize_t (*show)(struct walb_dev *, char *);
	ssize_t (*store)(struct walb_dev *, char *);
};

static inline struct walb_dev *get_wdev_from_kobj(struct kobject *kobj)
{
	if (!kobj) { return NULL; }
	return container_of(kobj, struct walb_dev, kobj);
}

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

ssize_t walb_attr_ldev_show(struct walb_dev *wdev, char *buf)
{
	dev_t ldevt = wdev->ldev->bd_dev;
	sprintf(buf, "%u:%u\n", MAJOR(ldevt), MINOR(ldevt));
	return strlen(buf);
}

ssize_t walb_attr_ddev_show(struct walb_dev *wdev, char *buf)
{
	dev_t ddevt = wdev->ddev->bd_dev;
	sprintf(buf, "%u:%u\n", MAJOR(ddevt), MINOR(ddevt));
	return strlen(buf);
}

ssize_t walb_attr_lsids_show(struct walb_dev *wdev, char *buf)
{
	struct lsid_set lsids;

	spin_lock(&wdev->lsid_lock);
	lsids = wdev->lsids;
	spin_unlock(&wdev->lsid_lock);
	sprintf(buf,
		"latest %" PRIu64 "\n"
		"flush %" PRIu64 "\n"
		"completed %" PRIu64 "\n"
		"permanent %" PRIu64 "\n"
		"written %" PRIu64 "\n"
		"prev_written %" PRIu64 "\n"
		"oldest %" PRIu64 "\n"
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
	return strlen(buf);
}

static struct walb_sysfs_attr walb_attr_ldev =
	__ATTR(ldev, S_IRUGO, walb_attr_ldev_show, NULL);
static struct walb_sysfs_attr walb_attr_ddev =
	__ATTR(ddev, S_IRUGO, walb_attr_ddev_show, NULL);
static struct walb_sysfs_attr walb_attr_lsids =
	__ATTR(lsids, S_IRUGO, walb_attr_lsids_show, NULL);

static const struct sysfs_ops walb_sysfs_ops = {
	.show = walb_attr_show,
};

static struct attribute *walb_attrs[] = {
	&walb_attr_ldev.attr,
	&walb_attr_ddev.attr,
	&walb_attr_lsids.attr,
	NULL,
};

static struct kobj_type walb_ktype = {
	.sysfs_ops = &walb_sysfs_ops,
	.default_attrs = walb_attrs,
};

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
