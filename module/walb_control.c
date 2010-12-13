/**
 * walb_control.c - control interface for walb.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * Written by: Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 */

#include <linux/module.h>
#include <linux/miscdevice.h>
#include <linux/slab.h>
#include <linux/compat.h>
#include <linux/rwsem.h>
#include <linux/uaccess.h>

#include "walb_kern.h"
#include "hashtbl.h"
#include "walb_control.h"
#include "walb_alldevs.h"

#include "../include/walb_ioctl.h"

/**
 * Allocate memory and call @copy_from_user().
 */
void* walb_alloc_and_copy_from_user(
        void __user *userbuf,
        size_t buf_size,
        gfp_t gfp_mask)
{
        void *buf;
        
        if (buf_size == 0 || userbuf == NULL) {
                goto error0;
        }

        buf = kmalloc(buf_size, gfp_mask);
        if (buf == NULL) {
                printk_e("memory allocation for walb_ctl.u2k.buf failed.\n");
                goto error0;
        }

        if (copy_from_user(buf, userbuf, buf_size)) {
                printk_e("copy_from_user failed\n");
                goto error1;
        }
        return buf;

error1:
        kfree(buf);
error0:
        return NULL;
}

/**
 * Call @copy_to_user() and free memory.
 *
 * @return 0 in success, or -1.
 *         kfree(buf) is also executed in error.
 */
int walb_copy_to_user_and_free(
        void __user *userbuf,
        void *buf,
        size_t buf_size)
{
        if (buf_size == 0 || userbuf == NULL || buf == NULL) {
                goto error0;
        }

        if (copy_to_user(userbuf, buf, buf_size)) {
                goto error0;
        }
        
        kfree(buf);
        return 0;

error0:
        kfree(buf);
        return -1;
}

/**
 * Alloc required memory and copy userctl data.
 *
 * @userctl userctl pointer.
 * @return gfp_mask mask for kmalloc.
 */
struct walb_ctl* walb_get_ctl(void __user *userctl, gfp_t gfp_mask)
{
        struct walb_ctl *ctl;
        
        /* Allocate walb_ctl memory. */
        ctl = kzalloc(sizeof(struct walb_ctl), gfp_mask);
        if (ctl == NULL) {
                printk_e("memory allocation for walb_ctl failed.\n");
                goto error0;
        }

        /* Copy ctl. */
        if (copy_from_user(ctl, userctl, sizeof(struct walb_ctl))) {
                printk_e("copy_from_user failed.\n");
                goto error1;
        }

        /* Allocate and copy ctl->u2k.__buf. */
        if (ctl->u2k.buf_size > 0) {
                ctl->u2k.__buf = walb_alloc_and_copy_from_user
                        ((void __user *)ctl->u2k.buf,
                         ctl->u2k.buf_size, gfp_mask);
                if (ctl->u2k.__buf == NULL) {
                        goto error1;
                }
        }
        /* Allocate ctl->k2u.__buf. */
        if (ctl->k2u.buf_size > 0) {
                ctl->k2u.__buf = kzalloc(ctl->k2u.buf_size, gfp_mask);
                if (ctl->k2u.__buf == NULL) {
                        goto error2;
                }
        }
        return ctl;

/* error3: */
/*         if (ctl->k2u.buf_size > 0) { */
/*                 kfree(ctl->k2u.__buf); */
/*         } */
error2:
        if (ctl->u2k.buf_size > 0) {
                kfree(ctl->u2k.__buf);
        }
error1:
        kfree(ctl);
error0:
        return NULL;
}

/**
 * Copy ctl data to userland and deallocate memory.
 *
 * @userctl userctl pointer.
 * @ctl ctl to put.
 *
 * @return 0 in success, or false.
 */
int walb_put_ctl(void __user *userctl, struct walb_ctl *ctl)
{
        /* Free ctl->u2k.__buf. */
        if (ctl->u2k.buf_size > 0) {
                kfree(ctl->u2k.__buf);
        }

        /* Copy and free ctl->k2u.__buf. */
        if (ctl->k2u.buf_size > 0) {
                if (walb_copy_to_user_and_free
                    (ctl->k2u.buf, ctl->k2u.__buf, ctl->k2u.buf_size) != 0) {
                        goto error0;
                }
        }

        /* Copy ctl. */
        if (copy_to_user(userctl, ctl, sizeof(struct walb_ctl))) {
                printk_e("copy_to_user failed.\n");
                goto error0;
        }
        
        kfree(ctl);
        return 0;
        
error0:
        kfree(ctl);
        return -1;
}

/**
 * Start walb device.
 *
 * @ctl walb_ctl data.
 *      command == WALB_IOCTL_DEV_START
 *      Input:
 *        u2k
 *          log_devt, data_devt,
 *          minor (even value --> v    : wdev minor,
 *                                v + 1: wlog minor,
 *                 WALB_DYNAMIC_MINOR means automatic assignment),
 *          buf_size (<= 64),
 *          (char *)__buf (name of walb device. terminated by '\0').
 *      Output:
 *        error: 0 in success.
 *        k2u
 *          walb_devt
 *          minor
 *
 * @return 0 in success, or -EFAULT.
 */
static int ioctl_dev_start(struct walb_ctl *ctl)
{
        dev_t ldevt, ddevt;
        unsigned int minor;
        struct walb_dev *wdev;
        
        
        ASSERT(ctl->command == WALB_IOCTL_DEV_START);
        
        ldevt = ctl->u2k.log_devt;
        ddevt = ctl->u2k.data_devt;

        wdev = prepare_wdev(minor, ldevt, ddevt);
        if (wdev == NULL) {
                goto error0;
        }
        
        alldevs_write_lock();
        
        if (ctl->u2k.minor == WALB_DYNAMIC_MINOR) {
                minor = get_free_minor();
        } else {
                minor = ctl->u2k.minor;
                ASSERT(minor % 2 == 0);
        }

        if (alldevs_add(wdev) != 0) {
                goto error1;
        }
        
        register_wdev(wdev);
        alldevs_write_unlock();

        /* Return values to userland. */
        ctl->k2u.minor = minor;
        ctl->k2u.walb_devt = wdev->devt;
        ctl->error = 0;
        
        return 0;
        
        /* not tested */

/* error2: */
/*         alldevs_dell(wdev); */
error1:
        destroy_wdev(wdev);
error0:
        return -EFAULT;
}

/**
 *
 * @ctl walb_ctl data.
 *
 * @return 0 in success, or -EFAULT.
 */
static int ioctl_dev_stop(struct walb_ctl *ctl)
{
        /* now editing */
        
        return -EFAULT;
}

/**
 * Dispatcher WALB_IOCTL_CONTROL
 *
 * @ctl walb_ctl data.
 *
 * @return 0 in success,
 *         -ENOTTY in invalid command,
 *         -EFAULT in command failed.
 */
static int dispatch_ioctl(struct walb_ctl *ctl)
{
        int ret = 0;
        ASSERT(ctl != NULL);
        
        switch(ctl->command) {
        case WALB_IOCTL_DEV_START:
                ret = ioctl_dev_start(ctl);
                break;
        case WALB_IOCTL_DEV_STOP:
                ret = ioctl_dev_stop(ctl);
                break;
        default:
                printk_e("dispatch_ioctl: command %d is not supported.\n",
                         ctl->command);
                ret = -ENOTTY;
        }
        return ret;
}

/**
 * Execute ioctl for /dev/walb/control.
 *
 * @command ioctl command.
 * @user walb_ctl data in userland.
 * 
 * @return 0 in success,
 *         -ENOTTY in invalid command,
 *         -EFAULT in command failed.
 */
static int ctl_ioctl(uint command, struct walb_ctl __user *user)
{
        int ret = 0;
        struct walb_ctl *ctl;

        if (command != WALB_IOCTL_CONTROL) {
                printk_e("ioctl cmd must be %08lx but %08x\n",
                         WALB_IOCTL_CONTROL, command);
                return -ENOTTY;
        }

        ctl = walb_get_ctl(user, GFP_KERNEL);
        if (ctl == NULL) { goto error0; }

        ret = dispatch_ioctl(ctl);
        
        if (walb_put_ctl(user, ctl) != 0) {
                printk_e("walb_put_ctl failed.\n");
                goto error0;
        }
        return ret;

error0:
        return -EFAULT;
}

static long walb_ctl_ioctl(struct file *file, uint command, ulong u)
{
        int ret;
        u32 version;
        
        if (command == WALB_IOCTL_VERSION) {
                version = WALB_VERSION;
                ret = __put_user(version, (u32 __user *)u);
        } else {
                ret = (long)ctl_ioctl(command, (struct walb_ctl __user *)u);
        }
        return ret;
}

#ifdef CONFIG_COMPAT
static long walb_ctl_compat_ioctl(struct file *file, uint command, ulong u)
{
        return (long)walb_ctl_ioctl(file, command, (ulong)compat_ptr(u));
}
#else
#define walb_ctl_compat_ioctl NULL
#endif

static const struct file_operations ctl_fops_ = {
        .open = nonseekable_open,
        .unlocked_ioctl = walb_ctl_ioctl,
        .compat_ioctl = walb_ctl_compat_ioctl,
        .owner = THIS_MODULE,
};

static struct miscdevice walb_misc_ = {
        .minor = MISC_DYNAMIC_MINOR,
        .name  = WALB_NAME,
        .nodename = WALB_DIR_NAME "/" WALB_CONTROL_NAME,
        .fops = &ctl_fops_,
};

/**
 * Init walb control device.
 *
 * @return 0 in success, or -1.
 */
int __init walb_control_init(void)
{
        int ret;

        ret = misc_register(&walb_misc_);
        if (ret < 0) { goto error0; }

        printk_i("walb control device minor %u\n", walb_misc_.minor);
        return 0;

error0:
        return -1;
}

/**
 * Exit walb control device.
 */
void walb_control_exit(void)
{
        
        misc_deregister(&walb_misc_);
}

MODULE_LICENSE("Dual BSD/GPL");
