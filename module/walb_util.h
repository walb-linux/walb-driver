/**
 * Utility macros and functions for walb.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */

#ifndef _WALB_UTIL_H
#define _WALB_UTIL_H

/**
 * For debug
 */
#if defined(WALB_DEBUG)
#define printk_d(fmt, args...) \
        printk(KERN_DEBUG "walb: " fmt, ##args)
#else
#define printk_d(fmt, args...)
#endif

#define printk_e(fmt, args...)                  \
        printk(KERN_ERR "walb: " fmt, ##args)
#define printk_w(fmt, args...)                  \
        printk(KERN_WARNING "walb: " fmt, ##args)
#define printk_n(fmt, args...)                  \
        printk(KERN_NOTICE "walb: " fmt, ##args)
#define printk_i(fmt, args...)                  \
        printk(KERN_INFO "walb: " fmt, ##args)


#ifdef ASSERT
#undef ASSERT
#endif

#ifdef WALB_DEBUG
#define ASSERT(cond) BUG_ON(!(cond))
#else
#define ASSERT(cond)
#endif

/**
 * For test.
 */
#ifdef WALB_CHECK
#undef WALB_CHECK
#endif
#define WALB_CHECK(cond) do {                                           \
                if (! (cond)) {                                         \
                        printk_e("CHECK FAILED in %s:%d:%s.\n",    \
                                 __FILE__, __LINE__, __func__);         \
                        goto error;                                     \
                }                                                       \
        } while(0)


#endif /* _WALB_UTIL_H */
