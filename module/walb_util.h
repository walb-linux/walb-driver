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
#ifdef WALB_DEBUG
#define printk_d(fmt, args...)                                          \
        printk(KERN_DEBUG "walb(%s:%d): " fmt, __func__, __LINE__, ##args)
#else
#define printk_d(fmt, args...)
#endif

#define printk_e(fmt, args...)                                  \
        printk(KERN_ERR "walb(%s): " fmt, __func__, ##args)
#define printk_w(fmt, args...)                                  \
        printk(KERN_WARNING "walb(%s): " fmt, __func__, ##args)
#define printk_n(fmt, args...)                                  \
        printk(KERN_NOTICE "walb(%s): " fmt, __func__, ##args)
#define printk_i(fmt, args...)                                  \
        printk(KERN_INFO "walb(%s): " fmt, __func__, ##args)


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
