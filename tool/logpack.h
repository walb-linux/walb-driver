/**
 * Logpack functions for walbctl.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef _WALB_LOGPACK_H
#define _WALB_LOGPACK_H

#include "walb.h"
#include "walb_log_record.h"
#include "walb_log_device.h"

bool read_logpack_header(int fd,
                         const walb_super_sector_t* super_sectp,
                         u64 lsid, walb_logpack_header_t* logpack);

void print_logpack_header(const walb_logpack_header_t* logpack);

#endif /* _WALB_LOGPACK_H */
