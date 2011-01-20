/**
 * walb_snapshot.c - Walb snapshot operations.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/rwsem.h>

#include "../include/walb_log_device.h"
#include "walb_util.h"
#include "walb_io.h"
#include "walb_sector.h"
#include "hashtbl.h"
#include "treemap.h"

#include "walb_snapshot.h"

/*******************************************************************************
 * Prototypes of static functions.
 *******************************************************************************/

static int get_n_records_in_snapshot_sector_detail(
        struct walb_snapshot_sector *snap_sect, int max_n);
static int get_n_records_in_snapshot_sector(struct sector_data *sect);
static int get_n_free_records_in_snapshot_sector(struct sector_data *sect);

/*******************************************************************************
 * Static functions.
 *******************************************************************************/

/**
 * Get number of snapshots in the sector.
 */
static int get_n_records_in_snapshot_sector_detail(
        struct walb_snapshot_sector *snap_sect, int max_n)
{
        int i, n;

        ASSERT(snap_sect != NULL);
        ASSERT(snap_sect->sector_type == SECTOR_TYPE_SNAPSHOT);
        
        n = 0;
        for (i = 0; i < max_n; i ++) {
                if (test_u64bits(i, &snap_sect->bitmap)) {
                        n ++;
                }
        }
        ASSERT(0 <= n && n <= 64);
        
        return n;
}

/**
 * Get number of active records in a snapshot sector.
 */
static int get_n_records_in_snapshot_sector(struct sector_data *sect)
{
        int max_n = max_n_snapshots_in_sector(sect->size);
        ASSERT_SNAPSHOT_SECTOR(sect);
        
        return get_n_records_in_snapshot_sector_detail(
                get_snapshot_sector(sect), max_n);
}

/**
 * Get number of free records in a snapshot sector.
 */
static int get_n_free_records_in_snapshot_sector(struct sector_data *sect)
{
        int max_n = max_n_snapshots_in_sector(sect->size);
        ASSERT_SNAPSHOT_SECTOR(sect);

        return max_n - get_n_records_in_snapshot_sector_detail(
                get_snapshot_sector(sect), max_n);
}

/*******************************************************************************
 * Create/destroy snapshots structure. 
 *******************************************************************************/

/**
 * NOT TESTED YET.
 *
 * Create snapshot_data structure. 
 *
 * @bdev Underlying log device.
 * @start_offset Offset of the first snapshot sector.
 * @end_offset Offset of the next of the end snapshot sector.
 * @gfp_mask Allocation mask.
 *
 * @return Pointer to created snapshot_data in success, or NULL.
 */
struct snapshot_data* snapshot_data_create(
        struct block_device *bdev,
        u64 start_offset, u64 end_offset, gfp_t gfp_mask)
{
        struct snapshot_data *snapd;
        struct snapshot_sector_control *ctl;
        u64 off;

        ASSERT(start_offset < end_offset);

        /* Allocate snapshot data. */
        snapd = kmalloc(sizeof(struct snapshot_data), gfp_mask);
        if (snapd == NULL) { goto nomem; }

        /* Initialize snapshot data. */
        init_rwsem(&snapd->lock);
        snapd->start_offset = start_offset;
        snapd->end_offset = end_offset;
        snapd->bdev = bdev;
        snapd->sector_size = bdev_physical_block_size(bdev);

        /* Create sector controls. */
        snapd->sectors = map_create(GFP_KERNEL);
        if (snapd->sectors == NULL) { goto nomem; }

        /* Allocate each snapshot sector control data. */
        for (off = start_offset; off < end_offset; off ++) {
                ctl = kmalloc(sizeof(struct snapshot_sector_control), gfp_mask);
                if (ctl == NULL) { goto nomem; }
                ctl->offset = off;
                ctl->n_free_records = -1; /* Invalid value. */
                ctl->sector = NULL;

                if (map_add(snapd->sectors, off, (unsigned long)ctl, GFP_KERNEL) != 0) {
                        kfree(ctl);
                        goto nomem;
                }
        }

        /* Create indexes. */
        snapd->id_idx = map_create(GFP_KERNEL);
        if (snapd->id_idx == NULL) { goto nomem; }

        snapd->name_idx = hashtbl_create(HASHTBL_MAX_BUCKET_SIZE, GFP_KERNEL);
        if (snapd->name_idx == NULL) { goto nomem; }
        
        snapd->lsid_idx = multimap_create(GFP_KERNEL);
        if (snapd->lsid_idx == NULL) { goto nomem; }
        
        return snapd;

nomem:
        snapshot_data_destroy(snapd);
        return NULL;
}

/**
 * NOT TESTED YET.
 *
 * Destroy snapshot_data structure.
 */
void snapshot_data_destroy(struct snapshot_data *snapd)
{
        struct snapshot_sector_control *ctl;
        map_curser_t curt;

        if (snapd == NULL) { return; }

        /* Deallocate Indexes. */
        if (snapd->lsid_idx) {
                multimap_destroy(snapd->lsid_idx);
        }
        if (snapd->name_idx) {
                hashtbl_destroy(snapd->name_idx);
        }
        if (snapd->id_idx) {
                map_destroy(snapd->id_idx);
        }

        if (snapd->sectors) {
                /* Deallocate all snapshot sector control data. */
                map_curser_init(snapd->sectors, &curt);
                map_curser_search(&curt, 0, MAP_SEARCH_BEGIN);
                while (map_curser_next(&curt)) {
                        ctl = (void *)map_curser_get(&curt);
                        ASSERT(ctl != NULL && ctl != (void *)TREEMAP_INVALID_VAL);
                        kfree(ctl);
                }
                ASSERT(map_curser_is_end(&curt));

                /* Deallocate sectors data. */
                map_destroy(snapd->sectors);
        }

        /* Deallocate snapshot_data. */
        kfree(snapd);
}

/**
 * Scan all snapshot sectors in the log device
 * then fill required data and indexes.
 *
 * @return 1 in success, or 0.
 */
int snapshot_data_initialize(struct snapshot_data *snapd)
{
        u64 off;
        unsigned long p;
        struct snapshot_sector_control *ctl;
        struct sector_data *sect;
        
        sect = sector_alloc(snapd->sector_size, GFP_KERNEL);
        if (sect == NULL) { goto error0; }
        ASSERT(snapd->sector_size == sect->size);

        /* For all snapshot sectors. */
        for (off = snapd->start_offset; off < snapd->end_offset; off ++) {

                /* Get control object. */
                p = map_lookup(snapd->sectors, off);
                ASSERT(p != TREEMAP_INVALID_VAL);
                ctl = (struct snapshot_sector_control *)p;
                ASSERT(ctl != NULL);
                ASSERT(ctl->offset == off);
                
                /* Load sector. */
                if (sector_io(READ, snapd->bdev, off, sect) != 0) {
                        printk_e("Read snapshot sector %"PRIu64" failed.\n", off);
                        goto error1;
                }
                
                /* Calculate n_free_records. */
                ctl->n_free_records = get_n_free_records_in_snapshot_sector(sect);
                
                /* now editing */
                
        }

        sector_free(sect);

        return 1;

error1:
        sector_free(sect);
error0:
        return 0;
}

/**
 *
 *
 *
 * @return 1 in success, or 0.
 */
int snapshot_data_finalize(struct snapshot_data *snapd)
{
        /* not yet implemented. */
        
        return 1;
}


/*******************************************************************************
 * Snapshot operations.
 *******************************************************************************/

/**
 *
 *
 *
 * @return 1 in succeed, or 0.
 */
int snapshot_add(struct snapshot_data *snapd, const char *name, u64 lsid, u64 timestamp)
{
        /* not yet implemented. */
        
        return 0;
}

/**
 *
 */
void snapshot_del(struct snapshot_data *snapd, const char *name)
{
        /* not yet implemented. */

}

/**
 *
 */
void snapshot_del_before_lsid(struct snapshot_data *snapd, u64 lsid)
{
        /* not yet implemented. */

}

/**
 *
 */
int snapshot_get(struct snapshot_data *snapd, const char *name)
{
        /* not yet implemented. */

        return 0;
}

/**
 *
 */
int snapshot_n_records(struct snapshot_data *snapd)
{
        /* not yet implemented. */

        return -1;
}

/**
 *
 */
int snapshot_list(struct snapshot_data *snapd, u8 *buf, size_t buf_size)
{

        /* not yet implemented. */

        return -1;
}

/*******************************************************************************
 * Lock operations. We use a big lock.
 *******************************************************************************/

/**
 * Big read lock of snapshot data.
 */
void snapshot_read_lock(struct snapshot_data *snapd)
{
        ASSERT(! in_interrupt() && ! in_atomic());
        down_read(&snapd->lock);
}

/**
 * Big read unlock of snapshot data.
 */
void snapshot_read_unlock(struct snapshot_data *snapd)
{
        ASSERT(! in_interrupt() && ! in_atomic());
        up_read(&snapd->lock);
}

/**
 * Big write lock of snapshot data.
 */
void snapshot_write_lock(struct snapshot_data *snapd)
{
        ASSERT(! in_interrupt() && ! in_atomic());
        down_write(&snapd->lock);
}

/**
 * Big write unlock of snapshot data.
 */
void snapshot_write_unlock(struct snapshot_data *snapd)
{
        ASSERT(! in_interrupt() && ! in_atomic());
        up_write(&snapd->lock);
}

MODULE_LICENSE("Dual BSD/GPL");
