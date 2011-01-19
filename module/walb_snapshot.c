/**
 * walb_snapshot.c - Walb snapshot operations.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/rwsem.h>

#include "walb_util.h"
#include "walb_snapshot.h"
#include "walb_io.h"
#include "hashtbl.h"
#include "treemap.h"

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
        map_curser_t curt;

        ASSERT(start_offset < end_offset);

        /* Allocate snapshot data. */
        snapd = kmalloc(sizeof(struct snapshot_data), gfp_mask);
        if (snapd == NULL) { goto nomem0; }

        /* Initialize snapshot data. */
        init_rwsem(&snapd->lock);
        snapd->start_offset = start_offset;
        snapd->end_offset = end_offset;
        snapd->bdev = bdev;
        snapd->sector_size = bdev_physical_block_size(bdev);

        /* Create sector controls. */
        snapd->sectors = map_create(GFP_KERNEL);
        if (snapd->sectors == NULL) { goto nomem1; }

        /* Allocate each snapshot sector control data. */
        for (off = start_offset; off < end_offset; off ++) {
                ctl = kmalloc(sizeof(struct snapshot_sector_control), gfp_mask);
                if (ctl == NULL) { goto nomem2; }
                ctl->offset = off;
                ctl->n_free_records = -1; /* Invalid value. */
                ctl->image = NULL;

                if (map_add(snapd->sectors, off, (unsigned long)ctl, GFP_KERNEL) != 0) {
                        kfree(ctl);
                        goto nomem2;
                }
        }

        /* Create indexes. */
        snapd->id_idx = map_create(GFP_KERNEL);
        if (snapd->id_idx == NULL) { goto nomem2; }

        snapd->name_idx = hashtbl_create(HASHTBL_MAX_BUCKET_SIZE, GFP_KERNEL);
        if (snapd->name_idx == NULL) { goto nomem3; }
        
        snapd->lsid_idx = multimap_create(GFP_KERNEL);
        if (snapd->lsid_idx == NULL) { goto nomem4; }
        
        return snapd;

/* nomem5: */
/*         multimap_destroy(snapd->lsid_idx); */
nomem4:
        hashtbl_destroy(snapd->name_idx);
nomem3:
        map_destroy(snapd->id_idx);
nomem2:
        map_curser_init(snapd->sectors, &curt);
        map_curser_search(&curt, 0, MAP_SEARCH_BEGIN);
        while (map_curser_next(&curt)) {
                ctl = (void *)map_curser_get(&curt);
                ASSERT(ctl != NULL && ctl != (void *)TREEMAP_INVALID_VAL);
                kfree(ctl);
        }
        ASSERT(map_curser_is_end(&curt));
        map_destroy(snapd->sectors);
nomem1:
        kfree(snapd);
nomem0:
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
        
        ASSERT(snapd != NULL);

        /* Deallocate Indexes. */
        multimap_destroy(snapd->lsid_idx); 
        hashtbl_destroy(snapd->name_idx);
        map_destroy(snapd->id_idx);

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
#if 1
        return 1;
#else
        u64 off;
        unsigned long p;
        struct snapshot_sector_control *ctl;
        void *sectp;

        sectp = walb_alloc_sector(GFP_KERNEL);

        /* For all snapshot sectors. */
        for (off = snapd->start_offset; off < snapd->end_offset; off ++) {

                /* Get control object. */
                p = map_lookup(snapd->sectors, off);
                ASSERT(p != TREEMAP_INVALID_VAL);
                ctl = (struct snapshot_sector_control *)p;
                ASSERT(ctl != NULL);
                
                /* Load sector. */
                if (walb_sector_io(READ, snapd->bdev, sectp, off) != 0) {
                        printk_e("load snapshot sector %"PRIu64" failed.\n", off);
                        goto error0;
                }
                
                
                /* Cal */

                
                
                
                /* now editing */
                
        }

        /* not yet implemented. */
        
        return 1;


error1:
        walb_free_sector(sectp);
error0:
        return 0;
#endif
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
