/**
 * walb_snapshot.c - Walb snapshot operations.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/rwsem.h>
#include <linux/list.h>

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

/* Search free records. */
static int get_n_records_in_snapshot_sector_detail(
        struct walb_snapshot_sector *snap_sect, int max_n);
static int get_n_records_in_snapshot_sector(struct sector_data *sect);
static int get_n_free_records_in_snapshot_sector(struct sector_data *sect);

/* Sector operations */
static int is_valid_snapshot_sector(const struct sector_data *sect);
static int sector_load(struct snapshot_data *snapd, u64 off);
static int sector_sync(struct snapshot_data *snapd, u64 off);
static int sector_sync_all(struct snapshot_data *snapd);
static void sector_evict(struct snapshot_data *snapd, u64 off);
static void sector_evict_all(struct snapshot_data *snapd);

/* Functions for internal snapshot sector. */
static int get_idx_in_snapshot_sector(
        const struct sector_data *sect, u32 snapshot_id);
static struct walb_snapshot_record* get_record_in_snapshot_sector(
        const struct sector_data *sect, u32 snapshot_id);

/* Record allocation. */
static u32 record_alloc(struct snapshot_data *snapd,
                        struct walb_snapshot_record **recp);
static int record_free(struct snapshot_data *snapd, u32 snapshot_id);

/* Allocation bit operations. */
static int is_alloc_snapshot_record(
        int nr, const struct sector_data *sect);
static void set_alloc_snapshot_record(int nr, struct sector_data *sect);
static void clear_alloc_snapshot_record(int nr, struct sector_data *sect);

/* Issue IO */
static int snapshot_sector_read(struct snapshot_data *snapd,
                                u64 offset,
                                struct sector_data *sect);
static int snapshot_sector_write(struct snapshot_data *snapd,
                                 u64 offset,
                                 const struct sector_data *sect);

/* Get control using index. */
static struct snapshot_sector_control* get_sector_control_with_offset(
        const struct snapshot_data *snapd, u64 off);
static struct snapshot_sector_control* get_sector_control_with_snapshot_id(
        const struct snapshot_data *snapd, u32 snapshot_id);

/* Get snapshot_id using index. */
static u32 get_snapshot_id_with_name(
        const struct snapshot_data *snapd, const char *name);
static struct tree_cell_head* get_snapshot_id_with_lsid(
        const struct snapshot_data *snapd, u64 lsid);

/* Manipulation with index. */
static int insert_snapshot_record_to_index(
        struct snapshot_data *snapd,
        const struct walb_snapshot_record *rec);
/**/static int delete_snapshot_record_from_index(
        struct snapshot_data *snapd,
        const struct walb_snapshot_record *rec);

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

/**
 * Check whether snapshot sector is valid.
 *
 * @sect sector data which must be snapshot sector.
 *
 * @return non-zero if valid, or 0.
 */
static int is_valid_snapshot_sector(const struct sector_data *sect)
{
        int count = 0;
        int i;
        const struct walb_snapshot_record *rec;

        ASSERT_SNAPSHOT_SECTOR(sect);
        
        for_each_snapshot_record_const(i, rec, sect) {

                if (is_alloc_snapshot_record(i, sect)) {
                        if (! is_valid_snapshot_record(rec)) {
                                count ++;
                        }
                } else {
                        if (rec->snapshot_id != INVALID_SNAPSHOT_ID) {
                                count ++;
                        }
                }
        }
        printk_d("snapshot sector invalid record: %d\n", count);
        return (count == 0);
}

/**
 * Load sector from storage into memory.
 *
 * This function actually issues read IO only if need.
 *
 * @snapd snapshot data.
 * @off sector offset to load.
 *
 * @return 0 in success, or -1.
 */
static int sector_load(struct snapshot_data *snapd, u64 off)
{
        struct snapshot_sector_control *ctl;
        struct sector_data *sect = NULL;

        ASSERT(snapd != NULL);
        ASSERT(snapd->start_offset <= off && off < snapd->end_offset);
        
        ctl = get_sector_control_with_offset(snapd, off);

        /* Allocate if need. */
        if (ctl->state == SNAPSHOT_SECTOR_CONTROL_FREE) {
                ASSERT_SNAPSHOT_SECTOR(ctl->sector);
                sect = sector_alloc(snapd->sector_size, GFP_KERNEL);
                if (sect == NULL) { goto error0; }
                ctl->state = SNAPSHOT_SECTOR_CONTROL_ALLOC;
                ctl->sector = sect;
        }
        ASSERT(ctl->sector != NULL);

        /* Read sector if need. */
        if (ctl->state == SNAPSHOT_SECTOR_CONTROL_ALLOC) {
                if (sector_io(READ, snapd->bdev, off, ctl->sector) != 0) {
                        printk_e("Read snapshot sector %"PRIu64" failed.\n", off);
                        goto error0;
                }
                ctl->state = SNAPSHOT_SECTOR_CONTROL_CLEAN;
        }
        return 0;
        
error0:
        return -1;
}

/**
 * Sync down sector from memory to storage.
 *
 * This function actually issues write IO only if need.
 *
 * @snapd snapshot data.
 * @off sector offset to sync.
 *
 * @reutrn 0 in success, or -1.
 */
static int sector_sync(struct snapshot_data *snapd, u64 off)
{
        struct snapshot_sector_control *ctl;
        
        ASSERT(snapd != NULL);
        ASSERT(snapd->start_offset <= off && off < snapd->end_offset);
        
        ctl = get_sector_control_with_offset(snapd, off);
        ASSERT(ctl != NULL);

        if (ctl->state == SNAPSHOT_SECTOR_CONTROL_DIRTY) {

                ASSERT_SNAPSHOT_SECTOR(ctl->sector);
                ASSERT(is_valid_snapshot_sector(ctl->sector));
                
                if (snapshot_sector_write(snapd, off, ctl->sector) != 0) {
                        goto error;
                }
                ctl->state = SNAPSHOT_SECTOR_CONTROL_CLEAN;
        }

        return 0;
error:
        return -1;
}


/**
 * Sync down all sectors.
 *
 * After call this, there is no DIRTY sectors.
 *
 * @snapd snapshot data.
 *
 * @return 0 in success, or -1.
 */
static int sector_sync_all(struct snapshot_data *snapd)
{
        int ret = 0;
        u64 off;
        struct snapshot_sector_control *ctl;

        ASSERT(snapd != NULL);

        for_each_snapshot_sector(off, ctl, snapd) {
                
                if (sector_sync(snapd, off) != 0) {
                        ret --;
                }
        }
        return ret;
}

/**
 * This function evicts sectors with CLEAN mark on the memory.
 *
 * If a sector is marked DIRTY,
 * call @sector_sync() before calling this.
 *
 * @snapd snapshot data.
 * @off snapshot sector offset.
 */
static void sector_evict(struct snapshot_data *snapd, u64 off)
{
        struct snapshot_sector_control *ctl;

        ASSERT(snapd != NULL);
        ASSERT(snapd->start_offset <= off && off < snapd->end_offset);

        ctl = get_sector_control_with_offset(snapd, off);
        ASSERT(ctl != NULL);
        ASSERT(ctl->offset == off);

        if (ctl->state == SNAPSHOT_SECTOR_CONTROL_CLEAN) {

                ASSERT_SNAPSHOT_SECTOR(ctl->sector);
                ASSERT(is_valid_snapshot_sector(ctl->sector));

                sector_free(ctl->sector);
                ctl->sector = NULL;
                ctl->state = SNAPSHOT_SECTOR_CONTROL_FREE;
        }
}

/**
 * Evict all sectors.
 *
 * After call this, there is no CLEAN sectors.
 *
 * @snapd snapshot data.
 */
static void sector_evict_all(struct snapshot_data *snapd)
{
        u64 off;
        struct snapshot_sector_control *ctl;

        ASSERT(snapd != NULL);

        for_each_snapshot_sector(off, ctl, snapd) {

                sector_evict(snapd, off);
        }
}

/**
 * Get index of snapshot record by a snapshot_id.
 *
 * @sect snapshot sector.
 * @snapshot_id snapshot id to find.
 *   DO NOT specify INVALID_SNAPSHOT_ID.
 *
 * @return record index in the snapshot sector if found, or -1.
 */
static int get_idx_in_snapshot_sector(
        const struct sector_data *sect, u32 snapshot_id)
{
        int i;
        const struct walb_snapshot_record *rec;
        
        ASSERT_SNAPSHOT_SECTOR(sect);
        ASSERT(snapshot_id != INVALID_SNAPSHOT_ID);

        for_each_snapshot_record_const(i, rec, sect) {

                if (rec->snapshot_id == snapshot_id) {
                        return i;
                }
        }
        return -1;
}

/**
 * Get snapshot record with a snapshot_id.
 */
static struct walb_snapshot_record* get_record_in_snapshot_sector(
        const struct sector_data *sect, u32 snapshot_id)
{
        int idx;

        idx = get_idx_in_snapshot_sector(sect, snapshot_id);
        if (idx >= 0) {
                return get_snapshot_record_by_idx(sect, idx);
        } else {
                return NULL;
        }
}

/**
 * Allocate snapshot record.
 *
 * 1. Search free record.
 * 2. Initialize the record and set allocation bit.
 * 3. Insert to id_idx.
 *
 * @snapd snapshot data.
 * @recp pointer to allocated record pointer.
 *
 * @return snapshot_id in success, or INVALID_SNAPSHOT_ID.
 */
static u32 record_alloc(struct snapshot_data *snapd,
                        struct walb_snapshot_record **recp)
{
        u64 off;
        struct snapshot_sector_control *ctl = NULL;
        struct walb_snapshot_record *rec;
        int i, max_n;
        
        ASSERT(snapd != NULL);

        /* Search sectors with n_free_records > 0. */
        for_each_snapshot_sector(off, ctl, snapd) {
                if (ctl->n_free_records > 0) { break; } /* found */
        }
        if (off == snapd->end_offset) { goto error; } /* not found */
        ASSERT(ctl->offset == off);

        /* Load the sectors. */
        if (sector_load(snapd, off) != 0) { goto error; }
        ASSERT(ctl->state == SNAPSHOT_SECTOR_CONTROL_CLEAN ||
               ctl->state == SNAPSHOT_SECTOR_CONTROL_DIRTY);
        ASSERT_SNAPSHOT_SECTOR(ctl->sector);

        /* Search free records in the sector. */
        for_each_snapshot_record(i, rec, ctl->sector) {
                if (! is_alloc_snapshot_record(i, ctl->sector)) { break; }
        }
        /* n_free_records > 0 so we can found free record. */
        ASSERT(i < max_n_snapshots_in_sector(ctl->sector->size));

        /* Set allocation bit. */
        set_alloc_snapshot_record(i, ctl->sector);

        /* Initialize and asssign snapshot_id. */
        rec = get_snapshot_record_by_idx(ctl->sector, i);
        snapshot_record_init(rec);
        rec->snapshot_id = snapd->next_snapshot_id ++;

        /* Change state and decrement n_free_records. */
        ctl->state = SNAPSHOT_SECTOR_CONTROL_DIRTY;
        ctl->n_free_records --;

        /* Insert to primary index. */
        if (map_add(snapd->id_idx, rec->snapshot_id,
                    (unsigned long)ctl, GFP_KERNEL) != 0) {
                /* snapshot_id must be unique. */
                BUG();
        }
        *recp = rec;
        return rec->snapshot_id;
        
error:
        return INVALID_SNAPSHOT_ID;
}

/**
 * Free allocated record.
 *
 * 1. Search control and index inside the sector.
 * 2. Delete from id_idx.
 * 3. Initialize the record and clear allocation bit.
 *
 * @return 0 in success, or -1.
 */
static int record_free(struct snapshot_data *snapd, u32 snapshot_id)
{
        struct snapshot_sector_control *ctl;
        struct walb_snapshot_record *rec;
        int idx;

        /* Get sector control. */
        ctl = get_sector_control_with_snapshot_id(snapd, snapshot_id);
        if (ctl == NULL) { goto error; }

        /* Load sector. */
        if (sector_load(snapd, ctl->offset) != 0) { goto error; }
        ASSERT(ctl->state == SNAPSHOT_SECTOR_CONTROL_CLEAN ||
               ctl->state == SNAPSHOT_SECTOR_CONTROL_DIRTY);
        ASSERT_SNAPSHOT_SECTOR(ctl->sector);

        /* Delete from primary index. */
        if (map_del(snapd->id_idx, snapshot_id) != (unsigned long)ctl) {
                BUG();
        }

        /* Get record index inside snapshot sector. */
        idx = get_idx_in_snapshot_sector(ctl->sector, snapshot_id);
        
        /* Clear allocation bit. */
        ASSERT(idx >= 0);
        clear_alloc_snapshot_record(idx, ctl->sector);

        /* Initialize record. */
        rec = get_snapshot_record_by_idx(ctl->sector, idx);
        snapshot_record_init(rec);

        /* Change state of control. */
        ctl->state = SNAPSHOT_SECTOR_CONTROL_DIRTY;
        ctl->n_free_records ++;

        return 0;
error:
        return -1;
}

/**
 * Check whether nr'th record is allocated in a snapshot sector.
 *
 * @return non-zero if exist, or 0.
 */
static int is_alloc_snapshot_record(
        int nr, const struct sector_data *sect)
{
        ASSERT_SNAPSHOT_SECTOR(sect);
        ASSERT(0 <= nr && nr < 64);
        
        return test_u64bits(nr, &get_snapshot_sector_const(sect)->bitmap);
}

/**
 * Set allocation bit of a snapshot sector.
 */
static void set_alloc_snapshot_record(
        int nr, struct sector_data *sect)
{
        ASSERT_SNAPSHOT_SECTOR(sect);
        ASSERT(0 <= nr && nr < 64);
        
        set_u64bits(nr, &get_snapshot_sector(sect)->bitmap);
}

/**
 * Clear exist bit of a snapshot sector.
 */
static void clear_alloc_snapshot_record(
        int nr, struct sector_data *sect)
{
        ASSERT_SNAPSHOT_SECTOR(sect);
        ASSERT(0 <= nr && nr < 64);

        clear_u64bits(nr, &get_snapshot_sector(sect)->bitmap);
}

/**
 * Read snapshot sector from disk.
 *
 * @snapd snapshot data.
 * @offset offset of snapshot sector.
 * @sect sector data to be stored.
 *
 * @return 0 in success, or -1.
 */
static int snapshot_sector_read(struct snapshot_data *snapd,
                                u64 offset,
                                struct sector_data *sect)
{
        ASSERT_SECTOR_DATA(sect);
        ASSERT(snapd->start_offset <= offset && offset < snapd->end_offset);
        
        /* Issue read IO. */
        if (sector_io(READ, snapd->bdev, offset, sect) != 0) {
                printk_e("Read snapshot sector %"PRIu64" failed.\n", offset);
                goto error0;
        }
        /* Check checksum. */
        if (checksum((u8 *)sect->data, sect->size) != 0) {
                printk_e("Checksum is bad.\n");
                goto error0;
        }

        /* Check validness of record array. */
        if (! is_valid_snapshot_sector(sect)) {
                printk_e("Snapshot is not valid.\n");
                goto error0;
        }

        return 0;
error0:
        return -1;
}

/**
 * Write snapshot sector to disk.
 *
 * @snapd snapshot data.
 * @offset offset of snapshot sector.
 * @sect sector data to write.
 *
 * @return 0 in success, or -1.
 */
static int snapshot_sector_write(
        struct snapshot_data *snapd, u64 offset,
        const struct sector_data *sect)
{
        struct sector_data *sect_tmp;
        struct walb_snapshot_sector *snap_sect;

        ASSERT_SNAPSHOT_SECTOR(sect);
        ASSERT(snapd->start_offset <= offset && offset < snapd->end_offset);
        
        sect_tmp = sector_alloc(sect->size, GFP_KERNEL);
        if (sect_tmp == NULL) { goto error0; }
        sector_copy(sect_tmp, sect);

        /* Check validness of record array. */
        if (! is_valid_snapshot_sector(sect_tmp)) {
                printk_e("snapshot sector is invalid.\n");
                goto error1;
        }

        /* Calculate checksum. */
        snap_sect = get_snapshot_sector(sect_tmp);
        snap_sect->checksum = 0;
        snap_sect->checksum = checksum((u8 *)snap_sect, sect_tmp->size);

        /* Issue write IO. */
        if (sector_io(WRITE, snapd->bdev, offset, sect_tmp) != 0) {
                printk_e("Write snapshot sector %"PRIu64" failed.\n", offset);
                goto error1;
        }

        sector_free(sect_tmp);
        return 0;
error1:
        sector_free(sect_tmp);
error0:
        return -1;        
}

/**
 * Get sector control with a offset.
 *
 * @snapd snapshot data.
 * @off sector offset.
 *
 * @return snapshot sector control. Never NULL.
 */
static struct snapshot_sector_control* get_sector_control_with_offset(
        const struct snapshot_data *snapd, u64 off)
{
        unsigned long p;
        struct snapshot_sector_control *ctl;
        
        ASSERT(snapd != NULL);
        ASSERT(snapd->start_offset <= off && off < snapd->end_offset);

        p = map_lookup(snapd->sectors, off);
        ASSERT(p != TREEMAP_INVALID_VAL);
        ctl = (struct snapshot_sector_control *)p;
        ASSERT(ctl != NULL);
        ASSERT(ctl->offset == off);
        
        return ctl;
}

/**
 * Get sector control with a snapshot id.
 *
 * @snapd snapshot data.
 * @snapshot_id snapshot id.
 *   DO NOT specify INVALID_SNAPSHOT_ID.
 *
 * @return snapshot sector control if found, or NULL.
 */
static struct snapshot_sector_control* get_sector_control_with_snapshot_id(
        const struct snapshot_data *snapd, u32 snapshot_id)
{
        unsigned long p;
        struct snapshot_sector_control *ctl;

        ASSERT(snapd != NULL);
        ASSERT(snapshot_id != INVALID_SNAPSHOT_ID);

        p = map_lookup(snapd->id_idx, snapshot_id);
        if (p != TREEMAP_INVALID_VAL) {
                ctl = (struct snapshot_sector_control *)p;
                ASSERT(ctl != NULL);
                return ctl;
        } else {
                return NULL;
        }
}

/**
 * Search snapshot id with name.
 *
 * @return snapshot id if found, or INVALID_SNAPSHOT_ID.
 */
static u32 get_snapshot_id_with_name(
        const struct snapshot_data *snapd, const char *name)
{
        int len;
        unsigned long val;
        
        len = strnlen(name, SNAPSHOT_NAME_MAX_LEN);
        
        val = hashtbl_lookup(snapd->name_idx, name, len);
        if (val == HASHTBL_INVALID_VAL) {
                return INVALID_SNAPSHOT_ID;
        } else {
                return (u32)val;
        }
}

/**
 * Search snapshot id with lsid.
 *
 * @return tree_cell_head if found, or NULL.
 */
static struct tree_cell_head* get_snapshot_id_with_lsid(
        const struct snapshot_data *snapd, u64 lsid)
{
        ASSERT(snapd != NULL);
        
        return multimap_lookup(snapd->lsid_idx, lsid);
}

/**
 * Insert snapshot record to indices of snapshot data.
 *
 * Index: name_idx, lsid_idx.
 *
 * @return 0 in success, or -1.
 */
static int insert_snapshot_record_to_index(
        struct snapshot_data *snapd,
        const struct walb_snapshot_record *rec)
{

        ASSERT(snapd != NULL);

        snapd->name_idx;
        snapd->lsid_idx;

        /* now editing */
        
        return -1;
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
                ctl->state = SNAPSHOT_SECTOR_CONTROL_FREE;
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
        struct snapshot_sector_control *ctl;
        struct sector_data *sect;
        u32 next_snapshot_id;
        int i;
        struct walb_snapshot_record *rec;
        
        sect = sector_alloc(snapd->sector_size, GFP_KERNEL);
        if (sect == NULL) { goto error0; }
        ASSERT(snapd->sector_size == sect->size);

        /* For all snapshot sectors. */
        for (off = snapd->start_offset; off < snapd->end_offset; off ++) {

                /* Get control object. */
                ctl = get_sector_control_with_offset(snapd, off);
                
                /* Load sector. */
                if (sector_io(READ, snapd->bdev, off, sect) != 0) {
                        printk_e("Read snapshot sector %"PRIu64" failed.\n", off);
                        goto error1;
                }
                
                /* Calculate n_free_records. */
                ctl->n_free_records = get_n_free_records_in_snapshot_sector(sect);

                /* For all records */
                for_each_snapshot_record(i, rec, ctl->sector) {

                        /* Free invalid record. */
                        
                        printk_d("%d\n", i);
                }
                
                
                /* Insert indexes. */

                /* now editing. */
                
        }

        /* Set next_snapshot_id. */
        
        
        
        sector_free(sect);
        return 1;

error1:
        sector_free(sect);
error0:
        return 0;
}

/**
 * Sync all dirty cache of snapshot sectors.
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
 * Add snapshot.
 *
 * @snapd snapshot data.
 * @rec snapshot record to add.
 *
 * @return 0 in succeed.
 *        -1 the name is already used.
 *        -2 no record space.
 *        -3 other error.
 */
int snapshot_add_nolock(struct snapshot_data *snapd,
                        const char *name, u64 lsid, u64 timestamp)
{
        struct walb_snapshot_record *dst_rec;
        u32 snapshot_id;
        
        /* Check name is unique. */
        if (get_snapshot_id_with_name(snapd, name) != INVALID_SNAPSHOT_ID) {
                goto non_unique_name;
        }

        /* Allocate record. */
        snapshot_id = record_alloc(snapd, &dst_rec);
        if (snapshot_id == INVALID_SNAPSHOT_ID) { goto nomem; }
        ASSERT(snapshot_id == dst_rec->snapshot_id);

        /* Assign and check record. */
        snapshot_record_assign(dst_rec, name, lsid, timestamp);
        if (! is_valid_snapshot_record(dst_rec)) {
                goto error0;
        }

        /* Insert into indexes. */
        if (insert_snapshot_record_to_index(snapd, dst_rec) != 0) {
                goto error0;
        }

        /* Sync and evict all sectors. */
        sector_sync_all(snapd);
        sector_evict_all(snapd);

        return 0;

error0:
        record_free(snapd, snapshot_id);
        return -3;
nomem:
        return -2;
non_unique_name:
        return -1;
}

int snapshot_add(struct snapshot_data *snapd,
                 const char *name, u64 lsid, u64 timestamp)
{
        int ret;
        
        ASSERT(snapd != NULL);
        
        snapshot_write_lock(snapd);
        ret = snapshot_add_nolock(snapd, name, lsid, timestamp);
        snapshot_write_unlock(snapd);
        return ret;
}

/**
 * Delete snapshot with a name.
 *
 * @snapd snapshot data.
 * @name snapshot name to delete.
 *
 * @return 1 in success, or 0.
 */
int snapshot_del_nolock(struct snapshot_data *snapd, const char *name)
{
        /* not yet implemented. */

        return 0;
}

int snapshot_del(struct snapshot_data *snapd, const char *name)
{
        int is_success;

        ASSERT(snapd != NULL);
        
        snapshot_write_lock(snapd);
        is_success = snapshot_del_nolock(snapd, name);
        snapshot_write_unlock(snapd);
        return is_success;
}

/**
 * Delete snapshots with a lsid range.
 *
 * @lsid0 start of the range.
 * @lsid1 end of the range.
 *        lsid0 <= lsid < lsid1.
 *
 * @return 1 in success, or 0.
 */
int snapshot_del_range_nolock(struct snapshot_data *snapd, u64 lsid0, u64 lsid1)
{
        /* not yet implemented */

        return 0;
}

int snapshot_del_range(struct snapshot_data *snapd, u64 lsid0, u64 lsid1)
{
        int is_success;

        ASSERT(snapd != NULL);

        snapshot_write_lock(snapd);
        is_success = snapshot_del_range_nolock(snapd, lsid0, lsid1);
        snapshot_write_unlock(snapd);
        return is_success;
}

/**
 * Get snapshot record with a name.
 *
 * @snapd snapshot data.
 * @name snapshot name.
 * @record out: snapshot record with the name.
 *
 * @return 1 in success, or 0.
 */
int snapshot_get_nolock(struct snapshot_data *snapd, const char *name,
                        struct walb_snapshot_record *rec)
{
        /* not yet implemented. */

        return 0;
}

int snapshot_get(struct snapshot_data *snapd, const char *name,
                 struct walb_snapshot_record *rec)
{
        int is_success;
        
        ASSERT(snapd != NULL);
        
        snapshot_read_lock(snapd);
        is_success = snapshot_get_nolock(snapd, name, rec);
        snapshot_read_unlock(snapd);
        
        return is_success;
}

/**
 * Get number of records in a lsid range.
 *
 * @snapd snapshot data.
 * @lsid0 start of the range.
 * @lsid1 end of the range.
 *        lsid0 <= lsid < lsid1.
 *
 * @return number of records in success, or -1.
 */
int snapshot_n_records_range_nolock(
        struct snapshot_data *snapd, u64 lsid0, u64 lsid1)
{
        /* not yet implemented. */
        
        return -1;
}

int snapshot_n_records_range(
        struct snapshot_data *snapd, u64 lsid0, u64 lsid1)
{
        int n;

        ASSERT(snapd != NULL);

        snapshot_read_lock(snapd);
        n = snapshot_n_records_range(snapd, lsid0, lsid1);
        snapshot_read_unlock(snapd);
        return n;
}

int snapshot_n_records(struct snapshot_data *snapd)
{
        return snapshot_n_records_range(snapd, 0, MAX_LSID + 1);
}

/**
 * Get list of snapshot with a lsid range.
 *
 * You get limited records without enough buffer.
 *
 * @snapd snapshot data.
 * @buf buffer to store result record array.
 * @buf buffer size.
 * @lsid0 start of the range.
 * @lsid1 end of the range.
 *        lsid0 <= lsid < lsid1.
 *
 * @return n records stored to @buf. n >= 0.
 */
int snapshot_list_range_nolock(struct snapshot_data *snapd,
                               u8 *buf, size_t buf_size,
                               u64 lsid0, u64 lsid1)
{
        /* not yet implemented. */
        
        return 0;
}

int snapshot_list_range(struct snapshot_data *snapd,
                        u8 *buf, size_t buf_size,
                        u64 lsid0, u64 lsid1)
{
        int n_rec;

        ASSERT(snapd != NULL);
        
        snapshot_read_lock(snapd);
        n_rec = snapshot_list_range_nolock
                (snapd, buf, buf_size, lsid0, lsid1);
        snapshot_read_unlock(snapd);
        return n_rec;
}

int snapshot_list(struct snapshot_data *snapd, u8 *buf, size_t buf_size)
{
        return snapshot_list_range(snapd, buf, buf_size, 0, MAX_LSID + 1);
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
