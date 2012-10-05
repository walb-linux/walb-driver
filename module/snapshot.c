/**
 * snapshot_kern.c - Walb snapshot operations.
 *
 * Copyright(C) 2010, Cybozu Labs, Inc.
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/rwsem.h>
#include <linux/list.h>

#include "walb/log_device.h"
#include "walb/sector.h"
#include "util.h"
#include "hashtbl.h"
#include "treemap.h"
#include "sector_io.h"
#include "snapshot.h"

/*******************************************************************************
 * Static data.
 *******************************************************************************/

/* All treemap(s) in this module will share a treemap memory manager. */
static atomic_t n_users_of_memory_manager_ = ATOMIC_INIT(0);
static struct treemap_memory_manager mmgr_;
#define TREE_NODE_CACHE_NAME "walb_snap_node_cache"
#define TREE_CELL_HEAD_CACHE_NAME "walb_snap_cell_head_cache"
#define TREE_CELL_CACHE_NAME "walb_snap_cell_cache"
#define N_SNAPSHOT_POOL (32 * 3) /* for snapd->sectors,
				    snapd->id_idx, and snapd->lsid_idx. */

/*******************************************************************************
 * Prototypes of static functions.
 *******************************************************************************/

/* For treemap memory manager. */
static bool treemap_memory_manager_inc(void);
static void treemap_memory_manager_dec(void);

/* Sector operations */
static bool sector_load(struct snapshot_data *snapd, u64 off);
static bool sector_sync(struct snapshot_data *snapd, u64 off);
static bool sector_sync_all(struct snapshot_data *snapd);
static bool sector_evict(struct snapshot_data *snapd, u64 off);
static bool sector_evict_all(struct snapshot_data *snapd);

/* Record allocation. */
static u32 record_alloc(struct snapshot_data *snapd,
			struct walb_snapshot_record **recp);
static bool record_free(struct snapshot_data *snapd, u32 snapshot_id);

/* Issue IO */
static bool sector_read(struct snapshot_data *snapd,
			u64 offset,
			struct sector_data *sect);
static bool sector_write(struct snapshot_data *snapd,
			u64 offset,
			struct sector_data *sect);

/* Get control/id/record functions. */
static struct snapshot_sector_control* get_control_by_offset(
	const struct snapshot_data *snapd, u64 off);
static struct snapshot_sector_control* get_control_by_id(
	const struct snapshot_data *snapd, u32 snapshot_id);
static u32 get_id_by_name(const struct snapshot_data *snapd, const char *name);
UNUSED static struct tree_cell_head* get_id_by_lsid(
	const struct snapshot_data *snapd, u64 lsid);
static struct walb_snapshot_record* get_record_by_id(
	struct snapshot_data *snapd, u32 snapshot_id);
static struct walb_snapshot_record* get_record_by_name(
	struct snapshot_data *snapd, const char *name);

/* Manipultion of primary index. */
static bool insert_snapshot_id(struct snapshot_data *snapd,
			u32 snapshot_id,
			const struct snapshot_sector_control *ctl);
static bool delete_snapshot_id(struct snapshot_data *snapd,
			u32 snapshot_id);

/* Manipulation of secondary indices. */
static bool insert_to_name_idx(
	struct snapshot_data *snapd,
	const struct walb_snapshot_record *rec);
static bool insert_to_lsid_idx(
	struct snapshot_data *snapd,
	const struct walb_snapshot_record *rec);
static bool insert_snapshot_record_to_index(
	struct snapshot_data *snapd,
	const struct walb_snapshot_record *rec);
static bool delete_from_name_idx(
	struct snapshot_data *snapd, const struct walb_snapshot_record *rec);
static bool delete_from_lsid_idx(
	struct snapshot_data *snapd, const struct walb_snapshot_record *rec);
static bool delete_snapshot_record_from_index(
	struct snapshot_data *snapd,
	const struct walb_snapshot_record *rec);

/* Helper functions. */
static bool snapshot_data_load_sector_and_insert(
	struct snapshot_data *snapd, u32 *next_snapshot_id_p,
	struct snapshot_sector_control *ctl,
	struct sector_data *sect);
UNUSED static bool is_all_sectors_free(const struct snapshot_data *snapd);

UNUSED static int get_n_snapshot_in_name_idx(
	const struct snapshot_data *snapd, u32 snapshot_id);
UNUSED static int get_n_snapshot_in_lsid_idx(
	const struct snapshot_data *snapd, u32 snapshot_id);

UNUSED static bool is_valid_snapshot_name_idx(
	const struct snapshot_data *snapd);
UNUSED static bool is_valid_snapshot_lsid_idx(
	const struct snapshot_data *snapd);
UNUSED static bool is_valid_snapshot_id_appearance(
	const struct snapshot_data *snapd);

UNUSED static bool is_sector_loaded(const struct snapshot_sector_control *ctl);

/*******************************************************************************
 * Static functions.
 *******************************************************************************/

/**
 * Increment n_users of treemap memory manager and
 * iniitialize mmgr_ if necessary.
 *
 * RETURN:
 *   true in success, or false.
 */
static bool treemap_memory_manager_inc(void)
{
	bool ret;
	
	if (atomic_inc_return(&n_users_of_memory_manager_) == 1) {
		ret = initialize_treemap_memory_manager(
			&mmgr_, N_SNAPSHOT_POOL,
			TREE_NODE_CACHE_NAME,
			TREE_CELL_HEAD_CACHE_NAME,
			TREE_CELL_CACHE_NAME);
		if (!ret) {
			atomic_dec(&n_users_of_memory_manager_);
			goto error;
		}
	}
	return true;
error:
	return false;
}

/**
 * Decrement n_users of treemap memory manager and
 * finalize mmgr_ if necessary.
 */
static void treemap_memory_manager_dec(void)
{
	if (atomic_dec_return(&n_users_of_memory_manager_) == 0) {
		finalize_treemap_memory_manager(&mmgr_);
	}
}
					
/**
 * Load sector from storage into memory.
 *
 * This function actually issues read IO only if need.
 *
 * @snapd snapshot data.
 * @off sector offset to load [physical block].
 *
 * RETURN:
 *   true in success, or false.
 */
static bool sector_load(struct snapshot_data *snapd, u64 off)
{
	struct snapshot_sector_control *ctl;
	struct sector_data *sect = NULL;
	bool retb;

	ASSERT(snapd);
	ASSERT(snapd->start_offset <= off);
	ASSERT(off < snapd->end_offset);
	
	ctl = get_control_by_offset(snapd, off);
	ASSERT(ctl);

	/* Allocate if need. */
	if (ctl->state == SNAPSHOT_SECTOR_CONTROL_FREE) {
		ASSERT(!ctl->sector);
		sect = sector_alloc(snapd->sector_size, GFP_KERNEL);
		if (!sect) { goto error0; }
		ctl->state = SNAPSHOT_SECTOR_CONTROL_ALLOC;
		ctl->sector = sect;
	}
	ASSERT_SNAPSHOT_SECTOR(ctl->sector);

	/* Read sector if need. */
	if (ctl->state == SNAPSHOT_SECTOR_CONTROL_ALLOC) {
		retb = sector_read(snapd, off, ctl->sector);
		if (!retb) {
			LOGe("Read snapshot sector %"PRIu64" failed.\n", off);
			goto error0;
		}
		ctl->state = SNAPSHOT_SECTOR_CONTROL_CLEAN;
	}
	return true;
	
error0:
	return false;
}

/**
 * Sync down sector from memory to storage.
 *
 * This function actually issues write IO only if need.
 *
 * @snapd snapshot data.
 * @off sector offset to sync.
 *
 * RETURN:
 *   true in success, or false.
 */
static bool sector_sync(struct snapshot_data *snapd, u64 off)
{
	struct snapshot_sector_control *ctl;
	
	ASSERT(snapd);
	ASSERT(snapd->start_offset <= off);
	ASSERT(off < snapd->end_offset);
	
	ctl = get_control_by_offset(snapd, off);
	ASSERT(ctl);

	if (ctl->state == SNAPSHOT_SECTOR_CONTROL_DIRTY) {

		ASSERT_SNAPSHOT_SECTOR(ctl->sector);
		if (!sector_write(snapd, off, ctl->sector)) {
			goto error;
		}
		ctl->state = SNAPSHOT_SECTOR_CONTROL_CLEAN;
	}
	return true;
	
error:
	return false;
}


/**
 * Sync down all sectors.
 *
 * After call this, there is no DIRTY sectors.
 *
 * @snapd snapshot data.
 *
 * RETURN:
 *   true in success, or false.
 */
static bool sector_sync_all(struct snapshot_data *snapd)
{
	bool ret = true;
	u64 off;
	struct snapshot_sector_control *ctl;

	ASSERT(snapd);

	for_each_snapshot_sector(off, ctl, snapd) {
		if (!sector_sync(snapd, off)) {
			ret = false;
		}
	}
	return ret;
}

/**
 * This function evicts sectors with CLEAN mark on the memory.
 *
 * If a sector is marked DIRTY, this function does nothing.
 * Call @sector_sync() before calling this function to evict it.
 *
 * @snapd snapshot data.
 * @off snapshot sector offset.
 *
 * RETURN:
 *   true when the sector state is finally free, or false.
 */
static bool sector_evict(struct snapshot_data *snapd, u64 off)
{
	struct snapshot_sector_control *ctl;

	ASSERT(snapd);
	ASSERT(snapd->start_offset <= off);
	ASSERT(off < snapd->end_offset);

	ctl = get_control_by_offset(snapd, off);
	ASSERT(ctl);
	ASSERT(ctl->offset == off);

	if (ctl->state == SNAPSHOT_SECTOR_CONTROL_CLEAN) {

		ASSERT_SNAPSHOT_SECTOR(ctl->sector);
		sector_free(ctl->sector);
		ctl->sector = NULL;
		ctl->state = SNAPSHOT_SECTOR_CONTROL_FREE;
	}

	return ctl->state == SNAPSHOT_SECTOR_CONTROL_FREE;
}

/**
 * Evict all sectors.
 *
 * After call this, there is no CLEAN sectors.
 *
 * @snapd snapshot data.
 *
 * RETURN:
 *   true if all sectors are free finally, or false.
 */
static bool sector_evict_all(struct snapshot_data *snapd)
{
	u64 off;
	struct snapshot_sector_control *ctl;
	bool ret = true;

	ASSERT(snapd);

	for_each_snapshot_sector(off, ctl, snapd) {
		if (!sector_evict(snapd, off)) {
			ret = false;
		}
	}
	return ret;
}

/**
 * Allocate snapshot record.
 *
 * 1. Search free record.
 * 2. Initialize the record and set allocation bit.
 * 3. Insert to id_idx.
 *
 * Secondary indexes are not modified.
 * You must manage them by yourself.
 *
 * @snapd snapshot data.
 * @recp pointer to allocated record pointer.
 *
 * RETURN:
 *   snapshot_id in success, or INVALID_SNAPSHOT_ID.
 */
static u32 record_alloc(struct snapshot_data *snapd,
			struct walb_snapshot_record **recp)
{
	u64 off;
	struct snapshot_sector_control *ctl = NULL;
	struct walb_snapshot_record *rec;
	int nr;
	bool retb;
	
	ASSERT(snapd);
	ASSERT(recp);

	/* Search sectors with n_free_records > 0. */
	for_each_snapshot_sector(off, ctl, snapd) {
		if (ctl->n_free_records > 0) { break; } /* found */
	}
	if (off == snapd->end_offset) { goto error0; } /* not found */
	ASSERT(ctl->offset == off);

	/* Load the sectors. */
	if (!sector_load(snapd, off)) { goto error0; }
	ASSERT(is_sector_loaded(ctl));
	ASSERT_SNAPSHOT_SECTOR(ctl->sector);

	/* Search free records in the sector. */
	for_each_snapshot_record(nr, rec, ctl->sector) {
		if (!is_alloc_snapshot_record(nr, ctl->sector)) { break; }
	}
	/* n_free_records > 0 so we can found free record. */
	ASSERT(0 <= nr);
	ASSERT(nr < get_max_n_records_in_snapshot_sector(ctl->sector->size));

	/* Set allocation bit. */
	set_alloc_snapshot_record(nr, ctl->sector);

	/* Initialize and asssign snapshot_id. */
	rec = get_snapshot_record_by_idx_in_sector(ctl->sector, nr);
	snapshot_record_init(rec);
	rec->snapshot_id = snapd->next_snapshot_id++;

	/* Change state and decrement n_free_records. */
	ctl->state = SNAPSHOT_SECTOR_CONTROL_DIRTY;
	ctl->n_free_records--;

	/* Insert to primary index. */
	retb = insert_snapshot_id(snapd, rec->snapshot_id, ctl);
	if (!retb) {
		goto error1;
	}
	*recp = rec;
	return rec->snapshot_id;

error1:
	ctl->n_free_records++;
	ASSERT(rec);
	snapshot_record_init(rec);
	clear_alloc_snapshot_record(nr, ctl->sector);
error0:
	return INVALID_SNAPSHOT_ID;
}

/**
 * Free allocated record.
 *
 * 1. Search control and index inside the sector.
 * 2. Delete from id_idx.
 * 3. Initialize the record and clear allocation bit.
 *
 * This function does not delete records from secondary indexes.
 * You must delete from them by yourself.
 *
 * @snapd snapshot data.
 * @snapshot_id snapshot id.
 *
 * RETURN:
 *   true in success, or false.
 */
static bool record_free(struct snapshot_data *snapd, u32 snapshot_id)
{
	struct snapshot_sector_control *ctl;
	struct walb_snapshot_record *rec;
	int idx;
	bool ret;

	/* Get sector control. */
	ctl = get_control_by_id(snapd, snapshot_id);
	if (!ctl) { goto error; }

	/* Load sector. */
	if (!sector_load(snapd, ctl->offset)) { goto error; }
	ASSERT(is_sector_loaded(ctl));
	ASSERT_SNAPSHOT_SECTOR(ctl->sector);

	/* Delete from primary index. */
	ret = delete_snapshot_id(snapd, snapshot_id);
	ASSERT(ret);

	/* Get record index inside snapshot sector. */
	idx = get_idx_of_snapshot_record(ctl->sector, snapshot_id);
	ASSERT(idx >= 0);
	
	/* Clear allocation bit. */
	clear_alloc_snapshot_record(idx, ctl->sector);

	/* Initialize record. */
	rec = get_snapshot_record_by_idx_in_sector(ctl->sector, idx);
	snapshot_record_init(rec);

	/* Change state of control. */
	ctl->state = SNAPSHOT_SECTOR_CONTROL_DIRTY;
	ctl->n_free_records++;

	return true;
	
error:
	return false;
}

/**
 * Read snapshot sector from disk.
 *
 * @snapd snapshot data.
 * @offset offset of snapshot sector.
 * @sect sector data to be stored.
 *
 * RETURN:
 *   true in success, or false.
 */
static bool sector_read(struct snapshot_data *snapd,
			u64 offset,
			struct sector_data *sect)
{
	ASSERT(snapd);
	ASSERT_SECTOR_DATA(sect);
	ASSERT(snapd->start_offset <= offset);
	ASSERT(offset < snapd->end_offset);
	
	/* Issue read IO. */
	if (!sector_io(READ, snapd->bdev, offset, sect)) {
		LOGe("Read snapshot sector %"PRIu64" failed.\n", offset);
		goto error0;
	}
	/* Check checksum. */
	if (checksum((u8 *)sect->data, sect->size) != 0) {
		LOGe("Bad checksum.\n");
		goto error0;
	}
	/* Check validness of record array. */
	if (!is_valid_snapshot_sector(sect)) {
		LOGe("Snapshot sector is not valid.\n");
		goto error0;
	}
	return true;
	
error0:
	return false;
}

/**
 * Write snapshot sector to disk.
 *
 * @snapd snapshot data.
 * @offset offset of snapshot sector.
 * @sect sector data to write.
 *
 * RETURN:
 *   true in success, or false.
 */
static bool sector_write(
	struct snapshot_data *snapd, u64 offset,
	struct sector_data *sect)
{
	struct walb_snapshot_sector *snap_sect;

	ASSERT_SNAPSHOT_SECTOR(sect);
	ASSERT(snapd->start_offset <= offset);
	ASSERT(offset < snapd->end_offset);
	
	/* Calculate checksum. */
	snap_sect = get_snapshot_sector(sect);
	snap_sect->checksum = 0;
	snap_sect->checksum = checksum((u8 *)snap_sect, sect->size);

	/* Issue write IO. */
	if (!sector_io(WRITE, snapd->bdev, offset, sect)) {
		LOGe("Write snapshot sector %"PRIu64" failed.\n", offset);
		goto error0;
	}
	return true;
	
error0:
	return false;
}

/**
 * Get sector control by offset.
 *
 * @snapd snapshot data.
 * @off sector offset.
 *
 * RETURN:
 *   snapshot sector control. Never NULL.
 */
static struct snapshot_sector_control* get_control_by_offset(
	const struct snapshot_data *snapd, u64 off)
{
	unsigned long p;
	struct snapshot_sector_control *ctl;
	
	ASSERT(snapd);
	ASSERT(snapd->start_offset <= off);
	ASSERT(off < snapd->end_offset);

	p = map_lookup(snapd->sectors, off);
	ASSERT(p != TREEMAP_INVALID_VAL);
	ctl = (struct snapshot_sector_control *)p;
	ASSERT(ctl);
	ASSERT(ctl->offset == off);
	
	return ctl;
}

/**
 * Get sector control by snapshot id.
 *
 * @snapd snapshot data.
 * @snapshot_id snapshot id.
 *   DO NOT specify INVALID_SNAPSHOT_ID.
 *
 * RETURN:
 *   snapshot sector control if found, or NULL.
 */
static struct snapshot_sector_control* get_control_by_id(
	const struct snapshot_data *snapd, u32 snapshot_id)
{
	unsigned long p;
	struct snapshot_sector_control *ctl = NULL;

	ASSERT(snapd);
	ASSERT(snapshot_id != INVALID_SNAPSHOT_ID);

	p = map_lookup(snapd->id_idx, snapshot_id);
	if (p != TREEMAP_INVALID_VAL) {
		ctl = (struct snapshot_sector_control *)p;
		ASSERT(ctl);
	}
	return ctl;
}

/**
 * Search snapshot id by name.
 *
 * @snapd snapshot data.
 * @name snapshot name.
 *
 * RETURN:
 *   snapshot id if found, or INVALID_SNAPSHOT_ID.
 */
static u32 get_id_by_name(
	const struct snapshot_data *snapd, const char *name)
{
	int len;
	unsigned long val;
	u32 sid = INVALID_SNAPSHOT_ID;

	len = get_snapshot_name_length(name);
	if (len <= 0) { goto fin; }
	
	val = hashtbl_lookup(snapd->name_idx, name, len);
	if (val != HASHTBL_INVALID_VAL) {
		sid = (u32)val;
	}
fin:
	return sid;
}

/**
 * Search snapshot id by lsid.
 *
 * @snapd snapshot data.
 * @lsid lsid.
 *
 * RETURN:
 *   tree_cell_head if found, or NULL.
 *   You can traverse snapshot_id
 *   by hlist_for_each_entry().
 *   Each entry is of struct tree_cell and
 *   cell->val must be snapshot_id.
 */
static struct tree_cell_head* get_id_by_lsid(
	const struct snapshot_data *snapd, u64 lsid)
{
	ASSERT(snapd);
	
	return multimap_lookup(snapd->lsid_idx, lsid);
}

/**
 * Get snapshot record from snapshot_id.
 *
 * @snapd snapshot data.
 * @snapshot_id snapshot id.
 *
 * RETURN:
 *   Pointer to record if found, or NULL.
 */
static struct walb_snapshot_record* get_record_by_id(
	struct snapshot_data *snapd, u32 snapshot_id)
{
	struct snapshot_sector_control *ctl;
	struct walb_snapshot_record *rec;

	ASSERT(snapd);
	ASSERT(snapshot_id != INVALID_SNAPSHOT_ID);
	
	ctl = get_control_by_id(snapd, snapshot_id);
	if (!ctl) {
		LOGe("snapshot id %"PRIu32" not found.\n", snapshot_id);
		goto error0;
	}

	if (!sector_load(snapd, ctl->offset)) {
		LOGe("snapshot sector %"PRIu64" load failed.\n", ctl->offset);
		goto error0;
	}
	ASSERT(is_sector_loaded(ctl));
	ASSERT(ctl->sector);
	
	rec = get_snapshot_record_in_sector(ctl->sector, snapshot_id);
	if (!rec) {
		LOGe("snapshot record %"PRIu32" not found in the sector %"PRIu64".\n",
			snapshot_id, ctl->offset);
		goto error0;
	}
	return rec;

error0:
	return NULL;
}

/**
 * Get snapshot record from snapshot_id.
 *
 * @snapd snapshot data.
 * @name snapshot name.
 *
 * RETURN:
 *   Pointer to record if found, or NULL.
 */
static struct walb_snapshot_record* get_record_by_name(
	struct snapshot_data *snapd, const char *name)
{
	u32 sid;
	struct walb_snapshot_record *rec;

	ASSERT(snapd);
	ASSERT(is_valid_snapshot_name(name));

	sid = get_id_by_name(snapd, name);
	if (sid == INVALID_SNAPSHOT_ID) { goto error0; }
	rec = get_record_by_id(snapd, sid);
	if (!rec) { goto error0; }
	return rec;
	
error0:
	return NULL;
}

/**
 * Insert into primary index.
 *
 * @snapd snapshot data.
 * @snapshot_id snapshot id.
 * @ctl snapshot control sector.
 *
 * RETURN:
 *   true in success, or false.
 */
static bool insert_snapshot_id(
	struct snapshot_data *snapd, u32 snapshot_id,
	const struct snapshot_sector_control *ctl)
{
	u64 key;
	unsigned long val;
	
	ASSERT(snapd);
	ASSERT(snapshot_id != INVALID_SNAPSHOT_ID);
	ASSERT(ctl);

	key = snapshot_id;
	val = (unsigned long)ctl;
	
	if (map_add(snapd->id_idx, key, val, GFP_KERNEL)) {
		goto error;
	}
	return true;
	
error:
	return false;
}

/**
 * Delete from primary index.
 *
 * @snapd snapshot data.
 * @snapshot_id snapshot id.
 *
 * RETURN:
 *   true in success, or false.
 */
static bool delete_snapshot_id(
	struct snapshot_data *snapd, u32 snapshot_id)
{
	u64 key;
	unsigned long ret;
	
	ASSERT(snapd);
	ASSERT(snapshot_id != INVALID_SNAPSHOT_ID);

	key = snapshot_id;
	ret = map_del(snapd->id_idx, key);
	if (ret == TREEMAP_INVALID_VAL) {
		goto error;
	}
	return true;
	
error:
	return false;
}
			      
/**
 * Insert to the name index.
 *
 * @snapd snapshot data.
 * @rec valid snapshot record.
 *
 * RETURN:
 *   true in success, or false.
 */
static bool insert_to_name_idx(
	struct snapshot_data *snapd,
	const struct walb_snapshot_record *rec)
{
	u8 *key;
	int key_size;
	unsigned long val;
	int ret;

	ASSERT(snapd);
	ASSERT(is_valid_snapshot_record(rec));

	/* u32 snapshot_id must be stored in unsigned long? */
	ASSERT(sizeof(u32) <= sizeof(unsigned long));
	
	/* key: snapshot name, val: snapshot_id. */
	key = (u8 *)rec->name;
	key_size = get_snapshot_name_length(rec->name);
	val = (unsigned long)rec->snapshot_id;
	
	ret = hashtbl_add(snapd->name_idx, key, key_size, val, GFP_KERNEL);
	if (ret) {
		LOGe("insert to name_idx failed.\n");
		goto error0;
	}
	return true;
	
error0:
	return false;
}

/**
 * Insert to the lsid index.
 *
 * @snapd snapshot data.
 * @rec valid snapshot record.
 *
 * RETURN:
 *   true in success, or false.
 */
static bool insert_to_lsid_idx(
	struct snapshot_data *snapd,
	const struct walb_snapshot_record *rec)
{
	u64 key;
	unsigned long val;
	int ret;
	
	ASSERT(snapd);
	ASSERT(is_valid_snapshot_record(rec));

	/* u32 snapshot_id must be stored in unsigned long? */
	ASSERT(sizeof(u32) <= sizeof(unsigned long));

	/* key: snapshot lsid, val: snapshot_id. */
	key = rec->lsid;
	val = (unsigned long)rec->snapshot_id;
	
	ret = multimap_add(snapd->lsid_idx, key, val, GFP_KERNEL);
	if (ret) {
		LOGe("insert to lsid_idx failed.\n");
		goto error0;
	}
	return true;
	
error0:
	return false;
}

/**
 * Insert a snapshot record to indices of snapshot data.
 *
 * Index: name_idx, lsid_idx.
 *
 * @snapd snapshot data.
 * @rec valid snapshot record to be inserted.
 *
 * RETURN:
 *   true in success, or false.
 */
static bool insert_snapshot_record_to_index(
	struct snapshot_data *snapd,
	const struct walb_snapshot_record *rec)
{
	bool ret;
	
	ASSERT(snapd);
	ASSERT(is_valid_snapshot_record(rec));

	ret = insert_to_name_idx(snapd, rec);
	if (!ret) { goto error0; }

	ret = insert_to_lsid_idx(snapd, rec);
	if (!ret) { goto error0; }
	
	return true;
error0:	       
	return false;
}

/**
 * Delete from name index.
 *
 * @snapd snapshot.
 * @rec valid snapshot record to be deleted from the index.
 *
 * RETURN:
 *   true in success, or false.
 */
static bool delete_from_name_idx(
	struct snapshot_data *snapd, const struct walb_snapshot_record *rec)
{
	const u8 *key;
	int key_size;
	unsigned long sid;
	
	ASSERT(snapd);
	ASSERT(is_valid_snapshot_record(rec));

	key = rec->name;
	key_size = get_snapshot_name_length(rec->name);
	sid = hashtbl_del(snapd->name_idx, key, key_size);
	if (sid != (unsigned long)rec->snapshot_id) {
		LOGe("delete from name_idx failed.\n");
		goto error0;
	}
	return true;
	
error0:
	return false;
}

/**
 * Delete from lsid index.
 *
 * @snapd snapshot.
 * @rec valid snapshot record to be deleted from the index.
 * 
 * RETURN:
 *   true in success, or false.
 */
static bool delete_from_lsid_idx(
	struct snapshot_data *snapd, const struct walb_snapshot_record *rec)
{
	u64 key;
	unsigned long val, val2;
	
	ASSERT(snapd);
	ASSERT(is_valid_snapshot_record(rec));

	key = rec->lsid;
	val = (unsigned long)rec->snapshot_id;
	val2 = multimap_del(snapd->lsid_idx, key, val);
	if (val != val2) {
		LOGe("delete from lsid_Idx failed.\n");
		goto error0;
	}
	return true;
	
error0:
	return false;
}

/**
 * Delete snapshot record from indices of snapshot data.
 *
 * Index: name_idx, lsid_idx.
 *
 * @snapd snapshot data.
 * @rec valid snapshot record to be deleted from the indexes.
 *
 * RETURN:
 *   true in success, or false.
 */
static bool delete_snapshot_record_from_index(
	struct snapshot_data *snapd,
	const struct walb_snapshot_record *rec)
{
	bool ret;
	
	ASSERT(snapd);
	ASSERT(is_valid_snapshot_record(rec));

	ret = delete_from_name_idx(snapd, rec);
	if (!ret) { goto error0; }

	ret = delete_from_lsid_idx(snapd, rec);
	if (!ret) { goto error0; }

	return true;

error0:
	return false;
}

/**
 * Read all records in a snapshot sector and insert into snapshot data.
 * called by snapshot_data_initialize().
 *
 * @snapd snapshot data.
 * @next_snapshot_id_p pointer to next_snapshot_id.
 * @ctl snapshot sector control.
 * @sect sector data.
 *
 * RETURN:
 *   true in success, or false.
 */
static bool snapshot_data_load_sector_and_insert(
	struct snapshot_data *snapd, u32 *next_snapshot_id_p,
	struct snapshot_sector_control *ctl,
	struct sector_data *sect)
{
	int i;
	struct walb_snapshot_record *rec;
	bool ret;

	ASSERT(snapd);
	ASSERT(next_snapshot_id_p);
	ASSERT_SNAPSHOT_SECTOR(sect);
	
	/* For all records */
	for_each_snapshot_record(i, rec, sect) {
		/* Check allocation bitmap. */
		if (!is_alloc_snapshot_record(i, sect)) {
			continue;
		}
				    
		/* Assign new snapshot_id. */
		rec->snapshot_id = (*next_snapshot_id_p)++;
			
		/* Free invalid record. */
		if (!is_valid_snapshot_record(rec)) {
			LOGw("Invalid snapshot record found (%"PRIu64", %s)."
				" Free it.\n",
				rec->lsid, rec->name);
			snapshot_record_init(rec);
			clear_alloc_snapshot_record(i, sect);
			continue;
		}
		PRINT_D_SNAPSHOT_RECORD(rec);
		
		/* Insert to id_idx. */
		ret = insert_snapshot_id(snapd, rec->snapshot_id, ctl);
		if (!ret) {
			LOGe("insert to primary index failed.\n");
			PRINT_E_SNAPSHOT_RECORD(rec);
			goto error0;
		}
			
		/* Insert to name_idx and lsid_idx. */
		ret = insert_snapshot_record_to_index(snapd, rec);
		if (!ret) {
			LOGe("insert to secondary index failed.\n");
			PRINT_E_SNAPSHOT_RECORD(rec);
			goto error0;
		}
	}
	return true;
	
error0:
	return false;
}

/**
 * Check the state of all sectors is FREE.
 *
 * @snapd snapshot data.
 * 
 * RETURN:
 *   true if all FREE, or false.
 */
static bool is_all_sectors_free(const struct snapshot_data *snapd)
{
	u64 off;
	struct snapshot_sector_control *ctl;
	
	ASSERT(snapd);

	for_each_snapshot_sector(off, ctl, snapd) {
		if (ctl->state != SNAPSHOT_SECTOR_CONTROL_FREE) {
			return false;
		}
	}
	return true;
}

/**
 * Get number of snapshots with a snapshot_id by scanning name_idx.
 * This function is used for test.
 *
 * @snapd snapshot data.
 * @snapshot_id snapshot id.
 *
 * RETURN:
 *   Number of found records.
 *   0 or 1 is valid, of course.
 */
static int get_n_snapshot_in_name_idx(
	const struct snapshot_data *snapd, u32 snapshot_id)
{
	hashtbl_cursor_t curt;
	hashtbl_cursor_t *cur = &curt;
	int count = 0;
	unsigned long val;

	ASSERT(snapshot_id != INVALID_SNAPSHOT_ID);
	
	hashtbl_cursor_init(snapd->name_idx, cur);
	hashtbl_cursor_begin(cur);
	while (hashtbl_cursor_next(cur)) {
		val = hashtbl_cursor_val(cur);
		ASSERT(val != HASHTBL_INVALID_VAL);
		if (snapshot_id == (u32)val) {
			count++;
		}
	}
	ASSERT(hashtbl_cursor_is_end(cur));
	
	return count;
}

/**
 * Get number of snapshots with a snapshot_id by scanning lsid_idx.
 * This function is used for test.
 *
 * @snapd snapshot data.
 * @snaphsot_id snapshot id.
 *
 * RETURN:
 *   Number of found records.
 *   0 or 1 is valid, of course.
 */
static int get_n_snapshot_in_lsid_idx(
	const struct snapshot_data *snapd, u32 snapshot_id)
{
	struct multimap_cursor curt;
	struct multimap_cursor *cur = &curt;
	int count = 0;
	unsigned long val;

	ASSERT(snapshot_id != INVALID_SNAPSHOT_ID);

	multimap_cursor_init(snapd->lsid_idx, cur);
	multimap_cursor_begin(cur);
	while (multimap_cursor_next(cur)) {
		val = multimap_cursor_val(cur);
		ASSERT(val != TREEMAP_INVALID_VAL);
		if (snapshot_id == (u32)val) {
			count++;
		}
	}
	ASSERT(multimap_cursor_is_end(cur));

	return count;
}

/**
 * Check snapshot_id is unique in the name index.
 * This is for test.
 *
 * RETURN:
 *   true if valid, or false.
 */
static bool is_valid_snapshot_name_idx(const struct snapshot_data *snapd)
{
	hashtbl_cursor_t curt;
	hashtbl_cursor_t *cur = &curt;
	int count = 0;
	unsigned long val;
	struct map *smap = NULL; /* map of snapshot_id -> 0. */
	u32 snapshot_id;
	int ret;

	smap = map_create(GFP_KERNEL, &mmgr_);
	if (!smap) {
		LOGe("map_create failed.\n");
		goto error;
	}
	
	hashtbl_cursor_init(snapd->name_idx, cur);
	hashtbl_cursor_begin(cur);
	while (hashtbl_cursor_next(cur)) {
		
		val = hashtbl_cursor_val(cur);
		ASSERT(val != HASHTBL_INVALID_VAL);
		snapshot_id = (u32)val;

		ret = map_add(smap, snapshot_id, 0, GFP_KERNEL);
		if (ret == -EEXIST) {
			count++;
		} else if (ret != 0) {
			LOGe("map_add failed.\n");
			goto error;
		}
	}
	ASSERT(hashtbl_cursor_is_end(cur));
	map_destroy(smap);
	return count == 0;

error:
	if (smap) { map_destroy(smap); }
	return false;
}

/**
 * Check snapshot_id is unique in the lsid index.
 * This is for test.
 *
 * @snapd snapshot data.
 *
 * RETURN:
 *   true if valid, or false.
 */
static bool is_valid_snapshot_lsid_idx(const struct snapshot_data *snapd)
{
	struct multimap_cursor curt;
	struct multimap_cursor *cur = &curt;
	int count = 0;
	unsigned long val;
	struct map *smap = NULL; /* map of snapshot_id -> 0. */
	u32 snapshot_id;
	int ret;

	smap = map_create(GFP_KERNEL, &mmgr_);
	if (!smap) {
		LOGe("map_create failed.\n");
		goto error;
	}
	
	multimap_cursor_init(snapd->lsid_idx, cur);
	multimap_cursor_begin(cur);
	while (multimap_cursor_next(cur)) {
		
		val = multimap_cursor_val(cur);
		ASSERT(val != TREEMAP_INVALID_VAL);
		snapshot_id = (u32)val;

		ret = map_add(smap, snapshot_id, 0, GFP_KERNEL);
		if (ret == -EEXIST) {
			count++;
		} else if (ret != 0) {
			LOGe("map_add failed.\n");
			goto error;
		}
	}
	ASSERT(multimap_cursor_is_end(cur));
	map_destroy(smap);
	return count == 0;

error:
	if (smap) { map_destroy(smap); }
	return false;
}

/**
 * Check property that each snapshot id are stored
 * at most once in name_idx and lsid_idx respectively.
 *
 * @snapd snapshot data.
 *
 * RETURN:
 *   true if valid, or false.
 */
static bool is_valid_snapshot_id_appearance(const struct snapshot_data *snapd)
{
	return (is_valid_snapshot_name_idx(snapd) &&
		is_valid_snapshot_lsid_idx(snapd));
}

/**
 * Check sector loaded or not.
 *
 * RETURN:
 *   true if loaded, or false.
 */
static bool is_sector_loaded(const struct snapshot_sector_control *ctl)
{
	ASSERT(ctl);
	
	return ctl->state == SNAPSHOT_SECTOR_CONTROL_CLEAN ||
		ctl->state == SNAPSHOT_SECTOR_CONTROL_DIRTY;
}

/*******************************************************************************
 * Create/destroy snapshots structure. 
 *******************************************************************************/

/**
 * Create snapshot_data structure. 
 *
 * @bdev Underlying log device.
 * @start_offset Offset of the first snapshot sector.
 * @end_offset Offset of the next of the end snapshot sector.
 *
 * RETURN:
 *   Pointer to created snapshot_data in success, or NULL.
 */
struct snapshot_data* snapshot_data_create(
	struct block_device *bdev, u64 start_offset, u64 end_offset)
{
	struct snapshot_data *snapd;
	struct snapshot_sector_control *ctl;
	u64 off;

	ASSERT(bdev);
	ASSERT(start_offset < end_offset);

	/* Initialize memory manager if necessary. */
	if (!treemap_memory_manager_inc()) {
		goto error0;
	}

	/* Allocate snapshot data. */
	snapd = kmalloc(sizeof(struct snapshot_data), GFP_KERNEL);
	if (!snapd) { goto nomem; }

	/* Initialize snapshot data. */
	init_rwsem(&snapd->lock);
	snapd->start_offset = start_offset;
	snapd->end_offset = end_offset;
	snapd->bdev = bdev;
	snapd->sector_size = bdev_physical_block_size(bdev);

	/* Create sector controls. */
	snapd->sectors = map_create(GFP_KERNEL, &mmgr_);
	if (!snapd->sectors) { goto nomem; }

	/* Allocate each snapshot sector control data. */
	for (off = start_offset; off < end_offset; off++) {
		ctl = kmalloc(sizeof(struct snapshot_sector_control), GFP_KERNEL);
		if (!ctl) { goto nomem; }
		ctl->offset = off;
		ctl->state = SNAPSHOT_SECTOR_CONTROL_FREE;
		ctl->n_free_records = -1; /* Invalid value. */
		ctl->sector = NULL;

		if (map_add(snapd->sectors, off, (unsigned long)ctl, GFP_KERNEL)) {
			kfree(ctl);
			goto nomem;
		}
	}

	/* Create primary/secondary indexes. */
	snapd->id_idx = map_create(GFP_KERNEL, &mmgr_);
	if (!snapd->id_idx) { goto nomem; }
	snapd->name_idx = hashtbl_create(HASHTBL_MAX_BUCKET_SIZE, GFP_KERNEL);
	if (!snapd->name_idx) { goto nomem; }
	snapd->lsid_idx = multimap_create(GFP_KERNEL, &mmgr_);
	if (!snapd->lsid_idx) { goto nomem; }
	
	return snapd;

nomem:
	snapshot_data_destroy(snapd);
error0:
	return NULL;
}

/**
 * Destroy snapshot_data structure.
 *
 * @snapd snapshot data to destroy.
 *
 * Call snapshot_data_finalize() to sync down and free sectors
 * before calling this.
 */
void snapshot_data_destroy(struct snapshot_data *snapd)
{
	struct snapshot_sector_control *ctl;
	struct map_cursor curt;

	if (!snapd) { return; }

	/* Deallocate Indexes. */
	if (snapd->lsid_idx) {
		multimap_destroy(snapd->lsid_idx);
		snapd->lsid_idx = NULL;
	}
	if (snapd->name_idx) {
		hashtbl_destroy(snapd->name_idx);
		snapd->name_idx = NULL;
	}
	if (snapd->id_idx) {
		map_destroy(snapd->id_idx);
		snapd->id_idx = NULL;
	}

	if (snapd->sectors) {
		/* Deallocate all snapshot sector control data. */
		map_cursor_init(snapd->sectors, &curt);
		map_cursor_search(&curt, 0, MAP_SEARCH_BEGIN);
		while (map_cursor_next(&curt)) {
			ctl = (struct snapshot_sector_control *)
				map_cursor_val(&curt);
			ASSERT(ctl && (unsigned long)ctl != TREEMAP_INVALID_VAL);
			ASSERT(!ctl->sector);
			ASSERT(ctl->state == SNAPSHOT_SECTOR_CONTROL_FREE);
			kfree(ctl);
		}
		ASSERT(map_cursor_is_end(&curt));

		/* Deallocate sectors data. */
		map_destroy(snapd->sectors);
		snapd->sectors = NULL;
	}

	/* Deallocate snapshot_data. */
	kfree(snapd);

	/* Finalize memory manager if necessary. */
	treemap_memory_manager_dec();
}

/**
 * Scan all snapshot sectors in the log device
 * then fill required data and indexes.
 *
 * @snapd snapshot data.
 *
 * RETURN:
 *   1 in success, or 0.
 */
int snapshot_data_initialize(struct snapshot_data *snapd)
{
	u64 off;
	struct snapshot_sector_control *ctl;
	struct sector_data *sect;
	u32 next_sid = 0;
	bool ret;

	sect = sector_alloc(snapd->sector_size, GFP_KERNEL);
	if (!sect) { goto error0; }

	/* For all snapshot sectors. */
	for (off = snapd->start_offset; off < snapd->end_offset; off++) {

		/* Get control object. */
		ctl = get_control_by_offset(snapd, off);
		
		/* Load the target sector. */
		ret = sector_read(snapd, off, sect);
		if (!ret) {
			LOGe("Read snapshot sector %"PRIu64" failed.\n", off);
			goto error1;
		}

		/* Check validness,
		   assign next_snapshot_id,
		   and insert into indices. */
		ret = snapshot_data_load_sector_and_insert(
			snapd, &next_sid, ctl, sect);
		if (!ret) { goto error1; }

		/* Calculate n_free_records. */
		ctl->n_free_records = get_n_free_records_in_snapshot_sector(sect);
		
		/* Save sector */
		ret = sector_write(snapd, off, sect);
		if (!ret) {
			LOGe("Write snapshot sector %"PRIu64" failed.\n", off);
			goto error1;
		}

		ASSERT(ctl->state == SNAPSHOT_SECTOR_CONTROL_FREE);
		ASSERT(!ctl->sector);
	}
	ASSERT(is_valid_snapshot_id_appearance(snapd));

	/* Set next_snapshot_id. */
	snapd->next_snapshot_id = next_sid;

	sector_free(sect);
	return 1;

error1:
	sector_free(sect);
error0:
	return 0;
}

/**
 * Sync all snapshot sectors if dirty and free them.
 *
 * You must call this before calling snapshot_data_destroy().
 *
 * @snapd snapshot data.
 * 
 * RETURN:
 *   1 in success, or 0;
 */
int snapshot_data_finalize(struct snapshot_data *snapd)
{
	bool ret;
	
	ASSERT(snapd);
	
	if (!sector_sync_all(snapd)) {
		LOGe("sector_sync_all() failed.\n");
		goto error;
	}
	ret = sector_evict_all(snapd);
	ASSERT(ret);
	ASSERT(is_all_sectors_free(snapd));
	
	return 1;
	
error:
	return 0;
}

/*******************************************************************************
 * Snapshot operations.
 *******************************************************************************/

/**
 * Add snapshot.
 *
 * @snapd snapshot data.
 * @name name of the snapshot.
 *   This must be unique in the walb device.
 * @lsid lsid of the snapshot. 
 * @timestamp timestamp (not used for indexing).
 *
 * RETURN:
 *   0 in success.
 *   -1 the name is already used.
 *   -2 no record space.
 *   -3 other error.
 */
int snapshot_add_nolock(struct snapshot_data *snapd,
			const char *name, u64 lsid, u64 timestamp)
{
	struct walb_snapshot_record *rec;
	u32 snapshot_id;
	bool ret;
#if 0
	struct snapshot_sector_control *ctl;
#endif
	
	/* Check the name is unique. */
	if (get_id_by_name(snapd, name) != INVALID_SNAPSHOT_ID) {
		goto non_unique_name;
	}
	
	/* Allocate record. */
	snapshot_id = record_alloc(snapd, &rec);
	if (snapshot_id == INVALID_SNAPSHOT_ID) { goto nomem; }
	ASSERT(snapshot_id == rec->snapshot_id);

	/* Assign and check record. */
	snapshot_record_assign(rec, name, lsid, timestamp);
	if (!is_valid_snapshot_record(rec)) {
		LOGe("Invalid snapshot record.\n");
		goto error0;
	}

	/* Insert into indices. */
	ret = insert_snapshot_record_to_index(snapd, rec);
	if (!ret) {
		LOGe("Insert into secondary indices failed.\n");
		goto error0;
	}

	/* Currently sync/evict must be executed by the caller. */
#if 0 
	/* Sync and evict all sectors. */
	ctl = get_control_by_id(snapd, snapshot_id);
	ASSERT(ctl);
	ret = sector_sync(snapd, ctl->offset);
	if (!ret) {
		LOGe("Snapshot sector sync failed.\n");
		goto error0;
	}
	ret = sector_evict(snapd, ctl->offset);
	ASSERT(ret);
#endif
	return 0;

error0:
	record_free(snapd, snapshot_id);
	return -3;
nomem:
	return -2;
non_unique_name:
	return -1;
}

/**
 * snapshot_add_nolock() with lock.
 */
int snapshot_add(struct snapshot_data *snapd,
		const char *name, u64 lsid, u64 timestamp)
{
	int ret;
	
	ASSERT(snapd);
	
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
 * RETURN:
 *    0 in success.
 *   -1 if there is no such snapshot record.
 */
int snapshot_del_nolock(struct snapshot_data *snapd, const char *name)
{
	struct walb_snapshot_record *rec;
	struct snapshot_sector_control *ctl;
	int ret;
	u32 sid;
	bool retb;

	ASSERT(snapd);
	ASSERT(is_valid_snapshot_name(name));

	ret = snapshot_get_nolock(snapd, name, &rec);
	if (!ret) { goto error_not_found; }
	ASSERT(is_valid_snapshot_record(rec));

	sid = rec->snapshot_id;
	ASSERT(sid != INVALID_SNAPSHOT_ID);
	
	retb = delete_snapshot_record_from_index(snapd, rec);
	ASSERT(retb);

	ctl = get_control_by_id(snapd, sid);
	ASSERT(ctl);
	
	retb = record_free(snapd, sid);
	ASSERT(retb);

	/* Currently sync/evict must be executed by caller. */
#if 0
	retb = sector_sync(snapd, ctl->offset);
	if (!retb) { goto error; }
	retb = sector_evict(snapd, ctl->offset);
	ASSERT(retb);
#endif
	
	return 0;
	
error_not_found:
	return -1;
}

/**
 * snapshot_del_nolock() with lock.
 */
int snapshot_del(struct snapshot_data *snapd, const char *name)
{
	int ret;

	ASSERT(snapd);
	
	snapshot_write_lock(snapd);
	ret = snapshot_del_nolock(snapd, name);
	snapshot_write_unlock(snapd);
	return ret;
}

/**
 * Delete snapshots with a lsid range.
 *
 * @lsid0 start of the range.
 * @lsid1 end of the range.
 *	  lsid0 <= lsid < lsid1.
 *
 * RETURN:
 *   number of deleted snapshots in success (>= 0), or -1.
 */
int snapshot_del_range_nolock(struct snapshot_data *snapd, u64 lsid0, u64 lsid1)
{
	u64 lsid;
	u32 sid;
	struct walb_snapshot_record *rec;
	bool retb;
	int ret;
	int n_rec = 0;
	struct multimap_cursor cur;
	
	ASSERT(snapd);
	ASSERT(lsid0 < lsid1);
	ASSERT(lsid1 != INVALID_LSID);
	
	multimap_cursor_init(snapd->lsid_idx, &cur);
	ret = multimap_cursor_search(&cur, lsid0, MAP_SEARCH_GE, 0);
	while (ret && multimap_cursor_key(&cur) < lsid1) {
		/* Get the record. */
		lsid = multimap_cursor_key(&cur);
		ASSERT(lsid != INVALID_LSID);
		sid = (u32)multimap_cursor_val(&cur);
		ASSERT(sid != INVALID_SNAPSHOT_ID);
		rec = get_record_by_id(snapd, sid);
		ASSERT(is_valid_snapshot_record(rec));
		ASSERT(rec->snapshot_id == sid);
		
		/* Delete from the name_idx. */
		retb = delete_from_name_idx(snapd, rec);
		ASSERT(retb);
		
		/* Deallocate. */
		retb = record_free(snapd, sid);
		ASSERT(retb);

		/* Delete from the lsid_idx and iterate. */
		ret = multimap_cursor_del(&cur);
		ASSERT(ret);
		n_rec++;

		/* The cursor indicates the next record. */
		ret = multimap_cursor_is_data(&cur);
	}
	retb = sector_sync_all(snapd);
	if (!retb) { goto error0; }
	return n_rec;

error0:
	return -1;
}

/**
 * snapshot_del_reage_nolock() with lock.
 */
int snapshot_del_range(struct snapshot_data *snapd, u64 lsid0, u64 lsid1)
{
	int ret;

	snapshot_write_lock(snapd);
	ret = snapshot_del_range_nolock(snapd, lsid0, lsid1);
	snapshot_write_unlock(snapd);
	return ret;
}

/**
 * Get snapshot record with a name.
 *
 * @snapd snapshot data.
 * @name snapshot name.
 * @record out: pointer to snapshot record with the name.
 *
 * @return 1 in success, or 0.
 */
int snapshot_get_nolock(
	struct snapshot_data *snapd, const char *name,
	struct walb_snapshot_record **recp)
{
	struct walb_snapshot_record *rec;

	ASSERT(snapd);
	ASSERT(is_valid_snapshot_name(name));
	ASSERT(recp);

	rec = get_record_by_name(snapd, name);
	if (!rec) { goto error0; }
	*recp = rec;
	return 1;
	
error0:
	return 0;
}

/**
 * snapshot_get_nolock() with the lock.
 */
int snapshot_get(struct snapshot_data *snapd, const char *name,
		struct walb_snapshot_record **recp)
{
	int ret;
	
	snapshot_read_lock(snapd);
	ret = snapshot_get_nolock(snapd, name, recp);
	snapshot_read_unlock(snapd);
	return ret;
}

/**
 * Get number of records in a lsid range.
 *
 * @snapd snapshot data.
 * @lsid0 start of the range.
 * @lsid1 end of the range.
 *	  lsid0 <= lsid < lsid1.
 *
 * RETURN:
 *   number of records.
 */
int snapshot_n_records_range_nolock(
	struct snapshot_data *snapd, u64 lsid0, u64 lsid1)
{
	u64 lsid;
	u32 sid;
	struct walb_snapshot_record *rec;
	int ret;
	int n_rec = 0;
	struct multimap_cursor cur;
	
	ASSERT(snapd);
	ASSERT(lsid0 < lsid1);
	ASSERT(lsid1 != INVALID_LSID);

	multimap_cursor_init(snapd->lsid_idx, &cur);
	ret = multimap_cursor_search(&cur, lsid0, MAP_SEARCH_GE, 0);
	while (ret && multimap_cursor_key(&cur) < lsid1) {
		/* Get the record. */
		lsid = multimap_cursor_key(&cur);
		ASSERT(lsid != INVALID_LSID);
		sid = (u32)multimap_cursor_val(&cur);
		ASSERT(sid != INVALID_SNAPSHOT_ID);
		rec = get_record_by_id(snapd, sid);
		ASSERT(is_valid_snapshot_record(rec));

		/* Iterate. */
		n_rec++;
		ret = multimap_cursor_next(&cur);
	}
	return n_rec;
}

/**
 * snapshot_n_records_range_nolock() with the lock.
 */
int snapshot_n_records_range(
	struct snapshot_data *snapd, u64 lsid0, u64 lsid1)
{
	int n;

	snapshot_read_lock(snapd);
	n = snapshot_n_records_range(snapd, lsid0, lsid1);
	snapshot_read_unlock(snapd);
	return n;
}

/**
 * snapshot_n_records_range over full range.
 */
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
 * @buf buffer size [walb_snapshot_record]
 * @lsid0 start of the range.
 * @lsid1 end of the range.
 *	  lsid0 <= lsid < lsid1.
 *
 * RETURN:
 *   n records stored to @buf. n >= 0.
 */
int snapshot_list_range_nolock(struct snapshot_data *snapd,
			struct walb_snapshot_record *buf, size_t buf_size,
			u64 lsid0, u64 lsid1)
{
	int idx = 0;
	struct multimap_cursor cur;
	int ret;
	u32 sid;
	struct walb_snapshot_record *rec;

	ASSERT(snapd);
	ASSERT(buf);
	ASSERT(buf_size > 0);
	ASSERT(lsid0 < lsid1);
	ASSERT(lsid1 != INVALID_LSID);
	
	multimap_cursor_init(snapd->lsid_idx, &cur);
	ret = multimap_cursor_search(&cur, lsid0, MAP_SEARCH_GE, 0);
	while (ret && idx < buf_size && multimap_cursor_key(&cur) < lsid1) {
		/* Get the record. */
		sid = (u32)multimap_cursor_val(&cur);
		ASSERT(sid != INVALID_SNAPSHOT_ID);
		rec = get_record_by_id(snapd, sid);
		ASSERT(rec);
		ASSERT(is_valid_snapshot_record(rec));

		/* Copy. */
		memcpy(&buf[idx], rec, sizeof(struct walb_snapshot_record));

		/* Iterate. */
		idx++;
		ret = multimap_cursor_next(&cur);
	}
	return idx;
}

/**
 * snapshot_list_range_nolock() with lock.
 */
int snapshot_list_range(struct snapshot_data *snapd,
			struct walb_snapshot_record *buf, size_t buf_size,
			u64 lsid0, u64 lsid1)
{
	int n_rec;

	snapshot_read_lock(snapd);
	n_rec = snapshot_list_range_nolock(
		snapd, buf, buf_size, lsid0, lsid1);
	snapshot_read_unlock(snapd);
	return n_rec;
}

/**
 * Get all snapshot list.
 */
int snapshot_list(struct snapshot_data *snapd,
		struct walb_snapshot_record *buf, size_t buf_size)
{
	return snapshot_list_range(snapd, buf, buf_size, 0, MAX_LSID + 1);
}

MODULE_LICENSE("Dual BSD/GPL");
