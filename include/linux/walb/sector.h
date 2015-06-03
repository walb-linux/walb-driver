/**
 * walb_sector.h - Sector operations.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_SECTOR_H
#define WALB_SECTOR_H

#include "walb.h"
#include "checksum.h"

#ifdef __cplusplus
extern "C" {
#endif

/*******************************************************************************
 * Data definitions.
 *******************************************************************************/

/**
 * Sector data in the memory.
 */
struct sector_data
{
	unsigned int size; /* sector size. */
	void *data; /* pointer to buffer. */
};

/**
 * Sector data array in the memory.
 */
struct sector_data_array
{
	unsigned int sector_size;
	unsigned int size; /* array size (number of sectors). */
	struct sector_data **array;
};

/*******************************************************************************
 * Assertions.
 *******************************************************************************/

#define ASSERT_SECTOR_DATA(sect)		\
	ASSERT(is_valid_sector_data(sect))
#define ASSERT_SECTOR_DATA_ARRAY(sect_ary)		\
	ASSERT(is_valid_sector_data_array(sect_ary))

/*******************************************************************************
 * Prototypes.
 *******************************************************************************/

static inline int is_valid_sector_data(const struct sector_data *sect);
#ifdef __KERNEL__
static inline struct sector_data* sector_alloc(unsigned int sector_size, gfp_t gfp_mask);
#else
static inline struct sector_data* sector_alloc(unsigned int sector_size);
static inline struct sector_data* sector_alloc_zero(unsigned int sector_size);
#endif
static inline void sector_free(struct sector_data *sect);
static inline void sector_zeroclear(struct sector_data *sect);
static inline void sector_copy(
	struct sector_data *dst, const struct sector_data *src);
static inline int is_same_size_sector(const struct sector_data *sect0,
				const struct sector_data *sect1);
static inline int is_same_sector(
	const struct sector_data *sect0, const struct sector_data *sect1);

static inline int is_valid_sector_data_array(
	const struct sector_data_array *sect_ary);
#ifdef __KERNEL__
static inline struct sector_data_array* sector_array_alloc(
	unsigned int sector_size, unsigned int n_sectors, gfp_t mask);
static inline int sector_array_realloc(
	struct sector_data_array *sect_ary,
	unsigned int n_sectors, gfp_t mask);
#else
static inline struct sector_data_array* sector_array_alloc(
	unsigned int sector_size, unsigned int n_sectors);
static inline int sector_array_realloc(
	struct sector_data_array *sect_ary, unsigned int n_sectors);
#endif

static inline void sector_array_free(struct sector_data_array *sect_ary);
static inline struct sector_data* get_sector_data_in_array(
	struct sector_data_array *sect_ary, unsigned int idx);
static inline const struct sector_data* get_sector_data_in_array_const(
	const struct sector_data_array *sect_ary, unsigned int idx);

static inline void sector_array_copy_detail(
	struct sector_data_array *sect_ary,
	unsigned int offset, void *data, unsigned int size, int is_from);
static inline void sector_array_copy_from(
	struct sector_data_array *sect_ary,
	unsigned int offset, const void *data, unsigned int size);
static inline void sector_array_copy_to(
	const struct sector_data_array *sect_ary,
	unsigned int offset, void *data, unsigned int size);

static inline int sector_array_compare(const struct sector_data_array *sect_ary0,
				const struct sector_data_array *sect_ary1);
static inline int sector_array_sprint(
	char *str, unsigned int str_size,
	const struct sector_data_array *sect_ary);

/*******************************************************************************
 * Functions for sector data.
 *******************************************************************************/

/**
 * Check sector data is valid.
 *
 * @return Non-zero if valid, or 0.
 */
static inline int is_valid_sector_data(const struct sector_data *sect)
{
	return sect && sect->size > 0 && sect->data;
}

/**
 * Allocate a sector.
 *
 * @sector_size sector size.
 * @flag GFP flag. This is for kernel code only.
 *
 * @return pointer to allocated sector data in success, or NULL.
 */
static inline struct sector_data* sector_alloc(
	unsigned int sector_size
#ifdef __KERNEL__
	, gfp_t gfp_mask)
#else
	)
#endif
{
	struct sector_data *sect;

	ASSERT(sector_size > 0);

	sect = (struct sector_data *)MALLOC(sizeof(struct sector_data), gfp_mask);
	if (!sect) { goto error0; }
	sect->size = sector_size;
	sect->data = AMALLOC(sector_size, sector_size, gfp_mask);
	if (!sect->data) { goto error1; }
	ASSERT_SECTOR_DATA(sect);
	return sect;

error1:
	FREE(sect);
error0:
	return NULL;
}

/**
 * Allocate a sector with zero-filled.
 */
#ifndef __KERNEL__
static inline struct sector_data* sector_alloc_zero(unsigned int sector_size)
{
	struct sector_data *sect;
	sect = sector_alloc(sector_size);
	if (sect) { sector_zeroclear(sect); }
	return sect;
}
#endif

/**
 * Deallocate sector.
 *
 * This must be used for memory allocated with @sector_alloc().
 */
static inline void sector_free(struct sector_data *sect)
{
	if (sect && sect->data) {
		FREE(sect->data);
	}
	FREE(sect);
}

/**
 * Zero-clear sector data.
 */
static inline void sector_zeroclear(struct sector_data *sect)
{
	ASSERT_SECTOR_DATA(sect);
	memset(sect->data, 0, sect->size);
}

/**
 * Copy sector image.
 *
 * @dst destination sector.
 * @src source sector.
 *	dst->size == src->size must be satisfied.
 */
static inline void sector_copy(struct sector_data *dst, const struct sector_data *src)
{
	ASSERT_SECTOR_DATA(dst);
	ASSERT_SECTOR_DATA(src);
	ASSERT(dst->size == src->size);

	memcpy(dst->data, src->data, src->size);
}

/**
 * Check size of both sectors are same or not.
 *
 * @return 1 if same, or 0.
 */
static inline int is_same_size_sector(const struct sector_data *sect0,
				const struct sector_data *sect1)
{
	ASSERT_SECTOR_DATA(sect0);
	ASSERT_SECTOR_DATA(sect1);

	return (sect0->size == sect1->size ? 1 : 0);
}

/**
 * Compare sector image.
 *
 * @sect0 1st sector.
 * @sect1 2nd sector.
 *
 * RETURN:
 *   1 if their size and their image is completely the same,
 *   or 0.
 */
static inline int is_same_sector(
	const struct sector_data *sect0, const struct sector_data *sect1)
{
	ASSERT_SECTOR_DATA(sect0);
	ASSERT_SECTOR_DATA(sect1);

	return is_same_size_sector(sect0, sect1) &&
		memcmp(sect0->data, sect1->data, sect1->size) == 0;
}

/*******************************************************************************
 * Functions for sector data array.
 *******************************************************************************/

/**
 * Check sector data array.
 *
 * @return Non-zero if valid, or 0.
 */
static inline int is_valid_sector_data_array(const struct sector_data_array *sect_ary)
{
	unsigned int i;
	struct sector_data **ary;

	if (!sect_ary) { return 0; }
	if (!sect_ary->array) { return 0; }
	ary = sect_ary->array;
	if (sect_ary->size == 0) { return 0; }

	for (i = 0; i < sect_ary->size; i++) {
		if (!is_valid_sector_data(ary[i])) { return 0; }
		if (ary[i]->size != sect_ary->sector_size) { return 0; }
	}
	return 1;
}

/**
 * Allocate sector data array.
 *
 * @n number of sectors.
 *    You should satisfy n <= PAGE_SIZE / sizeof(void*) inside kernel.
 * @sector_size sector size.
 *
 * @return pointer to allocated sector data array in success, or NULL.
 */
static inline struct sector_data_array* sector_array_alloc(
	unsigned int sector_size, unsigned int n_sectors
#ifdef __KERNEL__
	, gfp_t mask)
#else
	)
#endif
{
	unsigned int i;
	struct sector_data_array *sect_ary;
	struct sector_data *sect;

	ASSERT(n_sectors > 0);
	ASSERT(sector_size > 0);

	/* For itself. */
	sect_ary = (struct sector_data_array *)
		MALLOC(sizeof(struct sector_data_array), mask);
	if (!sect_ary) { goto nomem0; }

	/* For array of sector pointer. */
	sect_ary->size = n_sectors;
	sect_ary->sector_size = sector_size;
	sect_ary->array = (struct sector_data **)
		ZALLOC(sizeof(struct sector_data *) * n_sectors, mask);
	if (!sect_ary->array) { goto nomem1; }

	/* For each sector. */
	for (i = 0; i < n_sectors; i++) {
#ifdef __KERNEL__
		sect = sector_alloc(sector_size, mask);
#else
		sect = sector_alloc(sector_size);
#endif
		if (!sect) { goto nomem1; }
		sect_ary->array[i] = sect;
	}

	return sect_ary;
nomem1:
	sector_array_free(sect_ary);
nomem0:
	return NULL;
}

/**
 * Resize the number of sectors.
 *
 * @sect_ary sector data array.
 * @n_sectors new number of sectors. Must be n_sectors > 0.
 * @mask allocation mask (kernel code only).
 *
 * @return Non-zero in success, or 0.
 */
static inline int sector_array_realloc(
	struct sector_data_array *sect_ary,
	unsigned int n_sectors
#ifdef __KERNEL__
	, gfp_t mask)
#else
	)
#endif
{
	unsigned int i;
	struct sector_data **new_ary;
	unsigned int sector_size;

	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(n_sectors > 0);
	sector_size = sect_ary->sector_size;

	if (sect_ary->size > n_sectors) {
		/* Shrink */
		for (i = n_sectors; i < sect_ary->size; i++) {
			ASSERT(sect_ary->array[i]);
			sector_free(sect_ary->array[i]);
			sect_ary->array[i] = NULL;
		}
		sect_ary->size = n_sectors;

	} else if (sect_ary->size < n_sectors) {
		/* Grow */
		new_ary = (struct sector_data **)REALLOC(sect_ary->array,
				sizeof(struct sector_data *) * n_sectors, mask);
		if (!new_ary) { goto error0; }
		for (i = sect_ary->size; i < n_sectors; i++) {
			new_ary[i] = NULL;
		}
		sect_ary->array = new_ary;
		for (i = sect_ary->size; i < n_sectors; i++) {
#ifdef __KERNEL__
			sect_ary->array[i] = sector_alloc(sector_size, mask);
#else
			sect_ary->array[i] = sector_alloc(sector_size);
#endif
			if (!sect_ary->array[i]) { goto error1; }
		}
		sect_ary->sector_size = sector_size;
		sect_ary->size = n_sectors;
	} else {
		/* Unchanged */
		ASSERT(sect_ary->size == n_sectors);
	}

	return 1;

error1:
	/* Grow failed. */
	ASSERT(sect_ary->size < n_sectors);
	for (i = sect_ary->size; i < n_sectors; i++) {
		sector_free(sect_ary->array[i]);
		sect_ary->array[i] = NULL;
	}
	/* Real size of sect_ary->array is not changed... */
error0:
	return 0;
}

/**
 * Deallocate sector data array.
 *
 * @sect_ary sector data array to deallocate.
 */
static inline void sector_array_free(struct sector_data_array *sect_ary)
{
	unsigned int i;

	if (sect_ary && sect_ary->array) {
		ASSERT_SECTOR_DATA_ARRAY(sect_ary);
		for (i = 0; i < sect_ary->size; i++) {
			sector_free(sect_ary->array[i]);
			sect_ary->array[i] = NULL;
		}
		FREE(sect_ary->array);
		sect_ary->array = NULL;
	}
	FREE(sect_ary);
}

/**
 * Get sector data in sector data array.
 *
 * @sect_ary sector data ary.
 * @idx index in the array.
 *
 * @return pointer to sector data.
 */
static inline struct sector_data* get_sector_data_in_array(
	struct sector_data_array *sect_ary, unsigned int idx)
{
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(idx < sect_ary->size);
	return sect_ary->array[idx];
}

static inline const struct sector_data* get_sector_data_in_array_const(
	const struct sector_data_array *sect_ary, unsigned int idx)
{
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(idx < sect_ary->size);
	return sect_ary->array[idx];
}

/**
 * Copy from/to sector_data_array to/from a memory area.
 *
 * @offset offset from sector array start [byte].
 * @data buffer pointer.
 * @size copy size [byte].
 * @is_from Non-zero for sector_data_array <- memory.
 *	    0 for sector_data_array -> memory.
 */
static inline void sector_array_copy_detail(
	struct sector_data_array *sect_ary,
	unsigned int offset, void *data, unsigned int size, int is_from)
{
	unsigned int sect_size, copied;

	if (!data) { return; }
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);

	sect_size = sect_ary->sector_size;
	ASSERT(offset + size <= sect_ary->size * sect_size);

	copied = 0;
	while (copied < size) {
		unsigned int sect_idx = (offset + copied) / sect_size;
		unsigned int sect_off = (offset + copied) % sect_size;
		unsigned int tmp_size =
			get_min_value(sect_size - sect_off, size - copied);

		if (is_from) {
			memcpy((u8 *)sect_ary->array[sect_idx]->data + sect_off,
				(u8 *)data + copied,
				tmp_size);
		} else {
			memcpy((u8 *)data + copied,
				(u8 *)sect_ary->array[sect_idx]->data + sect_off,
				tmp_size);
		}
		copied += tmp_size;
	}
	ASSERT(copied == size);
}

/**
 * NOT_TESTED_YET
 * Copy sector_data_array from a buffer.
 *
 * @sect_ary sector array.
 * @offset offset in bytes inside sector array.
 * @data source data.
 * @size copy size in bytes.
 */
static inline void sector_array_copy_from(
	struct sector_data_array *sect_ary,
	unsigned int offset, const void *data, unsigned int size)
{
	sector_array_copy_detail(sect_ary, offset, (void *)data, size, 1);
}

/**
 * NOT_TESTED_YET
 * Copy sector_data_array to a buffer.
 *
 * @sect_ary sector array.
 * @offset offset in bytes inside sector array.
 * @data destination data.
 * @size copy size in bytes.
 */
static inline void sector_array_copy_to(
	const struct sector_data_array *sect_ary,
	unsigned int offset, void *data, unsigned int size)
{
	sector_array_copy_detail(
		(struct sector_data_array *)sect_ary, offset, data, size, 0);
}

/**
 * NOT_TESTED_YET
 * Compare two sector_data_array objects.
 *
 * @return 0 when their size and their contents are completely the same.
 */
static inline int sector_array_compare(
	const struct sector_data_array *sect_ary0,
	const struct sector_data_array *sect_ary1)
{
	unsigned int i, sect_size;
	ASSERT_SECTOR_DATA_ARRAY(sect_ary0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary1);
	sect_size = sect_ary0->sector_size;

	if (sect_ary0->size != sect_ary1->size) {
		return sect_ary0->size - sect_ary1->size;
	}

	for (i = 0; i < sect_ary0->size; i++) {
		int cmp = memcmp(sect_ary0->array[i]->data, sect_ary1->array[i]->data, sect_size);
		if (cmp) { return cmp; }
	}
	return 0; /* the same. */
}

/**
 * For debug.
 *
 * @return Non-zero if str_size is enough, or 0.
 */
static inline int sector_array_sprint(
	char *str, unsigned int str_size,
	const struct sector_data_array *sect_ary)
{
	unsigned int i, j;
	char tmp[4];
	unsigned int sect_size;

	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(str);
	ASSERT(str_size > 0);
	sect_size = sect_ary->sector_size;

	str[0] = '\0';
	for (i = 0; i < sect_ary->size; i++) {
		for (j = 0; j < sect_size; j++) {
			if ((i * sect_size + j + 1) * 3 + 1 > str_size) {
				return 0;
			}
			sprintf(tmp, "%02X ", ((u8 *)sect_ary->array[i]->data)[j]);
			strcat(str, tmp);
		}
	}
	return 1;
}

/**
 * Calculate checksum an arbitorary range a inside sector data array.
 *
 * @sect_ary sector data array.
 * @offset offset in bytes.
 * @size size in bytes.
 * @salt checksum salt.
 *
 * RETURN:
 *   calculated checksum.
 */
static inline u32 sector_array_checksum(
	struct sector_data_array *sect_ary,
	unsigned int offset, unsigned int size, u32 salt)
{
	unsigned int remaining = size;
	unsigned int sect_size;
	unsigned int idx, off;
	u32 sum = salt;
	unsigned int tsize;

	ASSERT(size > 0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(sect_ary->size > 0);
	sect_size = sect_ary->sector_size;

	idx = offset / sect_size;
	off = offset % sect_size;
	while (remaining > 0) {
		ASSERT(idx < sect_ary->size);
		tsize = get_min_value(sect_size - off, remaining);
		sum = checksum_partial(
			sum, &((u8 *)sect_ary->array[idx]->data)[off],
			tsize);
		remaining -= tsize;
		idx++;
		off = 0;
	}
	return checksum_finish(sum);
}

/**
 * Memset an arbitotrary range inside a sector data array.
 *
 * @sect_ary sector data array.
 * @offset offset in bytes.
 * @size size in bytes.
 * @val value to fill each byte.
 */
static inline void sector_array_memset(
	struct sector_data_array *sect_ary,
	unsigned int offset, unsigned int size, int val)
{
	unsigned int remaining = size;
	unsigned int ssize;
	unsigned int idx, off;
	unsigned int tsize;

	ASSERT(size > 0);
	ASSERT_SECTOR_DATA_ARRAY(sect_ary);
	ASSERT(sect_ary->size > 0);
	ssize = sect_ary->sector_size;

	idx = offset / ssize;
	off = offset % ssize;
	while (remaining > 0) {
		ASSERT(idx < sect_ary->size);
		tsize = get_min_value(ssize - off, remaining);
		memset(&((u8 *)sect_ary->array[idx]->data)[off], val, tsize);
		remaining -= tsize;
		idx++;
		off = 0;
	}
}

#ifdef __cplusplus
}
#endif

#endif /* WALB_SECTOR_H */
