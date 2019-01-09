/**
 * bio_util.h - Definition for bio utilities.
 *
 * @author HOSHINO Takashi <hoshino@labs.cybozu.co.jp>
 */
#ifndef WALB_BIO_UTIL_H_KERNEL
#define WALB_BIO_UTIL_H_KERNEL

#include "check_kernel.h"
#include <linux/types.h>
#include <linux/blkdev.h>
#include <linux/bio.h>
#include <linux/version.h>

#include "linux/walb/common.h"
#include "linux/walb/logger.h"
#include "linux/walb/checksum.h"

#define bio_begin_sector(bio) ((bio)->bi_iter.bi_sector)

static inline bool bvec_iter_is_overlap(
	const struct bvec_iter *iter0, const struct bvec_iter *iter1)
{
	return iter0->bi_sector + (iter0->bi_size >> 9) > iter1->bi_sector &&
		iter1->bi_sector + (iter1->bi_size >> 9) > iter0->bi_sector;
}

/**
 * @bio0
 * @iter0 initial value is treated as a starting position of bio0
 *   and will be set to the overlapped-starting position.
 * @bio1
 * @iter1 initial value is treated as a starting position of bio1
 *   and will be set to the overlapped-starting position.
 * @sectors_p will be set to overlapped size [logical block].
 */
static inline void bio_get_overlapped(
	const struct bio *bio0, struct bvec_iter *iter0,
	const struct bio *bio1, struct bvec_iter *iter1,
	uint *sectors_p)
{
	if (!bvec_iter_is_overlap(iter0, iter1)) {
		*sectors_p = 0;
		return;
	}

	if (iter0->bi_sector < iter1->bi_sector) {
		bio_advance_iter(
			(struct bio *)bio0, iter0,
			(iter1->bi_sector - iter0->bi_sector) << 9);
	} else if (iter0->bi_sector > iter1->bi_sector) {
		bio_advance_iter(
			(struct bio *)bio1, iter1,
			(iter0->bi_sector - iter1->bi_sector) << 9);
	}

	*sectors_p = min(iter0->bi_size, iter1->bi_size) >> 9;
}

/**
 * Calculate checksum.
 *
 * @bio target bio
 * @salt checksum salt.
 *
 * RETURN:
 *   checksum if bio->bi_size > 0, else 0.
 */
static inline u32 bio_calc_checksum_iter(const struct bio *bio, struct bvec_iter iter, u32 salt)
{
	struct bio *biox = (struct bio *)bio;
	struct bio_vec bvec;
	struct bvec_iter iterx;
	u32 sum = salt;

	ASSERT(bio);

	if (iter.bi_size == 0 || bio_op(biox) == REQ_OP_DISCARD)
		return 0;

	__bio_for_each_segment(bvec, biox, iterx, iter) {
		const uint len = bio_iter_len(bio, iterx);
		const uint off = bio_iter_offset(bio, iterx);

		u8 *buf = (u8 *)kmap_atomic(bio_iter_page(bio, iterx));
		sum = checksum_partial(sum, buf + off, len);
		kunmap_atomic(buf);
	}

	return checksum_finish(sum);
}

static inline u32 bio_calc_checksum(const struct bio *bio, u32 salt)
{
	return bio_calc_checksum_iter(bio, bio->bi_iter, salt);
}

#define SNPRINT_BIO_PROCEED(buf, size, w, s) do {			\
		if (s < 0) {						\
			pr_warning("snprint_bio: snprintf failed\n");	\
			return w;					\
		}							\
		buf += (s);						\
		size -= (s);						\
		w += (s);						\
		if (size == 0) {					\
			pr_warning("%s: buffer too small\n", __func__); \
			return w;					\
		}							\
	} while (0)

struct pair_u64_char {
	u64 value;
	const char *name;
};

static inline const char *get_req_op_str(uint op)
{
	uint i;
	const struct pair_u64_char tbl[] = {
		{REQ_OP_READ, "READ"},
		{REQ_OP_WRITE, "WRITE"},
		{REQ_OP_FLUSH, "FLUSH"},
		{REQ_OP_DISCARD, "DISCARD"},
#if LINUX_VERSION_CODE < KERNEL_VERSION(4, 20, 0)
		{REQ_OP_ZONE_REPORT, "ZONE_REPORT"},
#endif
		{REQ_OP_SECURE_ERASE, "SECURE_ERASE"},
		{REQ_OP_ZONE_RESET, "ZONE_RESET"},
		{REQ_OP_WRITE_SAME, "WRITE_SAME"},
		{REQ_OP_WRITE_ZEROES, "WRITE_ZEROES"},

		{REQ_OP_SCSI_IN, "SCSI_IN"},
		{REQ_OP_SCSI_OUT, "SCSI_OUT"},
		{REQ_OP_DRV_IN, "DRV_IN"},
		{REQ_OP_DRV_OUT, "DRV_OUT"},
	};
	for (i = 0; i < sizeof(tbl) / sizeof(tbl[0]); i++) {
		if (op == tbl[i].value)
			return tbl[i].name;
	}
	return "NOT_FOUND";
}

/**
 * Print request flags for debug.
 */
static inline int snprint_bio_flags(
	char *buf, size_t size, const struct bio *bio)
{
	uint i;
	int s, w = 0;
	const struct pair_u64_char tbl[] = {
		{REQ_FAILFAST_DEV, "FAILFAST_DEV"},
		{REQ_FAILFAST_TRANSPORT, "FAILFAST_TRANSPORT"},
		{REQ_FAILFAST_DRIVER, "FAILFAST_DRIVER"},
		{REQ_SYNC, "SYNC"},
		{REQ_META, "META"},
		{REQ_PRIO, "PRIO"},
		{REQ_NOMERGE, "NOMERGE"},
		{REQ_IDLE, "IDLE"},
		{REQ_INTEGRITY, "INTEGRITY"},
		{REQ_FUA, "FUA"},
		{REQ_PREFLUSH, "PREFLUSH"},
		{REQ_RAHEAD, "RAHEAD"},
		{REQ_BACKGROUND, "BACKGROUND"},
		{REQ_NOUNMAP, "NOUNMAP"},
		{REQ_NOWAIT, "NOWAIT"},
	};
	s = snprintf(buf, size, "REQ_OP: %s", get_req_op_str(bio_op(bio)));
	SNPRINT_BIO_PROCEED(buf, size, w, s);

	s = snprintf(buf, size, " REQ_FLAGS:");
	SNPRINT_BIO_PROCEED(buf, size, w, s);
	for (i = 0; i < sizeof(tbl) / sizeof(tbl[0]); i++) {
		if (!(bio->bi_opf & tbl[i].value))
			continue;
		s = snprintf(buf, size, " %s", tbl[i].name);
		SNPRINT_BIO_PROCEED(buf, size, w, s);
	}
	return w;
}

static inline int snprint_bvec_iter(
	char *buf, size_t size, const struct bvec_iter *iter)
{
        return snprintf(
		buf, size,
		"bvec_iter: sector %" PRIu64 " size %u idx %u bvec_done %u\n"
                , (u64)iter->bi_sector
                , iter->bi_size
                , iter->bi_idx
                , iter->bi_bvec_done);
}

static inline int snprint_bio_vec(
	char *buf, size_t size, const struct bio_vec *bv)
{
        return snprintf(
		buf, size,
		"bio_vec: page %p len %u offset %u\n"
                , bv->bv_page
                , bv->bv_len
                , bv->bv_offset);
}

static inline int snprint_bio(char *buf, size_t size, const struct bio *bio)
{
	struct bio *biox = (struct bio *)bio;
        struct bio_vec bv;
        struct bvec_iter iter;
	int s, w = 0;

        s = snprintf(
		buf, size,
		"bio %p\n"
                "  bi_next %p\n"
                "  bi_flags %x\n"
		"  bi_status %u\n"
                "  bi_opf %08x\n"
                "  bi_phys_segments %u\n"
                "  bi_seg_front_size %u\n"
                "  bi_seg_back_size %u\n"
                "  bi_remaining %d\n"
                "  bi_end_io %p\n"
                "  bi_private %p\n"
                "  bi_vcnt %u\n"
                "  bi_max_vecs %u\n"
                "  bi_cnt %d\n"
		"  bdev(%d:%d)\n"
		"  cur "
                , bio
                , bio->bi_next
                , bio->bi_flags
		, bio->bi_status
                , bio->bi_opf
                , bio->bi_phys_segments
                , bio->bi_seg_front_size
                , bio->bi_seg_back_size
                , atomic_read(&bio->__bi_remaining)
                , bio->bi_end_io
                , bio->bi_private
                , bio->bi_vcnt
                , bio->bi_max_vecs
                , atomic_read(&bio->__bi_cnt)
		, MAJOR(bio_dev(bio))
		, MINOR(bio_dev(bio)));
	SNPRINT_BIO_PROCEED(buf, size, w, s);
	s = snprint_bvec_iter(buf, size, &bio->bi_iter);
	SNPRINT_BIO_PROCEED(buf, size, w, s);

	s = snprintf(buf, size, "  ");
	SNPRINT_BIO_PROCEED(buf, size, w, s);
	s = snprint_bio_flags(buf, size, bio);
	SNPRINT_BIO_PROCEED(buf, size, w, s);
	s = snprintf(buf, size, "\n");
	SNPRINT_BIO_PROCEED(buf, size, w, s);

        bio_for_each_segment(bv, biox, iter) {
		s = snprintf(buf, size, "  ");
		SNPRINT_BIO_PROCEED(buf, size, w, s);
		s = snprint_bvec_iter(buf, size, &iter);
		SNPRINT_BIO_PROCEED(buf, size, w, s);
		s = snprintf(buf, size, "  ");
		SNPRINT_BIO_PROCEED(buf, size, w, s);
                s = snprint_bio_vec(buf, size, &bv);
		SNPRINT_BIO_PROCEED(buf, size, w, s);
        }
	return w;
}

static inline void print_bvec_iter(const struct bvec_iter *iter)
{
	char buf[512];
	int w;
	w = snprint_bvec_iter(buf, 512, iter);
	pr_info("%s", buf);
}

static inline void print_bio(const struct bio *bio)
{
	char buf[512];
	int w;
	w = snprint_bio(buf, 512, bio);
	pr_info("%s", buf);
}

static inline void print_bio_short(const char *prefix, const struct bio *bio)
{
	pr_info("%sbio %p pos %" PRIu64 " len %u"
		" bdev(%d:%d) opf %08x\n"
		, prefix, bio, (u64)bio_begin_sector(bio), bio_sectors(bio)
		, MAJOR(bio_dev(bio))
		, MINOR(bio_dev(bio))
		, bio->bi_opf);
}

static inline void print_bio_short_(const char *prefix, const struct bio *bio)
{
}

/**
 * Submit all bio(s) in a bio_list.
 * bio_list will be empty.
 */
static inline void submit_all_bio_list(struct bio_list *bio_list)
{
	struct bio *bio;

	if (bio_list_empty(bio_list))
		return;

	while ((bio = bio_list_pop(bio_list))) {
		print_bio_short_("submit_lr: ", bio);
		generic_make_request(bio);
	}
}

static inline void put_all_bio_list(struct bio_list *bio_list)
{
	struct bio *bio;

	while ((bio = bio_list_pop(bio_list))) {
		LOG_("endio: bio %p pos %" PRIu64 " len %u\n"
			, bio, bio_begin_sector(bio), bio_sectors(bio));
		bio_put(bio);
	}
}

static inline bool bio_has_flush(const struct bio *bio)
{
	if (bio && (bio_op(bio) == REQ_OP_FLUSH || (bio->bi_opf & REQ_PREFLUSH)))
		return true;

	return false;
}

/**
 * Clear REQ_PREFLUSH and REQ_FUA bit of a bio.
 */
static inline void bio_clear_flush_flags(struct bio *bio)
{
	const unsigned int mask = REQ_PREFLUSH | REQ_FUA;
	ASSERT(op_is_write(bio_op(bio)));
	bio->bi_opf &= ~mask;
}

/**
 * Clear REQ_PREFLUSH and REQ_FUA bit of all bios inside bio entry list.
 */
static inline void bio_clear_flush_flags_list(struct bio_list *bio_list)
{
	struct bio *bio;
	bio_list_for_each(bio, bio_list)
		bio_clear_flush_flags(bio);
}

static inline bool should_split_bio_for_chunk(
	struct bio *bio, uint chunk_sectors)
{
	sector_t bgn, last;

	if (chunk_sectors == 0)
		return false;

	bgn = bio_begin_sector(bio);
	last = bio_end_sector(bio) - 1;
	do_div(bgn, chunk_sectors);
	do_div(last, chunk_sectors);
	return bgn != last;
}

static inline bool split_bio_for_chunk(
	struct bio_list *bio_list, struct bio *bio,
	uint chunk_sectors, gfp_t gfp_mask)
{
	if (!bio_has_data(bio)) {
		bio_list_add(bio_list, bio);
		return true;
	}

	while (should_split_bio_for_chunk(bio, chunk_sectors)) {
		/* bio AAABBB --(split)--> new_bio AAA bio BBB. */
		struct bio *split;
		sector_t bgn = bio_begin_sector(bio);
		const int sectors = chunk_sectors - do_div(bgn, chunk_sectors);
#if LINUX_VERSION_CODE < KERNEL_VERSION(4, 18, 0)
		split = bio_split(bio, sectors, gfp_mask, fs_bio_set);
#else
		split = bio_split(bio, sectors, gfp_mask, &fs_bio_set);
#endif
		if (!split)
			return false;

		bio_chain(split, bio);
		bio_list_add(bio_list, split);
	}
	bio_list_add(bio_list, bio);
	return true;
}

/**
 * Do not call this in atomic context.
 */
static inline struct bio_list split_bio_for_chunk_never_giveup(
	struct bio *bio, uint chunk_sectors, gfp_t gfp_mask)
{
	struct bio_list bio_list;
	bio_list_init(&bio_list);
	while (!split_bio_for_chunk(&bio_list, bio, chunk_sectors, gfp_mask))
		schedule();

	return bio_list;
}

/**
 * Copy bio data partially.
 * This does not use dst_bio->bi_iter and src_bio->bi_iter.
 *
 * @dst_bio written bio.
 * @src_bio read bio.
 * @dst_iter start iterator of the dst_bio.
 * @src_iter start iterator of the src_bio.
 * @sectors copy size [logical block].
 *
 * RETURN:
 *   copied size [logical block].
 */
static inline uint bio_copy_data_partial(
	struct bio *dst_bio, struct bvec_iter dst_iter,
	struct bio *src_bio, struct bvec_iter src_iter, uint sectors)
{
	uint remaining = sectors << 9;

	while (remaining > 0 && src_iter.bi_size && dst_iter.bi_size) {
		struct page *src_page, *dst_page;
		u8 *src_p, *dst_p;
		uint src_off, dst_off, src_len, dst_len, bytes;

		src_off = bio_iter_offset(src_bio, src_iter);
		dst_off = bio_iter_offset(dst_bio, dst_iter);
		src_len = bio_iter_len(src_bio, src_iter);
		dst_len = bio_iter_len(dst_bio, dst_iter);
		src_page = bio_iter_page(src_bio, src_iter);
		dst_page = bio_iter_page(dst_bio, dst_iter);
		bytes = min3(src_len, dst_len, remaining);

		src_p = (u8 *)kmap_atomic(src_page);
		dst_p = (u8 *)kmap_atomic(dst_page);
		memcpy(dst_p + dst_off, src_p + src_off, bytes);
		kunmap_atomic(dst_p);
		kunmap_atomic(src_p);

		bio_advance_iter(src_bio, &src_iter, bytes);
		bio_advance_iter(dst_bio, &dst_iter, bytes);
		remaining -= bytes;
	}

	return sectors - (remaining >> 9);
}

#define bio_list_for_each_safe(bio, n, bl)				\
	for (bio = (bl)->head, n = (bio ? bio->bi_next : NULL);		\
	     bio; bio = n, n = (n ? n->bi_next : NULL))

/**
 * BEFORE:
 *   prev -> next
 * AFTER:
 *   prev -> bio -> next
 */
static inline void bio_list_insert(
	struct bio_list *bl, struct bio *bio, struct bio *prev)
{
	struct bio *next;
	ASSERT(bio);

	if (bio_list_empty(bl)) {
		ASSERT(!prev);
		bio_list_add(bl, bio);
		return;
	}
	if (!prev) {
		bio_list_add_head(bl, bio);
		return;
	}

	next = prev->bi_next;
	if (next) {
		ASSERT(bl->tail != prev);
		bio->bi_next = next;
		prev->bi_next = bio;
	} else {
		ASSERT(bl->tail == prev);
		bio->bi_next = NULL;
		prev->bi_next = bio;
		bl->tail = bio;
	}
}

/**
 * BEFORE:
 *   prev -> bio -> next
 * AFTER:
 *   prev -> next
 */
static inline void bio_list_del(
	struct bio_list *bl, struct bio *bio, struct bio *prev)
{
	ASSERT(bio);

	if (bl->head == bio && bl->tail == bio) {
		bio_list_get(bl);
		return;
	}
	if (bl->head == bio) {
		ASSERT(!prev);
		ASSERT(bio->bi_next);
		bl->head = bio->bi_next;
		bio->bi_next = NULL;
		return;
	}
	if (bl->tail == bio) {
		ASSERT(prev);
		ASSERT(!bio->bi_next);
		prev->bi_next = NULL;
		bl->tail = prev;
		return;
	}

	ASSERT(prev);
	ASSERT(bio->bi_next);
	prev->bi_next = bio->bi_next;
	bio->bi_next = NULL;
}

static inline bool bio_private_lsb_get(const struct bio *bio)
{
	const ulong mask = 1;
	return ((ulong)bio->bi_private & mask) != 0;
}

static inline void bio_private_lsb_set(struct bio *bio)
{
	const ulong mask = 1;
	*(ulong *)(&bio->bi_private) |= mask;
}

static inline void bio_private_lsb_clear(struct bio *bio)
{
	const ulong mask = 1;
	*(ulong *)(&bio->bi_private) &= ~mask;
}


#endif /* WALB_BIO_UTIL_H_KERNEL */
