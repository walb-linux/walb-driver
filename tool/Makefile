CC=gcc
CFLAGS=-fPIC -Wall -Wextra -I. -I./include -I../include \
       -D_FILE_OFFSET_BITS=64 -D_XOPEN_SOURCE=500 -D_GNU_SOURCE
ifeq ($(DEBUG),1)
	CFLAGS+=-DWALB_DEBUG -DDEBUG -g --coverage
	LDFLAGS+=-lgcov
else
	CFLAGS+=-DNDEBUG -O2
endif

BINARIES = walbctl trim test_rw
TEST_BINARIES = \
	test/test_rbtree test/test_checksum test/test_bitmap test/test_u64bits \
	test/test_sector test/test_super test/test_snapshot test/test_logpack

binaries: $(BINARIES) $(TEST_BINARIES)

clean:
	rm -f $(BINARIES) $(TEST_BINARIES) *.o lib/*.o test/*.o
	rm -f *.gcov *.gcda *.gcno # coverage files.
	rm -rf tmp # test files.

walbctl: walbctl.o util.o logpack.o snapshot.o
	$(CC) -o $@ $(CFLAGS) walbctl.o util.o logpack.o snapshot.o

trim: trim.o util.o
	$(CC) -o $@ $(CFLAGS) trim.o util.o

test_rw: test_rw.o util.o
	$(CC) -o $@ $(CFLAGS) test_rw.o util.o

test/test_checksum: test/test_checksum.o
	$(CC) -o $@ $(CFLAGS) test/test_checksum.o

test/test_bitmap: test/test_bitmap.o
	$(CC) -o $@ $(CFLAGS) test/test_bitmap.o

test/test_u64bits: test/test_u64bits.o
	$(CC) -o $@ $(CFLAGS) test/test_u64bits.o

test/test_snapshot: test/test_snapshot.o util.o snapshot.o
	$(CC) -o $@ $(CFLAGS) test/test_snapshot.o util.o snapshot.o

test/test_sector: test/test_sector.o util.o
	$(CC) -o $@ $(CFLAGS) test/test_sector.o util.o

test/test_super: test/test_super.o util.o
	$(CC) -o $@ $(CFLAGS) test/test_super.o util.o

test/test_logpack: test/test_logpack.o logpack.o util.o
	$(CC) -o $@ $(CFLAGS) test/test_logpack.o logpack.o util.o

test/test_rbtree: test/test_rbtree.o lib/rbtree.o
	$(CC) -o $@ $(CFLAGS) test/test_rbtree.o lib/rbtree.o

SRCS= \
	test/test_rbtree.c \
	test/test_bitmap.c \
	test/test_sector.c \
	test/test_super.c \
	test/test_snapshot.c \
	test/test_logpack.c \
	util.c logpack.c test_rw.c walbctl.c snapshot.c trim.c

.c.o:
	$(CC) -c $< -o $@ $(CFLAGS)

# Test
test: $(TEST_BINARIES)
	./run_unit_test.sh $(TEST_BINARIES)

depend: Makefile
	sed -e '/^# DO NOT DELETE/,$$d' Makefile > Makefile.new
	echo '# DO NOT DELETE' >> Makefile.new
	g++ -MM $(SRCS) $(CFLAGS) >> Makefile.new
	mv Makefile Makefile.org
	mv Makefile.new Makefile

# DO NOT DELETE
test_rbtree.o: test/test_rbtree.c include/rbtree.h
test_bitmap.o: test/test_bitmap.c ../include/walb/bitmap.h \
 ../include/walb/common.h ../include/walb/userland.h
test_sector.o: test/test_sector.c ../include/walb/sector.h \
 ../include/walb/walb.h ../include/walb/common.h \
 ../include/walb/userland.h ../include/walb/disk_name.h \
 ../include/walb/checksum.h random.h check_userland.h \
 ../include/walb/userland.h util.h ../include/walb/walb.h \
 ../include/walb/log_device.h ../include/walb/log_record.h \
 ../include/walb/util.h ../include/walb/super.h ../include/walb/sector.h \
 ../include/walb/block_size.h ../include/walb/snapshot.h \
 ../include/walb/u64bits.h
test_super.o: test/test_super.c util.h check_userland.h \
 ../include/walb/walb.h ../include/walb/common.h \
 ../include/walb/userland.h ../include/walb/disk_name.h \
 ../include/walb/log_device.h ../include/walb/log_record.h \
 ../include/walb/walb.h ../include/walb/util.h ../include/walb/checksum.h \
 ../include/walb/super.h ../include/walb/sector.h \
 ../include/walb/block_size.h ../include/walb/snapshot.h \
 ../include/walb/u64bits.h ../include/walb/super.h
test_snapshot.o: test/test_snapshot.c util.h check_userland.h \
 ../include/walb/walb.h ../include/walb/common.h \
 ../include/walb/userland.h ../include/walb/disk_name.h \
 ../include/walb/log_device.h ../include/walb/log_record.h \
 ../include/walb/walb.h ../include/walb/util.h ../include/walb/checksum.h \
 ../include/walb/super.h ../include/walb/sector.h \
 ../include/walb/block_size.h ../include/walb/snapshot.h \
 ../include/walb/u64bits.h snapshot.h ../include/walb/log_record.h \
 ../include/walb/snapshot.h
test_logpack.o: test/test_logpack.c ../include/walb/block_size.h \
 ../include/walb/common.h ../include/walb/userland.h util.h \
 check_userland.h ../include/walb/walb.h ../include/walb/disk_name.h \
 ../include/walb/log_device.h ../include/walb/log_record.h \
 ../include/walb/walb.h ../include/walb/util.h ../include/walb/checksum.h \
 ../include/walb/super.h ../include/walb/sector.h \
 ../include/walb/block_size.h ../include/walb/snapshot.h \
 ../include/walb/u64bits.h logpack.h ../include/walb/log_record.h \
 ../include/walb/sector.h
util.o: util.c ../include/walb/walb.h ../include/walb/common.h \
 ../include/walb/userland.h ../include/walb/disk_name.h \
 ../include/walb/block_size.h random.h check_userland.h \
 ../include/walb/userland.h util.h ../include/walb/log_device.h \
 ../include/walb/log_record.h ../include/walb/walb.h \
 ../include/walb/util.h ../include/walb/checksum.h \
 ../include/walb/super.h ../include/walb/sector.h \
 ../include/walb/block_size.h ../include/walb/snapshot.h \
 ../include/walb/u64bits.h
logpack.o: logpack.c ../include/walb/block_size.h \
 ../include/walb/common.h ../include/walb/userland.h util.h \
 check_userland.h ../include/walb/walb.h ../include/walb/disk_name.h \
 ../include/walb/log_device.h ../include/walb/log_record.h \
 ../include/walb/walb.h ../include/walb/util.h ../include/walb/checksum.h \
 ../include/walb/super.h ../include/walb/sector.h \
 ../include/walb/block_size.h ../include/walb/snapshot.h \
 ../include/walb/u64bits.h logpack.h ../include/walb/log_record.h \
 ../include/walb/sector.h
test_rw.o: test_rw.c random.h check_userland.h ../include/walb/userland.h \
 util.h ../include/walb/walb.h ../include/walb/common.h \
 ../include/walb/userland.h ../include/walb/disk_name.h \
 ../include/walb/log_device.h ../include/walb/log_record.h \
 ../include/walb/walb.h ../include/walb/util.h ../include/walb/checksum.h \
 ../include/walb/super.h ../include/walb/sector.h \
 ../include/walb/block_size.h ../include/walb/snapshot.h \
 ../include/walb/u64bits.h
walbctl.o: walbctl.c ../include/walb/walb.h ../include/walb/common.h \
 ../include/walb/userland.h ../include/walb/disk_name.h \
 ../include/walb/log_device.h ../include/walb/log_record.h \
 ../include/walb/walb.h ../include/walb/util.h ../include/walb/checksum.h \
 ../include/walb/super.h ../include/walb/sector.h \
 ../include/walb/block_size.h ../include/walb/snapshot.h \
 ../include/walb/u64bits.h ../include/walb/log_record.h \
 ../include/walb/ioctl.h random.h check_userland.h \
 ../include/walb/userland.h util.h logpack.h ../include/walb/sector.h \
 snapshot.h ../include/walb/snapshot.h walblog_format.h
snapshot.o: snapshot.c util.h check_userland.h ../include/walb/walb.h \
 ../include/walb/common.h ../include/walb/userland.h \
 ../include/walb/disk_name.h ../include/walb/log_device.h \
 ../include/walb/log_record.h ../include/walb/walb.h \
 ../include/walb/util.h ../include/walb/checksum.h \
 ../include/walb/super.h ../include/walb/sector.h \
 ../include/walb/block_size.h ../include/walb/snapshot.h \
 ../include/walb/u64bits.h snapshot.h ../include/walb/log_record.h \
 ../include/walb/snapshot.h
trim.o: trim.c ../include/walb/common.h ../include/walb/userland.h util.h \
 check_userland.h ../include/walb/walb.h ../include/walb/common.h \
 ../include/walb/disk_name.h ../include/walb/log_device.h \
 ../include/walb/log_record.h ../include/walb/walb.h \
 ../include/walb/util.h ../include/walb/checksum.h \
 ../include/walb/super.h ../include/walb/sector.h \
 ../include/walb/block_size.h ../include/walb/snapshot.h \
 ../include/walb/u64bits.h
