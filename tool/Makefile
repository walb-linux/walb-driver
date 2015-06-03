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
	test/test_rbtree test/test_checksum test/test_u64bits \
	test/test_sector test/test_super test/test_logpack

binaries: version_h $(BINARIES) $(TEST_BINARIES)

clean: clean_version_h
	rm -f $(BINARIES) $(TEST_BINARIES) *.o lib/*.o test/*.o
	rm -f *.gcov *.gcda *.gcno # coverage files.
	rm -rf tmp # test files.

rebuild:
	$(MAKE) clean
	$(MAKE) binaries

WALBCTL_OBJS = walbctl.o util.o walb_util.o logpack.o
walbctl: $(WALBCTL_OBJS)
	$(CC) -o $@ $(CFLAGS) $(WALBCTL_OBJS)

trim: trim.o util.o
	$(CC) -o $@ $(CFLAGS) trim.o util.o

test_rw: test_rw.o util.o
	$(CC) -o $@ $(CFLAGS) test_rw.o util.o

test/test_checksum: test/test_checksum.o
	$(CC) -o $@ $(CFLAGS) test/test_checksum.o

test/test_u64bits: test/test_u64bits.o
	$(CC) -o $@ $(CFLAGS) test/test_u64bits.o

test/test_sector: test/test_sector.o util.o walb_util.o
	$(CC) -o $@ $(CFLAGS) test/test_sector.o util.o walb_util.o

test/test_super: test/test_super.o util.o walb_util.o
	$(CC) -o $@ $(CFLAGS) test/test_super.o util.o walb_util.o

test/test_logpack: test/test_logpack.o logpack.o util.o walb_util.o
	$(CC) -o $@ $(CFLAGS) test/test_logpack.o logpack.o util.o walb_util.o

test/test_rbtree: test/test_rbtree.o lib/rbtree.o
	$(CC) -o $@ $(CFLAGS) test/test_rbtree.o lib/rbtree.o

SRCS= \
	test/test_rbtree.c \
	test/test_sector.c \
	test/test_super.c \
	test/test_logpack.c \
	util.c logpack.c test_rw.c walbctl.c trim.c

.c.o:
	$(CC) -c $< -o $@ $(CFLAGS)

version_h:
	$(shell ../make-ver.sh > version.h)

clean_version_h:
	rm -f version.h

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
test_sector.o: test/test_sector.c ../include/linux/walb/sector.h \
 ../include/linux/walb/walb.h ../include/linux/walb/common.h \
 ../include/linux/walb/userland.h ../include/linux/walb/disk_name.h \
 ../include/linux/walb/checksum.h random.h check_userland.h \
 ../include/linux/walb/userland.h util.h ../include/linux/walb/common.h \
 walb_util.h ../include/linux/walb/walb.h \
 ../include/linux/walb/log_device.h ../include/linux/walb/log_record.h \
 ../include/linux/walb/check.h ../include/linux/walb/util.h \
 ../include/linux/walb/u32bits.h ../include/linux/walb/super.h \
 ../include/linux/walb/sector.h ../include/linux/walb/block_size.h \
 ../include/linux/walb/print.h
test_super.o: test/test_super.c util.h ../include/linux/walb/common.h \
 ../include/linux/walb/userland.h walb_util.h check_userland.h \
 ../include/linux/walb/walb.h ../include/linux/walb/common.h \
 ../include/linux/walb/disk_name.h ../include/linux/walb/log_device.h \
 ../include/linux/walb/log_record.h ../include/linux/walb/walb.h \
 ../include/linux/walb/check.h ../include/linux/walb/util.h \
 ../include/linux/walb/u32bits.h ../include/linux/walb/checksum.h \
 ../include/linux/walb/super.h ../include/linux/walb/sector.h \
 ../include/linux/walb/block_size.h ../include/linux/walb/super.h
test_logpack.o: test/test_logpack.c ../include/linux/walb/block_size.h \
 ../include/linux/walb/common.h ../include/linux/walb/userland.h util.h \
 ../include/linux/walb/common.h walb_util.h check_userland.h \
 ../include/linux/walb/walb.h ../include/linux/walb/disk_name.h \
 ../include/linux/walb/log_device.h ../include/linux/walb/log_record.h \
 ../include/linux/walb/walb.h ../include/linux/walb/check.h \
 ../include/linux/walb/util.h ../include/linux/walb/u32bits.h \
 ../include/linux/walb/checksum.h ../include/linux/walb/super.h \
 ../include/linux/walb/sector.h ../include/linux/walb/block_size.h \
 logpack.h ../include/linux/walb/log_record.h
util.o: util.c ../include/linux/walb/common.h \
 ../include/linux/walb/userland.h ../include/linux/walb/logger.h \
 ../include/linux/walb/print.h ../include/linux/walb/common.h \
 ../include/linux/walb/block_size.h ../include/linux/walb/util.h random.h \
 check_userland.h ../include/linux/walb/userland.h util.h
logpack.o: logpack.c ../include/linux/walb/block_size.h \
 ../include/linux/walb/common.h ../include/linux/walb/userland.h \
 ../include/linux/walb/logger.h ../include/linux/walb/print.h util.h \
 ../include/linux/walb/common.h walb_util.h check_userland.h \
 ../include/linux/walb/walb.h ../include/linux/walb/disk_name.h \
 ../include/linux/walb/log_device.h ../include/linux/walb/log_record.h \
 ../include/linux/walb/walb.h ../include/linux/walb/check.h \
 ../include/linux/walb/util.h ../include/linux/walb/u32bits.h \
 ../include/linux/walb/checksum.h ../include/linux/walb/super.h \
 ../include/linux/walb/sector.h ../include/linux/walb/block_size.h \
 logpack.h ../include/linux/walb/log_record.h
test_rw.o: test_rw.c random.h check_userland.h \
 ../include/linux/walb/userland.h util.h ../include/linux/walb/common.h \
 ../include/linux/walb/userland.h
walbctl.o: walbctl.c ../include/linux/walb/walb.h \
 ../include/linux/walb/common.h ../include/linux/walb/userland.h \
 ../include/linux/walb/disk_name.h ../include/linux/walb/logger.h \
 ../include/linux/walb/print.h ../include/linux/walb/log_device.h \
 ../include/linux/walb/log_record.h ../include/linux/walb/walb.h \
 ../include/linux/walb/check.h ../include/linux/walb/util.h \
 ../include/linux/walb/u32bits.h ../include/linux/walb/checksum.h \
 ../include/linux/walb/super.h ../include/linux/walb/sector.h \
 ../include/linux/walb/block_size.h ../include/linux/walb/log_record.h \
 ../include/linux/walb/ioctl.h random.h check_userland.h \
 ../include/linux/walb/userland.h util.h ../include/linux/walb/common.h \
 walb_util.h logpack.h walb_log.h version.h
trim.o: trim.c ../include/linux/walb/common.h \
 ../include/linux/walb/userland.h ../include/linux/walb/logger.h \
 ../include/linux/walb/print.h ../include/linux/walb/common.h util.h
