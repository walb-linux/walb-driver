CC=gcc
CFLAGS=-fPIC -Wall -Wextra -I. -I../include \
       -D_FILE_OFFSET_BITS=64 -D_XOPEN_SOURCE=500 -D_GNU_SOURCE
ifeq ($(DEBUG),1)
	CFLAGS+=-DWALB_DEBUG -DDEBUG -g --coverage
	LDFLAGS+=-lgcov
else
	CFLAGS+=-DNDEBUG -O2
endif

BINARIES = walbctl trim test_rw
TEST_BINARIES = \
	test_checksum test_bitmap test_u64bits \
	test_sector test_super test_snapshot test_logpack

binaries: $(BINARIES) $(TEST_BINARIES)

clean:
	rm -f $(BINARIES) $(TEST_BINARIES) *.o
	rm -f *.gcov *.gcda *.gcno # coverage files.
	rm -rf tmp # test files.

HEADER_FILES = \
	../include/walb/bitmap.h \
	../include/walb/inttypes_kernel.h \
	../include/walb/userland.h \
	../include/walb/walb.h \
	../include/walb/ioctl.h \
	../include/walb/log_device.h \
	../include/walb/log_record.h \
	../include/walb/sector.h \
	../include/walb/snapshot.h \
	../include/walb/super.h

walbctl: walbctl.o util.o logpack.o snapshot.o
	$(CC) -o $@ $(CFLAGS) walbctl.o util.o logpack.o snapshot.o

trim: trim.o util.o
	$(CC) -o $@ $(CFLAGS) trim.o util.o

test_checksum: test_checksum.o
	$(CC) -o $@ $(CFLAGS) test_checksum.o

test_bitmap: test_bitmap.o
	$(CC) -o $@ $(CFLAGS) test_bitmap.o

test_u64bits: test_u64bits.o
	$(CC) -o $@ $(CFLAGS) test_u64bits.o

test_rw: test_rw.o util.o
	$(CC) -o $@ $(CFLAGS) test_rw.o util.o

test_snapshot: test_snapshot.o util.o snapshot.o
	$(CC) -o $@ $(CFLAGS) test_snapshot.o util.o snapshot.o

test_sector: test_sector.o util.o
	$(CC) -o $@ $(CFLAGS) test_sector.o util.o

test_super: test_super.o util.o
	$(CC) -o $@ $(CFLAGS) test_super.o util.o

test_logpack: test_logpack.o logpack.o util.o
	$(CC) -o $@ $(CFLAGS) test_logpack.o logpack.o util.o

test_bitmap.o: test_bitmap.c ../include/walb/bitmap.h
test_sector.o: test_sector.c ../include/walb/sector.h util.h
test_super.o: test_super.c ../include/walb/super.h util.h
test_snapshot.o: test_snapshot.c
test_logpack.o: test_logpack.c
util.o: util.c util.h $(HEADER_FILES)
logpack.o: logpack.c logpack.h util.h $(HEADER_FILES)
test_rw.o: test_rw.c util.h random.h $(HEADER_FILES)
walbctl.o: walbctl.c walblog_format.h $(HEADER_FILES)
snapshot.o: snapshot.c snapshot.h util.h $(HEADER_FILES)
trim.o: trim.c util.h $(HEADER_FILES)

.c.o:
	$(CC) -c $< -o $@ $(CFLAGS)


# Test
test: $(TEST_BINARIES)
	./run_unit_test.sh $(TEST_BINARIES)
