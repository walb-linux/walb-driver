CC=gcc
CFLAGS=-fPIC -Wall -Wextra -I../include \
       -D_FILE_OFFSET_BITS=64 -D_XOPEN_SOURCE=500 -D_GNU_SOURCE
ifeq ($(DEBUG),1)
	CFLAGS+=-DWALB_DEBUG -DDEBUG -g --coverage
	LDFLAGS+=-lgcov
else
	CFLAGS+=-DNDEBUG -O2
endif

BINARIES = walbctl test_rw
TEST_BINARIES = \
	test_checksum test_bitmap test_u64bits \
	test_sector test_super test_snapshot test_logpack

binaries: $(BINARIES) $(TEST_BINARIES)

clean:
	rm -f $(BINARIES) $(TEST_BINARIES) *.o
	rm -f *.gcov *.gcda *.gcno # coverage files.
	rm -rf tmp # test files.

HEADER_FILES = \
	../include/bitmap.h \
	../include/inttypes_kern.h \
	../include/userland.h \
	../include/walb.h \
	../include/walb_ioctl.h \
	../include/walb_log_device.h \
	../include/walb_log_file.h \
	../include/walb_log_record.h \
	../include/walb_sector.h \
	../include/walb_snapshot.h \
	../include/walb_super.h

walbctl: walbctl.o util.o logpack.o snapshot.o
	$(CC) -o $@ $(CFLAGS) walbctl.o util.o logpack.o snapshot.o

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

test_bitmap.o: test_bitmap.c ../include/bitmap.h
test_sector.o: test_sector.c ../include/walb_sector.h util.h
test_super.o: test_super.c ../include/walb_super.h util.h
test_snapshot.o: test_snapshot.c
test_logpack.o: test_logpack.c
util.o: util.c util.h $(HEADER_FILES)
logpack.o: logpack.c logpack.h util.h $(HEADER_FILES)
test_rw.o: test_rw.c util.h random.h $(HEADER_FILES)
walbctl.o: walbctl.c walblog_format.h $(HEADER_FILES)
snapshot.o: snapshot.c snapshot.h util.h $(HEADER_FILES)

.c.o:
	$(CC) -c $< -o $@ $(CFLAGS)


# Test
test: $(TEST_BINARIES)
	./run_unit_test.sh $(TEST_BINARIES)
