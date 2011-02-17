CC=gcc
CFLAGS=-fPIC -Wall -Wextra -I../include \
       -D_FILE_OFFSET_BITS=64 -D_XOPEN_SOURCE=500 -D_GNU_SOURCE
ifeq ($(DEBUG),1)
	CFLAGS+=-DWALB_DEBUG -DDEBUG -g
else
	CFLAGS+=-DNDEBUG -O2
endif

BINARIES = walbctl test_checksum test_bitmap test_u64bits test_rw

binaries: $(BINARIES)

clean:
	rm -f $(BINARIES) *.o

HEADER_FILES = ../include/walb.h \
	../include/walb_log_record.h \
	../include/walb_log_device.h \
	../include/walb_super.h \
	../include/walb_snapshot.h \
	../include/userland.h

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
	$(CC) -o $@ $(CFLAGS) test_snapshot_o util.o snapshot.o


test_bitmap.o: test_bitmap.c ../include/bitmap.h
test_snapshot.o: test_snapshot.c
util.o: util.c util.h $(HEADER_FILES)
logpack.o: logpack.c logpack.h util.h $(HEADER_FILES)
test_rw.o: test_rw.c util.h random.h
walbctl.o: walbctl.c walblog_format.h
snapshot.o: snapshot.c snapshot.h util.h $(HEADER_FILES)


.c.o:
	$(CC) -c $< -o $@ $(CFLAGS)
