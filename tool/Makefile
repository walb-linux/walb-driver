CC=gcc
CFLAGS=-fPIC -Wall -Wextra -I../include \
       -D_FILE_OFFSET_BITS=64 -D_XOPEN_SOURCE=500 -D_GNU_SOURCE
ifeq ($(DEBUG),1)
	CFLAGS+=-DDEBUG -g
else
	CFLAGS+=-DNDEBUG -O2
endif

BINARIES = walbctl test_checksum test_bitmap

binaries: $(BINARIES)

clean:
	rm -f $(BINARIES) *.o

HEADER_FILES = ../include/walb.h \
	../include/walb_log_record.h \
	../include/walb_log_device.h \
	../include/userland.h

walbctl: walbctl.o util.o
	$(CC) -o $@ $(CFLAGS) walbctl.o util.o

test_checksum: test_checksum.o
	$(CC) -o $@ $(CFLAGS) test_checksum.o

test_bitmap: test_bitmap.o
	$(CC) -o $@ $(CFLAGS) test_bitmap.o

test_bitmap.o: test_bitmap.c ../include/bitmap.h
util.o: util.c util.h

.c.o:
	$(CC) -c $< -o $@ $(CFLAGS)
