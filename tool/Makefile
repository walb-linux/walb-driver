

BINARIES = walbctl test_checksum 

binaries: $(BINARIES)

clean:
	rm -f $(BINARIES) *.o

HEADER_FILES = ../include/walb.h \
	../include/walb_log_record.h \
	../include/walb_log_device.h \
	../include/userland.h

walbctl: walbctl.c $(HEADER_FILES)
	$(CC) -Wall -o $@ walbctl.c

test_checksum: test_checksum.c $(HEADER_FILES)
	$(CC) -Wall -o $@ test_checksum.c
