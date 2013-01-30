.PHONY: all clean

CXX = g++-4.7.2
CC = gcc-4.7.2
CXXFLAGS = -Wall -Wextra -std=c++11 -pthread -I../../include
CFLAGS = -Wall -Wextra -I../../include

ifeq ($(DEBUG),1)
CXXFLAGS += -g -DDEBUG
CFLAGS += -g -DDEBUG
else
CXXFLAGS += -O2 -DNDEBUG
CFLAGS += -O2 -DNDEBUG
endif
ifeq ($(PROF),1)
CXXFLAGS += -pg
CFLAGS += -pg
else
endif

ifeq ($(STATIC),1)
LDFLAGS = -static -laio -Wl,--whole-archive -lpthread -Wl,--no-whole-archive
else
LDFLAGS = -laio -lpthread
endif

BINARIES = wlcat wlredo wlgen wlrestore bdiff
SRCS = wlcat.cpp wlredo.cpp wlgen.cpp wlrestore.cpp bdiff.cpp

all: $(BINARIES)

wlcat: wlcat.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
wlredo: wlredo.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
wlgen: wlgen.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
wlrestore: wlrestore.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
bdiff: bdiff.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

.cpp.o:
	$(CXX) $(CXXFLAGS) -c $<
.c.o:
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f $(BINARIES) *.o

depend: Makefile
	sed -e '/^# DO NOT DELETE/,$$d' Makefile > Makefile.new
	echo '# DO NOT DELETE' >> Makefile.new
	$(CXX) -MM $(SRCS) $(CXXFLAGS) >> Makefile.new
	mv Makefile Makefile.org
	mv Makefile.new Makefile

# DO NOT DELETE
wlcat.o: wlcat.cpp util.hpp walb_util.hpp ../../include/walb/super.h \
 ../../include/walb/walb.h ../../include/walb/common.h \
 ../../include/walb/userland.h ../../include/walb/disk_name.h \
 ../../include/walb/sector.h ../../include/walb/checksum.h \
 ../../include/walb/block_size.h ../../include/walb/util.h \
 ../../include/walb/log_device.h ../../include/walb/log_record.h \
 ../../include/walb/u32bits.h ../../include/walb/super.h \
 ../../include/walb/snapshot.h ../../include/walb/u64bits.h \
 ../../include/walb/log_record.h ../walblog_format.h \
 ../../include/walb/walb.h aio_util.hpp ../random.h ../check_userland.h \
 ../../include/walb/userland.h
wlredo.o: wlredo.cpp util.hpp walb_util.hpp ../../include/walb/super.h \
 ../../include/walb/walb.h ../../include/walb/common.h \
 ../../include/walb/userland.h ../../include/walb/disk_name.h \
 ../../include/walb/sector.h ../../include/walb/checksum.h \
 ../../include/walb/block_size.h ../../include/walb/util.h \
 ../../include/walb/log_device.h ../../include/walb/log_record.h \
 ../../include/walb/u32bits.h ../../include/walb/super.h \
 ../../include/walb/snapshot.h ../../include/walb/u64bits.h \
 ../../include/walb/log_record.h ../walblog_format.h \
 ../../include/walb/walb.h aio_util.hpp ../random.h ../check_userland.h \
 ../../include/walb/userland.h
wlgen.o: wlgen.cpp util.hpp walb_util.hpp ../../include/walb/super.h \
 ../../include/walb/walb.h ../../include/walb/common.h \
 ../../include/walb/userland.h ../../include/walb/disk_name.h \
 ../../include/walb/sector.h ../../include/walb/checksum.h \
 ../../include/walb/block_size.h ../../include/walb/util.h \
 ../../include/walb/log_device.h ../../include/walb/log_record.h \
 ../../include/walb/u32bits.h ../../include/walb/super.h \
 ../../include/walb/snapshot.h ../../include/walb/u64bits.h \
 ../../include/walb/log_record.h ../walblog_format.h \
 ../../include/walb/walb.h
wlrestore.o: wlrestore.cpp util.hpp walb_util.hpp \
 ../../include/walb/super.h ../../include/walb/walb.h \
 ../../include/walb/common.h ../../include/walb/userland.h \
 ../../include/walb/disk_name.h ../../include/walb/sector.h \
 ../../include/walb/checksum.h ../../include/walb/block_size.h \
 ../../include/walb/util.h ../../include/walb/log_device.h \
 ../../include/walb/log_record.h ../../include/walb/u32bits.h \
 ../../include/walb/super.h ../../include/walb/snapshot.h \
 ../../include/walb/u64bits.h ../../include/walb/log_record.h \
 ../walblog_format.h ../../include/walb/walb.h
bdiff.o: bdiff.cpp util.hpp
