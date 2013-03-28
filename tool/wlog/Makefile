.PHONY: all clean

CXX = g++-4.7.2
CC = gcc-4.7.2
CXXFLAGS = -Wall -Wextra -std=c++11 -pthread -I../../include -I..
CFLAGS = -Wall -Wextra -I../../include -I..

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
LDFLAGS = -Wl,-R,'$$ORIGIN' -laio -lpthread
endif

BINARIES = wlog-cat wlog-redo wlog-analyze \
wlog-gen wlog-restore wlog-update \
wldev-info bdiff \
write_random_data verify_written_data verify_wlog verify_wldev
SRCS = wlog-cat.cpp wlog-redo.cpp wlog-analyze.cpp \
wlog-gen.cpp wlog-restore.cpp wlog-update.cpp \
wldev-info.cpp bdiff.cpp \
write_random_data.cpp verify_written_data.cpp verify_wlog.cpp verify_wldev.cpp
TESTS = test_memory_buffer test_aio

all: $(BINARIES)
test: $(TESTS)

echo_binaries:
	@echo $(BINARIES)

wlog-cat: wlog-cat.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
wlog-redo: wlog-redo.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
wlog-analyze: wlog-analyze.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
wlog-gen: wlog-gen.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
wlog-restore: wlog-restore.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
wlog-update: wlog-update.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
wldev-info: wldev-info.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
bdiff: bdiff.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
write_random_data: write_random_data.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
verify_written_data: verify_written_data.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
verify_wlog: verify_wlog.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
verify_wldev: verify_wldev.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
test_memory_buffer: test_memory_buffer.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
test_aio: test_aio.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

.cpp.o:
	$(CXX) $(CXXFLAGS) -c $<
.c.o:
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f $(BINARIES) $(TESTS) *.o

rebuild:
	$(MAKE) clean
	$(MAKE) all

depend: Makefile
	sed -e '/^# DO NOT DELETE/,$$d' Makefile > Makefile.new
	echo '# DO NOT DELETE' >> Makefile.new
	$(CXX) -MM $(SRCS) $(CXXFLAGS) >> Makefile.new
	mv Makefile Makefile.org
	mv Makefile.new Makefile

# DO NOT DELETE
wlog-cat.o: wlog-cat.cpp util.hpp walb_log.hpp fileio.hpp \
 ../../include/walb/super.h ../../include/walb/walb.h \
 ../../include/walb/common.h ../../include/walb/userland.h \
 ../../include/walb/disk_name.h ../../include/walb/sector.h \
 ../../include/walb/checksum.h ../../include/walb/block_size.h \
 ../../include/walb/util.h ../../include/walb/log_device.h \
 ../../include/walb/log_record.h ../../include/walb/u32bits.h \
 ../../include/walb/super.h ../../include/walb/snapshot.h \
 ../../include/walb/u64bits.h ../../include/walb/log_record.h \
 ../walb_log.h ../../include/walb/walb.h aio_util.hpp \
 ../../include/walb/common.h memory_buffer.hpp
wlog-redo.o: wlog-redo.cpp util.hpp fileio.hpp memory_buffer.hpp \
 walb_log.hpp ../../include/walb/super.h ../../include/walb/walb.h \
 ../../include/walb/common.h ../../include/walb/userland.h \
 ../../include/walb/disk_name.h ../../include/walb/sector.h \
 ../../include/walb/checksum.h ../../include/walb/block_size.h \
 ../../include/walb/util.h ../../include/walb/log_device.h \
 ../../include/walb/log_record.h ../../include/walb/u32bits.h \
 ../../include/walb/super.h ../../include/walb/snapshot.h \
 ../../include/walb/u64bits.h ../../include/walb/log_record.h \
 ../walb_log.h ../../include/walb/walb.h aio_util.hpp \
 ../../include/walb/common.h
wlog-analyze.o: wlog-analyze.cpp util.hpp fileio.hpp memory_buffer.hpp \
 walb_log.hpp ../../include/walb/super.h ../../include/walb/walb.h \
 ../../include/walb/common.h ../../include/walb/userland.h \
 ../../include/walb/disk_name.h ../../include/walb/sector.h \
 ../../include/walb/checksum.h ../../include/walb/block_size.h \
 ../../include/walb/util.h ../../include/walb/log_device.h \
 ../../include/walb/log_record.h ../../include/walb/u32bits.h \
 ../../include/walb/super.h ../../include/walb/snapshot.h \
 ../../include/walb/u64bits.h ../../include/walb/log_record.h \
 ../walb_log.h ../../include/walb/walb.h aio_util.hpp \
 ../../include/walb/common.h
wlog-gen.o: wlog-gen.cpp util.hpp memory_buffer.hpp walb_log.hpp \
 fileio.hpp ../../include/walb/super.h ../../include/walb/walb.h \
 ../../include/walb/common.h ../../include/walb/userland.h \
 ../../include/walb/disk_name.h ../../include/walb/sector.h \
 ../../include/walb/checksum.h ../../include/walb/block_size.h \
 ../../include/walb/util.h ../../include/walb/log_device.h \
 ../../include/walb/log_record.h ../../include/walb/u32bits.h \
 ../../include/walb/super.h ../../include/walb/snapshot.h \
 ../../include/walb/u64bits.h ../../include/walb/log_record.h \
 ../walb_log.h ../../include/walb/walb.h
wlog-restore.o: wlog-restore.cpp util.hpp memory_buffer.hpp walb_log.hpp \
 fileio.hpp ../../include/walb/super.h ../../include/walb/walb.h \
 ../../include/walb/common.h ../../include/walb/userland.h \
 ../../include/walb/disk_name.h ../../include/walb/sector.h \
 ../../include/walb/checksum.h ../../include/walb/block_size.h \
 ../../include/walb/util.h ../../include/walb/log_device.h \
 ../../include/walb/log_record.h ../../include/walb/u32bits.h \
 ../../include/walb/super.h ../../include/walb/snapshot.h \
 ../../include/walb/u64bits.h ../../include/walb/log_record.h \
 ../walb_log.h ../../include/walb/walb.h
wlog-update.o: wlog-update.cpp util.hpp walb_log.hpp fileio.hpp \
 ../../include/walb/super.h ../../include/walb/walb.h \
 ../../include/walb/common.h ../../include/walb/userland.h \
 ../../include/walb/disk_name.h ../../include/walb/sector.h \
 ../../include/walb/checksum.h ../../include/walb/block_size.h \
 ../../include/walb/util.h ../../include/walb/log_device.h \
 ../../include/walb/log_record.h ../../include/walb/u32bits.h \
 ../../include/walb/super.h ../../include/walb/snapshot.h \
 ../../include/walb/u64bits.h ../../include/walb/log_record.h \
 ../walb_log.h ../../include/walb/walb.h aio_util.hpp \
 ../../include/walb/common.h
wldev-info.o: wldev-info.cpp util.hpp walb_log.hpp fileio.hpp \
 ../../include/walb/super.h ../../include/walb/walb.h \
 ../../include/walb/common.h ../../include/walb/userland.h \
 ../../include/walb/disk_name.h ../../include/walb/sector.h \
 ../../include/walb/checksum.h ../../include/walb/block_size.h \
 ../../include/walb/util.h ../../include/walb/log_device.h \
 ../../include/walb/log_record.h ../../include/walb/u32bits.h \
 ../../include/walb/super.h ../../include/walb/snapshot.h \
 ../../include/walb/u64bits.h ../../include/walb/log_record.h \
 ../walb_log.h ../../include/walb/walb.h aio_util.hpp \
 ../../include/walb/common.h
bdiff.o: bdiff.cpp util.hpp fileio.hpp
write_random_data.o: write_random_data.cpp checksum.hpp util.hpp \
 fileio.hpp io_recipe.hpp memory_buffer.hpp ../../include/walb/common.h \
 ../../include/walb/userland.h ../../include/walb/block_size.h \
 ../../include/walb/common.h
verify_written_data.o: verify_written_data.cpp checksum.hpp util.hpp \
 fileio.hpp memory_buffer.hpp io_recipe.hpp ../../include/walb/common.h \
 ../../include/walb/userland.h ../../include/walb/block_size.h \
 ../../include/walb/common.h
verify_wlog.o: verify_wlog.cpp util.hpp memory_buffer.hpp fileio.hpp \
 walb_log.hpp ../../include/walb/super.h ../../include/walb/walb.h \
 ../../include/walb/common.h ../../include/walb/userland.h \
 ../../include/walb/disk_name.h ../../include/walb/sector.h \
 ../../include/walb/checksum.h ../../include/walb/block_size.h \
 ../../include/walb/util.h ../../include/walb/log_device.h \
 ../../include/walb/log_record.h ../../include/walb/u32bits.h \
 ../../include/walb/super.h ../../include/walb/snapshot.h \
 ../../include/walb/u64bits.h ../../include/walb/log_record.h \
 ../walb_log.h ../../include/walb/walb.h io_recipe.hpp \
 ../../include/walb/common.h ../../include/walb/block_size.h
verify_wldev.o: verify_wldev.cpp util.hpp memory_buffer.hpp walb_log.hpp \
 fileio.hpp ../../include/walb/super.h ../../include/walb/walb.h \
 ../../include/walb/common.h ../../include/walb/userland.h \
 ../../include/walb/disk_name.h ../../include/walb/sector.h \
 ../../include/walb/checksum.h ../../include/walb/block_size.h \
 ../../include/walb/util.h ../../include/walb/log_device.h \
 ../../include/walb/log_record.h ../../include/walb/u32bits.h \
 ../../include/walb/super.h ../../include/walb/snapshot.h \
 ../../include/walb/u64bits.h ../../include/walb/log_record.h \
 ../walb_log.h ../../include/walb/walb.h io_recipe.hpp \
 ../../include/walb/common.h ../../include/walb/block_size.h
