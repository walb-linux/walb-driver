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

BINARIES = wlcat wlredo wlgen

all: $(BINARIES)

wlcat: wlcat.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
wlredo: wlredo.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
wlgen: wlgen.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

.cpp.o:
	$(CXX) $(CXXFLAGS) -c $<
.c.o:
	$(CC) $(CFLAGS) -c $<

wlcat.o: wlcat.cpp aio_util.hpp util.hpp walb_util.hpp
wlredo.o: wlredo.cpp aio_util.hpp util.hpp walb_util.hpp
wlgen.o: wlgen.cpp util.hpp walb_util.hpp

clean:
	rm -f $(BINARIES) *.o
