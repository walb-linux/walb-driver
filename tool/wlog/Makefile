.PHONY: all clean

CXX = g++-4.7.2
CC = gcc-4.7.2
CXXFLAGS = -Wall -Wextra -std=c++11 -pthread -I../../include
CFLAGS = -Wall -Wextra -I../../include

ifeq ($(DEBUG),1)
CXXFLAGS += -g
CFLAGS += -g
else
CXXFLAGS += -O2
CFLAGS += -O2
endif
ifeq ($(PROF),1)
CXXFLAGS += -pg
CFLAGS += -pg
else
endif

#LDFLAGS = -laio -lpthread
LDFLAGS = -static -laio -Wl,--whole-archive -lpthread -Wl,--no-whole-archive

BINARIES = wlcat wlredo

all: $(BINARIES)

wlcat: wlcat.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)
wlredo: wlredo.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

.cpp.o:
	$(CXX) $(CXXFLAGS) -c $<
.c.o:
	$(CC) $(CFLAGS) -c $<

wlcat.o: wlcat.cpp aio_util.hpp util.hpp walb_util.hpp
wlredo.o: wlredo.cpp aio_util.hpp util.hpp walb_util.hpp
#util.o: util.c util.h walb_random.h

clean:
	rm -f $(BINARIES) *.o
