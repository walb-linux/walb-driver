# Debug options. Choose one of the followings.
DEBUG = 0
DYNAMIC_DEBUG = 0
# You can specify the following parameters.
OVERLAP = 1 #overlapped writes serialization.
FAST_ALG = 1 # use fast algorithm instead of easy.
ASSERT = 0 # enable ASSERT even with relase build.

# Add your debugging flag (or not) to CFLAGS
ifeq ($(DYNAMIC_DEBUG),1)
  DEBFLAGS = -O -g -DDEBUG -DWALB_DEBUG -DUSE_DYNAMIC_DEBUG
endif
ifeq ($(DEBUG),1)
  DEBFLAGS = -O -g -DDEBUG -DWALB_DEBUG
endif
DEBFLAGS ?= -O2

ifeq ($(ASSERT),1)
  DEBFLAGS += -DASSERT_ON
endif

ifeq ($(OVERLAP),1)
  DEBFLAGS += -DWALB_OVERLAPPED_SERIALIZE
endif

ifeq ($(FAST_ALG),1)
  DEBFLAGS += -DWALB_FAST_ALGORITHM
endif

EXTRA_CFLAGS += $(DEBFLAGS)
EXTRA_CFLAGS += -I$(obj)/../include -I$(obj)

ifneq ($(KERNELRELEASE),)
# call from kernel build system

walb-mod-objs := \
walb.o control.o alldevs.o snapshot.o checkpoint.o \
super.o logpack.o io.o redo.o \
sector_io.o bio_entry.o bio_wrapper.o worker.o pack_work.o \
hashtbl.o treemap.o

test-hashtbl-mod-objs := test/test_hashtbl.o hashtbl.o
test-treemap-mod-objs := test/test_treemap.o treemap.o
test-memblk-data-mod-objs := test/test_memblk_data.o proto/memblk_data.o treemap.o
test-memblk-data-perf-mod-objs := test/test_memblk_data_perf.o proto/memblk_data.o treemap.o
test-sg-util-mod-objs := test/test_sg_util.o sg_util.o
test-size-list-mod-objs := test/test_size_list.o proto/size_list.o
test-kmem-cache-mod-objs := test/test_kmem_cache.o
test-workqueue-mod-objs := test/test_workqueue.o
test-kthread-mod-objs := test/test_kthread.o
test-deadlock-mod-objs := test/test_deadlock.o
test-serialized-task-mod-objs := test/test_serialized_task.o
test-completion-mod-objs := test/test_completion.o
test-vmalloc-mod-objs := test/test_vmalloc.o
test-bdev-mod-objs := test/test_bdev.o
test-sort-mod-objs := test/test_sort.o treemap.o
test-bio-entry-mod-objs := test/test_bio_entry.o bio_entry.o bio_wrapper.o

simple-blk-mod-objs := proto/simple_blk.o
BASE_BIO_OBJS = proto/base_bio.o proto/size_list.o
BASE_REQ_OBJS = proto/base_req.o proto/size_list.o
none-bio-mod-objs := $(BASE_BIO_OBJS) proto/none_bio.o
none-req-mod-objs := $(BASE_REQ_OBJS) proto/none_req.o
MEMBLK_DATA_OBJS = proto/memblk_data.o treemap.o
mem-bio-mod-objs := $(BASE_BIO_OBJS) $(MEMBLK_DATA_OBJS) proto/mem_bio.o
mem-req-mod-objs := $(BASE_REQ_OBJS) $(MEMBLK_DATA_OBJS) proto/mem_req.o
wrapper-blk-mod-objs := proto/wrapper_blk.o pack_work.o
WRAP_REQ_OBJS = proto/wrap_req.o
wrap-req-mod-objs := $(WRAP_REQ_OBJS)
WRAP_BIO_OBJS = proto/wrap_bio.o bio_entry.o
wrap-bio-mod-objs := $(WRAP_BIO_OBJS)
WALB_PROTO_REQ_OBJS = proto/walb_proto_req.o \
sector_io.o bio_entry.o proto/req_entry.o logpack.o treemap.o worker.o pack_work.o
walb-proto-req-mod-objs := $(WALB_PROTO_REQ_OBJS)
WALB_PROTO_BIO_OBJS = \
proto/walb_proto_bio.o io.o checkpoint.o super.o \
sector_io.o bio_entry.o bio_wrapper.o logpack.o treemap.o worker.o pack_work.o
walb-proto-bio-mod-objs := $(WALB_PROTO_BIO_OBJS)

obj-m := \
test-hashtbl-mod.o \
test-treemap-mod.o \
test-memblk-data-mod.o \
test-memblk-data-perf-mod.o \
test-kthread-mod.o \
test-deadlock-mod.o \
test-sg-util-mod.o \
test-size-list-mod.o \
test-kmem-cache-mod.o \
test-workqueue-mod.o \
test-serialized-task-mod.o \
test-completion-mod.o \
test-vmalloc-mod.o \
test-bdev-mod.o \
test-sort-mod.o \
test-bio-entry-mod.o \
simple-blk-mod.o \
none-bio-mod.o \
none-req-mod.o \
mem-bio-mod.o \
mem-req-mod.o \
wrapper-blk-mod.o \
wrap-req-mod.o \
wrap-bio-mod.o \
walb-proto-req-mod.o \
walb-proto-bio-mod.o \
walb-mod.o \

else

KERNELDIR ?= /lib/modules/$(shell uname -r)/build
PWD       := $(shell pwd)

default:
	@echo DEBUG=$(DEBUG) DYNAMIC_DEBUG=$(DYNAMIC_DEBUG) OVERLAP=$(OVERLAP) FAST_ALG=$(FAST_ALG)
	@echo DEBFLAGS=$(DEBFLAGS)
	$(MAKE) clean
	$(MAKE) buildmodule

buildmodule: build_date.h
	$(MAKE) -C $(KERNELDIR) M=$(PWD) modules

clean: clean_build_date
	rm -rf *.o *~ core .depend .*.cmd *.ko *.mod.c .tmp_versions

depend .depend dep:
	$(CC) $(CFLAGS) -M *.c > .depend

build_date.h: build_date.h.template
	cat $< | sed "s/XXXXX/`env LC_ALL=C date`/" > $@

clean_build_date:
	rm -f build_date.h

endif

ifeq (.depend,$(wildcard .depend))
include .depend
endif
