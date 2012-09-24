# Debug options. Choose one of the followings.
DEBUG = 0
DYNAMIC_DEBUG = 0
# You can specify the following parameters.
OVERLAP = 1 #overlapping writes serialization.
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
  DEBFLAGS += -DWALB_OVERLAPPING_SERIALIZE
endif

ifeq ($(FAST_ALG),1)
  DEBFLAGS += -DWALB_FAST_ALGORITHM
endif

EXTRA_CFLAGS += $(DEBFLAGS)
EXTRA_CFLAGS += -I$(obj)/../include

ifneq ($(KERNELRELEASE),)
# call from kernel build system

walb-mod-objs := walb.o control.o alldevs.o snapshot.o hashtbl.o treemap.o logpack.o
test-hashtbl-mod-objs := test_hashtbl.o hashtbl.o
test-treemap-mod-objs := test_treemap.o treemap.o
test-memblk-data-mod-objs := test_memblk_data.o treemap.o memblk_data.o
test-memblk-data-perf-mod-objs := test_memblk_data_perf.o treemap.o memblk_data.o
test-sg-util-mod-objs := test_sg_util.o sg_util.o
test-size-list-mod-objs := test_size_list.o size_list.o
test-kmem-cache-mod-objs := test_kmem_cache.o
test-workqueue-mod-objs := test_workqueue.o
test-kthread-mod-objs := test_kthread.o
test-deadlock-mod-objs := test_deadlock.o
test-serialized-task-mod-objs := test_serialized_task.o
test-completion-mod-objs := test_completion.o
simple-blk-mod-objs := simple_blk.o
SIMPLE_BLK_BIO_OBJS = simple_blk_bio.o size_list.o
SIMPLE_BLK_REQ_OBJS = simple_blk_req.o size_list.o
simple-blk-bio-none-mod-objs := $(SIMPLE_BLK_BIO_OBJS) simple_blk_bio_none.o
MEMBLK_DATA_OBJS = memblk_data.o treemap.o
simple-blk-bio-mem-mod-objs := \
$(SIMPLE_BLK_BIO_OBJS) $(MEMBLK_DATA_OBJS) simple_blk_bio_mem.o 
simple-blk-bio-mem-barrier-mod-objs := \
$(SIMPLE_BLK_BIO_OBJS) $(MEMBLK_DATA_OBJS) simple_blk_bio_mem_barrier.o
simple-blk-req-mem-mod-objs := \
$(SIMPLE_BLK_REQ_OBJS) $(MEMBLK_DATA_OBJS) simple_blk_req_mem.o
simple-blk-req-mem-barrier-mod-objs := \
$(SIMPLE_BLK_REQ_OBJS) $(MEMBLK_DATA_OBJS) simple_blk_req_mem_barrier.o
simple-blk-req-none-mod-objs := \
$(SIMPLE_BLK_REQ_OBJS) simple_blk_req_none.o
#SIMPLE_BLK_WRAP_OBJS = simple_blk_wrapper.o size_list.o
#simple-blk-wrap-mod-objs := $(SIMPLE_BLK_WRAP_OBJS)

wrapper-blk-mod-objs := wrapper_blk.o
WRAPPER_BLK_SIMPLE_REQ_OBJS = wrapper_blk_simple_req.o
wrapper-blk-simple-req-mod-objs := $(WRAPPER_BLK_SIMPLE_REQ_OBJS)
WRAPPER_BLK_SIMPLE_BIO_OBJS = wrapper_blk_simple_bio.o bio_entry.o
wrapper-blk-simple-bio-mod-objs := $(WRAPPER_BLK_SIMPLE_BIO_OBJS)
WRAPPER_BLK_WALB_REQ_OBJS = wrapper_blk_walb_req.o \
sector_io.o bio_entry.o req_entry.o logpack.o treemap.o
wrapper-blk-walb-req-mod-objs := $(WRAPPER_BLK_WALB_REQ_OBJS)
WRAPPER_BLK_WALB_BIO_OBJS = wrapper_blk_walb_bio.o \
sector_io.o bio_entry.o bio_wrapper.o logpack.o treemap.o worker.o
wrapper-blk-walb-bio-mod-objs := $(WRAPPER_BLK_WALB_BIO_OBJS)

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
simple-blk-mod.o \
simple-blk-bio-none-mod.o \
simple-blk-bio-mem-mod.o \
simple-blk-bio-mem-barrier-mod.o \
simple-blk-req-none-mod.o \
simple-blk-req-mem-mod.o \
simple-blk-req-mem-barrier-mod.o \
wrapper-blk-mod.o \
wrapper-blk-simple-req-mod.o \
wrapper-blk-simple-bio-mod.o \
wrapper-blk-walb-req-mod.o \
wrapper-blk-walb-bio-mod.o \
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
