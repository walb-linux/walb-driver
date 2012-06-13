# Comment/uncomment the following line to disable/enable debugging
#DEBUG = 1
#OVERLAP_DETECT = 1

# Add your debugging flag (or not) to CFLAGS
ifeq ($(DEBUG),1)
  DEBFLAGS = -O -g -DDEBUG -DWALB_DEBUG # "-O" is needed to expand inlines
else
  DEBFLAGS = -O2
endif

ifeq ($(OVERLAP_DETECT),1)
  DEBFLAGS += -DWALB_OVERLAPPING_DETECTION
endif

ifeq ($(FAST_ALG),1)
  DEBFLAGS += -DWALB_FAST_ALGORITHM
endif

EXTRA_CFLAGS += $(DEBFLAGS)
EXTRA_CFLAGS += -I$(obj)/../include

ifneq ($(KERNELRELEASE),)
# call from kernel build system

walb-mod-objs := walb.o control.o alldevs.o snapshot.o io.o hashtbl.o treemap.o datapack.o logpack.o
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
#wrapblk-mod-objs := wrapblk.o
#memblk-none-mod-objs := memblk.o treemap.o memblk_io_none.o
#memblk-simple-mod-objs := memblk.o treemap.o memblk_io_simple.o
simple-blk-mod-objs := simple_blk.o
SIMPLE_BLK_BIO_OBJS = simple_blk_bio.o size_list.o
SIMPLE_BLK_REQ_OBJS = simple_blk_req.o size_list.o
simple-blk-bio-none-mod-objs := $(SIMPLE_BLK_BIO_OBJS) simple_blk_bio_none.o
MEMBLK_DATA_OBJS = memblk_data.o treemap.o
simple-blk-bio-mem-mod-objs := \
$(SIMPLE_BLK_BIO_OBJS) $(MEMBLK_DATA_OBJS) simple_blk_bio_mem.o 
simple-blk-bio-mem-barrier-mod-objs := \
$(SIMPLE_BLK_BIO_OBJS) $(MEMBLK_DATA_OBJS) simple_blk_bio_mem_barrier.o
simple-blk-req-mem-barrier-mod-objs := \
$(SIMPLE_BLK_REQ_OBJS) $(MEMBLK_DATA_OBJS) simple_blk_req_mem_barrier.o
simple-blk-req-none-mod-objs := \
$(SIMPLE_BLK_REQ_OBJS) simple_blk_req_none.o
#SIMPLE_BLK_WRAP_OBJS = simple_blk_wrapper.o size_list.o
#simple-blk-wrap-mod-objs := $(SIMPLE_BLK_WRAP_OBJS)

wrapper-blk-mod-objs := wrapper_blk.o
WRAPPER_BLK_SIMPLE_OBJS = wrapper_blk_simple.o
wrapper-blk-simple-plug-mod-objs := $(WRAPPER_BLK_SIMPLE_OBJS) \
wrapper_blk_simple_plug.o
WRAPPER_BLK_WALB_OBJS = wrapper_blk_walb.o \
sector_io.o bio_entry.o logpack.o treemap.o
wrapper-blk-walb-easy-mod-objs := $(WRAPPER_BLK_WALB_OBJS) \
wrapper_blk_walb_easy.o

#obj-m := walb-mod.o \
#test-hashtbl-mod.o \
#test-treemap-mod.o \
#wrapblk-mod.o \
#memblk-none-mod.o \
#test-memblk-data-mod.o \
#test-sg-util-mod.o \
#memblk-simple-mod.o

obj-m := \
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
simple-blk-mod.o \
simple-blk-bio-none-mod.o \
simple-blk-bio-mem-mod.o \
simple-blk-bio-mem-barrier-mod.o \
simple-blk-req-none-mod.o \
simple-blk-req-mem-barrier-mod.o \
wrapper-blk-mod.o \
wrapper-blk-simple-plug-mod.o \
wrapper-blk-walb-easy-mod.o \

else

KERNELDIR ?= /lib/modules/$(shell uname -r)/build
PWD       := $(shell pwd)

default:
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
