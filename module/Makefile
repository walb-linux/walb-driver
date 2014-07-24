# Debug options. Choose one of the followings.
DEBUG=0
DYNAMIC_DEBUG=0

# You can specify the following parameters.
#overlapped writes serialization.
OVERLAP=1
# enable ASSERT even with relase build.
ASSERT=0
# enable performacne analysis of each bio wrapper.
PERF=0
# enable bio wrapper tracking
TRACK=0

# Add your debugging flag (or not) to CFLAGS
ifeq ($(DYNAMIC_DEBUG),1)
  DEBFLAGS = -O0 -g -DDEBUG -DWALB_DEBUG -DUSE_DYNAMIC_DEBUG
endif
ifeq ($(DEBUG),1)
  DEBFLAGS = -O0 -g -DDEBUG -DWALB_DEBUG
endif
DEBFLAGS ?= -O2 -g

ifeq ($(ASSERT),1)
  DEBFLAGS += -DASSERT_ON
endif

ifeq ($(OVERLAP),1)
  DEBFLAGS += -DWALB_OVERLAPPED_SERIALIZE
endif

ifeq ($(PERF),1)
  DEBFLAGS += -DWALB_PERFORMANCE_ANALYSIS
endif

ifeq ($(TRACK),1)
  DEBFLAGS += -DWALB_TRACK_BIO_WRAPPER
endif

EXTRA_CFLAGS += $(DEBFLAGS)
EXTRA_CFLAGS += -I$(obj)/../include -I$(obj)

ifneq ($(KERNELRELEASE),)
# call from kernel build system

walb-mod-objs := \
walb.o wdev_util.o wdev_ioctl.o sysfs.o control.o alldevs.o checkpoint.o \
super.o logpack.o overlapped_io.o pending_io.o io.o redo.o \
sector_io.o bio_entry.o bio_wrapper.o worker.o pack_work.o \
hashtbl.o treemap.o

test-hashtbl-mod-objs := test/test_hashtbl.o hashtbl.o
test-treemap-mod-objs := test/test_treemap.o treemap.o
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

obj-m := \
test-hashtbl-mod.o \
test-treemap-mod.o \
test-kthread-mod.o \
test-deadlock-mod.o \
test-kmem-cache-mod.o \
test-workqueue-mod.o \
test-serialized-task-mod.o \
test-completion-mod.o \
test-vmalloc-mod.o \
test-bdev-mod.o \
test-sort-mod.o \
test-bio-entry-mod.o \
walb-mod.o \

else

KERNELDIR ?= /lib/modules/$(shell uname -r)/build
PWD       := $(shell pwd)

default:
	@echo DEBUG=$(DEBUG) DYNAMIC_DEBUG=$(DYNAMIC_DEBUG) OVERLAP=$(OVERLAP)
	@echo DEBFLAGS=$(DEBFLAGS)
	$(MAKE) clean
	$(MAKE) buildmodule

buildmodule: build_date_h version_h
	$(MAKE) -C $(KERNELDIR) M=$(PWD) modules

clean: clean_build_date clean_version
	rm -rf *.o *~ core .depend .*.cmd *.ko *.mod.c .tmp_versions

depend .depend dep:
	$(CC) $(CFLAGS) -M *.c > .depend

build_date_h: build_date.h.template
	cat $< | sed "s/XXXXX/`env LC_ALL=C date`/" > build_date.h

clean_build_date:
	rm -f build_date.h

version_h:
	$(shell ../make-ver.sh > version.h)

clean_version:
	rm -f version.h

endif

ifeq (.depend,$(wildcard .depend))
include .depend
endif
