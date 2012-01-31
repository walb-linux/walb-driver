# Comment/uncomment the following line to disable/enable debugging
#DEBUG = 1

# Add your debugging flag (or not) to CFLAGS
ifeq ($(DEBUG),1)
  DEBFLAGS = -O -g -DWALB_DEBUG # "-O" is needed to expand inlines
else
  DEBFLAGS = -O2
endif

EXTRA_CFLAGS += $(DEBFLAGS)
EXTRA_CFLAGS += -I$(obj)/../include

ifneq ($(KERNELRELEASE),)
# call from kernel build system

walb-mod-objs := walb.o control.o alldevs.o snapshot.o io.o hashtbl.o treemap.o datapack.o logpack.o
test-hashtbl-mod-objs := test_hashtbl.o hashtbl.o
test-treemap-mod-objs := test_treemap.o treemap.o
test-memblk-data-mod-objs := test_memblk_data.o treemap.o memblk_data.o
test-sg-util-mod-objs := test_sglist.o sg_util.o
wrapblk-mod-objs := wrapblk.o
memblk-none-mod-objs := memblk.o treemap.o block_size.o memblk_io_none.o
memblk-simple-mod-objs := memblk.o treemap.o block_size.o memblk_io_simple.o


#obj-m := walb-mod.o \
#	test-hashtbl-mod.o \
#	test-treemap-mod.o \
#	wrapblk-mod.o \
#	memblk-none-mod.o \
#	test-memblk-data-mod.o \
#	test-sg-util-mod.o \
#	memblk-simple-mod.o

obj-m := test-memblk-data-mod.o test-sg-util-mod.o

else

KERNELDIR ?= /lib/modules/$(shell uname -r)/build
PWD       := $(shell pwd)

default: clean_build_date build_date.h buildmodule

buildmodule:
	$(MAKE) -C $(KERNELDIR) M=$(PWD) modules

endif


clean: clean_build_date
	rm -rf *.o *~ core .depend .*.cmd *.ko *.mod.c .tmp_versions

depend .depend dep:
	$(CC) $(CFLAGS) -M *.c > .depend

build_date.h: build_date.h.template
	cat $< | sed "s/XXXXX/`env LC_ALL=C date`/" > $@

clean_build_date:
	rm -f build_date.h

ifeq (.depend,$(wildcard .depend))
include .depend
endif
