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

walb-mod-objs := walb.o control.o alldevs.o snapshot.o io.o hashtbl.o treemap.o
test-hashtbl-mod-objs := test_hashtbl.o hashtbl.o
test-treemap-mod-objs := test_treemap.o treemap.o
wrapblk-mod-objs := wrapblk.o

obj-m := walb-mod.o test-hashtbl-mod.o test-treemap-mod.o wrapblk-mod.o

else

KERNELDIR ?= /lib/modules/$(shell uname -r)/build
PWD       := $(shell pwd)

default:
	$(MAKE) -C $(KERNELDIR) M=$(PWD) modules

endif

clean:
	rm -rf *.o *~ core .depend .*.cmd *.ko *.mod.c .tmp_versions

depend .depend dep:
	$(CC) $(CFLAGS) -M *.c > .depend


ifeq (.depend,$(wildcard .depend))
include .depend
endif
