# Comment/uncomment the following line to disable/enable debugging
#DEBUG = 1

# Add your debugging flag (or not) to CFLAGS
ifeq ($(DEBUG),1)
  DEBFLAGS = -O -g -DWALB_DEBUG # "-O" is needed to expand inlines
else
  DEBFLAGS = -O2
endif

EXTRA_CFLAGS += $(DEBFLAGS)
EXTRA_CFLAGS += -I..

ifneq ($(KERNELRELEASE),)
# call from kernel build system

walb-mod-objs  := walb.o walb_control.o hashtbl.o walb_alldevs.o
test-mod-objs  := test.o hashtbl.o
test2-mod-objs := test2.o treemap.o

obj-m := walb-mod.o test-mod.o test2-mod.o

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
