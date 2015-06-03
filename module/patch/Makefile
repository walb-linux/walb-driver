#
# WalB module makefile
#

walb-mod-y := walb.o wdev_util.o wdev_ioctl.o sysfs.o control.o alldevs.o checkpoint.o \
              super.o logpack.o overlapped_io.o pending_io.o io.o redo.o \
              sector_io.o bio_entry.o bio_wrapper.o worker.o pack_work.o \
              treemap.o

obj-$(CONFIG_BLK_DEV_WALB) := walb-mod.o 

ccflags-y := -g
ifeq ($(CONFIG_BLK_DEV_WALB_DEBUG),y)
ccflag-y += -O0 -DDEBUG -DWALB_DEBUG
else
ccflag-y += -O2
endif
ccflags-$(CONFIG_BLK_DEV_WALB_ASSERT) += -DASSERT_ON
ccflags-$(CONFIG_BLK_DEV_WALB_OVERLAP) += -DWALB_OVERLAPPED_SERIALIZE
ccflags-$(CONFIG_BLK_DEV_WALB_DYNAMIC_DEBUG) += -DUSE_DYNAMIC_DEBUG
ccflags-$(CONFIG_BLK_DEV_WALB_PERF) += -DWALB_PERFORMANCE_ANALYSIS
ccflags-$(COFNIG_BLK_DEV_WALB_TRACK) += -DWALB_TRACK_BIO_WRAPPER
