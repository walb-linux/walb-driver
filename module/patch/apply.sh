#!/bin/sh
#
# Execute this script at the top directory of the kernel source tree.
#

SCRIPT_DIR=$(dirname $0)

mkdir -p include/linux/walb
mkdir -p drivers/block/walb
cp -a $SCRIPT_DIR/../../include/linux/walb/*.h include/linux/walb/
make -C $SCRIPT_DIR/../ clean
make -C $SCRIPT_DIR/../ version_h
make -C $SCRIPT_DIR/../ build_date_h
cp -a $SCRIPT_DIR/../../module/*.h drivers/block/walb/
cp -a $SCRIPT_DIR/../../module/*.c drivers/block/walb/
cp -a $SCRIPT_DIR/Kconfig drivers/block/walb/
cp -a $SCRIPT_DIR/Makefile drivers/block/walb/
patch -p1 < $SCRIPT_DIR/Kconfig.patch
patch -p1 < $SCRIPT_DIR/Makefile.patch

