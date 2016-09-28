#!/bin/sh
#
# Execute this script at the top directory of the kernel source tree.
#

SCRIPT_DIR=$(dirname $0)
BASE_DIR=$SCRIPT_DIR/../../../
MOD_DIR=$SCRIPT_DIR/../../

mkdir -p include/linux/walb
mkdir -p drivers/block/walb
cp -a $BASE_DIR/include/linux/walb/*.h include/linux/walb/
make -C $MOD_DIR clean
make -C $MOD_DIR version_h
make -C $MOD_DIR build_date_h
cp -a $MOD_DIR/*.h drivers/block/walb/
cp -a $MOD_DIR/*.c drivers/block/walb/
cp -a $SCRIPT_DIR/Kconfig drivers/block/walb/
cp -a $SCRIPT_DIR/Makefile drivers/block/walb/
patch -p1 < $SCRIPT_DIR/Kconfig.patch
patch -p1 < $SCRIPT_DIR/Makefile.patch

