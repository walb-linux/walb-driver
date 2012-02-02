#!/bin/sh

TARGET_HOST=$1
TARGET_DIR=$2

if [ "$TARGET_HOST" = "" ]; then
  echo "specify host"
  exit
fi

SCRIPT_DIR=$(cd $(dirname $0);pwd)

list_in_module="\
walb-mod.ko \
insmod.sh \
test-hashtbl-mod.ko \
test-treemap-mod.ko \
memblk-mod.ko \
test-memblk-data-mod.ko \
test-sg-util-mod.ko \
simple-blk-mod.ko \
"
list_in_tool="\
walbctl \
test_rw \
"

for f in $list_in_module; do
  scp -p ${SCRIPT_DIR}/module/${f} ${TARGET_HOST}:${TARGET_DIR} &
done

for f in $list_in_tool; do
  scp -p ${SCRIPT_DIR}/tool/${f} ${TARGET_HOST}:${TARGET_DIR} &
done

wait
