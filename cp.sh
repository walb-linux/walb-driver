#!/bin/sh

TARGET=$1

if [ "$TARGET" = "" ]; then
  echo "specify host"
  exit
fi

SCRIPT_DIR=$(cd $(dirname $0);pwd)

scp -p ${SCRIPT_DIR}/module/walb.ko   ${TARGET}:;
scp -p ${SCRIPT_DIR}/module/insmod.sh ${TARGET}:;
scp -p ${SCRIPT_DIR}/tool/walbctl     ${TARGET}:;
scp -p ${SCRIPT_DIR}/tool/test_rw     ${TARGET}:;
