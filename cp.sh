#!/bin/sh

TARGET_HOST=$1
TARGET_DIR=$2

if [ "$TARGET_HOST" = "" ]; then
  echo "specify host"
  exit
fi

SCRIPT_DIR=$(cd $(dirname $0);pwd)

scp -p ${SCRIPT_DIR}/module/walb-mod.ko         ${TARGET_HOST}:${TARGET_DIR};
scp -p ${SCRIPT_DIR}/module/insmod.sh           ${TARGET_HOST}:${TARGET_DIR};
scp -p ${SCRIPT_DIR}/module/test-hashtbl-mod.ko ${TARGET_HOST}:${TARGET_DIR};
scp -p ${SCRIPT_DIR}/module/test-treemap-mod.ko ${TARGET_HOST}:${TARGET_DIR};
scp -p ${SCRIPT_DIR}/tool/walbctl               ${TARGET_HOST}:${TARGET_DIR};
scp -p ${SCRIPT_DIR}/tool/test_rw               ${TARGET_HOST}:${TARGET_DIR};
