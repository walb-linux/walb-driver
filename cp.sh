#!/bin/sh

TARGET_HOST=$1
TARGET_DIR=$2

if [ "$TARGET_HOST" = "" ]; then
  echo "specify host"
  exit
fi

SCRIPT_DIR=$(cd $(dirname $0);pwd)

list_in_tool="\
walbctl \
test_rw \
"

for f in *.ko; do
  scp -p ${SCRIPT_DIR}/module/${f} ${TARGET_HOST}:${TARGET_DIR} &
done

for f in $list_in_tool; do
  scp -p ${SCRIPT_DIR}/tool/${f} ${TARGET_HOST}:${TARGET_DIR} &
done

wait
