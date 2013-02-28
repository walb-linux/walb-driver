#!/bin/sh

#
# You can test walb-log related commands.
# You must have privilege of 'disk' group to use losetup commands.
# /dev/loop0 and /dev/loop1 will be used.
#

LDEV=ldev32M
DDEV=ddev32M
WLOG=wlog
CTL=../walbctl
BIN=.
LOOP0=/dev/loop0
LOOP1=/dev/loop1

format_ldev()
{
  dd if=/dev/zero of=$LDEV bs=1M count=32
  dd if=/dev/zero of=${DDEV}.0 bs=1M count=32
  losetup $LOOP0 $LDEV
  losetup $LOOP1 ${DDEV}.0
  $CTL format_ldev --ldev $LOOP0 --ddev $LOOP1
  RING_BUFFER_SIZE=$(${BIN}/wlinfo $LOOP0 |grep ringBufferSize |awk '{print $2}')
  echo $RING_BUFFER_SIZE
  sleep 1
  losetup -d $LOOP0
  losetup -d $LOOP1
}

echo_wlog_value()
{
  local wlogFile=$1
  local keyword=$2
  $CTL show_wlog < $wlogFile |grep $keyword |awk '{print $2}'
}

#
# Initialization.
#
format_ldev
${BIN}/wlgen -s 32M -z 16M --maxPackSize 4M -o ${WLOG}.0
#${BIN}/wlgen -s 32M -z 16M --minIoSize 512 --maxIoSize 512 --maxPackSize 1M -o ${WLOG}.0
endLsid0=$(echo_wlog_value ${WLOG}.0 end_lsid_really:)
nPacks0=$(echo_wlog_value ${WLOG}.0 n_packs:)
totalPadding0=$(echo_wlog_value ${WLOG}.0 total_padding_size:)
cp ${DDEV}.0 ${DDEV}.0z
${BIN}/wlredo ${DDEV}.0 < ${WLOG}.0
${BIN}/wlredo ${DDEV}.0z --zerodiscard < ${WLOG}.0

#
# Simple test.
#
${BIN}/wlrestore --verify $LDEV < ${WLOG}.0
${BIN}/wlcat $LDEV -v -o ${WLOG}.1
${BIN}/bdiff -b 512 ${WLOG}.0 ${WLOG}.1
if [ $? -ne 0 ]; then
  echo "TEST1_FAILURE"
  exit 1
fi

restore_test()
{
  local testId=$1
  local lsidDiff=$2
  local invalidLsid=$3
  local ret0
  local ret1
  local ret2

  dd if=/dev/zero of=${DDEV}.1 bs=1M count=32
  dd if=/dev/zero of=${DDEV}.1z bs=1M count=32
  dd if=/dev/zero of=${DDEV}.2 bs=1M count=32
  dd if=/dev/zero of=${DDEV}.3 bs=1M count=32
  ${BIN}/wlrestore $LDEV --verify --lsidDiff $lsidDiff --invalidLsid $invalidLsid < ${WLOG}.0
  ${BIN}/wlcat $LDEV -v -o ${WLOG}.1
  losetup $LOOP0 ${LDEV}
  $CTL cat_wldev --wldev $LOOP0 > ${WLOG}.2
  sleep 1
  losetup -d $LOOP0
  if [ $invalidLsid -lt 0 ]; then
    local endLsid0a=$(expr $endLsid0 + $lsidDiff - $nPacks0 - $totalPadding0)
    local endLsid1=$(echo_wlog_value ${WLOG}.1 end_lsid_really:)
    local endLsid2=$(echo_wlog_value ${WLOG}.2 end_lsid_really:)
    local nPacks1=$(echo_wlog_value ${WLOG}.1 n_packs:)
    local nPacks2=$(echo_wlog_value ${WLOG}.2 n_packs:)
    local totalPadding1=$(echo_wlog_value ${WLOG}.1 total_padding_size:)
    local totalPadding2=$(echo_wlog_value ${WLOG}.2 total_padding_size:)
    local endLsid1a=$(expr $endLsid1 - $nPacks1 - $totalPadding1)
    local endLsid2a=$(expr $endLsid2 - $nPacks2 - $totalPadding2)
    if [ $endLsid0a -ne $endLsid1a ]; then
      echo endLsid0a $endLsid0a does not equal to endLsid1a $endLsid1a
      echo "TEST${testId}_FAILURE"
      exit 1
    fi
    if [ $endLsid0a -ne $endLsid2a ]; then
      echo endLsid0a $endLsid0a does not equal to endLsid2a $endLsid2a
      echo "TEST${testId}_FAILURE"
      exit 1
    fi
  fi
  ${BIN}/bdiff -b 512 ${WLOG}.1 ${WLOG}.2
  if [ $? -ne 0 ]; then
    echo ${WLOG}.1 and ${WLOG}.2 differ.
    echo "TEST${testId}_FAILURE"
    exit 1
  fi

  ${BIN}/wlredo ${DDEV}.1 < ${WLOG}.1
  ${BIN}/wlredo ${DDEV}.1z --zerodiscard < ${WLOG}.1
  losetup $LOOP1 ${DDEV}.2
  $CTL redo_wlog --ddev $LOOP1 < ${WLOG}.1
  sleep 1
  losetup -d $LOOP1
  losetup $LOOP0 ${LDEV}
  losetup $LOOP1 ${DDEV}.3
  $CTL redo --ldev $LOOP0 --ddev $LOOP1
  sleep 1
  losetup -d $LOOP0
  losetup -d $LOOP1
  if [ $invalidLsid -eq -1 ]; then
    ${BIN}/bdiff -b 512 ${DDEV}.0 ${DDEV}.1
    if [ $? -ne 0 ]; then
      echo "failed: ${BIN}/bdiff -b 512 ${DDEV}.0 ${DDEV}.1"
      echo "TEST${testId}_FAILURE"
      exit 1
    fi
    ${BIN}/bdiff -b 512 ${DDEV}.0 ${DDEV}.2
    if [ $? -ne 0 ]; then
      echo "failed: ${BIN}/bdiff -b 512 ${DDEV}.0 ${DDEV}.2"
      echo "TEST${testId}_FAILURE"
      exit 1
    fi
    ${BIN}/bdiff -b 512 ${DDEV}.0 ${DDEV}.3
    if [ $? -ne 0 ]; then
      echo "failed: ${BIN}/bdiff -b 512 ${DDEV}.0 ${DDEV}.3"
      echo "TEST${testId}_FAILURE"
      exit 1
    fi
    ${BIN}/bdiff -b 512 ${DDEV}.0z ${DDEV}.1z
    if [ $? -ne 0 ]; then
      echo "failed: ${BIN}/bdiff -b 512 ${DDEV}.0z ${DDEV}.1z"
      echo "TEST${testId}_FAILURE"
      exit 1
    fi
  else
    ${BIN}/bdiff -b 512 ${DDEV}.1 ${DDEV}.2
    if [ $? -ne 0 ]; then
      echo "failed: ${BIN}/bdiff -b 512 ${DDEV}.1 ${DDEV}.2"
      echo "TEST${testId}_FAILURE"
      exit 1
    fi
    ${BIN}/bdiff -b 512 ${DDEV}.1 ${DDEV}.3
    if [ $? -ne 0 ]; then
      echo "failed: ${BIN}/bdiff -b 512 ${DDEV}.1 ${DDEV}.3"
      echo "TEST${testId}_FAILURE"
      exit 1
    fi
  fi
}

restore_test 3 $(expr $RING_BUFFER_SIZE - 1) -1
restore_test 4 $(expr $RING_BUFFER_SIZE - 2) -1
restore_test 5 $(expr $RING_BUFFER_SIZE - 1024) -1
restore_test 6 0 1024 #512KB
restore_test 7 0 8192 #4MB

echo TEST_SUCCESS
exit 0
