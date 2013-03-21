#!/bin/sh

. ./common.sh

ITER_ID=$1
RES_DIR=res

[ -d ${RES_DIR} ] || mkdir ${RES_DIR}

echo iteration ${ITER_ID}
echo write random data...
${BIN}/write_random_data $WDEV > ${RES_DIR}/recipe.${ITER_ID}
echo verify written data...
${BIN}/verify_written_data $WDEV < ${RES_DIR}/recipe.${ITER_ID} > ${RES_DIR}/result0.${ITER_ID}
grep ^NG ${RES_DIR}/result0.${ITER_ID}
sleep 1
sync

endLsid=$(${BIN}/walbctl get_permanent_lsid --wdev $WDEV)
echo $endLsid |tee ${RES_DIR}/end_lsid.${ITER_ID}

echo verify logs in the log device...
${BIN}/verify_wldev -e $endLsid -r ${RES_DIR}/recipe.${ITER_ID} $WLDEV > ${RES_DIR}/result1.${ITER_ID}
if [ $? -ne 0 ]; then
  echo "verify_wldev failed."
  exit 1
fi
grep ^NG ${RES_DIR}/result1.${ITER_ID}

echo wlog-cat...
${BIN}/wlog-cat --endLsid $endLsid $WLDEV > ${RES_DIR}/wlog.${ITER_ID}
echo verify wlog
${BIN}/verify_wlog -r ${RES_DIR}/recipe.${ITER_ID} -w ${RES_DIR}/wlog.${ITER_ID} > ${RES_DIR}/result2.${ITER_ID}
if [ $? -ne 0 ]; then
  echo "verify_wlog failed."
  exit 1
fi
grep ^NG ${RES_DIR}/result2.${ITER_ID}

echo update oldest_lsid...
${BIN}/walbctl set_oldest_lsid --lsid $endLsid --wdev $WDEV

echo done
