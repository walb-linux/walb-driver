#!/bin/sh

SCRIPT_DIR=$(cd $(dirname $0);pwd)

V1=$(cat ${SCRIPT_DIR}/VERSION |cut -f 1 -d .)
V2=$(cat ${SCRIPT_DIR}/VERSION |cut -f 2 -d .)
V3=$(cat ${SCRIPT_DIR}/VERSION |cut -f 3 -d .)

echo "#define WALB_VERSION ((${V1} << 16) + (${V2} << 8) + ${V3})"
echo "#define WALB_VERSION_STR \"${V1}.${V2}.${V3}\""
