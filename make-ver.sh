#!/bin/sh

SCRIPT_DIR=$(cd $(dirname $0);pwd)

version_str=$(cat ${SCRIPT_DIR}/VERSION)

pure_version_str=$(echo $version_str |cut -f 1 -d -)

v1=$(echo $pure_version_str |cut -f 1 -d .)
v2=$(echo $pure_version_str |cut -f 2 -d .)
v3=$(echo $pure_version_str |cut -f 3 -d .)

echo "#define WALB_VERSION ((${v1} << 16) + (${v2} << 8) + ${v3})"
echo "#define WALB_VERSION_STR \"${version_str}\""
