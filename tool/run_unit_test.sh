#!/bin/sh

test_binaries=$@

[ -d tmp ] || mkdir tmp
TS=`date +%Y%m%d-%H%M%S`

for exe in $test_binaries; do

    echo running $exe ...
    ./$exe > tmp/$exe.$TS.log 2>&1
    if [ $? -ne 0 ]; then echo "test failed " $exe; exit 1; fi
done
