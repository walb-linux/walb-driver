#!/bin/sh

test_binaries=$@

WORKDIR=$(cd $(dirname $0); pwd)
cd $WORKDIR

[ -d tmp ] || mkdir tmp
TS=`date +%Y%m%d-%H%M%S`


succeeded=0
total=0
for exe in $test_binaries; do

    echo -n running $exe ...
    exefile=$(basename $exe)
    ./$exe > tmp/$exefile.$TS.log 2>&1
    if [ $? -ne 0 ]; then
	echo "failed"
    else
	echo "success"
	succeeded=`expr $succeeded + 1`
    fi
    total=`expr $total + 1`
done

echo Test of $succeeded/$total passed.

if [ $succeeded -eq $total ]; then
    exit 0;
else
    exit 1;
fi
