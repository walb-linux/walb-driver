#!/bin/sh

sh prepare.sh
sleep 1
i=0
while [ $i -lt 100 ]; do
  #sh iter.sh $i
  sh iter.sh 0
  i=`expr $i + 1`
done

