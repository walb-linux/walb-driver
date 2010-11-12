#!/bin/sh

sudo dmesg -n7
sudo insmod walb.ko ddev_major=9 ddev_minor=10 ndevices=1
