#!/bin/sh

sudo dmesg -n7
sudo insmod walb.ko ddev_major=9 ddev_minor=10 ldev_major=9 ldev_minor=11 ndevices=1
