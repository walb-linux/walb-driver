#!/bin/sh

sudo dmesg -n7
sudo insmod walb.ko ddev_major=252 ddev_minor=32 ndevices=1
