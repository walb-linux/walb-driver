#!/bin/sh

. ./common.sh

sudo dmesg -C
sudo dmesg -n7
#sudo /etc/init.d/rsyslog restart
sudo insmod ${MODULE_DIR}/${MODULE_FILE}
sudo $BIN/walbctl format_ldev --ldev $LDEV --ddev $DDEV --n_snap 100
sudo $BIN/walbctl create_wdev --ldev $LDEV --ddev $DDEV
