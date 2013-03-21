#!/bin/sh

# LDEV size should be 3x of DDEV.
# ex. DDEV size 32MB, LDEV size 96MB.
LDEV=/dev/data/test-log
DDEV=/dev/data/test-data
WDEV=/dev/walb/0
WLDEV=/dev/walb/L0

BIN=..
MODULE_DIR=..
MODULE_FILE=walb-mod-fast-ol.ko


