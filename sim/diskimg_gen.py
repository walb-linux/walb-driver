#!/usr/bin/env python

"""
Disk image generator.

Input: size :: integer
Output: disk image :: [int] in cPickle format.

"""

import sys

from walb_util import dumpDiskImage, DiskImage

def main():
    if len(sys.argv) < 2:
        print "Usage: %s [disk size]" % sys.argv[0]
        exit(1)
    
    diskSize = int(sys.argv[1])
    diskImg = DiskImage(diskSize)
    diskImg.initRandomly()
    dumpDiskImage(diskImg, sys.stdout)

if __name__ == "__main__":
    main()
