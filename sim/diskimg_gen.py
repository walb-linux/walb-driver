#!/usr/bin/env python

"""
Disk image generator.

Input: size :: integer
Output: disk image :: [int]

"""

import sys
import random
import json

def main():
    diskSize = int(sys.argv[1])
    diskImg = []
    for i in xrange(0, diskSize):
        diskImg.append(random.randint(0, 255))
    json.dump(diskImg, sys.stdout, indent=4)

if __name__ == "__main__":
    main()
