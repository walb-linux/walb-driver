#!/usr/bin/env python

"""
Request generator.

This program generate a request list randomly.
with a given disk size

For simplicity, block size is 1 byte,
IO size is small data and

"""

import sys
import json
import random
import cPickle

from walb_util import *

class Config:

    def __init__(self, diskSize, minReqSize, maxReqSize, minPlugSize, maxPlugSize, writePct):
        """
        diskSize :: int
        minReqSize :: int
        maxReqSize :: int
        minPlugSize :: int
        maxPlugSize :: int
        writePct :: int

        """
        assert(isinstance(diskSize, int))
        assert(isinstance(minReqSize, int))
        assert(isinstance(maxReqSize, int))
        assert(isinstance(minPlugSize, int))
        assert(isinstance(maxPlugSize, int))
        assert(isinstance(writePct, int))
        assert(diskSize > 0)
        assert(minReqSize > 0)
        assert(maxReqSize >= minReqSize)
        assert(diskSize >= maxReqSize)
        assert(minPlugSize > 0)
        assert(maxPlugSize >= minPlugSize)
        assert(writePct >= 0)
        assert(writePct <= 100)

        self.diskSize = diskSize
        self.minReqSize = minReqSize
        self.maxReqSize = maxReqSize
        self.minPlugSize = minPlugSize
        self.maxPlugSize = maxPlugSize
        self.writePct = writePct

def generateRequestList(config):
    """
    config :: Config
    
    return :: [Request]
    
    """
    assert(isinstance(config, Config))
    
    plugSize = random.randint(config.minPlugSize, config.maxPlugSize)

    reqL = []
    def generateReq():
        size = random.randint(config.minReqSize, config.maxReqSize)
        assert(config.diskSize - size >= 0)
        addr = random.randint(0, config.diskSize - size)
        assert(addr + size <= config.diskSize)
        isWrite = random.randint(0, 99) < config.writePct
        return Request(addr, size, isWrite)
    
    for i in xrange(0, plugSize):
        reqL.append(generateReq())

    return reqL


def main():
    if len(sys.argv) < 8:
        print "%s numPlug diskSize minReqSize maxReqSize minPlugSize maxPlugSize writePct" % sys.argv[0]
        exit(1)
    
    numPlug = int(sys.argv[1])
    diskSize = int(sys.argv[2])
    minReqSize = int(sys.argv[3])
    maxReqSize = int(sys.argv[4])
    minPlugSize = int(sys.argv[5])
    maxPlugSize = int(sys.argv[6])
    writePct = int(sys.argv[7]) # percentage.

    # Request type is (addr :: int, size :: int, isWrite :: bool)

    config = Config(diskSize, minReqSize, maxReqSize, minPlugSize, maxPlugSize, writePct)
    
    plugL = []
    for i in xrange(0, numPlug):
        plugL.append(generateRequestList(config))

    #print plugL #debug
    plugPackL = createPlugPackList(plugL)
    #json.dump(packL, sys.stdout, indent=4)
    printPlugPackList(plugPackL)
    #dumpPackList(packL, sys.stdout)

if __name__ == "__main__":
    main()
