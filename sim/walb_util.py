#/usr/bin/env python

__all__ = ['Request', 'Pack',
           'createPlugPackList', 'loadPlugPackList', 'dumpPlugPackList', 'printPlugPackList']

import random
import cPickle

class Request:
    """
    A request.
    
    """
    def __init__(self, addr, size, isWrite):
        assert(isinstance(addr, int))
        assert(isinstance(size, int))
        assert(isinstance(isWrite, bool))
        self.__addr = addr
        self.__size = size
        self.__isWrite = isWrite

    def addr(self):
        return self.__addr
    def size(self):
        return self.__size
    def isWrite(self):
        return self.__isWrite

    def __str__(self):
        return str((self.addr(), self.size(), self.isWrite()))

class Pack:
    """
    A pack.
    
    """

    def __init__(self, isWrite):
        """
        isWrite :: bool

        """
        assert(isinstance(isWrite, bool))
        self.__isWrite = isWrite
        self.__reqL = []

    def add(self, req):
        assert(isinstance(req, Request))
        assert(req.isWrite() == self.__isWrite)
        self.__reqL.append(req)

    def getL(self):
        return self.__reqL

    def isWrite(self):
        return self.__isWrite

    def __str__(self):
        return '[' + ', '.join(map(str, self.getL())) + ']'

def createWritePack():
    return Pack(True)

def createReadPack():
    return Pack(False)
    
def isOverlap(a, b):
    """
    a :: Request | Pack
    b :: Request | Pack
    return :: bool
    
    """
    def toL(reqOrPack):
        if isinstance(reqOrPack, Pack):
            aL = reqOrPack.getL()
        else:
            assert(isinstance(reqOrPack, Request))
            aL = [reqOrPack]
        return aL
        
    aL = toL(a)
    bL = toL(b)

    for req0, req1 in [(req0, req1) for req0 in aL for req1 in bL if req0 is not req1]:
        if not (req0.addr() + req0.size() <= req1.addr() or
                req1.addr() + req1.size() <= req0.addr()):
            return True
    return False


def createPlugPackList(plugReqList):
    """
    plugReqList :: [[Request]]
    return :: [[Pack]]
        
    """
    assert(isinstance(plugReqList, list))

    plugL = []
    for plug in plugReqList:
        packL = []
        assert(isinstance(plug, list))
        wpack = createWritePack()
        rpack = createReadPack()
        for req in plug:
            assert(isinstance(req, Request))
            if req.isWrite():
                if isOverlap(req, wpack):
                    packL.append(wpack)
                    wpack = createWritePack()
                wpack.add(req)
            else:
                if isOverlap(req, rpack):
                    packL.append(rpack)
                    rpack = createReadPack()
                rpack.add(req)
        if len(wpack.getL()) > 0:
            packL.append(wpack)
        if len(rpack.getL()) > 0:
            packL.append(rpack)
        plugL.append(packL)

    # assert no overlap inside each pack.
    for packL in plugL:
        for pack in packL:
            assert(not isOverlap(pack, pack))
            
    return plugL
    
def loadPlugPackList(f):
    """
    f :: cPickle file object.
    return :: [[Pack]]
    
    """
    return cPickle.load(f)

def dumpPlugPackList(plugPackList, f):
    """
    plugPackList :: [[Pack]]
    f :: file object.
    return :: None
    
    """
    assert(isinstance(plugPackList, list))
    cPickle.dump(plugPackList, f, -1)
        
def printPlugPackList(plugPackList):
    """
    Print pack list for debug.
    return :: None
    
    """
    for packList in plugPackList:
        print '['
        for pack in packList:
            assert(isinstance(pack, Pack))
            print pack
        print ']'
