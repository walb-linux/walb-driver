#/usr/bin/env python

__all__ = ['Request', 'Pack', 'isOverlap', 'createPlugPackList',
           'loadPlugPackList', 'dumpPlugPackList', 'printPlugPackList', 
           'DiskImage', 'loadDiskImage', 'dumpDiskImage', 'Operation',
           'forAllAddrInPack']

import cPickle
import random
import copy


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
        self.__data = [0] * size
        self.__rid = -1

    def addr(self):
        return self.__addr
    def size(self):
        return self.__size
    def isWrite(self):
        return self.__isWrite

    def data(self):
        return self.__data

    def executeIO(self, diskImage):
        """
        execute IO.
        diskImage :: DiskImage
        return :: None
        
        """
        assert(isinstance(diskImage, DiskImage))
        i0 = self.addr()
        i1 = self.addr() + self.size()
        if self.isWrite():
            diskImage.disk()[i0:i1] = self.data()[0:]
        else:
            self.data()[0:] = diskImage.disk()[i0:i1]

    def rid(self):
        return self.__rid
    
    def setRid(self, rid):
        assert(isinstance(rid, int))
        assert(rid >= 0)
        self.__rid = rid

    def __str__(self):
        return str((self.rid(), self.addr(), self.size(),
                    self.isWrite(), self.data()))

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
        self.__pid = -1

    def add(self, req):
        assert(isinstance(req, Request))
        assert(req.isWrite() == self.__isWrite)
        self.__reqL.append(req)

    def getL(self):
        """
        return :: [Request]
        
        """
        return self.__reqL

    def isWrite(self):
        return self.__isWrite

    def pid(self):
        return self.__pid

    def setPid(self, pid):
        assert(isinstance(pid, int))
        assert(pid >= 0)
        self.__pid = pid

    def __str__(self):
        return str(self.pid()) + ' [' + ', '.join(map(str, self.getL())) + ']'

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


def hasAddr(a, addr):
    """
    a :: Request | Pack | DiskImage
    addr :: int
        block address.
    return :: bool

    """
    def isDiskImageHasAddr(diskImage):
        assert(isinstance(diskImage, DiskImage))
        return 0 <= addr and addr < len(diskImage.data())
    def isReqHasAddr(req):
        assert(isinstance(req, Request))
        return req.addr() <= addr and addr < req.addr() + req.size()
    def isPackHasAddr(pack):
        assert(isinstance(pack, Pack))
        for req in pack.getL():
            if isReqHasAddr(req):
                return True
        return False

    if isinstance(a, DiskImage):
        return isDiskImageHasAddr(a)
    elif isinstance(a, Pack):
        return isPackHasAddr(a)
    elif isinstance(a, Request):
        return isReqHasAddr(a)
    else:
        assert(False)


def isOverlapAt(a, b, addr):
    """
    a :: Request | Pack
    b :; Request | Pack
    addr :: int
        block address.
    return :: bool

    """
    assert(isinstance(a, Request) or isinstance(a, Pack))
    assert(isinstance(b, Request) or isinstance(b, Pack))
    assert(isinstance(addr, int))
    assert(addr >= 0)

    return hasAddr(a) and hasAddr(b)

def forAllAddrInReq(req):
    """
    req :: Request
    return :: generator(int)
        addr generator.
    
    """
    assert(isinstance(req, Request))
    return xrange(req.addr(), req.addr() + req.size())

def forAllAddrInPack(pack):
    """
    pack :: Pack
    return :: generator(int)
        addr generator.

    """
    assert(isinstance(pack, Pack))
    for req in pack.getL():
        for addr in forAllAddrInReq(req):
            yield addr

def dataAt(a, addr):
    """
    a :: Request | Pack | DiskImage
    addr :: int
    return :: int
        data of the block.
    
    """
    def getOverlapReq(pack, addr):
        for req in pack.getL():
            if hasAddr(req, addr):
                return req
        raise "does not overlap."
    def getDataFromReq(req, addr):
        return req.data()[addr - req.addr()]
    
    assert(isinstance(addr, int))
    if isinstance(a, DiskImage):
        return a.disk()[addr]
    elif isinstance(a, Request):
        assert(hasAddr(a, addr))
        return getDataFromReq(a, addr)
    else:
        assert(isinstance(a, Pack))
        assert(hasAddr(a, addr))
        return getDataFromReq(getOverlapReq(a, addr), addr)


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

class DiskImage:

    def __init__(self, size):
        assert(isinstance(size, int))
        assert(size > 0)
        self.__diskImage = [0] * size

    def disk(self):
        return self.__diskImage

    def initRandomly(self):
        size = len(self.__diskImage)
        for i in xrange(0, size):
            self.disk()[i] = random.randint(0, 255)

    def clone(self):
        """
        Clone an object.
        
        """
        size = len(self.__diskImage)
        ret = DiskImage(size)
        ret.__diskImage = copy.copy(self.__diskImage)
        assert(isinstance(ret, DiskImage))
        return ret


def loadDiskImage(f):
    """
    f :: cPickle file object.
    return :: DiskImage
    
    """
    diskImage = cPickle.load(f)
    assert(isinstance(diskImage, DiskImage))
    return diskImage

def dumpDiskImage(diskImage, f):
    """
    diskIamge :: DiskImage
    f :: file object.
    return :: None
    
    """
    assert(isinstance(diskImage, DiskImage))
    cPickle.dump(diskImage, f, -1)

def getDiskImageDiff(lhs, rhs):
    """
    lhs :: DiskImage
    rhs :: DiskImage
    return :: [(addr :: int, lhs :: int, rhs :: int)]

    """
    assert(isinstance(lhs, DiskImage))
    assert(isinstance(rhs, DiskImage))
    lL = lhs.disk()
    rL = rhs.disk()
    assert(len(lL) == len(rL))

    def g():
        i = 0
        while True:
            yield i
            i += 1

    def f((i, l, r)):
        assert(isinstance(l, int))
        assert(isinstance(r, int))
        return l != r
    
    return filter(f, zip(g(), lL, rL))

class Operation:
    """
    Base class for operation.

    """
    def __init__(self, pack, opId):

        assert(isinstance(pack, Pack))
        assert(isinstance(opId, int))
        assert(opId >= 0)
        
        self.__pack = pack
        self.__opId = opId

    def opId(self):
        return self.__opId

    def pack(self):
        return self.__pack

    def __str__(self):
        raise "You must use subclass instance."

