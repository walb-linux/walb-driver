#!/usr/bin/env python

"""
WalB Fast Algorithm.

"""

__all__ = ['PackState', 'ReadPackState', 'WritePackState',
           'PackStateManager']

import bisect
from walb_util import Pack, isOverlap, DiskImage

class PackState:

    def __init__(self, pack, nBits):
        assert(isinstance(pack, Pack))
        self.__pack = pack
        assert(isinstance(nBits, int))
        assert(nBits > 0)
        self.__state = [False] * nBits

    def pack(self):
        return self.__pack
        
    def st(self, stateBit):
        return self.__state[stateBit]

    def setSt(self, stateBit):
        assert(not self.st(stateBit))
        self.__state[stateBit] = True

    def getCandidates(self, packStateList):
        """
        Get operation candidates.
        
        packStateList :: [PackState]
        return :: [op :: int]
        
        """
        raise "Not defined."

    def execute(self, op, vStorage, rStorage):
        """
        Execute an operation on a disk image.
        op :: int
            operation type.
        vStorage :: DiskImage
            virtual storage.
        rStorage :: DiskImage
            real storage.
            
        """
        raise "Not defined."
        
    def isEnd(self):
        """
        return :: bool
            True if no more operation candidates.
        
        """
        return all(self.__state)
        
    @classmethod
    def opStr(clazz, opId):
        """
        opId :: int
        return :: str
        
        """
        assert(isinstance(opId, int))
        assert(0 <= opId)
        assert(opId < clazz.N_OP)
        return clazz.OP_NAME[opId]


class ReadPackState(PackState):

    N_OP = 4
    SUBMIT, READ_VSTORAGE, COMPLETE, END_REQ = range(N_OP)
    OP_NAME = ['SUBMIT', 'READ_VSTORAGE', 'COMPLETE', 'END_REQ']
    
    def __init__(self, pack):
        """
        """
        PackState.__init__(self, pack, ReadPackState.N_OP)
        assert(not pack.isWrite())

    def getCandidates(self, packStateList):
        """
        packStateList :: [PackState]
           from oldestNotEnded to previous packs.
        
        return :: [int]
            Operation candidates.

        """
        assert(isinstance(packStateList, list))
        
        candidates = [
            (ReadPackState.SUBMIT,
             not self.st(ReadPackState.SUBMIT)),
            (ReadPackState.READ_VSTORAGE,
             not self.st(ReadPackState.READ_VSTORAGE) and self.st(ReadPackState.SUBMIT)),
            (ReadPackState.COMPLETE,
             not self.st(ReadPackState.COMPLETE) and self.st(ReadPackState.READ_VSTORAGE)),
            (ReadPackState.END_REQ,
             not self.st(ReadPackState.END_REQ) and self.st(ReadPackState.COMPLETE))
            ]
        
        def p((op, isReady)):
            return isReady
        def f((op, isReady)):
            return op
        return map(f, filter(p, candidates))
        
    def execute(self, op, vStorage, rStorage):
        assert(isinstance(op, int))
        assert(isinstance(vStorage, DiskImage))
        assert(isinstance(rStorage, DiskImage))

        assert(op >= 0)
        assert(op < ReadPackState.N_OP)
        self.setSt(op)
        
        if op == ReadPackState.READ_VSTORAGE:
            self.executeIO(vStorage)

    def executeIO(self, storage):
        assert(isinstance(storage, DiskImage))
        #print "executeIO READ ", self.pack().pid()
        for req in self.pack().getL():
            req.executeIO(storage)

    def isEnd(self):
        return self.st(ReadPackState.END_REQ)


class WritePackState(PackState):

    N_OP = 7
    SUBMIT_LPACK, COMPLETE_LPACK, WRITE_VSTORAGE, END_REQ, \
    SUBMIT_DPACK, WRITE_RSTORAGE, COMPLETE_DPACK = range(N_OP)
    OP_NAME = [
        'SUBMIT_LPACK', 'COMPLETE_LPACK', 'WRITE_VSTORAGE', 'END_REQ',
        'SUBMIT_DPACK', 'WRITE_RSTORAGE', 'COMPLETE_DPACK'
        ]
    
    def __init__(self, pack):
        """
        """
        PackState.__init__(self, pack, WritePackState.N_OP)
        assert(pack.isWrite())

    def isReadyToSubmitDpack(self, packStateList):
        """
        packStateList :: [PackState]
            from firstNotEnded to previous packs.

        return :: bool
            True if all conditions are satisfied to submit datapack.

        Two conditions are required.
        (1) Logpack(j) for all j s.t. j <= i is completed.
        (2) There is no overlap between writepack(i) and writepack(j)
            for all j s.t. i0 <= j < i
            where datapack(i0) is not completed but datapack(i0) is completed
            for all k s.t. k < i0.
        
        """
        assert(isinstance(packStateList, list))
        assert(len(packStateList) >= 0)

        for packState in packStateList:
            assert(isinstance(packState, PackState))
            if isinstance(packState, WritePackState):
                # condition (1) is already satisfied by WRITE_VSTORAGE constraints.
                # condition (2)
                if isOverlap(packState.pack(), self.pack()) and \
                        not packState.st(WritePackState.COMPLETE_DPACK):
                    return False

        return True

    def isReadyToWriteVstorage(self, packStateList):
        """
        packStateList :: [PackState]
            from firstNotEnded to previous packs.
        return :: bool
            True if all conditions are satisfied to write virtual storage.
            
        """
        assert(isinstance(packStateList, list))
        assert(len(packStateList) >= 0)

        for packState in packStateList:
            assert(isinstance(packState, PackState))
            if isinstance(packState, WritePackState):
                # condition (1)
                if not packState.st(WritePackState.WRITE_VSTORAGE):
                    return False
        return True
        
    def getCandidates(self, packStateList):
        """
        packStateList :: [PackState]
            from oldestNotEnded to previous packs.
        
        return :: [int]
            Operation candidates.

        """
        assert(isinstance(packStateList, list))
        
        candidates = [
            (WritePackState.SUBMIT_LPACK,
             not self.st(WritePackState.SUBMIT_LPACK)),
            (WritePackState.COMPLETE_LPACK,
             not self.st(WritePackState.COMPLETE_LPACK) and \
                 self.st(WritePackState.SUBMIT_LPACK)),
            (WritePackState.WRITE_VSTORAGE,
             not self.st(WritePackState.WRITE_VSTORAGE) and \
                 self.st(WritePackState.COMPLETE_LPACK) and \
                 self.isReadyToWriteVstorage(packStateList)),
            (WritePackState.SUBMIT_DPACK,
             not self.st(WritePackState.SUBMIT_DPACK) and \
                 self.st(WritePackState.WRITE_VSTORAGE) and \
                 self.isReadyToSubmitDpack(packStateList)),
            (WritePackState.WRITE_RSTORAGE,
             not self.st(WritePackState.WRITE_RSTORAGE) and \
                 self.st(WritePackState.SUBMIT_DPACK)),
            (WritePackState.COMPLETE_DPACK,
             not self.st(WritePackState.COMPLETE_DPACK) and \
                 self.st(WritePackState.WRITE_RSTORAGE)),
            (WritePackState.END_REQ,
             not self.st(WritePackState.END_REQ) and \
                 self.st(WritePackState.WRITE_VSTORAGE))
            ]

        def p((op, isReady)):
            return isReady
        def f((op, isReady)):
            return op
        return map(f, filter(p, candidates))

    def execute(self, op, vStorage, rStorage):

        assert(isinstance(op, int))
        assert(isinstance(vStorage, DiskImage))
        assert(isinstance(rStorage, DiskImage))

        assert(op >= 0)
        assert(op < WritePackState.N_OP)
        self.setSt(op)

        if op == WritePackState.WRITE_RSTORAGE:
            #print 'Write rStorage', self.pack()
            self.executeIO(rStorage)
        if op == WritePackState.WRITE_VSTORAGE:
            #print 'Write vStorage', self.pack()
            self.executeIO(vStorage)

    def executeIO(self, storage):
        assert(isinstance(storage, DiskImage))
        #print "executeIO WRITE", self.pack().pid()
        for req in self.pack().getL():
            req.executeIO(storage)

    def isEnd(self):
        return self.st(WritePackState.END_REQ) and \
            self.st(WritePackState.COMPLETE_DPACK)

def createPackState(pack):
    """
    pack :: Pack
    return :: WritePackState | ReadPackState
    
    """
    assert(isinstance(pack, Pack))
    if pack.isWrite():
        return WritePackState(pack)
    else:
        return ReadPackState(pack)
    
        
class PackStateManager:
    """
    Simulator.

    self.__packPlugList :: [[Pack]] # readonly.
    self.__packStateList :: [ReadPackState | WritePackState]
    self.__firstPackIdList :: sorted [packId :: int]
    self.__firstNotEndedPackId :: int
        Its all previous packs have been ended but it is not yet.

    self.__totalNumPacks :: int
    self.__totalNumReqs :: int

    self.__diskImage :: DiskImage
    
    """
    def __init__(self, diskImage, plugPackList):
        """
        diskImage :: DiskImage
        plugPackList :: [[Pack]]

        """
        assert(isinstance(diskImage, DiskImage))
        self.__fStorage = diskImage
        self.__vStorage = diskImage.clone()
        self.__rStorage = diskImage.clone()
        assert(isinstance(plugPackList, list))
        self.__plugPackList = plugPackList

        packStateList = []
        firstPackIdList = []
        pid = 0 # pack id.
        rid = 0 # request id.
        for packList in plugPackList:
            assert(len(packList) > 0)
            firstPackIdList.append(packList[0].pid())
            for pack in packList:
                pack.setPid(pid)
                pid += 1
                packStateList.append(createPackState(pack))
                for req in pack.getL():
                    req.setRid(rid)
                    rid += 1

        self.__totalNumPacks = pid
        self.__totalNumReqs = rid
        self.__packStateList = packStateList
        self.__firstPackIdList = firstPackIdList

        self.setFirstNotEndedPackId(0)

    def vStorage(self):
        return self.__vStorage

    def rStorage(self):
        return self.__rStorage

    def getPlugId(self, packId):
        """
        Convert packId to plugId.

        """
        assert(isinstance(packId, int))
        assert(packId >= 0)
        ary = self.__firstPackIdList
        i = bisect.bisect_right(ary, packId)
        if i == 0:
            raise ValueError
        return i - 1

    def getUpperPackId(self, plugId):
        """

        """
        assert(isinstance(plugId, int))
        assert(plugId >= 0)
        ary = self.__firstPackIdList
        if plugId < len(ary):
            return ary[plugId]
        else:
            return self.__totalNumPacks

    def packStateList(self):
        return self.__packStateList

    def firstNotEndedPackId(self):
        return self.__firstNotEndedPackId

    def setFirstNotEndedPackId(self, packId):
        assert(isinstance(packId, int))
        assert(packId >= 0)
        self.__firstNotEndedPackId = packId

    def getCandidates(self, nPlug):
        """
        Get operation candidates.
        
        nPlug :: int
            Number of plug to search for.
        
        return :: [(packId :: int, op :: int)]
            If [], all operations have been finished.

        """
        assert(isinstance(nPlug, int))
        assert(nPlug > 0)
        
        packId0 = self.firstNotEndedPackId()
        plugId0 = self.getPlugId(packId0)
        packId1 = self.getUpperPackId(plugId0 + nPlug)

        targetPackStateList = self.packStateList()[packId0:packId1]

        ret = []
        for packState in targetPackStateList:
            packId = packState.pack().pid()
            assert(packId0 <= packId)
            assert(packId < packId1)
            for op in packState.getCandidates(self.packStateList()[packId0:packId]):
                ret.append((packId, op))

        return ret

    def execute(self, packId, op):
        """
        Execute specified operation at packId.

        return :: bool
            True if firstNotEndedPackId() has been changed.
        
        """
        assert(isinstance(packId, int))
        assert(isinstance(op, int))

        packState = self.packStateList()[packId]
        packState.execute(op, self.vStorage(), self.rStorage())
        packId = packState.pack().pid()

        # Change firstNotEndedPackId if needed.
        ret = False
        if packState.isEnd():
            packId0 = self.firstNotEndedPackId()
            if all(map(lambda x:x.isEnd(), self.packStateList()[packId0:packId])):
                #print map(lambda x:x.isEnd(), self.packStateList()[0:])  #debug
                packId1 = packId + 1
                while packId1 < self.__totalNumPacks:
                    packS = self.packStateList()[packId1]
                    if not packS.isEnd():
                        break
                    packId1 += 1
                self.setFirstNotEndedPackId(packId1)
                #print 'firstNotEndedPackId %d to %d.' % (packId, packId1)
                ret = True
        return ret

    def recovery(self):
        """
        Redo all write requests that is not ended but its logpack written.
        getDiskImageDiff(self.vStorage(), self.rStorage()) will be [].
        
        """
        raise 'NOT YET IMPLEMENTATION'
