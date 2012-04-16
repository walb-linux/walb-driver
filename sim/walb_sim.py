#/usr/bin/env python

"""
WalB Algorithm Simulator.

Input: DiskImage, [[Pack]], nPlug :: int, nLoop :: int.
Output: DiskImage, [([(packId :: int, op :: int)], DiskImage)]

(1) Create a correct result disk image without operation sort.
(2) Execute 'nLoop' times.
  (2.1) Generate an operation sequence randomly.
  (2.2) Execute the operation sequence.
  (2.3) Compare the disk image.
  (2.4) Compare the read result (Not supported yet).
  
"""

import sys
import random
from walb_util import loadDiskImage, loadPlugPackList, \
    DiskImage, getDiskImageDiff, printPlugPackList
#from walb_easy import PackStateManager, ReadPackState
#from walb_easy import PackStateManager
from walb_fast import PackStateManager

def simulate(diskImage, plugPackList, nPlug,
             shuffle=True, crashPctPerTick=0):
    """
    Simulate IO sequence with WalB constraints.

    diskImage :: DiskImage
       This is not unchanged.
    plugPackList :: const [[Pack]]
    nPlug :: const int
    shuffule :: bool
        If True, an operation is randomly chosen from available candidaates,
        else, the first candidate is chosen every time.
    crashPctPerTick :: int
        If it is more than 0, the simulator will randomly stop simulation
        in crashPctPerTick percentage at every execution tick.
    
    return :: [Operation], PackStateManager
        Operation :: (packId :: int, op :: int)
    
    """
    assert(isinstance(diskImage, DiskImage))
    assert(isinstance(plugPackList, list))
    assert(isinstance(nPlug, int))
    assert(isinstance(shuffle, bool))
    assert(isinstance(crashPctPerTick, int))
    assert(crashPctPerTick >= 0)
    
    mgr = PackStateManager(diskImage, plugPackList)

    numOp = 0
    opHistoryL = []
    candidates = mgr.getCandidates(nPlug)
    while candidates != []:
        #print candidates  #debug
        #print len(candidates)  #debug
        if shuffle:
            packId, op = random.choice(candidates)
        else:
            packId, op = candidates[0]
        changed = mgr.execute(packId, op)
        numOp += 1
        # if changed:
        #     print "END %d numOp(%d)" % (mgr.firstNotEndedPackId(), numOp)  #debug
        opHistoryL.append((packId, op))
        candidates = mgr.getCandidates(nPlug)

        randV = random.randint(0, 99)
        if randV < crashPctPerTick:
            """
            Stop simulation immediately.
            
            """
            break

    return opHistoryL, mgr


def main():
    if len(sys.argv) < 5:
        print "Usage: %s [diskImage.cpickle] [plugPackList.cpickle] " \
            "[nPlug] [nLoop] ([crashPctPerTick])" % sys.argv[0]
        exit(1)

    f = open(sys.argv[1])
    diskImage = loadDiskImage(f)
    f.close()
    f = open(sys.argv[2])
    plugPackList = loadPlugPackList(f)
    f.close()
    nPlug = int(sys.argv[3])
    assert(nPlug > 0)
    nLoop = int(sys.argv[4])
    assert(nLoop > 0)
    if len(sys.argv) >= 6:
        crashPctPerTick = int(sys.argv[5])
    else:
        crashPctPerTick = 0

    printPlugPackList(plugPackList)

    print "loop 0 start"
    opHistoryL, mgr = simulate(diskImage, plugPackList, nPlug,
                               shuffle=False, crashPctPerTick=0)
    assert(getDiskImageDiff(mgr.vStorage(), mgr.rStorage()) == [])
    testDiskImage = mgr.rStorage()
    print "testStorage:", testDiskImage.disk()

    numCheckCrashRecovery = 0
    resMap = {}
    for loop in xrange(1, nLoop):
        print "loop %d start" % loop
        #print diskImage.disk()  #debug
        opHistoryL, mgr = simulate(diskImage, plugPackList, nPlug,
                                   shuffle=True, crashPctPerTick=crashPctPerTick)
        packId = mgr.doCrashRecovery()
        print packId
        
        diff = getDiskImageDiff(mgr.vStorage(), mgr.rStorage())
        if diff != []:
            print "ERROR", opHistoryL
            print "vStorage: ", mgr.vStorage().disk()
            print "rStorage: ", mgr.rStorage().disk()
            
        # Validate results.
        if packId == mgr.totalNumPacks():
            diskDiff = getDiskImageDiff(testDiskImage, mgr.rStorage())
            if diskDiff != []:
                print "ERROR", opHistoryL
                print "DIFF", diskDiff
                print "testStorage:", testDiskImage.disk()
                print "resStorage: ", mgr.rStorage().disk()
            # for packS in filter(lambda x:isinstance(x, ReadPackState), mgr.packStateList()):
            #     print packS.pack()
            #print tmpDiskImage.disk()  #debug
        else:
            if packId in resMap:
                numCheckCrashRecovery += 1
                diskDiff = getDiskImageDiff(resMap[packId], mgr.rStorage())
                if diskDiff != []:
                    print "ERROR", opHistoryL
                    print "DIFF", diskDiff
                    print "testStorage:", testDiskImage.disk()
                    print "resStorage: ", mgr.rStorage().disk()
            else:
                resMap[packId] = mgr.rStorage()
    print "numCheckCrashRecovery %d" % numCheckCrashRecovery
    pass

if __name__ == '__main__':
    main()
