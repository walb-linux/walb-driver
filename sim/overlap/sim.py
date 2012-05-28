#!/usr/bin/env python

"""
Overlapping detection simulator.

"""

import sys
import random


class Req:
    """
    A request.
    
    """

    BEGIN, WAITING, SUBMITTED, END = range(0, 4)
    
    """
    States.
    
    """
    @classmethod
    def generate(clz, iD, diskSize):
        """
        iD :: int
        diskSize :: int
        
        return :: Req
        
        """
        addr = random.randint(0, diskSize - 1)
        size = random.randint(1, diskSize - addr)
        assert(addr + size <= diskSize)
        return Req(iD, addr, size)
    
    def __init__(self, iD, addr, size):
        self.iD = iD
        self.addr = addr
        self.size = size
        self.state = Req.BEGIN
        self.count = -1

    def __str__(self):
        return "(%d, %d, %d, " % (self.iD, self.addr, self.size) + \
            {0:'BEGIN', 1:'WAITING', 2:'SUBMITTED', 3:'END'}[self.state] \
            + ", %d)" % self.count

def isOverlap(req0, req1):
    """
    True if req0 and req1 is overlapping.
    
    req0 :: Req
    req1 :: Req

    """
    assert(isinstance(req0, Req))
    assert(isinstance(req1, Req))
    assert(req0.iD != req1.iD)
    assert(req0 is not req1)
    return (req0.addr + req0.size > req1.addr and
            req1.addr + req1.size > req0.addr)
        
class OverlappingData:

    def __init__(self):
        self.data = {} # iD -> Req

    def insert(self, req):
        assert(isinstance(req, Req))
        self.data[req.iD] = req

    def delete(self, req):
        assert(isinstance(req, Req))
        del self.data[req.iD]

    def getOverlapping(self, req):
        """
        req :: Req
        return :: [Req]

        """
        assert(isinstance(req, Req))
        return filter(lambda rq: isOverlap(req, rq), self.data.values())

    def check(self, req):
        """
        There is no overlapping requests.

        """
        l = filter(lambda rq: rq.iD < req.iD and isOverlap(req, rq), self.data.values())
        if l != []:
            print 'There are still overlapping requests:'
            for rq in l:
                print rq
            sys.exit(1)


class Operation:
    """
    """
    INSERT, DELETE = range(0, 2)
    
    def __init__(self, req, op):
        """
        req :: Req
        op :: int

        """
        self.req = req
        self.op = op

    def execute(self, od):
        """
        od :: OverlappingData
        return :: None
        
        """
        assert(isinstance(od, OverlappingData))
        if self.op == Operation.INSERT:
            assert(self.req.state == Req.BEGIN)
            overlappingL = od.getOverlapping(self.req)
            c = len(overlappingL)
            self.req.count = c
            if c == 0:
                self.req.state = Req.SUBMITTED
            else:
                self.req.state = Req.WAITING
            od.insert(self.req)
        elif self.op == Operation.DELETE:
            assert(self.req.state == Req.SUBMITTED)
            od.delete(self.req)
            overlappingL = od.getOverlapping(self.req)
            for req in overlappingL:
                req.count -= 1
                assert(req.state == Req.WAITING)
                if req.count == 0:
                    req.state = Req.SUBMITTED
                    od.check(req) #assertion
            self.req.state = Req.END
        else:
            assert(False)
        pass

    def __str__(self):
        return {0:"INS", 1:"DEL"}[self.op] + " " + str(self.req)
    
def generateOpCandidates(reqL):
    """
    reqL :: [Req]
    return :: [Operation]

    """
    opL = []
    isFirst = True
    for req in reqL:
        if req.state == Req.BEGIN and isFirst:
            opL.append(Operation(req, Operation.INSERT))
            isFirst = False
        elif req.state == Req.SUBMITTED:
            opL.append(Operation(req, Operation.DELETE))
    return opL
    

def simulate(diskSize, numReq):
    """
    Simulate overlapping data.
    
    diskSize :: int
      Disk size.
    numReq :: int
      Number of requests to execute.
    
    """
    assert(isinstance(diskSize, int))
    assert(diskSize > 0)
    assert(isinstance(numReq, int))
    assert(numReq > 0)

    reqL = []
    for i in xrange(0, numReq):
        reqL.append(Req.generate(i, diskSize))

    od = OverlappingData()

    opL = generateOpCandidates(reqL)
    while opL != []:
        op = random.choice(opL)
        before = str(op) #debug
        op.execute(od)
        after = str(op) #debug
        print before, '\t', after
        opL = generateOpCandidates(reqL)
        
    # now editing

    pass
    

def main():
    diskSize = int(sys.argv[1])
    numReq = int(sys.argv[2])
    simulate(diskSize, numReq)
    pass

if __name__ == '__main__':
    main()
