#!/usr/bin/env python
import sys

def Usage():
    print ("Usage:\n"
          "      Python AutLogParser.py input \n      ")

## line  key value
#  key=value
def getValueByKey(line, key):
    idx = line.find(key)
    end = line.find(" ", idx)
    end2 = line.find("  ", idx)
    okend = end
    if (end > end2):
        okend = end2
    print ("parse key:%s , value:%s"%(key, line[idx:okend]))
    return line[idx:okend]

def getDict(x):
    dirck = {}
    segs_list = x.split('\t')

    for item in segs_list:
        segs = item.split('=')
        if (len(segs) < 2):
           continue
        dirck[segs[0]] = segs[1]
    return dirck


def parseId(x):
    xx2 = getValueByKey(x, "job_")
    print("parseapp: " + xx2.strip())
    return xx2.strip()


def parseC(x):
    vc = JobItem()
    kvpair = getDict(x)


    vc.jobid = "NOT_FOUND"
    if (kvpair.has_key("jobId")):
        vc.jobid = kvpair.get("jobId")

    vc.ugi = "unknow"
    if (kvpair.has_key("ugi")):
        vc.ugi = kvpair.get("ugi")
    vc.start = x[1:20]
    vc.end = x[1:20]
    vc.open = 0
    vc.create = 0
    vc.rname = 0
    vc.getfileinfo = 0
    vc.listStatus = 0
    vc.unknow= 0
    opt = "unknow"

    if (kvpair.has_key("cmd")):
        opt = kvpair.get("cmd")

    if opt == "open":
        vc.open = 1
    elif opt == "create":
        vc.create = 1
    elif opt == "getfileinfo":
        vc.rname = 1
    elif opt == "rname":
        vc.getfileinfo = 1
    elif opt == "liststats":
        vc.liststatus = 1
    else:
        vc.unknow = 1
    
    return vc



#jobid  ugi  start  end  open create rname getfileinfo  liststats
class JobItem:
    def __init__(self):
        self.jobid = ""
        self.ugi = ""
        self.start = ""
        self.end = ""
        self.open = 0
        self.getfileinfo = 0
        self.rname = 0
        self.create = 0
        self.listStatus = 0
        self.unknow = 0

def toString(self):
    print ("JobID=%s, ugi=%s, start=%s, end=%s, open=%d, getfileinfo=%d, rname=%d, create=%d, listStatus=%d, unknow=%d"
    %( self.jobid, self.ugi, self.start, self.end, self.open, self.getfileinfo, self.rname, self.create, self.listStatus, self.unknow))

def parseLine(line, host2container):
    vc = parseC(line)

    if vc.jobid in host2container:
        hc = host2container.get(vc.jobid)
        hc.end = vc.end
        hc.ugi = vc.ugi
        hc.open += vc.open
        hc.getfileinfo += vc.getfileinfo
        hc.rname += vc.rname
        hc.create += vc.create
        hc.listStatus += vc.listStatus
        hc.unknow += vc.unknow
        host2container[hc.jobid] = hc
    else:
        host2container[vc.jobid] = vc


def analyzer(f):
    # host, JobItem
    host2container=dict()

    xf = open(f, 'r')
    while True:
        line = xf.readline()
        if not line:
            break;
        parseLine(line, host2container)
        #containerid=

    for (k, v) in host2container.iteritems():
        print toString(v)


def main(argv):
    for arg in argv:
        print arg
    Usage()
    test="[2017-01-11T06:43:19.135+08:00] [INFO] namenode.FSNamesystem.audit.logAuditEvent(FSNamesystem.java 7190) [IPC Server handler 106 on 8020] : allowed=true    ugi=mart_cmo (auth:SIMPLE)  erp=yarn    jobId=job_1484087550239_1614    ip=/172.19.166.74   cmd=open    src=/tmp/hive/mart_cmo/b8966a0b-83bc-4488-a6fa-a7597dac6fab/hive_2017-01-11_06-29-16_710_592227212358736427-1/-mr-10038/413c5e0a-3b7e-4cc5-b54b-ead6ebe9a3ad/map.xml    dst=null    perm=null"
    #parseId(test)
    #print parseC(test)
    #host2container=dict()
    #parseLine(test, host2container)
    
    #for (k, v) in host2container.iteritems():
    #    print toString(v)

    path = "/data1/hadoop-audit-logs_bak/hdfs-audit.log.8"
    analyzer(path)


if __name__ == "__main__":
    main(sys.argv)
