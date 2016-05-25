import re
import datetime
import socket
import time
from os.path import basename


# --------------------------
# Carbon Server Details
# ---------------------------
CARBON_SERVER = '192.168.173.93'
CARBON_PORT = 2003

ipFile = "/Users/abhishek.choudhary/codebase/misc/hdfs/hadoop-root-datanode-gvs-neap-cmp12.neap.corp.telstra.com.log.2016-03-07"

textfile = open(ipFile, 'r')

logFileName = basename(ipFile)
serverRe = r'.*datanode-(.*)\.log.*'
result = re.match(serverRe, logFileName)
if result is not None:
    serverName = result.group(1).replace(".", "-")
else:
    serverName = "dummy-server"

serverName = "abhishek-try"
print serverName


# -----------------------------
# TIme Parsing From Logs
# -----------------------------
timeFormat = "%Y-%m-%d %H:%M:%S,%f"
timeFormatRegEx = '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}'

clientTraceString = "org.apache.hadoop.hdfs.server.datanode.DataNode.clienttrace"
pattern = r"(%s).*(?:%s:\s)(.*)" % (timeFormatRegEx, clientTraceString)


def getEpochTIme(ipTimeString):
    iptime = datetime.datetime.strptime(ipTimeString, timeFormat)
    utc_epoc = (iptime - datetime.datetime(1970, 1, 1)).total_seconds()
    return int(utc_epoc)

p = re.compile(pattern)
q = re.compile(r'(?P<key>\w+):\s(?P<value>[\w\s\.:/\-]+),?')


def sendToGraphite(serverName, operation, metrics_type, enteries):
    sock = socket.socket()
    sock.connect((CARBON_SERVER, CARBON_PORT))
    for keys in sorted(enteries):
        message = '%s.%s.%s %f %d\n' % (
            serverName, operation, metrics_type, long(enteries[keys]), keys)
        sock.sendall(message)
    sock.close()

enteries = []
for line in textfile:
    if clientTraceString in line:
        result = p.match(line)
        if result is not None:
            timeStamp = getEpochTIme(result.group(1))
            values = result.group(2)
            key_value = q.findall(values)
            for kvTuple in key_value:
                if kvTuple[0] == "src":
                    sourceIp = kvTuple[1].split(
                        ":")[0].replace("/", "").strip()
                elif kvTuple[0] == "dest":
                    destIp = kvTuple[1].split(":")[0].replace("/", "").strip()
                elif kvTuple[0] == "bytes":
                    bytesProcessed = kvTuple[1]
                elif kvTuple[0] == "op":
                    operationCategory = kvTuple[1]
                elif kvTuple[0] == "duration":
                    duration = kvTuple[1]
                elif kvTuple[0] == "blockid":
                    bpid = kvTuple[1]
            enteries.append((timeStamp, sourceIp, destIp,
                             operationCategory, bytesProcessed, duration, bpid))
textfile.close()
hdfsRead = {}
hdfsWrite = {}
hdfsReadMax = {}

maxDuration = 0
BP = None

for entry in enteries:
    timeStamp = entry[0]
    op = entry[3]
    bytesProcessed = entry[4]
    duration = long(long(entry[5]) / 1000000)
    block = entry[6]
    if op == "HDFS_READ":
        print "Byte Read :" + bytesProcessed + " Duration :" + entry[5] + " BP ID: " + block

        if long(entry[5]) > maxDuration:
            maxDuration = long(entry[5])
            BP = block

        if timeStamp in hdfsRead:
            hdfsRead[timeStamp] = hdfsRead[timeStamp] + 1

            if duration > hdfsReadMax[timeStamp]:
                hdfsReadMax[timeStamp] = duration

        else:
            hdfsRead[timeStamp] = 1
            hdfsReadMax[timeStamp] = duration

    elif op == "HDFS_WRITE":
        if timeStamp in hdfsWrite:
            hdfsWrite[timeStamp] = hdfsWrite[timeStamp] + 1
        else:
            hdfsWrite[timeStamp] = 1

print "Total Enteries : " + str(len(enteries))
print "Sending HDFS-READ Data " + str(len(hdfsRead))
sendToGraphite(serverName, "HDFS-READ", "count", hdfsRead)
sendToGraphite(serverName, "HDFS-READ", "max-duration", hdfsReadMax)

print "Sending HDFS-WRITE Data " + str(len(hdfsWrite))
sendToGraphite(serverName, "HDFS-WRITE", "count", hdfsWrite)

print "----------------------"
print maxDuration
print BP
