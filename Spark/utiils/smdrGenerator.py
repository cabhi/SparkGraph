import sys
import pexpect
import os
import json
import logging
import time
import multiprocessing
import random
import math
import commands
try:
    import cPickle as pickle
except:
    import pickle

os.chdir(os.path.dirname(os.path.realpath(__file__)))
path = os.getcwd()
libPath = "%s/../lib" %(path)

sys.path.append(libPath)

#from pyasn1 import debug
from pyasn1.type import univ, namedtype
from pyasn1.codec.ber import encoder, decoder
from smdrGrammar_collector import *
#from smdrFormat_1 import * 
from optparse import OptionParser


logger = logging.getLogger('SMDR_GENERATOR')
logger.setLevel(logging.DEBUG)
#Check if the folder exists else create folder
if (not os.path.exists("/data/generator")):
    try:
        os.makedirs("/data/generator")
    except:
        os.mkdir("/data/generator")
fh = logging.FileHandler('/data/generator/smdrgenerationlog.log')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)

logger.addHandler(ch)
logger.addHandler(fh)


class Generator:
    def __init__(self):
        #self.grammerFile = grammerFile
        self.productIB = []
        self.messageStatus = {1:'message-enroute', 2:'message-delevered', 3:'message-expired', 4:'message-deleted', 5:'message-undeliverable', 6:'message-accepted', 7:'message-unknown', 8:'message-rejected'}
        self.cdrType = {1:'cdr-o-side', 2:'cdr-t-side'}
        self.recordType = {1:'record-sms-ao', 2:'record-sms-mo', 3:'record-sms-inc-o', 4:'record-sms-at', 5:'record-sms-mt', 6:'record-sms-inc-t', 7:'record-sms-notify'}
        self.cdrStatus = {0:'status-delievered', 2:'status-deleted', 7:'status-undeleiverable', 8:'status-passed-on', 9:'status-rejected'}
        self.addressType = {0:'addtype-unknown', 1:'addtype-internationl', 2:'addtype-national', 3:'addtype-networkSpecific', 4:'addtype-subscriberNumber', 5:'addtype-alphanumeric', 6:'addtype-abbreviated', 7:'addtype-other'}
        self.npi = {0:'npi-unknown', 1:'npi-ISDN-e-164', 6:'npi-landMobile', 8:'npi-national', 9:'npi-private', 14:'npi-internal-IP', 15:'npi-others'}
        self.transparentId = {0:'tp-pid',1:'protocol-id'}
        self.loadIBToDict()

    def loadIBToDict(self):
        fileHandler = open("ibMap.json",'r')
        ibDict = json.load(fileHandler)
        for key in ibDict.keys():
            if key == "product":
                fh = open("%s" % ibDict[key], 'r')
                temp = fh.readlines()
                for l in temp:
                    self.productIB.append(l.strip())
                fh.close()

    def random_with_N_digits(self, n):
        range_start = 10**(n-1)
        range_end = (10**n)-1
        return random.randint(range_start, range_end)

    def random_with_N_digits1(self, n, rn):
        range_start = 10**(n-1)
        range_end = (10**n)-1
        num = random.randint(range_start, range_end)
        # HACK!! Can go into infinite loop if random generator generates same number again and again 
        while (num == rn):
            num = random.randint(range_start, range_end)
        return num

    def setAddress(self, msisdnNumber, rand):
        address = Address()

        address.setComponentByName('addressType', self.addressType[rand.choice(self.addressType.keys())])
        address.setComponentByName('npi', (self.npi[rand.choice(self.npi.keys())]))
        address.setComponentByName('addressDigit', str(int(self.random_with_N_digits(15))))
        return address

    def convertTS(self, ets):
        t = time.strftime('%Y%m%d%H%M%S', time.localtime(ets))
        ts = str(t) + '000-800'
        return ts

    def encodeData(self, recDict, st, et):
        toplevel = TopLevel()
        smsRecord = SmsRecord()

        for key in recDict.keys():
            if ":" not in key:
                if ('TimeStamp' in key):
                    continue
                smsRecord.setComponentByName('%s' %(key), recDict[key])
            else:
                address = Address()
                (name, addressType, npi, addressDigit) = recDict[key].split(':')
                address.setComponentByName('addressType', addressType)
                address.setComponentByName('npi', npi)
                address.setComponentByName('addressDigit', addressDigit)
                smsRecord.setComponentByName('%s' %(name), address)

        terminateTimeStamp = random.randint(st,et)
        termTS = self.convertTS(terminateTimeStamp)
        smsRecord.setComponentByName('terminateTimeStamp', termTS)
        submitTimeStamp = random.randint(st, terminateTimeStamp)
        subTS = self.convertTS(submitTimeStamp)
        smsRecord.setComponentByName('submitTimeStamp', subTS)
        toplevel.setComponentByName('sMSRecord', smsRecord)
        substrate = encoder.encode(toplevel,defMode=False,)
        return substrate

    def str2Octet(self,strList):
        bytes = []
        for i in strList:
            bytes.append(int(i))
        return ''.join(map(chr,bytes))

    def infoOctetEncoding(self,strList):
        i = 0
        bytes = []
        for i in strList:
            b = int(i)
            bytes.append(b)
        return ''.join(map(chr,bytes))


    def formIntlMobileSubIdStr(self, str):
        #print str
        strList = str.split(':')
        locStr = []
        octList = []
        mcc = strList[1]
        mnc = strList[2]
        locStr.append(mcc[1])
        locStr.append(mcc[0])
        octList.append(''.join(locStr))
        locStr = []
        locStr.append(mcc[2])

        if len(mnc) < 3:
           filler = 'F'
           locStr.append(filler)
        else:
           locStr.append(mnc[2])

        octList.append(''.join(locStr))
        locStr = []

        locStr.append(mnc[1])
        locStr.append(mnc[0])
        octList.append(''.join(locStr))
        locStr = []

        lacLen = len(strList[3])
        if lacLen%2 != 0:
            lacLen = lacLen + 1
        ciLen = len(strList[4])
        if ciLen%2 != 0:
            ciLen = ciLen + 1

        octList.append((strList[3])[0:lacLen/2])
        octList.append((strList[3])[lacLen/2:])

        octList.append((strList[4])[0:ciLen/2])
        octList.append((strList[4])[ciLen/2:])

        locHexCode = self.infoOctetEncoding(octList)
        #print locHexCode
        return locHexCode



    def encodeData1(self, recDict):
        smsRecord = SmsRecord()
        cdr = CallDetailRecord()
        #recipAd = RecipAddress()

        for key in ['origAddressGSM', 'recipAddressGSM', 'ogtiAddressGSM', 'lengthOfMessage', 'status', 'messageReference', 'callingLineIdGSM', 'origIntlMobileSubId', 'recipIntlMobileSubId', 'deliveryAttempts', 'msgError', 'cdrType', 'origOperatorId', 'destOperatorId', 'origSubsProfileId', 'destSubsProfileId']:
            cdr.setComponentByName('%s' % key, recDict[key])
            smsRecord.setComponentByName('callDetailRecord', cdr)

        period = Period()
        (label,hours,minutes) = recDict['validityPeriod:hh:mm'].split(':')
        period.setComponentByName('hours', hours)
        period.setComponentByName('minutes', minutes)
        cdr.setComponentByName('validityPeriod', validityPeriod().setComponentByName('hours', hours).setComponentByName('minutes', minutes))
        smsRecord.setComponentByName('callDetailRecord', cdr)

        (label, ton, npi, pid, msisdn, msisdnUTF8) = recDict['recipAddress:ton:npi:pid:msisdn:msisdnUTF8'].split(':')
        cdr.setComponentByName('%s' % label, recipAdd().setComponentByName('ton', ton).setComponentByName('npi', npi).setComponentByName('pid', pid).setComponentByName('msisdn', msisdn).setComponentByName('msisdnUTF8', msisdnUTF8))
        smsRecord.setComponentByName('callDetailRecord', cdr)

        (label,ton,npi,pid,msisdn,msisdnUTF8) = recDict['origAddress:ton:npi:pid:msisdn:msisdnUTF8'].split(':')
        cdr.setComponentByName('%s' % label, origAdd().setComponentByName('ton', ton).setComponentByName('npi', npi).setComponentByName('pid', pid).setComponentByName('msisdn', msisdn).setComponentByName('msisdnUTF8', msisdnUTF8))
        smsRecord.setComponentByName('callDetailRecord', cdr)

        (label,ton,npi,pid,msisdn,msisdnUTF8) = recDict['callingLineId:ton:npi:pid:msisdn:msisdnUTF8'].split(':')
        cdr.setComponentByName('%s' % label, callLineId().setComponentByName('ton', ton).setComponentByName('npi', npi).setComponentByName('pid', pid).setComponentByName('msisdn', msisdn).setComponentByName('msisdnUTF8', msisdnUTF8))
        smsRecord.setComponentByName('callDetailRecord', cdr)

        for key in ['submitDate:yy:mm:dd', 'submitTime:hh:mm:ss', 'terminDate:yy:mm:dd', 'terminTime:hh:mm:ss', 'orglSubmitDate:yy:mm:dd', 'orglSubmitTime:hh:mm:ss']:
            dateList = recDict[key].split(':')
            (label, dateLst) = dateList[0], dateList[1:]
            hexDateStr = self.str2Octet(dateLst)
            cdr.setComponentByName('%s' % label, hexDateStr)
            smsRecord.setComponentByName('callDetailRecord', cdr)

        '''
        for key in recDict.keys():
            if ':' not in key:
                cdr.setComponentByName('%s' % key, recDict[key])
            elif 'validityPeriod' in key:
                period = Period()
                (label,hours,minutes) = recDict[key].split(':')
                period.setComponentByName('hours', hours)
                period.setComponentByName('minutes', minutes)
                cdr.setComponentByName('validityPeriod', validityPeriod().setComponentByName('hours', hours).setComponentByName('minutes', minutes))
                #del period 
            elif 'recipAddress' in key:
                #adrInfo = AddressInformation()
                (label,ton,npi,pid,msisdn,msisdnUTF8) = recDict[key].split(':')
                #origAdd.setComponentByName('%s' %(label), adrInfo)
                cdr.setComponentByName('%s' % label, recipAdd().setComponentByName('ton', ton).setComponentByName('npi', npi).setComponentByName('pid', pid).setComponentByName('msisdn', msisdn).setComponentByName('msisdnUTF8', msisdnUTF8))

            elif 'origAddress' in key:
                #adrInfo = AddressInformation()
                (label,ton,npi,pid,msisdn,msisdnUTF8) = recDict[key].split(':')
                #origAdd.setComponentByName('%s' %(label), adrInfo)
                cdr.setComponentByName('%s' % label, origAdd().setComponentByName('ton', ton).setComponentByName('npi', npi).setComponentByName('pid', pid).setComponentByName('msisdn', msisdn).setComponentByName('msisdnUTF8', msisdnUTF8))

            elif 'callingLineId' in key:
                #adrInfo = AddressInformation()
                (label,ton,npi,pid,msisdn,msisdnUTF8) = recDict[key].split(':')
                #origAdd.setComponentByName('%s' %(label), adrInfo)
                cdr.setComponentByName('%s' % label, callLineId().setComponentByName('ton', ton).setComponentByName('npi', npi).setComponentByName('pid', pid).setComponentByName('msisdn', msisdn).setComponentByName('msisdnUTF8', msisdnUTF8))

            elif 'Date' in key or 'Time' in key:
                date = Date()
                dateList = recDict[key].split(':')
                (label, dateLst) = dateList[0], dateList[1:]
                hexDateStr = self.str2Octet(dateLst)
                cdr.setComponentByName('%s' % label, hexDateStr)
            elif 'IntlMobileSubId' in key:
                fieldLabel = key.split(':')[0]
                hexLocation = self.formIntlMobileSubIdStr(recDict[key])
                cdr.setComponentByName('%s' %(fieldLabel), hexLocation)
            else:
                fieldLabel = recDict[key].split(':')[0]
                valList = recDict[key].split(':')[1:]
                hexStr = self.str2Octet(valList)
                cdr.setComponentByName('%s' %(fieldLabel), hexStr)

            smsRecord.setComponentByName('callDetailRecord', cdr)
        '''
        substrate = encoder.encode(smsRecord, defMode=False,)
        return substrate


    def generateData(self, st, et, filename,noOfRec,ip,destPath):
        # instantiate the mavenir classes
        #toplevel = TopLevel()
        #smsRecord = SmsRecord()
        notificationRecord = NotificationRecord()
        #address = Address()
        #addressType = AddressType()
        #npi = NPI()
        cdrType = CdrType()
        recordType = RecordType()
        cdrStatus = CdrStatus()
        messageStatus = MessageStatus()
        filenameWithPath = "../output/%s" %(filename)
        feedFileHdl = open(filenameWithPath,'wb')
        logger.info("Setting the seed for the random generator")
        random.seed()
        i=0
        for i in range(0,noOfRec):
            toplevel = TopLevel()
            smsRecord = SmsRecord()
            address = Address()
            #generating the record
            prodLen = len(self.productIB)
            prodIndex = random.randrange(0, prodLen)
            smsRecord.setComponentByName('product', str(self.productIB[prodIndex]))
            smsRecord.setComponentByName('cdrType', (self.cdrType[random.choice(self.cdrType.keys())]))
            smsRecord.setComponentByName('recordType', (self.recordType[random.choice(self.recordType.keys())]))
            smsRecord.setComponentByName('cdrStatus', (self.cdrStatus[random.choice(self.cdrStatus.keys())]))
            smsRecord.setComponentByName('callID', str(int(self.random_with_N_digits(24))))
            smsRecord.setComponentByName('transparentPID', (self.transparentId[random.choice(self.transparentId.keys())]))

            # Setting Calling party number
            msisdnNumber = self.random_with_N_digits(15)
            address.setComponentByName('addressType', self.addressType[random.choice(self.addressType.keys())])
            address.setComponentByName('npi', (self.npi[random.choice(self.npi.keys())]))
            address.setComponentByName('addressDigit', str(msisdnNumber))
            smsRecord.setComponentByName('callingPartyNumber', address)

            # Setting Calling party IMSI assuming transaction type to be MO (To read spec for more details)
            smsRecord.setComponentByName('callingPartyIMSI', str(int(self.random_with_N_digits(15))))

            #Setting Called Party Number
            msisdnCPN = self.random_with_N_digits1(15,msisdnNumber)
            address.setComponentByName('addressType', self.addressType[random.choice(self.addressType.keys())])
            address.setComponentByName('npi', (self.npi[random.choice(self.npi.keys())]))
            address.setComponentByName('addressDigit', str(msisdnCPN))
            smsRecord.setComponentByName('calledPartyNumber', address)

            #Setting called party IMSI
            smsRecord.setComponentByName('calledPartyIMSI', str(int(self.random_with_N_digits(15))))

            #Setting message ID
            smsRecord.setComponentByName('messageID', str(int(self.random_with_N_digits(5))))


            terminateTimeStamp = random.randint(st,et)
            termTS = self.convertTS(terminateTimeStamp)
            smsRecord.setComponentByName('terminateTimeStamp', termTS)
            submitTimeStamp = random.randint(st, terminateTimeStamp)
            subTS = self.convertTS(submitTimeStamp)
            smsRecord.setComponentByName('submitTimeStamp', subTS)
            toplevel.setComponentByName('sMSRecord',smsRecord)
            substrate = encoder.encode(toplevel,defMode=False,)
            #time.sleep(5)
            feedFileHdl.write(substrate)
        print "DONE WITH %s with %s records" %(filename,i)
        feedFileHdl.close()
        self.compressAndTransfer(filenameWithPath,ip,destPath)

    def copyFileToInputDir(self, destination,file_name, trans_file_name,username, password):
        cmd = "scp %s %s@%s"%(trans_file_name,username,destination)
        # print "Copying %s to %s ..."%(trans_file_name,destination)
        child = pexpect.spawn(cmd)
        i = child.expect (['password: ',pexpect.TIMEOUT,pexpect.EOF], timeout=10)
        if i == 0:
            child.sendline ('%s'%password)
        elif i == 2:
                data = child.before
                for line in data.split("\n"):
                    if line.find("Offending key") != -1:
                        file_name = line.split(" in ")[1].split(":")[0].strip()
                        line_number= line.split(" in ")[1].split(":")[1].strip()
                        if sys.platform.find("darwin") != -1:
                            cmd = "sed -i 'tmp' '%sd' %s"%(line_number,file_name)
                        else:
                            cmd = "sed -i '%sd' %s"%(line_number,file_name)
                        os.system("%s"%cmd)
                        print "Line %s is deleted from %s for %s"%(line_number,file_name,destination)
                        cmd = "scp %s %s@%s"%(trans_file_name,username,destination)
                        print "Copying %s to %s ..."%(trans_file_name,destination)
                        child = pexpect.spawn(cmd)
                        i = child.expect(['password: ', pexpect.TIMEOUT, pexpect.EOF], timeout=10)
                        if i == 0:
                            child.sendline ('%s'%password)

        child.expect (['# ','$ ',pexpect.EOF], timeout=60)
        child.close()

        if trans_file_name != file_name:
            cmd = "sftp %s@%s"%(username,destination)
            child = pexpect.spawn (cmd)

            i = child.expect (['password:','sftp> ',pexpect.TIMEOUT,pexpect.EOF], timeout=10)
            if i == 0:
                child.sendline ('%s'%password)

            if i != 1:
                child.expect('sftp>', timeout=10)
            child.sendline ('rename %s %s'% (os.path.basename(trans_file_name), os.path.basename(file_name)))

            child.expect('sftp> ', timeout=10)
            child.sendline ('exit')
            #child.expect ('# ', timeout=10)

            child.close()

    def compressAndTransfer(self,filenameWithPath, destination):
        cmd = "gzip -f %s" %(filenameWithPath)
        (status,output) = commands.getstatusoutput(cmd)
        if (status != 0):
            logger.info("File %s not compressed successfully" %(filenameWithPath))
        filenameWithPath += ".gz"
        trans_file_name = filenameWithPath + ".tmp"
        os.rename(filenameWithPath,trans_file_name)
        self.copyFileToInputDir(destination, filenameWithPath, trans_file_name, "admin", "admin@123")
        (st1, op) = commands.getstatusoutput("rm -f %s" % trans_file_name)
        if st1 == 0:
            pass
        else:
            print "%s deletion failed from local directory" % filenameWithPath


def progArguments():
    parser = OptionParser()
    parser.add_option("-s", "--now", dest="start_time", help="start_time now -> current time , now-3d -> current time - 3 days, now+2m -> current time + 2 minutes , values supported d -> days, h-> hours , m -> minutes")

    parser.add_option("-e", "--endEpochTime", dest="endEpochTime", help="End time now -> current time , now-3d -> current time - 3 days, now+2m -> current time + 2 minutes , values supported d -> days, h-> hours , m -> minutes")
    parser.add_option("-n", "--fps", dest="fps", help="fps for generating the smdr records")
    parser.add_option("-i", "--ip", dest="ip", help="Node IP to transfer the generated files")
    parser.add_option("-d", "--destPath", dest="destPath", help="Destination path to transfer the generated files")
    parser.add_option("-c", "--csv", dest="csv", help="Path of the CSV file to be used")
    (options, args) = parser.parse_args()

    if options.start_time != None and options.endEpochTime != None:
        start_time = options.start_time
        current_time = time.time()
        if start_time == "now":
            st = int(current_time)
        else:
            type_op = start_time[3:4]
            unit = start_time[-1]
            dur = int(start_time.split(type_op)[1].strip(unit).strip())

            if unit == "m":
                dur = dur*60
            elif unit == "h":
                dur = dur*60*60
            elif unit == "d":
                dur = dur*60*60*24

            if type_op == "+":
                st = int(current_time) + dur
            else:
                st = int(current_time) -dur
        et = options.endEpochTime
        if et == "now":
            et = int(current_time)
        else:
            type_op = et[3:4]
            unit = et[-1]
            dur = int(et.split(type_op)[1].strip(unit).strip())

            if unit == "m":
                dur = dur*60
            elif unit == "h":
                dur = dur*60*60
            elif unit == "d":
                dur = dur*60*60*24

            if type_op == "+":
                et = int(current_time) + dur
            else:
                et = int(current_time) -dur

        if et<=st:
            print "Endtime should be greater than Starttime: You entered endtime :%s , starttime : %s"%(options.start_time,options.endEpochTime)
            sys.exit()

        if (options.fps == None):
            logger.warn("FPS: Required FPS not mentioned, considering default as 15 fps and proceeding")
            fps = 15
        else:
            fps = int(options.fps)

        if (options.ip == None):
            logger.warn("Transfer: Transfer of files will not occur since the IP is not provided!")
            ip = 0
        else:
            ip = options.ip

        if (options.destPath == None):
            logger.warn("Dest Path: Destination path not provided. Exiting...")
            exit(-1)
        else:
            destPath = options.destPath

        csv_file = ""
        if __debug__:
            if options.csv is None:
                print "CSV file not provided."
                sys.exit()
            else:
                csv_file = options.csv
                if not os.path.exists(csv_file):
                    print "Given CSV file does not exist. Please check the path."
                    sys.exit()

        return int(st), int(et), fps, ip, destPath, csv_file
    else:
       logger.warn("BIN TIME: The start time and end time is not specified. Please provide proper timestamps.")
       exit(-1)


def loadPickles(pickledfile):
    print "Loading pickle file '%s'"%pickledfile
    pkl_file = open(pickledfile, 'rb')
    listOrig = pickle.load(pkl_file)
    pkl_file.close()
    print "Pickle file '%s' loaded successfully"%pickledfile
    totalPickle = len(listOrig)
    shuffledList = listOrig[:]
    random.shuffle(shuffledList)
    return totalPickle,shuffledList


def getHIndexes(rem):
    hList = 23*[rem/23]
    hList[0] = hList[0]+rem%23
    return hList


def getDIndexes(rem):
    hList = 29*[rem/29]
    hList[-1] = hList[-1]+rem%29
    return hList


def getSchedule(totalMSISDN):
    dd = 1
    dayDict = {}
    while dd <31:
        hh = 1
        if dd ==1:
            lastDayCount = int(math.ceil(totalMSISDN*.8))
            remD = totalMSISDN-lastDayCount
            remDList = getDIndexes(remD)
            hourDict ={}
            while hh < 25 :
                if hh == 1:
                    lastHourCount = int(math.ceil(totalMSISDN*.2))
                    rem = int(math.ceil(totalMSISDN*.8))-lastHourCount
                    remList = getHIndexes(rem)
                    hourDict[hh] = (0,lastHourCount)
                    hh +=1
                    dayDict[dd]=hourDict
                else:
                    step = 0
                    for i in remList:
                        step += i
                        hourDict[hh] = (step,lastHourCount+i)
                        lastHourCount += i
                        hh +=1
                        dayDict[dd]=hourDict
            dd +=1
        else:
            dStep = 0
            for j in remDList:
                dStep += j
                hh = 1
                hourDict ={}
                while hh < 25 :
                    if hh == 1:
                        lastHourCount = int(math.ceil(totalMSISDN*.2))
                        rem = int(math.ceil(totalMSISDN*.8))-lastHourCount
                        remList = getHIndexes(rem)
                        hourDict[hh] = (dStep,lastHourCount+dStep)
                        dayDict[dd]=hourDict
                        hh +=1
                    else:
                        step = 0
                        for i in remList:
                            step += i
                            hourDict[hh] = (step+dStep,lastHourCount+i+dStep)
                            lastHourCount += i
                            dayDict[dd]=hourDict
                            hh +=1
                dd +=1
    return dayDict


def generate_file(args):
    filenameWithPath, litem, initTime, msisdnList, totalMSISDN, cdr_status, delivery_attempts, msg_error, cdr_type, labelList, ip, dest_path, csv_records = args
    mcount = 0

    genObj = Generator()
    output_file = open(filenameWithPath, 'wb')
    for records in range(litem):
        myTerm = random.randint(initTime, initTime + 299)
        myOrig = random.randint(initTime - 2, myTerm - 1)
        terminDate, terminTime, submitDate, submitTime = time.strftime("%y:%m:%d", time.gmtime(myTerm)), time.strftime("%H:%M:%S", time.gmtime(myTerm)), time.strftime("%y:%m:%d", time.gmtime(myOrig)), time.strftime("%H:%M:%S", time.gmtime(myOrig))
        if not __debug__:
            fieldList = ['12345678912345678912', 'recipAddressGSM', 'ogtiAddressGSM',
                         'origAddress:0:2:34:44%s:326821111111' % msisdnList[mcount],
                         'recipAddress:1:3:35:88%s:326832222222' % msisdnList[totalMSISDN-mcount-1],
                         'submitDate:%s' % submitDate, 'submitTime:%s' % submitTime, 'terminDate:%s' % terminDate,
                         'terminTime:%s' % terminTime, 'orglSubmitDate:13:29:12', 'orglSubmitTime:15:04:17', '999',
                         '%d' % random.choice(cdr_status), 'messageReference', 'validityPeriod:17:39', 'callingLineIdGSM',
                         'callingLineId:2:4:36:1279325:326843333333', 'TBCDorig', 'TBCDrecp',
                         '%d' % random.choice(delivery_attempts), '%d' % random.choice(msg_error),
                         '%d' % random.choice(cdr_type), '3', '5', '55555', '6']
        else:
            fieldList = csv_records[mcount % len(csv_records)].strip('\n').split(',')
            fieldList[5] = 'submitDate:%s' % submitDate
            fieldList[6] = 'submitTime:%s' % submitTime
            fieldList[7] = 'terminDate:%s' % terminDate
            fieldList[8] = 'terminTime:%s' % terminTime
        recDict = dict(zip(labelList, fieldList))
        substrate = genObj.encodeData1(recDict)
        output_file.write(substrate)

        mcount += 1

        if mcount == totalMSISDN - 1:
            mcount = 0

    output_file.close()
    cmd = "gzip -f %s" % filenameWithPath
    (status, output) = commands.getstatusoutput(cmd)
    if status != 0:
        print "File %s not compressed successfully" % filenameWithPath
    filenameWithPath += ".gz"
    trans_file_name = filenameWithPath + ".tmp"
    os.rename(filenameWithPath, trans_file_name)
    os.system('scp -q %s admin@%s:%s' % (trans_file_name, ip, dest_path))
    os.system('ssh -q root@%s "mv %s/%s %s/%s"' % (ip, dest_path, os.path.basename(trans_file_name), dest_path, os.path.basename(filenameWithPath)))
    os.system("rm -f %s" % trans_file_name)

if __name__ == "__main__":
    msisdn_pkl_file = "msisdn_15M.pkl"
    output_path = "../output/csv"
    recordsPerFile = 50000
    cdr_type = [1, 3]
    cdr_status = 36 * [0]
    cdr_status.extend([1, 2, 7, 9])
    delivery_attempts = range(1, 65536)
    msg_error = range(0, 65535)

    try:
        os.makedirs(output_path)
    except OSError:
        pass

    totalMSISDN = 0
    msisdnList = []
    if not __debug__:
        totalMSISDN, origMsisdnList = loadPickles(msisdn_pkl_file)
    datagenStartTime, datagenEndTime, fps, ip, dest_path, csv_file = progArguments()

    csv_records = []
    if __debug__:
        csv_file_handler = open(csv_file)
        csv_records = csv_file_handler.readlines()
        csv_file_handler.close()

    totalFilesPer5Min = ((fps * 60 * 5) / recordsPerFile) * [recordsPerFile]
    if (fps * 60 * 5) % recordsPerFile != 0:
        totalFilesPer5Min.append((fps * 60 * 5) % recordsPerFile)
    print "Total files per bin: %s" % len(totalFilesPer5Min)
    if not __debug__:
        schedule = getSchedule(totalMSISDN)
    hourCount = 1
    dayCount = 1
    hTime = datagenStartTime
    dTime = datagenStartTime
    labelList = ['origAddressGSM', 'recipAddressGSM', 'ogtiAddressGSM', 'origAddress:ton:npi:pid:msisdn:msisdnUTF8',
                 'recipAddress:ton:npi:pid:msisdn:msisdnUTF8', 'submitDate:yy:mm:dd', 'submitTime:hh:mm:ss',
                 'terminDate:yy:mm:dd', 'terminTime:hh:mm:ss', 'orglSubmitDate:yy:mm:dd', 'orglSubmitTime:hh:mm:ss',
                 'lengthOfMessage', 'status', 'messageReference', 'validityPeriod:hh:mm', 'callingLineIdGSM',
                 'callingLineId:ton:npi:pid:msisdn:msisdnUTF8', 'origIntlMobileSubId', 'recipIntlMobileSubId',
                 'deliveryAttempts', 'msgError', 'cdrType', 'origOperatorId', 'destOperatorId', 'origSubsProfileId',
                 'destSubsProfileId']

    while datagenStartTime < datagenEndTime:
        init = time.time()
        initTime = datagenStartTime - datagenStartTime % 300
        print "Starting bin: %s" % time.strftime("%Y/%m/%d %H:%M:%S",time.gmtime(initTime))
        if not __debug__:
            msisdnStartIndex = schedule[dayCount][hourCount][0]
            msisdnEndIndex = schedule[dayCount][hourCount][1]
            msisdnList = origMsisdnList[msisdnStartIndex:msisdnEndIndex]
            totalMSISDN = len(msisdnList)
        file_count = 0
        process_list = []

        for litem in totalFilesPer5Min:
            timeStr = time.strftime("%Y%m%d%H%M",time.gmtime(float(initTime)))
            filename = "Mavenir_CDR_%s_%d_%d.ber" %(timeStr, file_count, recordsPerFile)
            filenameWithPath = "%s/%s" % (output_path, filename)
            file_count += 1

            if file_count <= multiprocessing.cpu_count() * 0.8:
                p = multiprocessing.Process(target=generate_file, args=((filenameWithPath, litem, initTime, msisdnList, totalMSISDN, cdr_status, delivery_attempts, msg_error, cdr_type, labelList, ip, dest_path, csv_records),))
                process_list.append(p)
                p.start()
            else:
                process_list[0].join()
                process_list.remove(process_list[0])
                p = multiprocessing.Process(target=generate_file, args=((filenameWithPath, litem, initTime, msisdnList, totalMSISDN, cdr_status, delivery_attempts, msg_error, cdr_type, labelList, ip, dest_path, csv_records),))
                process_list.append(p)
                p.start()

            # generate_file((filenameWithPath, litem, initTime, msisdnList, totalMSISDN, cdr_status, delivery_attempts, msg_error, cdr_type, labelList, ip, dest_path))

        for process in process_list:
            process.join()

        print "Time taken for bin: %s: %s s" % (time.strftime("%Y/%m/%d %H:%M:%S",time.gmtime(initTime)), time.time() - init)
        if datagenStartTime >= hTime+3600:
            hTime += 3600
            hourCount += 1

        if datagenStartTime >= dTime+86400:
            dTime += 86400
            dayCount += 1
            hourCount = 1

        datagenStartTime += 300