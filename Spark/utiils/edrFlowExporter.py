#!/usr/bin/env python
import time
import random
import os
import multiprocessing
import optparse
import math
import pexpect
import sys
import commands
import pickle

os.chdir(os.path.dirname(os.path.realpath(__file__)))


def progArguments():
    parser = optparse.OptionParser()
    parser.add_option("-s", "--now", dest="start_time",
                      help="start_time now -> current time , now-3d -> current time - 3 days, now+2m -> current time + 2 minutes , values supported d -> days, h-> hours , m -> minutes")

    parser.add_option("-e", "--endEpochTime", dest="endEpochTime",
                      help="End time now -> current time , now-3d -> current time - 3 days, now+2m -> current time + 2 minutes , values supported d -> days, h-> hours , m -> minutes")
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
                dur *= 60
            elif unit == "h":
                dur = dur * 60 * 60
            elif unit == "d":
                dur = dur * 60 * 60 * 24

            if type_op == "+":
                st = int(current_time) + dur
            else:
                st = int(current_time) - dur
        et = options.endEpochTime
        if et == "now":
            et = int(current_time)
        else:
            type_op = et[3:4]
            unit = et[-1]
            dur = int(et.split(type_op)[1].strip(unit).strip())

            if unit == "m":
                dur *= 60
            elif unit == "h":
                dur = dur * 60 * 60
            elif unit == "d":
                dur = dur * 60 * 60 * 24

            if type_op == "+":
                et = int(current_time) + dur
            else:
                et = int(current_time) - dur

        if et <= st:
            print "Endtime should be greater than Starttime: You entered endtime :%s , starttime : %s" % (
                options.start_time, options.endEpochTime)
            sys.exit()

        if options.fps == None:
            print "FPS: Required FPS not mentioned, considering default as 15 fps and proceeding"
            fps = 15
        else:
            fps = int(options.fps)

        if options.ip == None:
            print "Transfer: Transfer of files will not occur since the IP is not provided!"
            ip = 0
        else:
            ip = options.ip

        if options.destPath == None:
            print "Dest Path: Destination path not provided. Exiting..."
            exit(-1)
        else:
            destPath = options.destPath

        if options.csv is None:
            print "CSV file missing. Exiting..."
            exit(-1)
        else:
            csv_file = options.csv
        return int(st), int(et), fps, ip, destPath, csv_file
    else:
        print "BIN TIME: The start time and end time is not specified. Please provide proper timestamps."
        exit(-1)


def loadPickles(pickledfile):
    print "Loading pickle file '%s'" % pickledfile
    pkl_file = open(pickledfile, 'rb')
    listOrig = pickle.load(pkl_file)
    pkl_file.close()
    print "Pickle file '%s' loaded successfully" % pickledfile
    totalPickle = len(listOrig)
    shuffledList = listOrig[:]
    random.shuffle(shuffledList)
    return totalPickle, shuffledList


def getHIndexes(rem):
    hList = 23 * [rem / 23]
    hList[0] = hList[0] + rem % 23
    return hList


def getDIndexes(rem):
    hList = 29 * [rem / 29]
    hList[-1] = hList[-1] + rem % 29
    return hList


def getSchedule(totalMSISDN):
    dd = 1
    dayDict = {}
    while dd < 31:
        hh = 1
        if dd == 1:
            lastDayCount = int(math.ceil(totalMSISDN * .8))
            remD = totalMSISDN - lastDayCount
            remDList = getDIndexes(remD)
            hourDict = {}
            while hh < 25:
                if hh == 1:
                    lastHourCount = int(math.ceil(totalMSISDN * .4))
                    rem = int(math.ceil(totalMSISDN * .8)) - lastHourCount
                    remList = getHIndexes(rem)
                    hourDict[hh] = (0, lastHourCount)
                    hh += 1
                    dayDict[dd] = hourDict
                else:
                    step = 0
                    for i in remList:
                        step += i
                        hourDict[hh] = (step, lastHourCount + i)
                        lastHourCount += i
                        hh += 1
                        dayDict[dd] = hourDict
            dd += 1
        else:
            dStep = 0
            for j in remDList:
                dStep += j
                hh = 1
                hourDict = {}
                while hh < 25:
                    if hh == 1:
                        lastHourCount = int(math.ceil(totalMSISDN * .4))
                        rem = int(math.ceil(totalMSISDN * .8)) - lastHourCount
                        remList = getHIndexes(rem)
                        hourDict[hh] = (dStep, lastHourCount + dStep)
                        dayDict[dd] = hourDict
                        hh += 1
                    else:
                        step = 0
                        for i in remList:
                            step += i
                            hourDict[hh] = (step + dStep, lastHourCount + i + dStep)
                            lastHourCount += i
                            dayDict[dd] = hourDict
                            hh += 1
                dd += 1
    return dayDict


def copyFileToInputDir(destination, file_name, trans_file_name, username, password):
    cmd = "scp %s %s@%s" % (trans_file_name, username, destination)
    # print "Copying %s to %s ..."%(trans_file_name,destination)
    child = pexpect.spawn(cmd)
    i = child.expect(['password: ', pexpect.TIMEOUT, pexpect.EOF], timeout=10)
    if i == 0:
        child.sendline('%s' % password)
    elif i == 2:
        data = child.before
        for line in data.split("\n"):
            if line.find("Offending key") != -1:
                file_name = line.split(" in ")[1].split(":")[0].strip()
                line_number = line.split(" in ")[1].split(":")[1].strip()
                if sys.platform.find("darwin") != -1:
                    cmd = "sed -i 'tmp' '%sd' %s" % (line_number, file_name)
                else:
                    cmd = "sed -i '%sd' %s" % (line_number, file_name)
                os.system("%s" % cmd)
                print "Line %s is deleted from %s for %s" % (line_number, file_name, destination)
                cmd = "scp %s %s@%s" % (trans_file_name, username, destination)
                print "Copying %s to %s ..." % (trans_file_name, destination)
                child = pexpect.spawn(cmd)
                i = child.expect(['password: ', pexpect.TIMEOUT, pexpect.EOF], timeout=10)
                if i == 0:
                    child.sendline('%s' % password)

    child.expect(['# ', '$ ', pexpect.EOF], timeout=60)
    child.close()

    if trans_file_name != file_name:
        cmd = "sftp %s@%s" % (username, destination)
        child = pexpect.spawn(cmd)

        i = child.expect(['password:', 'sftp> ', pexpect.TIMEOUT, pexpect.EOF], timeout=10)
        if i == 0:
            child.sendline('%s' % password)

        if i != 1:
            child.expect('sftp>', timeout=10)
        child.sendline('rename %s %s' % (os.path.basename(trans_file_name), os.path.basename(file_name)))

        child.expect('sftp> ', timeout=10)
        child.sendline('exit')
        # child.expect ('# ', timeout=10)
        child.close()


def transfer_files(queue, ip, dest_path):
    while True:
        trans_file_name = queue.get()
        filenameWithPath = trans_file_name.replace('.tmp', '')
        if trans_file_name == "DONE":
            break
        os.system('scp -q %s admin@%s:%s' % (trans_file_name, ip, dest_path))
        os.system('ssh -q root@%s "mv %s/%s %s/%s"' % (
        ip, dest_path, os.path.basename(trans_file_name), dest_path, os.path.basename(filenameWithPath)))
        # copyFileToInputDir(destination, filenameWithPath, trans_file_name, "admin", "admin@123")
        os.system("rm -f %s" % trans_file_name)


def generate_file(args):
    filename_with_path, litem, csv_record_list, csv_header_list, init_time, msisdn_list, total_msisdn, local_list, total_local_locations, roaming_list, total_roaming_locations, total_csv_records, ip, dest_path = args
    mcount = 0
    ccount = 0
    location_list = 8 * ['l']
    location_list.extend(['r', 'r'])
    continuous_subscriber = msisdn_list[10]  # Fixing a subscriber for continuous sessions
    continuous_subscriber_location = random.choice('lr')
    intermittent_subscriber = msisdn_list[11]  # Fixing a subscriber for intermittent sessions
    intermittent_subscriber_location = random.choice('lr ')
    # Reshuffle subslist for right set of
    msisdn_list = random.sample(msisdn_list, litem)
    total_msisdn = len(msisdn_list)
    msisdn_list[10] = continuous_subscriber
    msisdn_list[11] = intermittent_subscriber

    output_file = open(filename_with_path, 'w')
    output_file.write('%s\n' % (",".join(csv_header_list)))
    for records in range(litem):
        current_record = csv_record_list[ccount].strip().split(',')
        subscriber_id = msisdn_list[mcount]
        if subscriber_id == intermittent_subscriber:
            if intermittent_subscriber_location == "":
                mcount += 1
                subscriber_id = msisdn_list[mcount]
            elif intermittent_subscriber_location == "l":
                current_record[csv_header_list.index('bearer-3gpp user-location-information')] = local_list[
                    subscriber_id % total_local_locations]
            else:
                current_record[csv_header_list.index('bearer-3gpp user-location-information')] = roaming_list[
                    subscriber_id % total_roaming_locations]
        elif subscriber_id == continuous_subscriber:
            if continuous_subscriber_location == "l":
                current_record[csv_header_list.index('bearer-3gpp user-location-information')] = local_list[
                    subscriber_id % total_local_locations]
            else:
                current_record[csv_header_list.index('bearer-3gpp user-location-information')] = roaming_list[
                    subscriber_id % total_roaming_locations]
        else:
            location_choice = random.choice(location_list)
            if location_choice == "l":
                current_record[csv_header_list.index('bearer-3gpp user-location-information')] = local_list[
                    subscriber_id % total_local_locations]
            else:
                current_record[csv_header_list.index('bearer-3gpp user-location-information')] = roaming_list[
                    subscriber_id % total_roaming_locations]
        current_record[csv_header_list.index('sn-flow-end-time')] = str(random.randint(init_time, init_time + 299))
        current_record[csv_header_list.index('radius-calling-station-id')] = str(subscriber_id)
        mcount += 1
        ccount += 1
        current_record = ",".join(current_record)
        output_file.write("%s\n" % current_record)
        if mcount == total_msisdn - 1:
            mcount = 0
        if ccount == total_csv_records - 1:
            ccount = 0

    output_file.close()
    cmd = "gzip -f %s" % filename_with_path
    (status, output) = commands.getstatusoutput(cmd)
    if status != 0:
        print "File %s not compressed successfully" % filename_with_path
    filename_with_path += ".gz"
    trans_file_name = filename_with_path + ".tmp"
    os.rename(filename_with_path, trans_file_name)

    os.system('scp -q %s admin@%s:%s' % (trans_file_name, ip, dest_path))
    os.system('ssh -q root@%s "mv %s/%s %s/%s"' % (
    ip, dest_path, os.path.basename(trans_file_name), dest_path, os.path.basename(filename_with_path)))
    os.system("rm -f %s" % trans_file_name)


if __name__ == "__main__":
    bin_interval = 5  # in minutes
    msisdn_pkl_file = "msisdn_70M.pkl"
    records_per_file = 100000
    output_path = "output/"
    local_list = ['310-160-3521-14d3801', '310-200-115-35131', '310-210-345a-14ef801', '310-220-34752-32043',
                  '310-230-34755-5442', '310-240-3523-14d2902', '310-250-3b38-14cfa03', '310-260-41397-16041',
                  '310-270-345a-14ef801', '310-310-115-35131', '310-490-41397-16041', '310-660-3521-14d3801',
                  '310-800-34755-5442']
    total_local_locations = len(local_list)
    roaming_list = ['311-160-3521-14d3801', '313-200-115-35131', '210-210-345a-14ef801', '110-220-34752-32043',
                    '410-230-34755-5442', '510-240-3523-14d2902', '610-250-3b38-14cfa03', '710-260-41397-16041',
                    '810-270-345a-14ef801', '910-310-115-35131', '310-500-41397-16041', '310-600-3521-14d3801',
                    '310-700-34755-5442']
    total_roaming_locations = len(roaming_list)

    try:
        os.makedirs(output_path)
    except OSError:
        pass

    generation_start_time, generation_end_time, fps, ip, dest_path, csv_file = progArguments()
    total_msisdn, orig_msisdn_list = loadPickles(msisdn_pkl_file)
    records_per_bin = fps * bin_interval * 60
    total_files_per_5min = (records_per_bin / records_per_file) * [records_per_file]
    if records_per_bin % records_per_file != 0:
        total_files_per_5min.append(records_per_bin % records_per_file)
    print "Total files per bin: %s" % len(total_files_per_5min)
    schedule = getSchedule(total_msisdn)
    hour_count = 1
    day_count = 1
    h_time = generation_start_time
    d_time = generation_start_time

    csv_file_fh = open(csv_file)
    csv_header_list = csv_file_fh.readline().lstrip('#').strip('\n').split(',')
    csv_record_list = [line.strip('\n') for line in csv_file_fh]
    csv_file_fh.close()
    total_csv_records = len(csv_record_list)

    while generation_start_time < generation_end_time:
        init = time.time()
        init_time = generation_start_time - generation_start_time % 300
        print "Starting bin: %s" % time.strftime("%Y/%m/%d %H:%M:%S", time.gmtime(init_time))
        msisdn_start_index = schedule[day_count][hour_count][0]
        msisdn_end_index = schedule[day_count][hour_count][1]
        msisdn_list = orig_msisdn_list[msisdn_start_index:msisdn_end_index]
        total_msisdn = len(msisdn_list)

        # queue = multiprocessing.Queue()
        # transfer_files_process = multiprocessing.Process(target=transfer_files, args=(queue, ip, dest_path,))
        # transfer_files_process.start()

        file_count = 0
        process_list = []

        for litem in total_files_per_5min:
            time_str = time.strftime("%m%d%Y%H%M%S", time.gmtime(float(init_time)))
            filename = "gurgaon_MURAL-edr-%d_%s_%d.csv" % (records_per_file, time_str, file_count)
            filename_with_path = "%s/%s" % (output_path, filename)
            file_count += 1
            if file_count <= multiprocessing.cpu_count() * 0.8:
                p = multiprocessing.Process(target=generate_file, args=((filename_with_path, litem, csv_record_list,
                                                                         csv_header_list, init_time, msisdn_list,
                                                                         total_msisdn, local_list,
                                                                         total_local_locations, roaming_list,
                                                                         total_roaming_locations, total_csv_records, ip,
                                                                         dest_path),))
                process_list.append(p)
                p.start()
            else:
                process_list[0].join()
                process_list.remove(process_list[0])
                p = multiprocessing.Process(target=generate_file, args=((filename_with_path, litem, csv_record_list,
                                                                         csv_header_list, init_time, msisdn_list,
                                                                         total_msisdn, local_list,
                                                                         total_local_locations, roaming_list,
                                                                         total_roaming_locations, total_csv_records, ip,
                                                                         dest_path),))
                process_list.append(p)
                p.start()
                # generate_file((filename_with_path, litem, csv_record_list, csv_header_list, start_time, end_time, msisdn_list, total_msisdn, local_list, total_local_locations, roaming_list, total_roaming_locations, dest, total_csv_records))

        for process in process_list:
            process.join()

        # queue.put('DONE')
        # transfer_files_process.join()

        print "Time taken for bin: %s: %s s" % (
        time.strftime("%Y/%m/%d %H:%M:%S", time.gmtime(init_time)), time.time() - init)
        if generation_start_time >= h_time + 3600:
            h_time += 3600
            hour_count += 1

        if generation_start_time >= d_time + 86400:
            d_time += 86400
            day_count += 1
            hour_count = 1

        generation_start_time += 300
