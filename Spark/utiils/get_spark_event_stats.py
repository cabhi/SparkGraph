#!/usr/bin/python

import json
import sys
import os

conc_flows = {}
host_conc_flows = {}
stage_data = {}
stage_headers = ["Start time","End time","Task type","Executor ID","Host","Locality","Failed","Speculative","Total time (s)","Run time","Deserialize time","Serialize time","GC time","Result time","Input method","Input bytes (KB)","Result size","Spilled bytes","Disk spilled","SF_Rm_Blks","SF_Lc_Blks","SF_Ft_Tm","SF_Wr_Tm","SF_Rm_Rd_By","SF_Wr_By"]
stage_to_job = {}
job_data = {}

def get_column_min(stage, col_name):
    return get_column_stats(stage, col_name)[0]

def get_column_max(stage, col_name):
    return get_column_stats(stage, col_name)[1]

def get_column_mean(stage, col_name):
    return get_column_stats(stage, col_name)[2]

def get_column_median(stage, col_name):
    return get_column_stats(stage, col_name)[3]

def get_column_std_dev(stage, col_name):
    return get_column_stats(stage, col_name)[4]

def get_column_skew(stage, col_name):
    return get_column_stats(stage, col_name)[5]

def get_column_stats(stage, col_name):
    col_index = stage_headers.index(col_name)
    col_data = []
    for task in stage_data[stage]:
        col_data.append(stage_data[stage][task][col_index])
    col_sum = sum(col_data) * 1.0
    col_len = len(col_data)
    col_avg = col_sum / col_len
    if col_len % 2 == 1:
        col_median = sorted(col_data)[(col_len + 1) / 2 - 1]
    else:
        col_median = (sorted(col_data)[col_len / 2 - 1] + sorted(col_data)[col_len / 2]) / 2.0
    col_min = min(col_data)
    col_max = max(col_data)

    col_variance_series = [pow((x - col_avg), 2) for x in col_data]
    col_variance = sum(col_variance_series) / col_len
    col_std_dev = pow(col_variance, 0.5)

    return [col_min, col_max, col_avg, col_median, col_std_dev]

if (len(sys.argv) == 2):
    input = open(sys.argv[1], "r")
else:
    input = sys.stdin

for line in input:
    if "SparkListenerApplicationStart" in line:
        l_json = json.loads(line)
        out_file_name = "spark_" + l_json['App ID']
        out_csv = open(out_file_name + "_det.csv", 'w')
        #out_sum = open(out_file_name + ".sum", 'w')
        out_flows = open(out_file_name + "_flows.csv", 'w')
        out_timeline = open(out_file_name + "_timeline.csv", 'w')
        out_stage = open(out_file_name + "_stage.csv", 'w')
        out_err = None

        out_csv.write("Start time,End time,Task type,Stage ID,Task ID,Executor ID,Host,Locality,Failed,Speculative,Total time (s),Run time,Deserialize time,Serialize time,GC time,Result time,Input method,Input bytes (KB),Result size,Spilled bytes,Disk spilled,SF_Rm_Blks,SF_Lc_Blks,SF_Ft_Tm,SF_Wr_Tm,SF_Rm_Rd_By,SF_Wr_By" + os.linesep)
        out_flows.write("Time,Parallel Tasks" + os.linesep)
        out_stage.write("Start time,End time,Stage ID,Duration,TTotal-min,TTotal-max,TTotal-mean,TTotal-median,TTotal-stdDev,TGC-min,TGC-max,TGC-mean,TGC-median,TGC-stdDev,TInBytes-min,TInBytes-max,TInBytes-mean,TInBytes-median,TInBytes-stdDev,TSF_Ft_Tm-min,TSF_Ft_Tm-max,TSF_Ft_Tm-mean,TSF_Ft_Tm-median,TSF_Ft_Tm-stdDev,TSF_Wr_Tm-min,TSF_Wr_Tm-max,TSF_Wr_Tm-mean,TSF_Wr_Tm-median,TSF_Wr_Tm-stdDev,TSF_Wr_By-min,TSF_Wr_By-max,TSF_Wr_By-mean,TSF_Wr_By-median,TSF_Wr_By-stdDev" + os.linesep)

    if "SparkListenerJobStart" in line:
        l_json = json.loads(line)
        job_id = l_json['Job ID']
        job_stage_ids = l_json['Stage IDs']
        print "JOB: " + str(job_id) + "  -  Stages:", job_stage_ids
        for j_stage in job_stage_ids:
            stage_to_job[j_stage] = job_id

    if "SparkListenerTaskEnd" in line:
        l_json = json.loads(line)
        task_info = l_json['Task Info']
        task_id = task_info['Task ID']
        task_start_time = int(task_info['Launch Time']) / 1000.0
        task_end_time = int(task_info['Finish Time']) / 1000.0
        t_start = int(task_start_time)
        t_end = int(task_end_time)
        task_type = l_json['Task Type']
        stage_id = l_json['Stage ID']
        task_host = task_info['Host']
        task_failed = task_info['Failed']

        task_time = task_end_time - task_start_time
        task_executor_id = task_info['Executor ID']
        task_locality = task_info['Locality']
        task_speculative = task_info['Speculative']
        result_time = int(task_info['Getting Result Time']) / 1000.0

        if task_failed == False:
            task_metrics = l_json['Task Metrics']
        else:
            if out_err == None:
                out_err = open(out_file_name + ".err", 'w')
                print "ERROR file generate: " + out_file_name + ".err"
            task_end_reason = l_json['Task End Reason']
            if 'Class Name' in task_end_reason:
                err_line = "Task FAILED: Stage ID: " + str(stage_id) + " Task ID: " + str(task_id) + " Task type: " + task_type + " Host: " + task_host + " Task End Reason: " + task_end_reason['Reason'] + " Class: " + task_end_reason['Class Name'] + " Description: " + task_end_reason['Description'] 
                #print err_line
                out_err.write(err_line + os.linesep)
            elif 'Block Manager Address' in task_end_reason:
                fetch_block = task_end_reason['Block Manager Address']
                err_line = "Task FAILED: Stage ID: " + str(stage_id) + " Task ID: " + str(task_id) + " Task type: " + task_type + " Host: " + task_host + " Task End Reason: " + task_end_reason['Reason'] + " Fetch-Host: " + fetch_block['Host'] + ":" + str(fetch_block['Port']) + " Shuffle ID: " + str(task_end_reason['Shuffle ID']) + " Map ID: " + str(task_end_reason['Map ID']) + " Reduce ID: " + str(task_end_reason['Reduce ID']) + " Description: " + task_end_reason['Message'].split('\n')[0]
                #print err_line
                out_err.write(err_line + os.linesep)
            #else:
            #    print "Task FAILED: Stage ID: " + str(stage_id) + " Task ID: " + str(task_id) + " Task type: " + task_type + " Host: " + task_host + " Task End Reason: " + task_end_reason['Reason']
            csv_line = [ (task_start_time / (24*3600.0)), (task_end_time / (24*3600.0)), task_type, stage_id, task_id, task_executor_id, task_host, task_locality, task_failed, task_speculative, task_time, "", "", "", "", result_time, "", "", "", "", "", "", "", "", "", "", "" ]
            out_csv.write(",".join([str(t) for t in csv_line]) + os.linesep)
            continue

        job_id = stage_to_job[stage_id]
        if job_id not in job_data:
            job_data[job_id] = {}

        if stage_id not in job_data[job_id]:
            job_data[job_id][stage_id] = {}

        if stage_id not in stage_data:
            stage_data[stage_id] = {}

        if task_host not in host_conc_flows:
            host_conc_flows[task_host] = {}

        if t_start in conc_flows:
            conc_flows[t_start] += 1
        else:
            conc_flows[t_start] = 1
        if t_end in conc_flows:
            conc_flows[t_end] -= 1
        else:
            conc_flows[t_end] = -1

        if t_start in host_conc_flows[task_host]:
            host_conc_flows[task_host][t_start] += 1
        else:
            host_conc_flows[task_host][t_start] = 1
        if t_end in host_conc_flows[task_host]:
            host_conc_flows[task_host][t_end] -= 1
        else:
            host_conc_flows[task_host][t_end] = -1

        task_deserialize_time = int(task_metrics['Executor Deserialize Time']) / 1000.0
        task_serialize_time = int(task_metrics['Result Serialization Time']) / 1000.0
        task_run_time = int(task_metrics['Executor Run Time']) / 1000.0
        task_gc_time = int(task_metrics['JVM GC Time']) / 1000.0

        task_result_size = int(task_metrics['Result Size']) / 1024.0
        task_spilled_bytes = int(task_metrics['Memory Bytes Spilled']) / 1024.0
        task_disk_spill = int(task_metrics['Disk Bytes Spilled']) / 1024.0

        if 'Shuffle Read Metrics' in task_metrics:
            shuffle_metrics = task_metrics['Shuffle Read Metrics']
            shuffle_remote_blocks = shuffle_metrics['Remote Blocks Fetched']
            shuffle_local_blocks = shuffle_metrics['Local Blocks Fetched']
            shuffle_fetch_time = int(shuffle_metrics['Fetch Wait Time']) / 1000.0
            shuffle_remote_read = int(shuffle_metrics['Remote Bytes Read']) / 1024.0
        else:
            shuffle_remote_blocks = 0
            shuffle_local_blocks = 0
            shuffle_fetch_time = 0
            shuffle_remote_read = 0

        if 'Shuffle Write Metrics' in task_metrics:
            shuffle_write_metrics = task_metrics['Shuffle Write Metrics']
            shuffle_write_bytes = int(shuffle_write_metrics['Shuffle Bytes Written']) / 1024.0
            shuffle_write_time = int(shuffle_write_metrics['Shuffle Write Time']) / 1000.0
        else:
            shuffle_write_bytes = 0
            shuffle_write_time = 0

        if 'Input Metrics' in task_metrics:
            task_input_method = task_metrics['Input Metrics']['Data Read Method']
            task_input_bytes = int(task_metrics['Input Metrics']['Bytes Read']) / 1024.0
        else:
            task_input_method = 'NA'
            task_input_bytes = 0

        stage_data[stage_id][task_id] = [task_start_time, task_end_time, task_type, task_executor_id, task_host, task_locality, task_failed, task_speculative, task_time, task_run_time, task_deserialize_time, task_serialize_time, task_gc_time, result_time, task_input_method, task_input_bytes, task_result_size, task_spilled_bytes, task_disk_spill, shuffle_remote_blocks, shuffle_local_blocks, shuffle_fetch_time, shuffle_write_time, shuffle_remote_read, shuffle_write_bytes]
        job_data[job_id][stage_id][task_id] = stage_data[stage_id][task_id]

        csv_line = [ (task_start_time / (24*3600.0)), (task_end_time / (24*3600.0)), task_type, stage_id, task_id, task_executor_id, task_host, task_locality, task_failed, task_speculative, task_time, task_run_time, task_deserialize_time, task_serialize_time, task_gc_time, result_time, task_input_method, task_input_bytes, task_result_size, task_spilled_bytes, task_disk_spill, shuffle_remote_blocks, shuffle_local_blocks, shuffle_fetch_time, shuffle_write_time, shuffle_remote_read, shuffle_write_bytes ]
        out_csv.write(",".join([str(t) for t in csv_line]) + os.linesep)

# Calculate all flows information
total_flows = 0
for ts in sorted(conc_flows.keys()):
    total_flows += conc_flows[ts]
    time = ts / (24*3600.0)
    out_flows.write(str(time) + "," + str(total_flows) + os.linesep)

for host in host_conc_flows:
    out_flows.write(os.linesep + host + os.linesep)
    total_flows = 0
    for ts in sorted(host_conc_flows[host].keys()):
        total_flows += host_conc_flows[host][ts]
        time = ts / (24*3600.0)
        out_flows.write(str(time) + "," + str(total_flows) + os.linesep)

out_csv.close()
out_flows.close()

# Get per stage statistics
for stage in stage_data:
    stage_start_time = get_column_min(stage, "Start time")
    stage_end_time = get_column_max(stage, "End time")
    stage_duration = stage_end_time - stage_start_time

    min_task_total_time = get_column_min(stage, "Total time (s)")
    max_task_total_time = get_column_max(stage, "Total time (s)")
    avg_task_total_time = get_column_mean(stage, "Total time (s)")
    median_task_total_time = get_column_median(stage, "Total time (s)")
    std_dev_task_total_time = get_column_std_dev(stage, "Total time (s)")

    min_task_gc_time = get_column_min(stage, "GC time")
    max_task_gc_time = get_column_max(stage, "GC time")
    avg_task_gc_time = get_column_mean(stage, "GC time")
    median_task_gc_time = get_column_median(stage, "GC time")
    std_dev_task_gc_time = get_column_std_dev(stage, "GC time")

    min_task_in_bytes = get_column_min(stage, "Input bytes (KB)")
    max_task_in_bytes = get_column_max(stage, "Input bytes (KB)")
    avg_task_in_bytes = get_column_mean(stage, "Input bytes (KB)")
    median_task_in_bytes = get_column_median(stage, "Input bytes (KB)")
    std_dev_task_in_bytes = get_column_std_dev(stage, "Input bytes (KB)")

    min_task_sf_ft_time = get_column_min(stage, "SF_Ft_Tm")
    max_task_sf_ft_time = get_column_max(stage, "SF_Ft_Tm")
    avg_task_sf_ft_time = get_column_mean(stage, "SF_Ft_Tm")
    median_task_sf_ft_time = get_column_median(stage, "SF_Ft_Tm")
    std_dev_task_sf_ft_time = get_column_std_dev(stage, "SF_Ft_Tm")

    min_task_sf_wr_time = get_column_min(stage, "SF_Wr_Tm")
    max_task_sf_wr_time = get_column_max(stage, "SF_Wr_Tm")
    avg_task_sf_wr_time = get_column_mean(stage, "SF_Wr_Tm")
    median_task_sf_wr_time = get_column_median(stage, "SF_Wr_Tm")
    std_dev_task_sf_wr_time = get_column_std_dev(stage, "SF_Wr_Tm")

    min_task_sf_wr_bytes = get_column_min(stage, "SF_Wr_By")
    max_task_sf_wr_bytes = get_column_max(stage, "SF_Wr_By")
    avg_task_sf_wr_bytes = get_column_mean(stage, "SF_Wr_By")
    median_task_sf_wr_bytes = get_column_median(stage, "SF_Wr_By")
    std_dev_task_sf_wr_bytes = get_column_std_dev(stage, "SF_Wr_By")

    stage_line = [ (stage_start_time / (24*3600.0)), (stage_end_time / (24*3600.0)), stage, stage_duration, min_task_total_time, max_task_total_time, avg_task_total_time, median_task_total_time, std_dev_task_total_time, min_task_gc_time, max_task_gc_time, avg_task_gc_time, median_task_gc_time, std_dev_task_gc_time, min_task_in_bytes, max_task_in_bytes, avg_task_in_bytes, median_task_in_bytes, std_dev_task_in_bytes, min_task_sf_ft_time, max_task_sf_ft_time, avg_task_sf_ft_time, median_task_sf_ft_time, std_dev_task_sf_ft_time, min_task_sf_wr_time, max_task_sf_wr_time, avg_task_sf_wr_time, median_task_sf_wr_time, std_dev_task_sf_wr_time, min_task_sf_wr_bytes, max_task_sf_wr_bytes, avg_task_sf_wr_bytes, median_task_sf_wr_bytes, std_dev_task_sf_wr_bytes ]
    out_stage.write(",".join([str(t) for t in stage_line]) + os.linesep)

# Generate timeline graph data
timeseries = sorted(conc_flows.keys())
job_t = {}
stage_t = {}
task_t = {}
for job in job_data:
    job_start = 9999999999
    job_end = 0
    for stage in job_data[job]:
        stage_start = 9999999999
        stage_end = 0
        for task in job_data[job][stage]:
            task_start = job_data[job][stage][task][stage_headers.index("Start time")]
            task_end = job_data[job][stage][task][stage_headers.index("End time")]
            task_t[task] = [int(task_start), int(task_end)]

            # Get stage timings
            if task_start < stage_start:
                stage_start = task_start
            if task_end > stage_end:
                stage_end = task_end
        # Get job timings
        if stage_start < job_start:
            job_start = stage_start
        if stage_end > job_end:
            job_end = stage_end
        stage_t[stage] = [int(stage_start), int(stage_end)]
    job_t[job] = [int(job_start), int(job_end)]

timeline_line = ["Time"]
for task in task_t:
    timeline_line.append("Task-" + str(task))
for stage in stage_t:
    timeline_line.append("Stage-" + str(stage))
for job in job_t:
    timeline_line.append("Job-" + str(job))
out_timeline.write(",".join(timeline_line) + os.linesep)

for ts in sorted(conc_flows.keys()):
    timeline_line = [str(ts / (24 * 3600.0))]
    # Check if this time stamp is valid for this task
    for task in task_t:
        if task_t[task][0] <= ts and ts <= task_t[task][1]:
            timeline_line.append(str(task))
        else:
            timeline_line.append("")
    for stage in stage_t:
        if stage_t[stage][0] <= ts and ts <= stage_t[stage][1]:
            timeline_line.append(str(stage))
        else:
            timeline_line.append("")
    for job in job_t:
        if job_t[job][0] <= ts and ts <= job_t[job][1]:
            timeline_line.append(str(job))
        else:
            timeline_line.append("")
    out_timeline.write(",".join(timeline_line) + os.linesep)

print "Succefully generated stats"
