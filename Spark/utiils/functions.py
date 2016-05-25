import pytz,datetime

def getParentStages(stageInfo):
    return {stageInfo["Stage ID"]: stageInfo["Parent IDs"]}


def getStageRDDList(stageInfo):
    stageRDDs = []
    for rddInfo in stageInfo["RDD Info"]:
        stageRDDs.append(rddInfo["RDD ID"])
    return stageRDDs

def epochToTime(epochTime):
    s = datetime.datetime.fromtimestamp(epochTime,pytz.utc).strftime('%H:%M:%S')
    return s
