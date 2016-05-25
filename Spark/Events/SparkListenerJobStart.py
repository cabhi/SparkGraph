from SparkEvents import SparkEvents


class SparkListenerJobStart(SparkEvents):
    """docstring for SparkListenerJobStart"""

    def __init__(self, arg):
        super(SparkListenerJobStart, self).__init__(arg)
        self.id = arg["Job ID"]

    def getStartTime(self):
        return self.eventJson["Submission Time"]

    def getEndTime(self):
        pass

    def getJobID(self):
        return self.eventJson["Job ID"]

    def getJobName(self):
        tName = None
        if ("Properties" in self.eventJson):
            if("spark.job.description" in self.eventJson["Properties"]):
                tName= self.eventJson["Properties"]["spark.job.description"]
        if tName is not None:
            return tName
        else:
            return self.eventJson["Job ID"]

    def getJobStageIDs(self):
        return self.eventJson["Stage IDs"]

    def getJobStageInfos(self):
        out = []
        for stageInfo in self.eventJson["Stage Infos"]:
            tempDict = {}
            tempDict["Stage ID"] = stageInfo["Stage ID"]
            tempDict["Stage Name"] = stageInfo["Stage Name"]
            tempDict["Parent IDs"] = stageInfo["Parent IDs"]
            rddinfoList = []
            for rddInfo in stageInfo["RDD Info"]:
                rddDict = {}
                rddDict["RDD ID"] = rddInfo["RDD ID"]
                rddDict["Parent IDs"] = rddInfo["Parent IDs"]
                rddDict["Name"] = rddInfo["Name"]
                rddinfoList.append(rddDict)
            tempDict["RDD Info"] = rddinfoList
            out.append(tempDict)
        return out
