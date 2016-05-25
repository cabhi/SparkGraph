from SparkEvents import SparkEvents


class SparkListenerTaskEnd(SparkEvents):

    def __init__(self, arg):
        super(SparkListenerTaskEnd, self).__init__(arg)
        self.id = arg['Task Info']['Task ID']

    def getStartTime(self):
        pass

    def getEndTime(self):
        return self.eventJson['Task Info']['Finish Time']

    def getStageID(self):
        return self.eventJson['Stage ID']

    def getTaskID(self):
        return self.eventJson['Task Info']['Task ID']

    def getExecutorID(self):
        return self.eventJson['Task Info']['Executor ID']

    def getTaskHost(self):
        return self.eventJson['Task Info']['Host']

    def getTaskLocalityType(self):
        return self.eventJson['Task Info']['Locality']


"""SparkListenerTaskEnd
{
    "Event": "SparkListenerTaskEnd",
    "Stage Attempt ID": 0,
    "Stage ID": 0,
    "Task End Reason": {
        "Reason": "Success"
    },
    "Task Info": {
        "Accumulables": [],
        "Attempt": 0,
        "Executor ID": "6",
        "Failed": false,
        "Finish Time": 1455186098134,
        "Getting Result Time": 0,
        "Host": "YARN-DN-2",
        "Index": 9,
        "Launch Time": 1455186082725,
        "Locality": "NODE_LOCAL",
        "Speculative": false,
        "Task ID": 9
    },
    "Task Metrics": {
        "Disk Bytes Spilled": 0,
        "Executor Deserialize Time": 1577,
        "Executor Run Time": 13656,
        "Host Name": "YARN-DN-2",
        "Input Metrics": {
            "Bytes Read": 0,
            "Data Read Method": "Hadoop",
            "Records Read": 0
        },
        "JVM GC Time": 132,
        "Memory Bytes Spilled": 0,
        "Result Serialization Time": 0,
        "Result Size": 2274,
        "Shuffle Write Metrics": {
            "Shuffle Bytes Written": 0,
            "Shuffle Records Written": 0,
            "Shuffle Write Time": 12652
        }
    },
    "Task Type": "ShuffleMapTask"
}
"""
