from SparkEvents import SparkEvents


class SparkListenerTaskStart(SparkEvents):
    """docstring for SparkListenerTaskStart"""

    def __init__(self, arg):
        super(SparkListenerTaskStart, self).__init__(arg)

    def getStartTime(self):
        return self.eventJson['Task Info']['Launch Time']

    def getEndTime(self):
        pass

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


"""
SparkListenerTaskStart
{
    "Event": "SparkListenerTaskStart",
    "Stage Attempt ID": 0,
    "Stage ID": 0,
    "Task Info": {
        "Accumulables": [],
        "Attempt": 0,
        "Executor ID": "5",
        "Failed": false,
        "Finish Time": 0,
        "Getting Result Time": 0,
        "Host": "YARN-DN-1",
        "Index": 0,
        "Launch Time": 1455186082694,
        "Locality": "NODE_LOCAL",
        "Speculative": false,
        "Task ID": 0
    }
}
"""
