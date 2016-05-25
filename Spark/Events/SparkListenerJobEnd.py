from SparkEvents import SparkEvents


class SparkListenerJobEnd(SparkEvents):
    """docstring for SparkListenerJobEnd"""

    def __init__(self, arg):
        super(SparkListenerJobEnd, self).__init__(arg)
        self.id = arg["Job ID"]

    def getStartTime(self):
        None

    def getEndTime(self):
        return self.eventJson['Completion Time']

    def getJobID(self):
        return self.eventJson['Job ID']


"""
SparkListenerJobEnd
{
    "Completion Time": 1455186455553,
    "Event": "SparkListenerJobEnd",
    "Job ID": 4,
    "Job Result": {
        "Result": "JobSucceeded"
    }
}

"""
