from SparkEvents import SparkEvents


class SparkListenerStageCompleted (SparkEvents):
    """docstring for SparkListenerStageCompleted """

    def __init__(self, arg):
        super(SparkListenerStageCompleted, self).__init__(arg)
        self.id = arg["Stage Info"]["Stage ID"]

    def getStartTime(self):
        return self.eventJson['Stage Info']['Submission Time']

    def getEndTime(self):
        return self.eventJson['Stage Info']['Completion Time']

    def getStageTotalTask(self):
        return self.eventJson['Stage Info']['Number of Tasks']



"""

"""
