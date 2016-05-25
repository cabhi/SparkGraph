from SparkEvents import SparkEvents


class SparkListenerStageSubmitted(SparkEvents):
    """docstring for SparkListenerStageSubmitted"""

    def __init__(self, arg):
        super(SparkListenerStageSubmitted, self).__init__(arg)
        self.id = arg["Stage Info"]["Stage ID"]

    def getStartTime(self):
        pass

    def getEndTime(self):
        pass
