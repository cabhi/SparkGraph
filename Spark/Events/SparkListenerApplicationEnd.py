from SparkEvents import SparkEvents


class SparkListenerApplicationEnd(SparkEvents):
    """docstring for SparkListerApplicationEnd"""

    def __init__(self, arg):
        super(SparkListenerApplicationEnd, self).__init__(arg)

    def getStartTime(self):
        return None

    def getEndTime(self):
        return self.eventJson['Timestamp']
