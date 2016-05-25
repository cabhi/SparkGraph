from SparkEvents import SparkEvents


class SparkListenerApplicationStart(SparkEvents):
    """docstring for SparkListenerApplicationStart"""

    def __init__(self, arg):
        super(SparkListenerApplicationStart, self).__init__(arg)
        self.id = arg["App ID"]

    def getStartTime(self):
        return self.eventJson['Timestamp']

    def getEndTime(self):
        return None

    def getAppName(self):
        return self.eventJson['App Name']

    def getAppId(self):
        return self.eventJson['App ID']
