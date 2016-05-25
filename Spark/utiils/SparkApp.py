from Events.SparkListenerStageCompleted import SparkListenerStageCompleted
from Events.SparkListenerJobStart import SparkListenerJobStart
from Events.SparkListenerTaskEnd import SparkListenerTaskEnd
from Events.SparkListenerTaskStart import SparkListenerTaskStart
from Events.SparkListenerApplicationEnd import SparkListenerApplicationEnd
from sets import Set
class SparkApp() :
    def __init__(self):
        self.app = {}
        self.jobs = []
        self.stages = []
        self.allEvents = []
        self.allRdds = {}
        self.eventTimeLine=[]


    def __populateRddStageMapping(self):
        pass

    def __processTimeLine(self):
        tSet = Set()
        for sEvent in self.allEvents:
            if not isinstance(sEvent,(SparkListenerTaskEnd, SparkListenerTaskStart,SparkListenerApplicationEnd)):
                startTime = sEvent.getStartTime()
                endTime = sEvent.getEndTime()
                if startTime is not None:
                    tSet.add(startTime/1000)
                if endTime is not None:
                    tSet.add(endTime/1000)

        self.eventTimeLine = sorted(tSet)

    def addSparkEvent(self,sparkEvent):
        if ("SparkEvents" in sparkEvent.parentName()):
            self.allEvents.append(sparkEvent)
        else:
            print "Not a SparkEvents"

    def getAllEvents(self):
        return self.allEvents

    def getStageById(self,stageId):
        return [ x for x in self.allEvents if  isinstance(x, SparkListenerStageCompleted) and x.getId()==stageId ].pop()

    def getJobById(self,jobId):
        return [ x for x in self.allEvents if  isinstance(x, SparkListenerJobStart) and x.getId()==jobId ].pop()

    def getTaskById(self,taskId):
        return [ x for x in self.allEvents if  isinstance(x, SparkListenerTaskEnd) and x.getId()==taskId ].pop()

    def getTimeLine(self):
        if len(self.eventTimeLine) == 0 :
            self.__processTimeLine()

        return self.eventTimeLine



