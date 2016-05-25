import json
import pprint
from sets import Set

from utiils.functions import *
from utiils.ClassFactory import ClassFactory
from utiils.SparkApp import SparkApp
from Events.SparkListenerJobStart import SparkListenerJobStart
#from Graph.JobGraph import JobGraph
from Graph.NwxGraph import NwxGraph as NwG


pp = pprint.PrettyPrinter(indent=4)
outFile = None

def writeFormattedValues(sparkEventData):
    global outFile
    if outFile == None :
        outFile = open("/Users/abhishek.choudhary/MY_HOME/SparkEnhancements/EVENT_LOG_FORMATTED_2.json", 'w')
    json.dump(sparkEventData, outFile, indent=4, sort_keys=True)
    outFile.write('\n---------->\n')


# Load the Input File
lines = [line.rstrip('\n') for line in open(
    '/Users/abhishek.choudhary/MY_HOME/SparkEnhancements/EVENT_LOG')]

# Get All the Instance For Events

myApp = SparkApp()
#myJobGraph = JobGraph()
myJobGraph = NwG()
myJobGraph.setAppInstance(myApp)

for sparkEvent in lines:
    sparkEventData = json.loads(sparkEvent)
    writeFormattedValues(sparkEventData)

    newEventClass = ClassFactory().str_to_class("Events." + sparkEventData['Event'].encode(
        'utf-8'), sparkEventData['Event'].encode('utf-8',), sparkEventData)

    if (newEventClass is not None):
        myApp.addSparkEvent(newEventClass)


for sparkStageEvent in myApp.getAllEvents():
    if isinstance(sparkStageEvent, SparkListenerJobStart):

        currJobId = sparkStageEvent.getJobID()
        currJobName = sparkStageEvent.getJobName()

        jobStageIDs = sparkStageEvent.getJobStageIDs()
        stageInfos = sparkStageEvent.getJobStageInfos()

        myJobGraph.addJobs(currJobId, currJobName)
        myJobGraph.addJobStages(currJobId, jobStageIDs)
        myJobGraph.addDetailedStages(stageInfos)


myJobGraph.processStageGraph()
myJobGraph.processRDDGraph()

exit(0)
