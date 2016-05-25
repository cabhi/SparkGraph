import json
import pprint
from sets import Set

from Spark.utiils.functions import *
from Spark.utiils.ClassFactory import ClassFactory
from Spark.Events.SparkListenerJobStart import SparkListenerJobStart


pp = pprint.PrettyPrinter(indent=4)


# Load the File
lines = [line.rstrip('\n') for line in open(
    '/Users/abhishek.choudhary/MY_HOME/SparkEnhancements/EVENT_LOG')]

allEvents = Set()
classLists = []

# Get All the Instance For Events

# outFile = open(
#    "/Users/abhishek.choudhary/MY_HOME/SparkEnhancements/EVENT_LOG_FORMATTED.json", 'w')

for sparkEvent in lines:
    sparkEventData = json.loads(sparkEvent)

    # json.dump(sparkEventData, outFile, indent=4, sort_keys=True)
    # outFile.write('\n---------->\n')
    newEventClass = ClassFactory().str_to_class("Spark.Events." + sparkEventData['Event'].encode(
        'utf-8'), sparkEventData['Event'].encode('utf-8',), sparkEventData)

    if (newEventClass is not None):
        allEvents.add(sparkEventData['Event'].encode('utf-8'))
        classLists.append(newEventClass)

for sparkStageEvent in classLists:
    if(isinstance(sparkStageEvent, SparkListenerJobStart)):
        print sparkStageEvent.getJobID()
        print sparkStageEvent.getJobStageIDs()
        stageInfo = sparkStageEvent.getJobStageInfos()
        for allStages in stageInfo:
            print json.dumps(getParentStages(allStages), indent=4, sort_keys=True)
            print json.dumps(getStageRDDList(allStages), indent=4, sort_keys=True)
        print "-------->"
exit(0)


for sparkEvent in allEvents:
    tempEvent = next(
        x for x in classLists if x.__class__.__name__ == sparkEvent)
    print "---------------------->"
    print sparkEvent
    tempEvent.printClassdump()
