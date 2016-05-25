import pydotplus.graphviz as pydot
from pydotplus.graphviz import Dot
from pydotplus.graphviz import Node
from pydotplus.graphviz import Edge

class JobGraph ():

    def __init__(self):
        self.jobStageGraph = Dot(graph_type="digraph",bgcolor="black")
        self.jobStageGraph.set_node_defaults(shape="box",fillcolor="white",style="filled",color="white",fontcolor="black",URL=str('http://www.google.com'))
        self.jobStageGraph.set_edge_defaults(color="white",fontcolor="white")
        self.jobRDDGraph = Dot(graph_type="digraph",fontcolor="yellow")
        self.jobRDDGraph.set_node_defaults(shape="box")
        self.jobs = {}
        self.jobStageIdMapping = {}
        self.stageDetailedInfo = {}
        self.rddDetailedInfo = {}

    def addJobs(self, jobId, jobName):
        self.jobs[jobId] = jobName

    def addJobStages(self, jobId, jobStages):
        self.jobStageIdMapping[jobId] = jobStages

    def addDetailedStages(self, stageDetails):
        for stageInfo in stageDetails:
            if (stageInfo["Stage ID"] not in self.stageDetailedInfo):
                self.stageDetailedInfo[stageInfo["Stage ID"]] = {
                    "Stage Name": stageInfo["Stage Name"],
                    "Parent IDs": stageInfo["Parent IDs"]
                }

                for rddInfo in stageInfo["RDD Info"]:
                    self.rddDetailedInfo[rddInfo["RDD ID"]] = {
                        "Name": rddInfo["Name"],
                        "Parent IDs": rddInfo["Parent IDs"]
                    }

    def processStageGraph(self):
        # First Create node with All the Stages
        for stageId, stageDetail in self.stageDetailedInfo.iteritems():
            tNode = Node(name=stageId, label='"' +
                         str(stageDetail["Stage Name"]) + '"',)
            self.jobStageGraph.add_node(tNode)

        # Now For Each Job Create Edges

        for jobId, jobName in self.jobs.iteritems():
            for jobStage in self.jobStageIdMapping[jobId]:
                stageParents = self.stageDetailedInfo[jobStage]["Parent IDs"]
                if len(stageParents) > 0:
                    for tParents in stageParents:
                        srcVertics = self.jobStageGraph.get_node(str(tParents))
                        destVertics = self.jobStageGraph.get_node(
                            str(jobStage))
                        tEdge = Edge(srcVertics, destVertics,label="")
                        tEdge.set_label(str(jobId))
                        self.jobStageGraph.add_edge(tEdge)
                else:
                    if(jobId == jobName):
                        srcVertics = Node(name='"JobID_' + str(jobId))
                    else:
                        srcVertics = Node(name='"' + str(jobName) + '"')
                    self.jobStageGraph.add_node(srcVertics)
                    destVertics = self.jobStageGraph.get_node(str(jobStage))
                    tEdge = Edge(srcVertics, destVertics,label=str(jobId))

                    self.jobStageGraph.add_edge(tEdge)

        self.jobStageGraph.write_dot("/Users/abhishek.choudhary/SPARK_JOB.dot")

    def processRDDGraph(self):

        # First Create node with All the RDDs
        for rddId, rddDetail in self.rddDetailedInfo.iteritems():
            tNode = Node(name=rddId, label='"' +
                         str(rddDetail["Name"]) + '[' + str(rddId) + ']"')
            self.jobRDDGraph.add_node(tNode)

        # For All RDDs Lets Create Parent to Child Link
        for rddId, rddDetail in self.rddDetailedInfo.iteritems():
            parentRDDs = rddDetail["Parent IDs"]
            if(len(parentRDDs) > 0):
                for tParents in parentRDDs:
                    srcVertics = self.jobRDDGraph.get_node(str(tParents))
                    destVertics = self.jobRDDGraph.get_node(str(rddId))
                    tEdge = Edge(srcVertics, destVertics)
                    self.jobRDDGraph.add_edge(tEdge)
            else:
                pass
                # print "No Parent RDD for RDD ID :" + str(rddId)

        print "Done"
        self.jobRDDGraph.write_pdf(
            "/Users/abhishek.choudhary/SPARK_JOB_RDD.dot")
        # self.jobRDDGraph.write_png("/Users/abhishek.choudhary/SPARK_JOB_RDD.png")
