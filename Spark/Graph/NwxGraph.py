import networkx as nx
from utiils import functions as fn
import pydotplus.graphviz as pydot
from sets import Set



class NwxGraph() :
    def __init__(self):
        self.G = nx.DiGraph()
        self.R = nx.DiGraph()

        self.jobs = {}
        self.jobStageIdMapping = {}
        self.stageDetailedInfo = {}
        self.rddDetailedInfo = {}
        self.timestampStageMapping={}
        self.appInstance = None

    def setAppInstance(self,appInstance):
        self.appInstance = appInstance

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

        # Lets Add all the Stages as Node First

        for stageId, stageDetail in self.stageDetailedInfo.iteritems():
            self.G.add_node(stageId,label='"' +
                         str(stageDetail["Stage Name"]) + '"',URL='http://www.google.com'
                            ,shape="box",fillcolor="white",style="filled",color="white",fontcolor="black"
                            )


        # Now For Each Job Create Edges

        nonParentStages = []

        for jobId, jobName in self.jobs.iteritems():
            for jobStage in self.jobStageIdMapping[jobId]:
                stageParents = self.stageDetailedInfo[jobStage]["Parent IDs"]
                if len(stageParents) > 0:
                    for tParents in stageParents:
                        self.G.add_edge(tParents,jobStage,label=str(jobId),color="white",fontcolor="white")
                else:
                    if(jobId == jobName):
                        tempEdge = ('JobID_' + str(jobId),jobStage,str(jobId))
                    else:
                        tempEdge =(str(jobName),jobStage,str(jobId))
                    nonParentStages.append(tempEdge)

        # Lets Add Job Start Boxes

        for tEdge in nonParentStages:
            if(self.G.out_degree(tEdge[1]) ==0):
                self.G.remove_node(tEdge[1])
            else:
                self.G.add_node(tEdge[0],shape="diamond",fillcolor="white",style="filled",color="white",fontcolor="black")
                timeStamp = (self.appInstance.getJobById(int(tEdge[2])).getStartTime())/1000
                self.G.add_edge(tEdge[0],tEdge[1],label = tEdge[2],color="white",fontcolor="white")

                if timeStamp in self.timestampStageMapping:
                    self.timestampStageMapping[timeStamp].append(tEdge[0])
                else:
                    self.timestampStageMapping[timeStamp] = [tEdge[0]]

        for stageId in  self.G.nodes() :
            if stageId in self.stageDetailedInfo :
                timeStamp = (self.appInstance.getStageById(stageId).getEndTime())/1000
                if timeStamp in self.timestampStageMapping:
                    self.timestampStageMapping[timeStamp].append(stageId)
                else:
                    self.timestampStageMapping[timeStamp] = [stageId]


        # Lets Get Into Pydot Format

        P = nx.nx_pydot.to_pydot(self.G)
        P.set_graph_defaults(rankdir="TB",bgcolor="black");
        P.set_edge_defaults(color="white",fontcolor="white")

        timeLinePlot = pydot.Subgraph(graph_name="timeseries")
        timeLinePlot.set_node_defaults(shape="plaintext",fontcolor="white")


        timeLineData = []

        for key in self.timestampStageMapping.keys():
            timeLineData.append(key)
        timeLineData.sort()

        for timeStamp in timeLineData:
            timeLinePlot.add_node(pydot.Node(name=timeStamp, label = fn.epochToTime(timeStamp),shape="plaintext",fontcolor="white"))

        for i in range(len(timeLineData)-1):
            timeLinePlot.add_edge(pydot.Edge(str(timeLineData[i]),str(timeLineData[i+1])))

        P.add_subgraph(timeLinePlot)


        for timeStamp in timeLineData:
            if timeStamp in self.timestampStageMapping:

                subGraphs =  pydot.Subgraph(graph_name=str(timeStamp),rank = "same")
                subGraphs.set_edge_defaults(color="white",fontcolor="white")
                subGraphs.set_node_defaults(shape="box",fillcolor="white",style="filled",color="white",fontcolor="black")

                for nodes in self.timestampStageMapping[timeStamp]:
                    tNode = P.get_node(str(nodes))[0]
                    subGraphs.add_node(tNode)

                subGraphs.add_node(timeLinePlot.get_node(str(timeStamp))[0])
                P.add_subgraph(subGraphs)
            else:
                timeLinePlot.del_node(str(timeStamp))
                print "No timeStamp Event for :" + str(timeStamp)


        P.write_dot("/Users/abhishek.choudhary/networx_stage.dot")
        P.write_png("/Users/abhishek.choudhary/networx_stage.png")


    def processRDDGraph(self):
        # First Create node with All the RDDs
        for rddId, rddDetail in self.rddDetailedInfo.iteritems():
            self.R.add_node(rddId, label='"' +
                         str(rddDetail["Name"]) + '[' + str(rddId) + ']"')

        nonParentRDDs = []
        # For All RDDs Lets Create Parent to Child Link
        for rddId, rddDetail in self.rddDetailedInfo.iteritems():
            parentRDDs = rddDetail["Parent IDs"]
            if(len(parentRDDs) > 0):
                for tParents in parentRDDs:
                    self.R.add_edge(tParents,rddId)
            else:
                nonParentRDDs.append(rddId)


        # lets iterate for all the non parent nodes and find max edge graph

        graphId = {}
        graphEdge = {}
        id = 0;

        for rddId in nonParentRDDs :
            graph_id = None

            if rddId in graphId :
                continue
            else :
                # find all the edges with starting point
                visited = False
                allEdges = list(nx.dfs_edges(self.R,rddId))
                tList = []
                tSet = Set()
                for tEdge in allEdges:
                    sourceVertex = tEdge[0]
                    destinationVertex = tEdge[1]
                    if sourceVertex in graphId or destinationVertex in graphId:
                        visited = True
                        try:
                            graph_id = graphId[sourceVertex]["graph_id"]
                        except KeyError :
                            graph_id = graphId[destinationVertex]["graph_id"]

                    tSet.add(sourceVertex)
                    tSet.add(destinationVertex)

                if visited is not True :
                    graph_id = id
                    id = id + 1
                for v in tSet:
                    graphId[v]= {"graph_id" : graph_id }

                if graph_id in graphEdge:
                    graphEdge[graph_id] = graphEdge[graph_id].append(allEdges)
                else:
                    graphEdge[graph_id] = allEdges


        print "Total Distinct Graphs : " + str(id)
        print "-------------"
        print graphId

        # Lets Clean internal RDDs

        P = nx.nx_pydot.to_pydot(self.R)
        P.write_dot("/Users/abhishek.choudhary/networx_rdd.dot")

