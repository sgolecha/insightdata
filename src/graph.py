from sortedcontainers import SortedDict, SortedList
from operator import attrgetter
import json
import pprint

class Edge(object):
    """Edge represents a single edge in a graph

    Attributes:
        tstamp(int) :   unix timestamp of edge
        source(str) :   source of the edge
        target(str) :   target of the edge
        name(str)   :   name of the edge 
    """

    def __init__(self, tstamp, node1, node2):
        """Initialize the attributes of an Edge.
        Since the edge is to be treated as undirected, we sort the 
        names of the node1 and node2 lexicographically and use the 
        earlier node as source and the later one as target.
        The name of the edge is the contatenantion of
        source and target
        """
        self.tstamp = tstamp
        self.name = None
        if node1 < node2:
            self.source = node1
            self.target = node2
        else:
            self.source = node2
            self.target = node1

        self.name = Edge.derive_name(node1, node2)

    def __repr__(self):
        return 'Edge(ts=%d, name=%s)' % (self.tstamp, self.name)

    @staticmethod
    def derive_name(node1, node2):
        if node1 < node2:
            return ":".join([node1, node2])
        return ":".join([node2, node1])

class Node(object):
    """Node represents a single node in a graph

    Attributes:
        degree(int) :   degree of the node
        name(str)   :   name of the node 
    """

    def __init__(self, name):
        self.name = name
        self.degree = 0

    def __repr__(self):
        return 'Node(name=%s)' % (self.name)


class EdgeList(object):
    def __init__(self, tstamp):
        self.tstamp = tstamp
        self.edges = SortedDict()


class TxGraph(object):
    WINDOW_SIZE = 60
    def __init__(self):
        self.median = 0
        self.highMarker = TxGraph.WINDOW_SIZE
        self.lowMarker = 1
        self.txMap = SortedDict() #sorted by time stamp
        self.edgeMap = SortedDict() #sorted by edge name
        self.nodeMap = SortedDict() #sorted by node name

    def __calculate_median(self):
        #pprint.pprint(self.nodeMap)
        degreeList = SortedList()
        for node in self.nodeMap.itervalues():
            if node.degree > 0:
                degreeList.add(node.degree)

        #pprint.pprint(degreeList)
        listLen = len(degreeList)
        if listLen == 0:
            raise Exception("No items in the degreeList")

        if listLen == 1:
            return degreeList[0]/1.0

        if (listLen % 2) == 0: 
            medianIdx1 = listLen/2
            medianIdx2 = medianIdx1 - 1
            return (degreeList[medianIdx2] + degreeList[medianIdx1]) / 2.0
        
        medianIdx = listLen/2
        return degreeList[medianIdx]/1.0

    def __get_edgelist(self, tstamp, create=True):
        edgeList = self.txMap.get(tstamp, None)
        if edgeList is None and create is True:
            edgeList = EdgeList(tstamp)
            self.txMap[tstamp] = edgeList
        return edgeList

    def __getnode_with_name(self, name, create=True):
        node = self.nodeMap.get(name, None)
        if node is None and create is True:
            node = Node(name)
            self.nodeMap[name] = node
        return node

    def process_transaction(self, tstamp, source, target):
        #basic sanity checks
        if source is None or target is None:
            raise Exception("Invalid node")

        if len(source) == 0 or len(target) == 0:
            raise Exception("Invalid node")

        if source == target:
            raise Exception("source and target cannot be the same")
        
        #timestamp is old and can be ignored
        if tstamp < self.lowMarker:
            return

        #create a new edge representing this transaction     
        newEdge = Edge(tstamp, source, target)
        
        pprint.pprint("processing edge with name: '%s' and tstamp: '%d, highMarker: %d'" %(newEdge.name, newEdge.tstamp, self.highMarker))
        #tx is within the window
        if tstamp <= self.highMarker:
            #check if this edge was seen in the current window
            pprint.pprint("withing the window, checking if edge exists")
            if newEdge.name in self.edgeMap:
                pprint.pprint("found old edge with name: '%s' in edgeMap" %newEdge.name)
                self.__update_tstamp_for_existing_edge(newEdge.name, tstamp)
                #no need to recalculate the median here since degree does not change
                return
            
            """handle new edge
            1. find the edgelist with the same timestamp (if not create it)
            2. add this edge to the edgelist and edgemap
            4. create new Nodes for the edges if needed or update their degrees
            5. calculate the median
            """
            edgeList = self.__get_edgelist(tstamp)
            
            pprint.pprint("adding new edge '%s' to list with ts: '%d'" %(newEdge.name, edgeList.tstamp))
            edgeList.edges[newEdge.name] = newEdge
            self.edgeMap[newEdge.name] = newEdge

            node1 = self.__getnode_with_name(newEdge.source)
            node2 = self.__getnode_with_name(newEdge.target)

            node1.degree = node1.degree + 1
            node2.degree = node2.degree + 1

            self.median = self.__calculate_median()
            return

        """this transaction is newer and we need to move the window
        1. update the low and high markers of the timestamp window
        2. create edgelist with this newer timestamp
        2. add the new edge to the edgelist
        3. add the new edge to the edgemap
        4. create new Nodes of the edges if needed or update their degrees
        5. 
        """
        #this tx is newer and we need to move the window
        pprint.pprint("handling window move - old high: %d, new timestamp: %d" %(self.highMarker, tstamp))
        self.highMarker = tstamp
        self.lowMarker = tstamp - TxGraph.WINDOW_SIZE + 1

        self.__update_tx_window()
        
        if newEdge.name in self.edgeMap:
            pprint.pprint("found old edge with name: '%s' in edgeMap" %newEdge.name)
            self.__update_tstamp_for_existing_edge(newEdge.name, tstamp)
        else:
            edgeList = self.__get_edgelist(tstamp)
            pprint.pprint("adding '%s' to list with ts: '%d'" %(newEdge.name, edgeList.tstamp))
            edgeList.edges[newEdge.name] = newEdge
            self.edgeMap[newEdge.name] = newEdge

            node1 = self.__getnode_with_name(newEdge.source)
            node2 = self.__getnode_with_name(newEdge.target)
        
            node1.degree = node1.degree + 1
            node2.degree = node2.degree + 1

        self.median = self.__calculate_median()
        
    def __remove_edge(self, edge):
        src = self.__getnode_with_name(edge.source, create=False)
        if src:
            src.degree = src.degree - 1
            if src.degree == 0:
                del self.nodeMap[src.name]
    
        target = self.__getnode_with_name(edge.target, create=False)
        if target:
            target.degree = target.degree - 1
            if target.degree == 0:
                del self.nodeMap[target.name]
  
        pprint.pprint("removing edge with name: '%s' and ts: '%s'" %(edge.name, edge.tstamp))
        del self.edgeMap[edge.name]

    def __update_tstamp_for_existing_edge(self, edgeName, tstamp):
        """updates the timestamp for an existing edge and moves
        the edge to the appropriate EdgeList
        """
        
        currEdge = self.edgeMap[edgeName]
        if not currEdge:
            return
        
        if tstamp <= currEdge.tstamp:
            return #ignore older transactions within the window
        
        #remove the edge from the edgelist with old timestamp
        edgeList = self.__get_edgelist(currEdge.tstamp, create=False)
        del edgeList.edges[currEdge.name]

        #update the tstamp in the edge
        currEdge.tstamp = tstamp

        #move this edge to the correct edgelist
        edgeList = self.__get_edgelist(tstamp)
        edgeList.edges[currEdge.name] = currEdge
        return

    def __update_tx_window(self):
        """
        1. Get the edgeList's that are below the lowMarker
        2. Go through the edges and delete them from the edgeMap
        3. Update the degree of the nodes (i.e. reduce them)
        4. Move the window by deleting the stale edgeLists
        """
        pprint.pprint("lowMarker: %d" %self.lowMarker)
        tsIter = self.txMap.irange(None, self.lowMarker, inclusive=(True,False))
        lastTStamp = None
        for tstamp in tsIter:
            lastTStamp  = tstamp
            edgeList = self.txMap[tstamp]
        
            pprint.pprint("processing edgeList with tstamp: %d" %tstamp)
    
            for edge in edgeList.edges.itervalues():
                self.__remove_edge(edge)

        if lastTStamp:
            lowIdx = self.txMap.index(lastTStamp)
            del self.txMap.iloc[:lowIdx+1]
