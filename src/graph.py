from sortedcontainers import SortedDict, SortedList
from operator import attrgetter
import json
import pprint

class Edge(object):
    """represents a single edge in a graph

    Attributes:
        tstamp(int) :   unix epoch of the transaction(tx) that
                        this edge represents
        source(str) :   source of the edge
        target(str) :   target of the edge
        name(str)   :   name of the edge 
    """

    def __init__(self, tstamp, node1, node2):
        """Initializes the attributes of an Edge.
        Since the edge is to be treated as undirected, we sort the 
        names of the node1 and node2 lexicographically and use the 
        earlier node as source and the later one as target.
        The name of the edge is the contatenantion of
        source and target with a ":" in between
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
    """represents a single node in a graph

    Attributes:
        degree(int) :   degree of the node
        name(str)   :   name of the node 
    """

    def __init__(self, name):
        self.name = name
        self.degree = 0

    
    def __repr__(self):
        return 'Node(name=%s)' % (self.name)

    
    def incr_degree(self):
        self.degree = self.degree + 1

    
    def decr_degree(self):
        self.degree = self.degree - 1


class EdgeList(object):
    """represents a collection of edges with the
    same timestamp
    
    Attributes:
        tstamp(int) :   unix epoch of the transactions
                        this EdgeList contains
        edges(dict) :   sorted dict of edges with edge name
                        being the key
    """

    def __init__(self, tstamp):
        self.tstamp = tstamp
        self.edges = SortedDict()


class TxGraph(object):
    """represents a graph of all transactions
    within the current window

    Attributes:
        median(float)   :   the current median of the degree of the nodes
        highMarker(int) :   the latest timestamp seen so far
        lowMarker(int)  :   the earliest timestamp of the window we are
                            interested in
        txMap(dict)     :   this is a collection of EdgeList's with key being
                            the timestamp and the value an instance of EdgeList
        edgeMap(dict)   :   this is collection of all Edges within a window
                            with key being the name of an Edge
        nodeMap(dict)   :   this represents a collection of Nodes with a window
                            with key being the name of the Node
        
    """

    WINDOW_SIZE = 60
    def __init__(self):
        self.median = 0
        self.highMarker = TxGraph.WINDOW_SIZE
        self.lowMarker = 1
        self.txMap = SortedDict() #sorted by unix epoch (timestamp)
        self.edgeMap = SortedDict() #sorted by edge name
        self.nodeMap = SortedDict() #sorted by node name

    
    def __calculate_median(self):
        """calculates median by adding degrees to a sortedlist
        """
        degreeList = SortedList()
        for node in self.nodeMap.itervalues():
            if node.degree > 0:
                degreeList.add(node.degree)

        listLen = len(degreeList)
        if listLen == 0:
            raise Exception("No items in the degreeList")

        if listLen == 1:
            return degreeList[0]/1.0

        if (listLen % 2) == 0: 
            return (degreeList[listLen/2] + degreeList[(listLen/2) - 1]) / 2.0
        
        return degreeList[listLen/2]/1.0

    
    def __get_edgelist(self, tstamp, create=True):
        """returns an instance of EdgeList with matching
        timestamp and creates one if needed
        """
        edgeList = self.txMap.get(tstamp, None)
        if edgeList is None and create is True:
            edgeList = EdgeList(tstamp)
            self.txMap[tstamp] = edgeList
        return edgeList

    
    def __getnode_with_name(self, name, create=True):
        """returns an instance of Node with matching name
        and creates one if necessary

        Args:
            name(str)   :   name of the edge
            create(bool):   flag to indicate whether to create a 
                            missing node
        """
        
        node = self.nodeMap.get(name, None)
        if node is None and create is True:
            node = Node(name)
            self.nodeMap[name] = node
        return node

    
    def __incr_degree_of_edge_nodes(self, edge):
        """increments the degree of the two nodes
        of an edge
        """

        node = self.__getnode_with_name(edge.source)
        node.incr_degree()
        
        node = self.__getnode_with_name(edge.target)
        node.incr_degree()
    
    
    def __decr_degree_of_edge_nodes(self, edge):
        """decrements the degree of the two nodes
        of an edge
        """
        
        self.__decr_degree_of_node(edge.source)
        self.__decr_degree_of_node(edge.target)
   
  
    def __decr_degree_of_node(self, name):
        """decrements the degree of a node
        and removes it from the nodeMap if degree is 0
        """
        
        node = self.__getnode_with_name(name, create=False)
        node.decr_degree()
        
        if node.degree == 0:
            del self.nodeMap[node.name]


    def __remove_edge(self, edge):
        """removes an edge from the graph and updates the 
        degree of a node. If degree of a node goes to 0, then
        remove the node as well
        
        Args:
            egde(Edge)   :   An instance of Edge class
        """

        self.__decr_degree_of_edge_nodes(edge)
        del self.edgeMap[edge.name]

    
    def __update_tstamp_for_existing_edge(self, edgeName, tstamp):
        """updates the timestamp for an existing edge and moves
        the edge to an appropriate EdgeList
        
        Args:
            edgeName(str)   :   name of the edge to be updated
            tstamp(int)     :   unix epoch of the timstamp
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

    
    def __update_tx_window(self):
        """updates the transaction window of the graph
        This method is called when a newer transaction out the 
        window arrives. It does the following:
        1. Gets the edgeList's that are below the lowMarker
        2. Goes through the edges and deletes them from the edgeMap
        3. Update the degree of the nodes
        4. Moves the window by deleting the stale edgeLists
        """
        tsIter = self.txMap.irange(None, self.lowMarker, inclusive=(True,False))
        lastTStamp = None
        for tstamp in tsIter:
            lastTStamp  = tstamp
            edgeList = self.txMap[tstamp]
        
            for edge in edgeList.edges.itervalues():
                self.__remove_edge(edge)

        #lets delete the stale edgelists
        if lastTStamp:
            lowIdx = self.txMap.index(lastTStamp)
            del self.txMap.iloc[:lowIdx+1]
    
    
    def process_transaction(self, tstamp, source, target):
        """this is the starting point of transaction processing.
        We first check whether the tx is within the window.
        If it is, then we update the Edge (if it already exists) or 
        create a new Edge if necessary and update the median.
        If the tx is not within the window and is newer, we then
        move the window and remove all stale(older) edges and create
        a new edge for the newer transaction and finally update the
        median
        """
        
        #basic sanity checks
        if source is None or target is None:
            raise Exception("Invalid node")

        if len(source) == 0 or len(target) == 0:
            raise Exception("Invalid node")

        if source == target:
            raise Exception("source and target cannot be the same")
        
        #timestamp of the transaction is old and can be ignored
        if tstamp < self.lowMarker:
            return

        #create a new edge representing this transaction     
        newEdge = Edge(tstamp, source, target)
        
        if tstamp <= self.highMarker:
            if newEdge.name in self.edgeMap:
                self.__update_tstamp_for_existing_edge(newEdge.name, tstamp)
                #no need to recalculate the median here since degree does not change
                return
            
            """handle new edge
            1. find the edgelist with the same timestamp (if not create it)
            2. add this edge to the edgelist and edgemap
            4. create new Nodes for the edges if needed or update their degrees
            5. update the median
            """
            edgeList = self.__get_edgelist(tstamp)
            edgeList.edges[newEdge.name] = newEdge
            self.edgeMap[newEdge.name] = newEdge

            self.__incr_degree_of_edge_nodes(newEdge)
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
        self.highMarker = tstamp
        self.lowMarker = tstamp - TxGraph.WINDOW_SIZE + 1

        self.__update_tx_window()
        
        if newEdge.name in self.edgeMap:
            self.__update_tstamp_for_existing_edge(newEdge.name, tstamp)
        else:
            edgeList = self.__get_edgelist(tstamp)
            edgeList.edges[newEdge.name] = newEdge
            self.edgeMap[newEdge.name] = newEdge
            self.__incr_degree_of_edge_nodes(newEdge)

        self.median = self.__calculate_median()
