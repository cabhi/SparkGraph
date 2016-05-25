import pydotplus.graphviz as pydot

node1 = pydot.Node(1)
node2 = pydot.Node(2)
node3 = pydot.Node(3)
node4 = pydot.Node(4)
node5 = pydot.Node(5)
node6 = pydot.Node(6)


P = pydot.Dot()

S = pydot.Subgraph(graph_name="cluster_hello", rank="same")
T = pydot.Subgraph(graph_name="cluster_Suup", rank="same")

S.add_node(node3)
S.add_node(node4)
S.add_node(node6)
T.add_node(node1)
T.add_node(node2)
T.add_node(node6)
T.add_node(node5)


P.add_subgraph(S)
P.add_subgraph(T)

P.add_edge(pydot.Edge(node1, node2))
P.add_edge(pydot.Edge(node2, node3))
P.add_edge(pydot.Edge(node1, node4))
P.add_edge(pydot.Edge(node1,node5))
P.add_edge(pydot.Edge(node4,node5))
P.add_edge(pydot.Edge(node1,node6))
P.add_edge(pydot.Edge(node5,node6))


P.write_dot('hello.dot')
P.write_pdf("hello.pdf")
