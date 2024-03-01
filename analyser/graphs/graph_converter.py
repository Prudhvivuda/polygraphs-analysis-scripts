import dgl
import networkx as nx

class Graphs:
    def get_graph_object(self, filepath):
        graph, _ = dgl.load_graphs(filepath)
        return graph
    
    def convert_graph_networkx(self, graph):
        graph = dgl.remove_self_loop(graph[0])
        G = nx.Graph(dgl.to_networkx(graph))
        return G
    
    def get_networkx_object(self, filepath):
        return self.convert_graph_networkx(self.get_graph_object(filepath))