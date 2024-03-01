import networkx as nx

class AddAttributes:
    def __init__(self, graph_converter, belief_processor):
        self.graph_converter = graph_converter
        self.belief_processor = belief_processor
    
    def density(self, dataframe):
        density_list = [nx.density(self.graph_converter.get_networkx_object(bin_file_path)) for bin_file_path in dataframe['bin_file_path']]
        self.dataframe['density'] = density_list
    
    def majority(self, dataframe):
        majority_list = []
        for hd5_file_path, bin_file_path in zip(dataframe['hd5_file_path'], dataframe['bin_file_path']):
            iterations = self.belief_processor.get_beliefs(hd5_file_path, bin_file_path, self.graph_converter)
            majority_list.append(self.belief_processor.get_majority(iterations))
        self.dataframe['majority'] = majority_list
    
    def custom(self, dataframe):
        pass