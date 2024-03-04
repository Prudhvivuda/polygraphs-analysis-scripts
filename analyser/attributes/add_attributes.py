import networkx as nx
import json 

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
    
    
    def add_from_config(self, key_path):
        values = []
        for congif_path in self.dataframe['config_json_path']:
            with open(congif_path, 'r') as file:
                json_str = file.read()
            json_obj = json.loads(json_str)
            keys = key_path.split('.')
            value = None
            current_obj = json_obj
            for key in keys:
                if key in current_obj:
                    current_obj = current_obj[key]
                    value = current_obj
                else:
                    value = None
                    break
            values.append(value)
        column_name = key_path.replace('.', '_').replace(' ', '')
        self.dataframe[column_name] = values
