import os
import pandas as pd
import re
import json
import dgl
import networkx as nx
import matplotlib.pyplot as plt
import h5py
class PolygraphProcessor:
    
    def __init__(self):
        self.root_folder_path = ''
        
    def get_graph_object(self, filepath):
        graph, _ = dgl.load_graphs(filepath)
        return graph
        
    
    def convert_graph_networkx(self, graph):
        graph = dgl.remove_self_loop(graph[0])
        G = nx.Graph(dgl.to_networkx(graph))
        return G
    
    def get_networkx_object(self, filepath):
        return self.convert_graph_networkx(self.get_graph_object(filepath))
        
    
    def get_beliefs(self, hd5_file_path, bin_file_path):
        
        graph = self.get_graph_object(bin_file_path)
        G = self.convert_graph_networkx(graph)
        
        fp = h5py.File(os.path.join(hd5_file_path), "r")
        _keys = [int(key) for key in fp["beliefs"].keys()]
        _keys = sorted(_keys)
        
        iterations = [(0, graph[0].ndata["beliefs"])]

        # Iterate over the keys and append to iterations
        for key in _keys:
            beliefs = fp["beliefs"][str(key)]
            # _ = {"iteration": key}
            iterations.append((key, list(beliefs)))
        
            
        index = pd.MultiIndex.from_product([[0, *_keys], list(G.nodes())], names=['iteration', 'node'])
        iterations = pd.DataFrame(index=index)
        iterations.loc[0, 'beliefs'] = graph[0].ndata["beliefs"].tolist()

        # Iterate over the keys and append to iterations
        for key in _keys:
            beliefs = fp["beliefs"][str(key)]
            iterations.loc[key, 'beliefs'] = list(beliefs)
        
        return iterations
            
    
    def get_majority(self, iterations):
        average_by_iteration = iterations.groupby(level='iteration').mean()
        iterations_above_threshold = average_by_iteration[average_by_iteration['beliefs'] > 0.5]
        iterations_list = iterations_above_threshold.index.tolist()
        return iterations_list[0]
    
            
    def add_majority(self, dataframe):
        majority_list = []
        
        for hd5_file_path, bin_file_path in zip(dataframe.hd5_file_path, dataframe.bin_file_path):
            iterations = self.get_beliefs(hd5_file_path, bin_file_path)
            majority_list.append(self.get_majority(iterations))
            
        dataframe['majority'] = majority_list
        return dataframe
        
        
    
    def add_density(self, dataframe):
        
        # Calculate density for each graph
        density_list = [nx.density(self.get_networkx_object(bin_file_path)) for bin_file_path in dataframe['bin_file_path']]
        
        # Assign density values to a new column 'density' in the original dataframe
        dataframe['density'] = density_list
        
        return dataframe
        
        
    def extract_params(self, config_json_path):
        with open(config_json_path, "r") as f:
            config_data = json.load(f)
        return (
            config_data.get("trials"),
            config_data.get("network", {}).get("size"),
            config_data.get("network", {}).get("kind"),
            config_data.get("op"),
            config_data.get("epsilon"),
        )


    def process_subfolder(self, subfolder_path):
        '''
            processes each simulation the date folder
        '''
        # Get list of files in the subfolder
        files = os.listdir(subfolder_path)
        
        # Filter files for required extensions
        bin_files = sorted([f for f in files if f.endswith('.bin')], key=lambda x: int(re.search(r'(\d+)\.bin', x).group(1)))
        hd5_files = sorted([f for f in files if f.endswith('.hd5')], key=lambda x: int(re.search(r'(\d+)\.hd5', x).group(1)))
        config_file = [f for f in files if f == 'configuration.json']
        csv_file = [f for f in files if f.endswith('.csv')]
        
        # Initialize an empty DataFrame
        df = pd.DataFrame()

        # Add paths to bin files
        df['bin_file_path'] = [os.path.join(subfolder_path, f) for f in bin_files]
        
        # Add paths to hd5 files
        df['hd5_file_path'] = [os.path.join(subfolder_path, f) for f in hd5_files]
        
        # Add path to configuration file
        if config_file:
            config_json_path = os.path.join(subfolder_path, config_file[0])
            df['config_json_path'] = config_json_path
            # Extract parameters from configuration file
            trials, network_size, network_kind, op, epsilon = self.extract_params(config_json_path)
            df['trials'] = trials
            df['network_size'] = network_size
            df['network_kind'] = network_kind
            df['op'] = op
            df['epsilon'] = epsilon
        else:
            df['config_json_path'] = None

        
        # Check if data.csv exists
        if csv_file:
            # Load data from data.csv into a DataFrame
            csv_path = os.path.join(subfolder_path, csv_file[0])
            csv_df = pd.read_csv(csv_path)
            
            # Check if the number of rows in csv_df matches the number of bin and hd5 files
            num_files = min(len(bin_files), len(hd5_files))
            if len(csv_df) != num_files:
                print("Warning: Number of rows in data.csv does not match the number of bin and hd5 files.")
            
            # Concatenate the bin and hd5 file paths with the data from data.csv
            df = pd.concat([df[:num_files], csv_df], axis=1)
        else:
            # If data.csv is missing, fill the corresponding columns with NULL values
            df[['steps', 'duration', 'action', 'undefined', 'converged', 'polarized', 'uid']] = None  
            
        return df
    

    def process_simulations(self, root_folder_path):
        '''
            this processes the date folder 
        '''
        if root_folder_path:
            self.root_folder_path = os.path.expanduser(root_folder_path)
        # Get list of subfolders in the root folder
        subfolders = [os.path.join(self.root_folder_path, folder) for folder in os.listdir(self.root_folder_path) if os.path.isdir(os.path.join(self.root_folder_path, folder))]

        result_df = pd.DataFrame()
        
        # Process each subfolder
        for subfolder_path in subfolders:
            subfolder_df = self.process_subfolder(subfolder_path)
            result_df = pd.concat([result_df, subfolder_df], ignore_index=True)
        return result_df

    

    
#####################################################################################
'''
to process the simulations: PolygraphProcessor().process_simulations('~/polygraphs-cache/results/2024-02-21/')
to get the density: processor.add_density(df) -> returns desnity_df with density column added 
to get the graph: processor.get_graph(result_df['bin_file_path'][1]) -> return graph obbject

'''