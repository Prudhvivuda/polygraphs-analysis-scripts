import os
import pandas as pd
import re
import json
import dgl
import networkx as nx
import matplotlib.pyplot as plt
class PolgraphProcessor:
    
    def __init__(self, root_folder_path):
        self.root_folder_path = os.path.expanduser(root_folder_path)
        
    def get_graph(self, filepath):
        graphs, _ = dgl.load_graphs(filepath)
        graph = graphs[0]

        # Remove self-loops
        graph = dgl.remove_self_loop(graph)
        
        G = nx.Graph(dgl.to_networkx(graph))

        nx.draw(G, pos=nx.circular_layout(G))
        plt.show()
        
        
    def add_density(self, dataframe):
        density_list = []
        for bin_file_path in dataframe['bin_file_path']:
            graphs, _ = dgl.load_graphs(bin_file_path)
            graph = graphs[0]

            # Remove self-loops
            graph = dgl.remove_self_loop(graph)

            # Convert graph to networkx format
            graphx = dgl.to_networkx(graph)

            # Collect graph statistics
            density = nx.density(graphx)
            density_list.append(density)

            print(f"The density for the graph at {bin_file_path} is {density}")
        
        # Create a new DataFrame with the density column
        density_df = pd.DataFrame({'density': density_list})
        return density_df
        
        
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
    

    def process_root_folder(self):
        # Get list of subfolders in the root folder
        subfolders = [os.path.join(self.root_folder_path, folder) for folder in os.listdir(self.root_folder_path) if os.path.isdir(os.path.join(self.root_folder_path, folder))]

        result_df = pd.DataFrame()
        
        # Process each subfolder
        for subfolder_path in subfolders:
            subfolder_df = self.process_subfolder(subfolder_path)
            result_df = pd.concat([result_df, subfolder_df], ignore_index=True)
        
        return result_df
    

if __name__ == "__main__":
    processor = PolgraphProcessor("~/polygraphs-cache/results/2024-02-21/")
    result_df = processor.process_root_folder()
    print(result_df)
    df = result_df[['bin_file_path', 'undefined', 'uid', 'epsilon', 'network_size', 'network_kind', 'trials', 'network_kind']]
    print(df)
    print(result_df.columns)
    df_with_density = processor.add_density(df)
    result_df = pd.concat([df, df_with_density], axis=1)
    # df = result_df[['bin_file_path', 'undefined', 'uid', 'epsilon', 'network_size', 'network_kind', 'trials', 'network_kind']]
    print(result_df)
    processor.get_graph("/Users/prudhvivuda/polygraphs-cache/results/2024-02-21/a3aa2ab4a9a84396ba683ef2e07a3008/01.bin")
    
    
