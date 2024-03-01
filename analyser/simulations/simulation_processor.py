import os
import pandas as pd
import re
import json


class SimulationProcessor:
    def __init__(self, graph_converter, belief_processor):
        self.root_folder_path = ''
        self.dataframe = None
        self.graph_converter = graph_converter
        self.belief_processor = belief_processor
    
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

    def process_simulations(self, root_folder_path):
        if root_folder_path:
            self.root_folder_path = os.path.expanduser(root_folder_path)

        subfolders = [os.path.join(self.root_folder_path, folder) for folder in os.listdir(self.root_folder_path) if os.path.isdir(os.path.join(self.root_folder_path, folder))]
        result_df = pd.DataFrame()

        for subfolder_path in subfolders:
            subfolder_df = self.process_subfolder(subfolder_path)
            result_df = pd.concat([result_df, subfolder_df], ignore_index=True)

        self.dataframe = result_df

    def process_subfolder(self, subfolder_path):
        # This method is responsible for processing each subfolder in the root folder.
        files = os.listdir(subfolder_path)
        bin_files = sorted([f for f in files if f.endswith('.bin')], key=lambda x: int(re.search(r'(\d+)\.bin', x).group(1)))
        hd5_files = sorted([f for f in files if f.endswith('.hd5')], key=lambda x: int(re.search(r'(\d+)\.hd5', x).group(1)))
        config_file = [f for f in files if f == 'configuration.json']
        csv_file = [f for f in files if f.endswith('.csv')]

        df = pd.DataFrame()
        df['bin_file_path'] = [os.path.join(subfolder_path, f) for f in bin_files]
        df['hd5_file_path'] = [os.path.join(subfolder_path, f) for f in hd5_files]

        if config_file:
            config_json_path = os.path.join(subfolder_path, config_file[0])
            df['config_json_path'] = config_json_path
            trials, network_size, network_kind, op, epsilon = self.extract_params(config_json_path)
            df['trials'] = trials
            df['network_size'] = network_size
            df['network_kind'] = network_kind
            df['op'] = op
            df['epsilon'] = epsilon
        else:
            df['config_json_path'] = None

        if csv_file:
            csv_path = os.path.join(subfolder_path, csv_file[0])
            csv_df = pd.read_csv(csv_path)
            num_files = min(len(bin_files), len(hd5_files))
            if len(csv_df) != num_files:
                print("Warning: Number of rows in data.csv does not match the number of bin and hd5 files.")
            df = pd.concat([df[:num_files], csv_df], axis=1)
        else:
            df[['steps', 'duration', 'action', 'undefined', 'converged', 'polarized', 'uid']] = None

        return df
