import os
import pandas as pd
import re
import json


class SimulationProcessor:
    def __init__(self, graph_converter, belief_processor):
        self.path = ''
        self.dataframe = None
        self.graph_converter = graph_converter
        self.belief_processor = belief_processor
        
    def _is_valid_uuid_folder(self, dirname):
        # Check if the folder name is a valid UUID format
        if len(dirname) == 32 and all(c in '0123456789abcdefABCDEF' for c in dirname):
            return True
        return False
    
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

    def process_simulations_old(self, path):
        if path:
            self.path = os.path.expanduser(path)
            
        result_df = pd.DataFrame()
        
        root_folder = path.split('/')[-1]
        
        if len(root_folder) == 10 and '-' in root_folder:
            subfolders = [os.path.join(self.path, folder) for folder in os.listdir(self.path) if os.path.isdir(os.path.join(self.path, folder))]
            for subfolder_path in subfolders:
                subfolder_df = self.process_subfolder(subfolder_path)
                result_df = pd.concat([result_df, subfolder_df], ignore_index=True)
        else:
            path = os.path.expanduser(path)
            date_folders = os.listdir(path)
            date_folders = [os.path.join(path, folder) for folder in date_folders if len(folder) == 10 and '-' in folder]
            sim_folders = []
            for date_folder_name in date_folders:
                for folder in os.listdir(date_folder_name):
                    subfolders = [os.path.join(date_folder_name, folder) for folder in os.listdir(date_folder_name) if os.path.isdir(os.path.join(date_folder_name, folder))]
                sim_folders.extend(subfolders)
            for subfolder_path in sim_folders:
                subfolder_df = self.process_subfolder(subfolder_path)
                result_df = pd.concat([result_df, subfolder_df], ignore_index=True)

        self.dataframe = result_df
    
    def process_simulations(self, path):
        if path:
            self.path = os.path.expanduser(path)
        
        uuid_folders = []
        try:
            for dirpath, dirnames, filenames in os.walk(self.path):
                for dirname in dirnames:
                    folder_path = os.path.join(dirpath, dirname)
                    if self._is_valid_uuid_folder(dirname):
                        uuid_folders.append(folder_path)
        except (FileNotFoundError, PermissionError) as e:
            print(f"Error accessing folder: {e}")
            
        result_df = pd.DataFrame()
        
        for folder in uuid_folders:
            subfolder_df = self.process_subfolder(folder)
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
            df[['steps', 'duration', 'action', 'undefined', 'converged', 'polarized']] = None
            df['uid'] = subfolder_path.split('/')[-1]

        return df
