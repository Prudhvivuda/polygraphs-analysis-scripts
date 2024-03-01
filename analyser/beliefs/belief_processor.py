import pandas as pd
import h5py

class Beliefs:
    def get_beliefs(self, hd5_file_path, bin_file_path, graph_converter):
        graph = graph_converter.get_graph_object(bin_file_path)
        G = graph_converter.convert_graph_networkx(graph)

        with h5py.File(hd5_file_path, "r") as fp:
            _keys = sorted(map(int, fp["beliefs"].keys()))
            iterations = [(0, graph[0].ndata["beliefs"].tolist())]

            for key in _keys:
                beliefs = fp["beliefs"][str(key)]
                iterations.append((key, list(beliefs)))

        index = pd.MultiIndex.from_product([[0, *_keys], list(G.nodes())], names=['iteration', 'node'])
        iterations_df = pd.DataFrame(index=index, columns=['beliefs'])

        for key, beliefs in iterations:
            iterations_df.loc[key, 'beliefs'] = beliefs

        return iterations_df

    def get_majority(self, iterations):
        average_by_iteration = iterations.groupby(level='iteration').mean()
        iterations_above_threshold = average_by_iteration[average_by_iteration['beliefs'] > 0.5]
        return iterations_above_threshold.index.tolist()[0]