from graphs.graph_converter import Graphs
from beliefs.belief_processor import Beliefs
from attributes.add_attributes import AddAttributes
from simulations.simulation_processor import SimulationProcessor    

class PolygraphAnalysis(SimulationProcessor, AddAttributes):
    def __init__(self, root_folder_path, graph_converter=None, belief_processor=None):
        if graph_converter is None:
            graph_converter = Graphs()
        if belief_processor is None:
            belief_processor = Beliefs()
        super().__init__(graph_converter, belief_processor)
        self.process_simulations(root_folder_path)
        
    def add(self, *methods):
        def column(func):
            def wrapper(*args, **kwargs):
                func(self, *args, **kwargs)
            return wrapper

        for method in methods:
            column(method)

    def get(self):
        return self.dataframe

