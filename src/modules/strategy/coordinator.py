
from src.modules.instance_manager import InstanceManager
from src.utils.app_logger import app_logger
from src.utils.app_logger import func_call_logger
from src.modules.strategy import provider_overwrite as spo

class StrategyCoordinator():
    def __init__(self,general_handlers):
        self.strategy_provider = self.decide_operation('overwrite')
        self.strategy_instances = None

    def set_strategies(self, general_handlers):
        self.strategy_provider = self.decide_operation('overwrite')
        self.attach_strategies(self.strategy_provider,general_handlers)

    def execute_new_task(self, msg):
        if msg["operation"] == "search":
            self.execute_new_search(msg)
        elif msg["operation"] == "merge":
            self.execute_merge(msg)


    def execute_new_search(self,msg):
        if msg["source_type"] == "based_on_scope":
            InstanceManager.get_input_instance(msg["module_name"]).search_based_on_scope()
        elif msg["source_type"] == "based_on_output":
            InstanceManager.get_input_instance(msg["module_name"]).search_based_on_output(msg["source"])

    def execute_merge(self,msg):
        output_instance = InstanceManager.get_output_instance(msg["module_name"])
        if msg["source_type"] == "all":
            output_instance.link_strategy.get_data_update_from_all_inputs()
        if  msg["source_type"] == "from_input_source":
            output_instance.link_strategy.get_data_update_from_input(msg["source"])

    def decide_operation(self,params='default'):
        if params == 'overwrite' or params == 'default':
            return spo.StrategyProviderOverwrite
        elif params == 'append':
            pass
        elif params == 'trusted':
            pass
        elif params == 'rulebased':
            pass
        else:
            raise ValueError("Invalid parameters for operation decision")
        
    def attach_strategies(self,strategy_provider,general_handlers):
        for instance in InstanceManager.get_full_output_instances().values():
            instance["instance"].set_strategy(strategy_provider(general_handlers, instance))

