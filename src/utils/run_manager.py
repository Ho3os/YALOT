from src.utils.config_controller import ConfigManager
from src.utils.app_logger import app_logger
from src.utils import database
from src.utils import tinydb_cache
from src.utils import scope
from src.utils import data_utils
from src.modules import module_controller
from src.modules.strategy import coordinator as scoo
from src.modules.strategy import task_manager as tm


def setup():
    #Setup database
    config = ConfigManager().get_config()
    db = database.OSINTDatabase(config["PROJECT_NAME"] +'.db')
    if config["CACHING"]["USE_CACHING"] == True:
        cache_db = tinydb_cache.TinyDBCache(cache_folder=config["CACHING"]["CACHE_DIRECTORY_PATH"])
    else:
        cache_db = None
    sc = scope.Scope(db)
    sc.insert_from_file()
    general_handlers = {
        'osint_database': db,
        'scope': sc,
        'cache_db': cache_db,
        'data_timeout_threshold': 365 #TODO not in use
    }
    #Setup Logic
    strategy_coordinator = scoo.StrategyCoordinator(general_handlers)
    general_handlers.update({'strategy_coordinator':strategy_coordinator})
    task_manager = tm.TaskManager(general_handlers)
    general_handlers.update({'task_manager':task_manager})
    return general_handlers

def run(general_handlers):
    config = ConfigManager().get_config()
    #try:
    if config["RUN"] == True:
        module_controller.register_input_modules(general_handlers)
        module_controller.register_output_modules(general_handlers) # always after input
        general_handlers["strategy_coordinator"].set_strategies(general_handlers)
       
        general_handlers["task_manager"].execute_workflow_search_based_scope('default')
        general_handlers["task_manager"].execute_scope_update_all
        general_handlers["task_manager"].execute_workflow_merge_all('default')
        general_handlers["task_manager"].execute_scope_update_all

        general_handlers["task_manager"].execute_workflow_search_based_output('')
        general_handlers["task_manager"].execute_scope_update_all
        #general_handlers["task_manager"].execute_manual('default')
        

    if config["EXPORT"]["DUMP_ALL_MODULES"]:
        data_utils.dump_all_module_tables_to_csv(general_handlers["osint_database"])
    if config["EXPORT"]["DUMP_ALL_TABLES"]:
        data_utils. dump_all_tables_to_csv(general_handlers["osint_database"])
    #except KeyError as e:
    #        app_logger.error(f"Config file configuration contains an error. Key error: {e}")
    #except Exception as e:
    #        app_logger.error(f"An error occurred: {e}")

    app_logger.info(f"YALOT did its magic.")

        