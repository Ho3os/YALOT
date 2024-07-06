from utils.config_controller import ConfigManager
from utils.app_logger import app_logger
from modules import module_controller
from utils import database
from utils import tinydb_cache
from utils import scope
from utils import data_utils


def setup():
    config = ConfigManager().get_config()
    db = database.OSINTDatabase(config["PROJECT_NAME"] +'.db')
    if config["CACHING"]["USE_CACHING"] == 'True':
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
    return general_handlers

def run(general_handlers):
    config = ConfigManager().get_config()
    try:
        if config["RUN"] == True:
            module_controller.run_output_modules(general_handlers)
            module_controller.run_input_modules(general_handlers)
        if config["EXPORT"]["DUMP_ALL_MODULES"]:
            data_utils.dump_all_module_tables_to_csv(general_handlers["osint_database"])
        if config["EXPORT"]["DUMP_ALL_TABLES"]:
            data_utils. dump_all_tables_to_csv(general_handlers["osint_database"])
    except KeyError:
            app_logger.error(f"Config file configuration contains an error. Key error: {e}")
    except Exception as e:
            app_logger.error(f"An error occurred: {e}")

    app_logger.info(f"YALOT did its magic.")

        