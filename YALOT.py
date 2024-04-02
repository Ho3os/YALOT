from utils.configmanager import ConfigManager
from configparser import ExtendedInterpolation
from modules.input import shodan
from modules.input import columbus
from modules.input import crtsh
from modules.input import dnsresolver
from modules.output import collection
from utils import database
from utils import tinydbcache
from utils import scope
from utils.instancemanager import InstanceManager
from utils import datautils
from utils.applogger import app_logger
from utils.applogger import func_call_logger
import sys
import argparse
import json
import time


start_time = time.time()
last_time = start_time




def run_output_modules(general_handlers):
    if datautils.is_output_module_config_present("collection"):
        collection.Collection(general_handlers)

def run_input_modules(general_handlers):
    config = datautils.get_config()
    log_elapsed_time('Init & scope')
    if datautils.is_input_module_config_present("columbus"):
        #columbus.Columbus(general_handlers).run()
        log_elapsed_time('columbus')  
    if datautils.is_input_module_config_present("crtsh"):
        #crtsh.Crtsh(general_handlers).run()
        log_elapsed_time('certsh')
    if datautils.is_input_module_config_present("dnsresolver"):
        #dnsresolver.Dnsresolver(general_handlers).run()
        log_elapsed_time('dns_resolver')
    if datautils.is_input_module_config_present("shodan"):
        shodan.Shodan(general_handlers,api_key=config["modules"]["input"]["shodan"]["API_KEY"]).run()
        log_elapsed_time('shodan')
    #censys.CensysHostsSearch(db,scope,api_id=censys_api_id,api_secret=censys_api_secret).run()
    #log_elapsed_time('censys')

def log_elapsed_time(msg=''):
    global last_time
    total_elapsed_time = time.time() - start_time
    last_elapsed_time = time.time() - last_time
    last_time = time.time()
    app_logger.info(f"========================================================================================================")
    app_logger.info(f"Execution time in total: {total_elapsed_time} seconds ---  Module {msg}: {last_elapsed_time}")
    app_logger.info(f"========================================================================================================")



def command_usage():
    print("\n+----------------------------------------------------------------------------+")
    print(f"| {'Yet Another Lame OSINT Tool'.center(76)}|")
    print("+----------------------------------------------------------------------------+")

    parser = argparse.ArgumentParser(description='YALOT Proof-of-Concept Use at own risk!', epilog='Use at own risk! Information is power. Be kind!')
    parser.add_argument('-p','--projectname', help='Specify the project name')
    parser.add_argument('-s','--scope', help='Specify the scope by pointing it to a scope file where each item is in a newline. Currently supported are IP, subnet and domain names')
    parser.add_argument('-r','--run', action='store_true', help='Run setup modules. Currently this is static.')
    parser.add_argument('-a','--aggression', type=int, choices=range(1, 6), default=3, help='Specify the aggression level (1-5) where 1 is default and is passive. Currently there is 1 or more')
    parser.add_argument('-ca','--cache', action='store_true', default=True, help='Specify if you want to cache API calls to files to e.g. reduce quota or noise. Default is true.')
    parser.add_argument('-co','--config', default='config/config.json', help='Path to the configuration file')
    parser.add_argument('-d','--dump', help='Specify a output csv file where the result is dumped to')
    parser.add_argument('--file-log-level', default='DEBUG', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Set log level for file handler')
    parser.add_argument('--console-log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Set log level for console handler')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 0.1', help='Show program version and exit')
    args = parser.parse_args()

    if not args.config:
        app_logger.error("Config is required for now.")
        SystemExit(1)
    ConfigManager().load_config_from_file(args.config)
    config = ConfigManager().get_config()

    if args.projectname:
        projectname = args.projectname
    else:
        if config and "projectname" in config:
            projectname = config["projectname"]
        else:
            app_logger.error("No projectname defined in config nor via cli.")
            sys.exit(1)
    
    db = database.OSINTDatabase('YALOT_'+projectname+'.db')
    cache_db = tinydbcache.TinyDBCache(cache_folder='cache_db')
    sc = scope.Scope(db)
    general_handlers = {
        'osint_database': db,
        'scope': sc,
        'cache_db': cache_db,
        'data_timeout_threshold': 365
    }

    run_output_modules(general_handlers)
    if args.scope:
        sc.insert_from_file(args.scope)
    if args.aggression:
        datautils.set_aggression(args.aggression)
    else:
        datautils.set_aggression(1)
    if args.cache:
        datautils.set_cache(True)
    else:
         datautils.set_cache(False)
    if args.run:
        run_input_modules(general_handlers)
    if args.dump:
        datautils.dump_all_tables_to_csv(db)
    db.conn.close()
    app_logger.info(f"YALOT")





if __name__ == "__main__":
    command_usage()





    

