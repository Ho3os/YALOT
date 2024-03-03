
from configparser import ExtendedInterpolation
from modules.input import shodan
from modules.input import columbus
from modules.input import crtsh
from modules.input import DNSresolver
from modules.output import collection
from utils import database
from utils import scope
from utils.instancemanager import InstanceManager
from utils import datautils
import sys
import argparse
import json
import time
from  utils.applogger import app_logger
from  utils.applogger import func_call_logger



page = 1
num_results = 10
start_time = time.time()
last_time = start_time

def run_output_modules(db,sc):
    collection.Collection(db,sc)

def run_input_modules(db,sc):
    log_elapsed_time('Init & scope')
    columbus.Columbus(db,sc).run()
    log_elapsed_time('columbus')  
    crtsh.Certsh(db,sc).run()
    log_elapsed_time('certsh')
    DNSresolver.DNSresolver(db,sc).run()
    log_elapsed_time('dns_resolver')
    shodan.Shodan(db,sc,api_key=shodan_API_Key).run()
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


    parser = argparse.ArgumentParser(description='YALOT Proof-of-Concept Use at own risk!', epilog='Use at own risk! YALOT done its magic. Information is power. Be kind!')
    parser.add_argument('-p','--projectname', required=True, help='Specify the project name')
    parser.add_argument('-s','--scope', help='Specify the scope by pointing it to a scope file where each item is in a newline. Currently supported are IP, subnet and domain names')
    parser.add_argument('-r','--run', action='store_true', help='Run setup modules. Currently this is static.')
    parser.add_argument('-a','--aggression', type=int, choices=range(1, 6), default=3, help='Specify the aggression level (1-5) where 1 is default and is passive. Currently there is 1 or more')
    parser.add_argument('-c','--cache', action='store_true', default=True, help='Specify if you want to cache API calls to files to e.g. reduce quota or noise. Default is true.')
    parser.add_argument('-d','--dump', help='Specify a output csv file where the result is dumped to')
    parser.add_argument('--file-log-level', default='DEBUG', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Set log level for file handler')
    parser.add_argument('--console-log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Set log level for console handler')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 0.1', help='Show program version and exit')
    args = parser.parse_args()


    
    db = database.OSINTDatabase('YALOT_'+args.projectname+'.db')
    sc = scope.Scope(db)
    run_output_modules(db,sc)
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
        run_input_modules(db,sc)
    if args.dump:
        datautils.dump_all_tables_to_csv(db)
    db.conn.close()
    app_logger.info(f"YALOT")





if __name__ == "__main__":
    command_usage()





    

