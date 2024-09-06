from src.utils import config_controller
import argparse



def command_usage():
    print("\n+----------------------------------------------------------------------------+")
    print(f"| {'Yet Another Lame OSINT Tool'.center(76)}|")
    print("+----------------------------------------------------------------------------+")
    parser = argparse.ArgumentParser(description='YALOT Proof-of-Concept Use at own risk! \n \
                                     Usage of a config file is recommended. All CMD arguments overwrite the config!'
                                     , epilog='Use at own risk! Information is power. Be kind!')
    parser.add_argument('-p','--projectname', help='Specify the project name')
    parser.add_argument('-s','--scope', help='Specify the scope by pointing it to a scope file where each item is in a newline. Currently supported are IP, subnet and domain names')
    parser.add_argument('-r','--run', action='store_true', help='Run setup modules. Currently this is static.')
    parser.add_argument('-a','--aggression', type=int, choices=range(1, 6), default=3, help='Specify the aggression level (1-5) where 1 is default and is passive. Currently there is 1 or more')
    parser.add_argument('-ca','--cache_directory', default='cache_db', help='Specify if you want to cache API calls to files to e.g. reduce quota or noise. Default is true.')
    parser.add_argument('-co','--config', default='config/config.json', help='Path to the configuration file')
    parser.add_argument('-d','--dump', help='Specify a output csv file where the result is dumped to')
    parser.add_argument('--file-log-level', default='DEBUG', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Set log level for file handler')
    parser.add_argument('--console-log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Set log level for console handler')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 0.1', help='Show program version and exit')
    args = parser.parse_args()


    config_controller.setup_config(args)

    import src.utils.run_manager
    src.utils.run_manager.run(src.utils.run_manager.setup())


if __name__ == "__main__":
    command_usage()





    

