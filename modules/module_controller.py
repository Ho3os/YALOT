from utils.config_controller import ConfigManager
from utils.app_logger import app_logger
from modules.input import shodan
from modules.input import columbus
from modules.input import crtsh
from modules.input import dnsresolver
from modules.input import importData, whoisxml_subdomains
from modules.output import collection
from utils import data_utils
import time
from utils.metadata_analysis import db_metadata_analysis
from utils.time_logger import time_logger



config = ConfigManager().get_config()

def run_output_modules(general_handlers):
    if data_utils.is_output_module_config_present("collection"):
        collection.Collection(general_handlers)
        #collection.Collection(general_handlers).scope_receive("redo scope manually")


@db_metadata_analysis()
@time_logger('Init & scope')
def run_input_modules(general_handlers):
    if data_utils.is_input_module_config_present("import"):
        run_import(general_handlers)
    if data_utils.is_input_module_config_present("columbus"):
        run_columbus(general_handlers)
    if data_utils.is_input_module_config_present("columbus"):
        run_whoisxml_SubDomains(general_handlers)
    if data_utils.is_input_module_config_present("crtsh"):
        run_crtsh(general_handlers)
    if data_utils.is_input_module_config_present("dnsresolver"):
        run_dns_resolver(general_handlers)  
    if data_utils.is_input_module_config_present("shodan"):
        run_shodan(general_handlers)

    #censys.CensysHostsSearch(db,scope,api_id=censys_api_id,api_secret=censys_api_secret).run()

@time_logger('Import') 
def run_import(general_handlers):
    for data in config["modules"]["input"]["import"]["data_list"]:
        import_handlers = general_handlers.copy() | {
            "name": data["name"],
            "data_model_path": data["data_model_path"],
            "data_path": data["data_path"]
        }
        try:
            importData.ImportData(import_handlers).import_data()
        except FileNotFoundError as e:
            app_logger.error(f"Initialization failed: {e}")
        except Exception as e:
            app_logger.error(f"Unexpected error during initialization: {e}")


@time_logger('columbus')  
def run_columbus(general_handlers):
    columbus.Columbus(general_handlers).run()

@time_logger('WhoisXml_SubDomains')  
def run_whoisxml_SubDomains(general_handlers):
    whoisxml_subdomains.WhoisXml_SubDomains(general_handlers,api_key=config["modules"]["input"]["whoisxml_subdomains"]["API_KEY"]).run()
        
@time_logger('certsh')
def run_crtsh(general_handlers):
    crtsh.Crtsh(general_handlers).run()

@time_logger('dns_resolver')
def run_dns_resolver(general_handlers):  
    dnsresolver.Dnsresolver(general_handlers).run()

@time_logger('shodan')
def run_shodan(general_handlers):
        config = data_utils.get_config()
        shodan.Shodan(general_handlers,api_key=config["modules"]["input"]["shodan"]["API_KEY"]).run()
        