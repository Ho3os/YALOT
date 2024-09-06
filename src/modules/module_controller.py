from src.utils.config_controller import ConfigManager
from src.utils.app_logger import app_logger
from src.modules.input import shodan
from src.modules.input import columbus
from src.modules.input import crtsh
from src.modules.input import dns_resolver
from src.modules.input import importData, whoisxml_subdomains
from src.modules.output import collection
from src.utils import data_utils
from src.utils.metadata_analysis import db_metadata_analysis
from src.utils.time_logger import time_logger



config = ConfigManager().get_config()

def register_output_modules(general_handlers):
    if data_utils.is_output_module_config_present("collection"):
        collection.Collection(general_handlers)
        #collection.Collection(general_handlers).scope_receive("redo scope manually")


@db_metadata_analysis()
@time_logger('Init & scope')
def register_input_modules(general_handlers):
    if data_utils.is_input_module_config_present("import"):
        register_import(general_handlers)
    if data_utils.is_input_module_config_present("columbus"):
        register_columbus(general_handlers)
    if data_utils.is_input_module_config_present("columbus"):
        register_whoisxml_SubDomains(general_handlers)
    if data_utils.is_input_module_config_present("crtsh"):
        register_crtsh(general_handlers)
    if data_utils.is_input_module_config_present("dns_resolver"):
        register_dns_resolver(general_handlers)  
    if data_utils.is_input_module_config_present("shodan"):
        register_shodan(general_handlers)

    #censys.CensysHostsSearch(db,scope,api_id=censys_api_id,api_secret=censys_api_secret).register()

@time_logger('Import') 
def register_import(general_handlers):
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
def register_columbus(general_handlers):
    columbus.Columbus(general_handlers)

@time_logger('WhoisXml_SubDomains')  
def register_whoisxml_SubDomains(general_handlers):
    whoisxml_subdomains.WhoisXml_SubDomains(general_handlers,api_key=config["modules"]["input"]["whoisxml_subdomains"]["API_KEY"])
        
@time_logger('certsh')
def register_crtsh(general_handlers):
    crtsh.Crtsh(general_handlers)

@time_logger('dns_resolver')
def register_dns_resolver(general_handlers):  
    dns_resolver.Dnsresolver(general_handlers)

@time_logger('shodan')
def register_shodan(general_handlers):
        config = data_utils.get_config()
        shodan.Shodan(general_handlers,api_key=config["modules"]["input"]["shodan"]["API_KEY"])
        