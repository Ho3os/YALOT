from urllib.parse import urlparse
import requests
import datetime
import logging
from src.utils.app_logger import app_logger
from src.utils.app_logger import func_call_logger
from src.utils import data_utils
import json
import os
from src.modules.input.base_input_sources import BaseInputSources
from src.utils.metadata_analysis import db_metadata_analysis_module

class Columbus(BaseInputSources):
    """description of class"""
    def __init__(self, general_handlers, name='columbus', api_key=''):
        self.column_mapping = {
            'id': ('INTEGER PRIMARY KEY', 'id', 'meta'),
            'time_created': ('TEXT', '', 'meta'),
            'time_modified': ('TEXT', '', 'meta'),
            'scope_status': ('TEXT', '', 'meta'),
            'domain': ('TEXT','primary', ''),
            'dns_type': ('TEXT', 'primary', ''),
            'dns_value': ('TEXT', 'primary', ''),
            'time_seen': ('TEXT', '', '')
        }
        super().__init__(general_handlers, name, self.column_mapping)
        self.BASE_URL = "https://columbus.elmasy.com/api"
        self.api = api_key
    
    @db_metadata_analysis_module()
    @func_call_logger(log_level=logging.INFO)
    def run(self):
        self.search_based_on_scope()
        self.scope_receive("redo in out")
        self.update_collection()
        pass

    '''
    Receiver function from parent class, which is called if new data from an output table can be used to query new data. Make sure that the column corresponds to the correct primary value
    '''
    @func_call_logger(log_level=logging.DEBUG)
    def receiver_search_by_primary_values(self,rows):
        result_set = set(row[0] for row in rows)
        for result in result_set:
                if result and not self._check_existing_domain(result) and self.scope.check_domain_in_scope(result):
                    self.search_historic_dns(result)

    

    def _safe_request(self, url):
        if self.config["CACHING"]:
            parsed_url = urlparse(url)
            query = os.path.basename(parsed_url.path.replace('/', '_').replace(':', '-'))
            identifier = self.cache_db._generate_identifier(query, self.name)
            cached_response = self.cache_db.get(identifier)
            if cached_response:
                return cached_response['value']

        try:
            response = requests.get(url)
            response.raise_for_status()
            response_text = json.loads(response.text)

            if self.config["CACHING"]:
                self.cache_db.set(identifier,response_text)
            return response_text
        except requests.exceptions.RequestException as e:
            if self.config["CACHING"]["USE_CACHING"]:
                self.cache_db.set(identifier, self.config.get("CACHE_UNSUCCESSFUL_CONTENT_PATTERN", "error"))
            app_logger.error(f"Request error: {e}")
            return None
        except Exception as e:
            app_logger.error(f"Error: {e}")
            return None




    def lookup_subdomain(self,domain):
        url = f"{self.BASE_URL}/lookup/{domain}"
        return self._safe_request(url)

    def lookup_starting_domains(self,domain):
        url = f"{self.BASE_URL}/starts/{domain}"
        return self._safe_request(url)
    '''
    ["fr","com",...]
    '''
    def find_tlds(self,domain):
        url = f"{self.BASE_URL}/tld/{domain}"
        return self._safe_request(url)
    '''
    [
      {
        "Domain": "XXX.hu",
        "Records": [
          {
            "type": 1,
            "value": "81.XXX.XXX.33",
            "time": 1698270713
          },
      '''
    def domain_history(self,domain):
        url = f"{self.BASE_URL}/history/{domain}"
        return self._safe_request(url)

    def server_info(self):
        url = f"{self.BASE_URL}/stat"
        return self._safe_request(url)

    def get_tld_from_fqdn(self,fqdn):
        url = f"{self.BASE_URL}/tools/tld/{fqdn}"
        return self._safe_request(url)

    def get_domain_from_fqdn(self,fqdn):
        url = f"{self.BASE_URL}/tools/domain/{fqdn}"
        return self._safe_request(url)

    def get_subdomain_from_fqdn(self,fqdn):
        url = f"{self.BASE_URL}/tools/subdomain/{fqdn}"
        return self._safe_request(url)

    def is_valid_fqdn(self,fqdn):
        url = f"{self.BASE_URL}/tools/isvalid/{fqdn}"
        return self._safe_request(url)

    '''Search'''
    @func_call_logger(log_level=logging.DEBUG)
    def search_TLD_domains_by_domain_withoutTLD(self,domain_without_TLD):
        search_results = self.find_tlds(domain_without_TLD)
        if search_results:
            list_domains = [f"{domain_without_TLD}.{tld}" for tld in search_results if not tld == ""]
            #self._insert_domain(list_domains)
            self.insert_input_data(list_domains)

    @func_call_logger(log_level=logging.DEBUG)
    def search_subdomains(self, domain):
        search_results = self.lookup_subdomain(domain)
        if search_results:
            list_domains = [f"{subdomain}.{domain}" for subdomain in search_results if not subdomain == ""]
            #self._insert_domain(list_domains)
            self.insert_input_data(list_domains)

    @func_call_logger(log_level=logging.DEBUG)
    def search_historic_dns(self,domain):
        search_results = self.domain_history(domain)
        if search_results:
            app_logger.log(logging.DEBUG,json.dumps(search_results))
            self.insert_input_data(search_results)
        

    @func_call_logger(log_level=logging.INFO)
    def search_based_on_scope(self,timethreshold_refresh_in_days=365):
        for scope_item in self.scope.get_scope("Domain"):
            row = self.db.execute_sql_fetchone('''SELECT domain FROM columbus WHERE domain =  ? 
            ''', (scope_item['scope_value'],))
            if not row:
                self.search_historic_dns(scope_item['scope_value'])

   
    @func_call_logger(log_level=logging.DEBUG)
    def prepare_input_insertion_data(self,json_objs):
        time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        base_insertion_data = {
            'time_created': time_now,
            'time_modified': time_now,
            'scope_status': 'in',
        }
        result = []
        if json_objs:
            for json_obj in json_objs:
                if json_obj['Records']:
                    for record in json_obj['Records']:
                        insertion_data = base_insertion_data.copy()
                        insertion_data.update({'domain': json_obj.get('Domain', '')})
                        insertion_data.update(self._get_json_api_values(record))
                        result.append(insertion_data.copy())
                else:
                    if json_obj['Domain']:
                        insertion_data = base_insertion_data.copy()
                        insertion_data.update({'domain': json_obj.get('Domain', '')})
                        insertion_data.update(self._get_json_api_values({}))
                        result.append(insertion_data.copy())
        return result

    def _get_json_api_values(self, json_obj):
        return {
            'dns_type': json_obj.get('type', ''),
            'dns_value': json_obj.get('value', ''),
            'time_seen': json_obj.get('time', '')            
        }