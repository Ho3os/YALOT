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

class WhoisXml_SubDomains(BaseInputSources):
    """description of class"""
    def __init__(self, general_handlers, name='whoisxml_subdomains', api_key=''):
        self.column_mapping = {
            'id': ('INTEGER PRIMARY KEY', 'id', 'meta'),
            'time_created': ('TEXT', '', 'meta'),
            'time_modified': ('TEXT', '', 'meta'),
            'scope_status': ('TEXT', '', 'meta'),
            'domain': ('TEXT','primary', '')
        }
        super().__init__(general_handlers, name, self.column_mapping)
        self.BASE_URL = "https://domains-subdomains-discovery.whoisxmlapi.com/api/v1"
        self.api_key = api_key
    

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
                    self.search_subdomains(result)

    '''Source API'''
    @func_call_logger(log_level=logging.DEBUG)
    def _safe_request(self, url, headers, data):
        if self.config["CACHING"]["USE_CACHING"]:
            query = self.create_cache_query(data)
            identifier = self.cache_db._generate_identifier(query, self.name)
            cached_response = self.cache_db.get(identifier)
            if cached_response:
                return cached_response['value']

        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
            response_text = json.loads(response.text)

            if self.config["CACHING"]["USE_CACHING"]:
                self.cache_db.set(identifier,response_text)
            return response_text
        except requests.exceptions.RequestException as e:
            app_logger.error(f"Request error: {e}")
            return None
        except Exception as e:
            app_logger.error(f"Error: {e}")
            return None
        
    def create_cache_query(self,data):
        filtered_data = {
            "domains": data.get("domains", {}),
            "subdomains": data.get("subdomains", {})
        }
        return json.dumps(filtered_data) 
        
    '''
    {
        "domainsCount": 1101,
        "domainsList": [
            "amazon.properties",
            "amazon.rentals",
            "amazon.tattoo",
            "amazon.top",
            "amazon.bayern",
            ...
        ]
    }

      '''
    def _domains_request(self,domain):
        
        url = f"{self.BASE_URL}"
        headers = {
            "Content-Type": "application/json",
        }
        data = {
            "apiKey": self.api_key,
            "domains": {
                "include": [
                    f"*{domain}*"
                ]
            },
            "subdomains": {
                "include": [
                    f"*{domain}*"
                ]
            }
        }
        return self._safe_request(url, headers, data)


    '''Search'''
    @func_call_logger(log_level=logging.DEBUG)
    def search_subdomains(self,domain):
        search_results = self._domains_request(domain)
        if search_results:
            app_logger.log(logging.DEBUG,json.dumps(search_results))
            self.insert_input_data(search_results)
        

    @func_call_logger(log_level=logging.INFO)
    def search_based_on_scope(self,timethreshold_refresh_in_days=365):
        to_search = []
        for scope_item in self.scope.get_scope("Domain"):
            row = self.db.execute_sql_fetchone('''SELECT domain FROM whoisxml_subdomains WHERE domain = ? 
            ''', (scope_item['scope_value'],))
            if not row:
                to_search.append(scope_item['scope_value'])

        to_search_summarized = set()
        for search in to_search:
            extracted_domain, extracted_tld = data_utils.extract_domain_and_tld(search)
            to_search_summarized.add(extracted_domain)
        for search in to_search_summarized:
            self.search_subdomains(search)

   
    @func_call_logger(log_level=logging.DEBUG)
    def prepare_input_insertion_data(self,json_obj):
        time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        base_insertion_data = {
            'time_created': time_now,
            'time_modified': time_now,
            'scope_status': 'TBD',
        }
        result = []
        if 'domainsList' in json_obj:
            for domain in json_obj['domainsList']:
                insertion_data = base_insertion_data.copy()
                insertion_data.update({'domain': domain})
                result.append(insertion_data.copy())
        return result