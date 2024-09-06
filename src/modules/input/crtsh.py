import requests
import datetime
from  src.utils.app_logger import app_logger
from  src.utils.app_logger import func_call_logger
from urllib.parse import urlparse
import os
from src.modules.input.base_input_sources import BaseInputSources
from  src.utils.app_logger import app_logger
from  src.utils.app_logger import func_call_logger
from src.utils import data_utils
import logging
from src.utils.metadata_analysis import db_metadata_analysis_module
import json

class Crtsh(BaseInputSources):
    """description of class"""
    def __init__(self, general_handlers, name = "crtsh", api_key = '', timethreshold_refresh_in_days=365):
        self.column_mapping = {
            'id': ('INTEGER PRIMARY KEY', 'id','meta'),
            'time_created': ('TEXT', '','meta'),
            'time_modified': ('TEXT', '','meta'),
            'scope_status': ('TEXT', '','meta'),
            'domain': ('TEXT', 'primary',''),
            'cert_issuer_ca_id': ('TEXT', '',''),
            'cert_issuer': ('TEXT', 'secondary',''),
            'name_value': ('TEXT', '',''),
            'entry_timestamp': ('TEXT', '',''),
            'cert_issued': ('TEXT', 'secondary',''),
            'cert_expires': ('TEXT', 'secondary',''),
            'serial_number': ('TEXT', '',''),
            'crtsh_id': ('TEXT', '',''),
        }
        super().__init__(general_handlers, name, self.column_mapping)
        self.api_prot = "https://"
        self.BASE_URL = "crt.sh/"
        self.URL_IDENTITY = "?output=json&q="
        self.api = api_key
        

    @db_metadata_analysis_module()
    @func_call_logger(log_level=logging.INFO)
    def run(self):
        self.search_based_on_scope()
        #self.search_based_on_collection() TODO breaks
        self.scope_receive("redo in out")
        self.update_collection()
        pass
    

    '''
    Receiver function from parent class, which is called if new data from an output table can be used to query new data. Make sure that the column corresponds to the correct primary value
    '''
    def receiver_search_by_primary_values(self,rows):
        result_set = set(row[0] for row in rows)
        for result in result_set:
            if result and not self._check_existing_domain(result) and self.scope.check_domain_in_scope(result):
                resp = self.search_domain(result)
                if resp:
                    self.insert_input_data(resp)            

    #time is missing # AND time_modified < ?
    def search_based_on_scope(self,timethreshold_refresh_in_days=365):
        for scope_item in self.scope.get_scope("Domain"):
            row = self.db.execute_sql_fetchone('''
                SELECT domain, time_modified FROM crtsh WHERE domain LIKE  ? 
            ''', ('%' + scope_item['scope_value'],))
            if not row:
                resp = self.search_domain(scope_item['scope_value'])
                if resp:
                    self.insert_input_data(resp)

   
    def search_domain(self,domain_name):
        url = self.api_prot + self.BASE_URL + self.URL_IDENTITY + domain_name
        resp = self.safe_request(url)
        return resp


        

    def safe_request(self, url):
        headers = {
            'User-Agent': 'YourCustomUserAgent/1.0'
        }
        if self.config["CACHING"]["USE_CACHING"]:
            parsed_url = urlparse(url)
            query = os.path.basename(parsed_url.query.split('=')[-1])
            identifier = self.cache_db._generate_identifier(query, self.name)
            cached_response = self.cache_db.get(identifier)
            if cached_response:
                return cached_response['value']

        try:
            response = requests.get(url)
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

    def _get_json_api_values(self, record):
            return {
                'domain': record.get('common_name', ""),
                'cert_issuer_ca_id': record.get('issuer_ca_id', ""),
                'cert_issuer': record.get('issuer_name', ""),
                'name_value': record.get('name_value', ""),
                'entry_timestamp': record.get('entry_timestamp', ""),
                'cert_issued': record.get('not_before', ""),
                'cert_expires': record.get('not_after', ""),
                'serial_number': record.get('serial_number', ""),
                'crtsh_id': record.get('id', "")
        }

    def prepare_input_insertion_data(self,json_objs):
         
        time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        base_insertion_data = {
            'time_created': time_now,
            'time_modified': time_now,
            'scope_status': 'TBD',
        }
        result = []
        for json_obj in json_objs:
            insertion_data = base_insertion_data.copy()
            insertion_data.update(self._get_json_api_values(json_obj))
            name_values = json_obj.get('name_value', '').split('\n')
            if len(name_values) == 0:
                result.append(insertion_data.copy())
            else:
                for domain in name_values:
                    insertion_data.update({'domain': str(domain)})
                    result.append(insertion_data.copy())
        return result
    


    '''
    Overwritten to address MAX(certsh_id) to keep only one entry in select input
    Overwrite if domain does not exists or if secoundary values of primary group have a NULL
    '''
    def _get_update_meta_data_query_for_insert_into_output_data(self,parent_query):
        return parent_query.replace("ORDER BY time_modified DESC", "ORDER BY crtsh_id DESC")
    
    '''
    Overwritten to address MAX(certsh_id) to keep only one entry in select input
    Overwrite if domain does not exists or if secoundary values of primary group have a NULL
    '''
    def _get_find_subset_weak_update_query_for_insert_into_output_data(self,parent_query):
        return parent_query.replace("ORDER BY time_modified DESC", "ORDER BY crtsh_id DESC")
    
    def _get_find_subset_strong_update_query_for_insert_into_output_data(self,parent_query):
        return parent_query.replace("ORDER BY time_modified DESC", "ORDER BY crtsh_id DESC")


    '''
    Overwritten to address MAX(certsh_id) to keep only one entry in select input
    Overwrite if domain does not exists or if secoundary values of primary group have a NULL
    '''
    def _get_insert_query_for_insert_into_output_data(self,parent_query):
        return parent_query.replace("ORDER BY time_modified DESC", "ORDER BY crtsh_id DESC")



