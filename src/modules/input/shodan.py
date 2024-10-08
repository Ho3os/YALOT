from src.modules.input.base_input_sources import BaseInputSources
import shodan
from urllib.parse import urlparse
import requests
import datetime
from  src.utils.app_logger import app_logger
from  src.utils.app_logger import func_call_logger
import json
import os
import time
import ast
from src.modules.instance_manager import InstanceManager
from src.utils import data_utils
import logging
from src.utils.metadata_analysis import db_metadata_analysis_module

SUBNET_SEARCH = True

class Shodan(BaseInputSources):
    def __init__(self, general_handlers, name="shodan", api_key=""):
        self.column_mapping = {
            'id': ('INTEGER PRIMARY KEY', 'id', 'meta'),
            'time_created': ('TEXT', '', 'meta'),
            'time_modified': ('TEXT', '', 'meta'),
            'scope_status': ('TEXT', '', 'meta'),
            'domain': ('TEXT','primary', ''),
            'dns_type': ('TEXT','primary', ''),
            'dns_value': ('TEXT','primary', ''),
            'ip': ('TEXT','primary', ''),
            'port': ('TEXT','primary', ''),
            'hash': ('TEXT', '', ''),
            'opts': ('TEXT','', ''),
            '_timestamp': ('TEXT','', ''),
            'isp': ('TEXT','secondary', ''),
            'data': ('TEXT', '', ''),
            'shodan_id': ('TEXT', '', ''),
            'shodan_region': ('TEXT','', ''),
            'shodan_options': ('TEXT','', ''),
            'shodan_module': ('TEXT','', ''),
            'shodan_crawler': ('TEXT','', ''),
            'tags': ('TEXT','secondary', ''),
            'vulns': ('TEXT','secondary', ''),
            'hostnames': ('TEXT','secondary', ''),
            'location_country': ('TEXT','secondary', ''),
            'location_region': ('TEXT','', ''),
            'location_city': ('TEXT','secondary', ''),
            'location_longitude': ('TEXT','', ''),
            'location_latitude': ('TEXT','', ''),
            'org': ('TEXT','secondary', ''),
            'os': ('TEXT','secondary', ''),
            'asn': ('TEXT','secondary', ''),
            'transport': ('TEXT','secondary', ''),
            'ssl_ja3s': ('TEXT','secondary', ''),
            'ssl_jarm': ('TEXT','secondary', ''),
            'http_status': ('TEXT','', ''),
            'http_redirects': ('TEXT','secondary', ''),
            'http_title': ('TEXT','secondary', ''),
            'http_host': ('TEXT','secondary', ''),
            'http_server': ('TEXT','secondary', ''),
            'http_components': ('TEXT','secondary', ''),
            'http_waf': ('TEXT','secondary', ''),
            'dns_resolver_hostname': ('TEXT','', ''),
            'dns_recursive': ('TEXT','', ''),
            'dns_resolver_id': ('TEXT','', ''),
            'dns_software': ('TEXT','', ''),
            '_serial': ('TEXT','', ''),
            'cert_subject': ('TEXT','secondary', ''),
            'cert_issuer': ('TEXT','secondary', ''),            
            'cert_sig_alg': ('TEXT','', ''),
            'cert_issued': ('TEXT','secondary', ''),
            'cert_expires': ('TEXT','secondary', ''),
            'cert_expired': ('TEXT','', ''),
            'fingerprint_sha256': ('TEXT','', ''),
            'fingerprint_sha1': ('TEXT','', ''),
            'pubkey_type': ('TEXT','', ''),
            'pubkey_bits': ('TEXT','', '')
        }
        super().__init__(general_handlers, name, self.column_mapping)
        self.api_prot = "https://"
        self.BASE_URL = "api.shodan.io/shodan/"
        self.HOST_ENDPOINT = "host/"
        self.api_key = api_key
        self.api = shodan.Shodan(api_key)
 

    @db_metadata_analysis_module()
    @func_call_logger(log_level=logging.INFO)
    def run(self):
        self.search_based_on_scope()
        self.search_based_on_collection()
        self.scope_receive("redo in out")
        self.update_collection()
        pass


    '''
    Receiver function from parent class, which is called if new data from an output table can be used to query new data. Make sure that the column corresponds to the correct primary value
    '''
    def receiver_search_by_primary_values(self,rows):
        result_set = set()
        for row in rows:
            if row[1] == '1':
                result_set.add(row[2])
        for result in result_set:
                if result:
                    self._search_host_by_ip_api(result) 

    def search_based_on_scope(self,timethreshold_refresh_in_days=365):
        for scope_item in self.scope.get_scope():
            if scope_item['scope_type'] == 'IP':
                row = self.db.execute_sql_fetchone(''' SELECT ip, time_modified FROM shodan WHERE ip =  ? ''', (scope_item['scope_value'],))
                if not row:
                    self._search_host_by_ip_api(scope_item['scope_value'])
            if SUBNET_SEARCH:
                if scope_item['scope_type'] == 'Subnet':
                    for ip in data_utils.get_subnet_ips(scope_item['scope_value']):
                        row = self.db.execute_sql_fetchone(''' SELECT ip, time_modified FROM shodan WHERE ip =  ? ''', (str(ip),))
                        if not row:
                            self._search_host_by_ip_api(str(ip))

    def update_collection(self):
        time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        meta_data = {
            "time_created" : time_now,
            "time_modified" : time_now,
            'scope_status': 'TBD',
            "dns_type" : "1"
            }
        self.sync_input_into_output(meta_data)
        self._postprocessing_and_clenaup(self.target_table)

    '''Scope'''
    @func_call_logger(log_level=logging.INFO)
    def scope_receive(self, message):
        column_fields = ['domain','ip','dns_type', 'dns_value']
        columns = ', '.join(f'{col}' for col in column_fields)
        rows = self.db.execute_sql(f'''SELECT id, {columns} FROM {self.tablename}''')
        scope_data = self.scope.get_scope()
        to_activate = set()
        to_deactivate = set(range(len(rows)))

        domains = self.summerize_ids(rows, 1)
        ips = self.summerize_dns_ip_ids(rows, 3,4)

        for scope_item in scope_data:
            if scope_item['scope_type'] == 'Domain':
                 for row, row_ids in domains.items():
                    if row.endswith(scope_item['scope_value']):
                        to_activate.update(row_ids)
        for scope_item in scope_data:
            if scope_item['scope_type'] == 'Subnet':
                 for row, row_ids in ips.items():
                    if data_utils.is_ip_in_subnet(row, scope_item['scope_value']):
                        to_activate.update(row_ids)
            if scope_item['scope_type'] == 'IP':
                for row, row_ids in ips.items():
                    if scope_item['scope_value'] == row:
                        to_activate.update(row_ids)
            
        to_deactivate.difference_update(to_activate)

        if to_activate:
            self.batch_update_scope_status(to_activate, 'in')
        if to_deactivate:
            self.batch_update_scope_status(to_deactivate, 'out')
    

    def summerize_ids(self, rows, row_column_index):
        summerize_to_ids = {}
        for row in rows:
            row_id = row[0]
            domain = row[row_column_index]
            if domain not in summerize_to_ids:
                summerize_to_ids[domain] = []
            summerize_to_ids[domain].append(row_id)
        return summerize_to_ids
    
    def summerize_dns_ip_ids(self, rows, row_dns_type_index, row_dns_value_index):
        summerize_to_ids = {}
        for row in rows:
            row_id = row[0]
            dns_type = row[row_dns_type_index]
            dns_value = row[row_dns_value_index]
            if dns_type == '1' or dns_type == '28':
                if dns_value not in summerize_to_ids:
                    summerize_to_ids[dns_value] = []
                summerize_to_ids[dns_value].append(row_id)
        return summerize_to_ids




    def _search_host_by_ip_api(self, ip):
        #time.sleep(1)
        url = self.api_prot +  self.BASE_URL  + self.HOST_ENDPOINT + ip + '?key=' + self.api_key
        search_result =  self._safe_request(url)
        if not search_result or search_result == 'error':
            return
        if search_result:
            if 'data' in search_result: 
                self.insert_input_data(search_result.get('data',''))

    
    def _safe_request(self, url):
        if self.config["CACHING"]["USE_CACHING"]:
            parsed_url = urlparse(url)
            query = os.path.basename(parsed_url.path.replace('/', '_').replace(':', '-'))
            identifier = self.cache_db._generate_identifier(query, self.name)
            cached_response = self.cache_db.get(identifier)
            if cached_response:
                if self.enforce_cached_error_policy(cached_response['value']):
                    return cached_response['value']

        try:
            response = requests.get(url)
            response.raise_for_status()
            response_text = json.loads(response.text)

            if self.config["CACHING"]["USE_CACHING"]:
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



    def _get_json_api_values(self, json_obj):
        if data_utils.check_ip_version(json_obj.get('ip_str', '')) == "IPv4":
            dns_ip_version = '1'
        elif data_utils.check_ip_version(json_obj.get('ip_str', '')) == "IPv6":
            dns_ip_version = '28'
        return {
            'hash': json_obj.get('hash', 0),
            'opts': json.dumps(json_obj.get('opts', {})),
            '_timestamp': json_obj.get('timestamp', ''),
            'isp': json_obj.get('isp', ''),
            'data': json_obj.get('data', ''),
            'shodan_id': json_obj['_shodan'].get('id', ''),
            'shodan_region': json_obj['_shodan'].get('region', ''),
            'shodan_options': json.dumps(json_obj['_shodan'].get('options', {})),
            'shodan_module': json_obj['_shodan'].get('module', ''),
            'shodan_crawler': json_obj['_shodan'].get('crawler', ''),
            'location_country': json_obj.get('location', {}).get('country_name', ''),
            'location_region': json_obj.get('location', {}).get('region_name', ''),
            'location_city': json_obj.get('location', {}).get('city', ''),
            'location_longitude': json_obj.get('location', {}).get('longitude', 0.0),
            'location_latitude': json_obj.get('location', {}).get('latitude', 0.0),
            'port': json_obj.get('port', ''),
            'hostnames': json.dumps(json_obj.get('hostnames', [])),
            'ip': json_obj.get('ip_str', ''),
            'dns_type': dns_ip_version,
            'dns_value': json_obj.get('ip_str', ''),
            'org': json_obj.get('org', ''),
            'os': json_obj.get('os', ''),
            'asn': json_obj.get('asn', ''),
            'tags': json.dumps(json_obj.get('tags','')),
            'vulns': json.dumps(json_obj.get('vulns','')),
            'transport': json_obj.get('transport', ''),
            'http_status': json_obj.get('http', {}).get('status', ''),
            'http_redirects': json.dumps(json_obj.get('http', {}).get('redirects', [])),
            'http_title': json_obj.get('http', {}).get('title', ''),
            'http_host': json_obj.get('http', {}).get('host', ''),
            'http_server': json_obj.get('http', {}).get('server', ''),
            'http_components': json.dumps(json_obj.get('http', {}).get('components', [])),
            'http_waf': json_obj.get('http', {}).get('waf', ''),
            'dns_resolver_hostname': json_obj.get('dns', {}).get('resolver_hostname', ''),
            'dns_recursive': json_obj.get('dns', {}).get('recursive', ''),
            'dns_resolver_id': json_obj.get('dns', {}).get('resolver_id', ''),
            'dns_software': json_obj.get('dns', {}).get('software', ''),
            'ssl_ja3s': json_obj.get('ssl', {}).get('ja3s', ''),
            'ssl_jarm': json_obj.get('ssl', {}).get('jarm', ''),
            'cert_sig_alg': json_obj.get('ssl', {}).get('cert', {}).get('sig_alg', ''),
            'cert_issued': json_obj.get('ssl', {}).get('cert', {}).get('issued', ''),
            'cert_expires': json_obj.get('ssl', {}).get('cert', {}).get('expires', ''),
            'cert_expired': 1 if not json_obj.get('ssl', {}).get('cert', {}).get('expired', False) else 0,
            'fingerprint_sha256': json_obj.get('ssl', {}).get('cert', {}).get('fingerprint', {}).get('sha256', ''),
            'fingerprint_sha1': json_obj.get('ssl', {}).get('cert', {}).get('fingerprint', {}).get('sha1', ''),
            '_serial': str(json_obj.get('ssl', {}).get('cert', {}).get('serial', '')),
            'cert_subject': json.dumps(json_obj.get('ssl', {}).get('cert', {}).get('subject', {})),
            'pubkey_type': json_obj.get('ssl', {}).get('cert', {}).get('pubkey', {}).get('type', ''),
            'pubkey_bits': json_obj.get('ssl', {}).get('cert', {}).get('pubkey', {}).get('bits', ''),
            'cert_issuer': json.dumps(json_obj.get('ssl', {}).get('cert', {}).get('issuer', {})),
        }


    def prepare_input_insertion_data(self,json_objs):
         
        time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        base_insertion_data = {
            'time_created': time_now,
            'time_modified': time_now,
            'scope_status': 'in',
        }
        result = []
        for json_obj in json_objs:
            insertion_data = base_insertion_data.copy()
            insertion_data.update(self._get_json_api_values(json_obj))
            hostnames = json_obj.get('hostnames', [])
            if len(hostnames) == 0:
                insertion_data.update({'domain': ''})
                result.append(insertion_data.copy())
            else:
                for domain in hostnames:
                    insertion_data.update({'domain': str(domain)})
                    result.append(insertion_data.copy())
        return result
       
    def condiction_select_for_subset_updates_input_into_output(self):
        return "AND (t2.dns_value = t1.ip AND (t2.dns_type = '1' OR t2.dns_type = '28') OR t2.dns_value IS NULL)"
    
    def condiction_select_based_on_output(self):
        return "AND t1.ip IS NOT NULL"