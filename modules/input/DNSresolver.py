import dns.resolver 
from  utils.app_logger import app_logger
from  utils.app_logger import func_call_logger
import datetime
import time
from utils.instance_manager import InstanceManager
from modules.input.basedatasources import BaseDataSources
from utils import data_utils
import logging
from utils.metadata_analysis import db_metadata_analysis_module

#Using this class is not opsec safe and will result in DNS queries being actively resolved at target infrastructure.
class Dnsresolver(BaseDataSources):
    """description of class"""
    def __init__(self, general_handlers, name='dns_resolver', api_key="", dns_timeout=0, dns_resolver_ips=["8.8.8.8"]):
        self.column_mapping = {
            'id': ('INTEGER PRIMARY KEY', 'id', 'meta'),
            'time_created': ('TEXT', '', 'meta'),
            'time_modified': ('TEXT', '', 'meta'),
            'scope_status': ('TEXT', '', 'meta'),
            'domain': ('TEXT','primary', ''),
            'dns_type': ('TEXT', 'primary', ''),
            'dns_value': ('TEXT', 'primary', ''),
            'resolver': ('TEXT', '', '')
        }
        super().__init__(general_handlers, name, self.column_mapping)
        self.api = api_key
        self.dns_resolver_ip = "8.8.8.8" #multiple resolvers are currently not supported
        self.dns_timeout = dns_timeout

    @db_metadata_analysis_module()
    @func_call_logger(log_level=logging.INFO)
    def run(self):
        self.search_based_on_scope()
        self.search_based_on_collection()
        self.scope_receive("redo in out")
        self.update_collection()
        pass

    def search_based_on_scope(self):
        for scope_item in self.scope.get_scope("Domain"):
            rows = self.db.execute_sql('''
            SELECT domain, time_modified FROM dns_resolver WHERE domain =  ? 
            ''', (scope_item['scope_value'],))
            if not rows:
                self.resolve_handler(scope_item['scope_value'])
        self.update_collection()
        



    '''
    Receiver function from parent class, which is called if new data from an output table can be used to query new data. Make sure that the column corresponds to the correct primary value
    '''
    def receiver_search_by_primary_values(self,rows,originating_output_table_name):
        result_set = set(row[0] for row in rows)
        for result in result_set:
                if result:
                    self.resolve_handler(result)
        self.update_collection()


    '''Scope'''
    def scope_receive(self, message):
        rows = self.db.execute_sql('''SELECT id, domain, dns_type, dns_value FROM dns_resolver''')
        scope_data = self.scope.get_scope()
        for row in rows:
            row_id = row[0]
            is_scope_active = False 
            for scope_item in scope_data:
                if scope_item['scope_type'] == 'Domain' and (str(row[1]).endswith(scope_item['scope_value']) or scope_item['scope_value'] in str(row[3])):
                    is_scope_active = True
                    break
                elif scope_item['scope_type'] == 'Subnet' and data_utils.is_ip_in_subnet(row[3], scope_item['scope_value']):
                    is_scope_active = True
                    break
                elif scope_item['scope_type'] == 'IP' and scope_item['scope_value'] in row[3]:
                    is_scope_active = True
                    break
                else:
                    pass

            if is_scope_active:
                self.activate_scope(row_id)
            else:
                self.deactivate_scope(row_id)

    def activate_scope(self, row_id):
        return self.db.execute_sql('''UPDATE dns_resolver SET scope_status = 'in' WHERE id = ?''', (row_id,))

    def deactivate_scope(self, row_id):
        return self.db.execute_sql('''UPDATE dns_resolver SET scope_status = 'out' WHERE id = ?''', (row_id,))


    '''Search'''
    def resolve_handler(self, domain):
        if self.config["AGGRESSIVE_SCANS"]:
            for ans in self.resolve_all_common_types(domain):
                app_logger.debug("DNS " + domain + " " + ', '.join(map(str,ans)))
                self.insert_input_data((domain,ans))
        else:
            status, response = self.resolve(domain)
            for ans in response:
                app_logger.debug("DNS " +domain + " "+ ', '.join(map(str,ans)))
                self.insert_input_data((domain,ans))
        

    def resolve_all_common_types(self,domain_name):
        record_types = ['A', 'AAAA', 'CNAME', 'MX', 'NS', 'PTR', 'SOA', 'SRV', 'TXT']
        dns_type_mapping = {
        'A': 1, 'AAAA': 28, 'CNAME': 5, 'MX': 15, 'NS': 2, 'PTR': 12, 'SOA': 6, 'SRV': 33, 'TXT': 16
        }
        dns_answers = []
        domain_exists = True
        for record_type in record_types:
            if domain_exists:
                status, response = self.resolve(domain_name, record_type)
                if 'error' in response and  'does not exist' in response['error']:
                    domain_exists = False
            dns_answers.append((record_type, dns_type_mapping[record_type], status, response))
        return dns_answers
       

    def resolve(self, domain_name, record="A"):
        if self.config["CACHING"]["USE_CACHING"]:
            identifier = self.cache_db._generate_identifier(domain_name+'_'+record, self.name)
            cached_response = self.cache_db.get(identifier)
            app_logger.log(logging.DEBUG,f"DNS request from cache: " + str(cached_response ))
            if cached_response:
                if self.config.get("CACHE_UNSUCCESSFUL_CONTENT_PATTERN", "error") in cached_response :
                     return False, cached_response['value'] 
                return True, cached_response['value'] 
        try:
            response = dns.resolver.query(domain_name, record)
            app_logger.log(logging.DEBUG,f"DNS request live: " + str(response))
            time.sleep(self.dns_timeout)
            response_text = self.resolve_build_return(record, response)
            if self.config["CACHING"]["USE_CACHING"]:
                self.cache_db.set(identifier, response_text)
            return True, response_text
        except dns.resolver.NoAnswer:
            if self.config["CACHING"]["USE_CACHING"]:
                self.cache_db.set(identifier, {self.config.get("CACHE_UNSUCCESSFUL_CONTENT_PATTERN", "error"): 'No answer found'})
            return False, ["No answer found"]
        except dns.resolver.NXDOMAIN as e:
            if self.config["CACHING"]["USE_CACHING"]:
                self.cache_db.set(identifier, {self.config.get("CACHE_UNSUCCESSFUL_CONTENT_PATTERN", "error"): f"{str(e)}"})
            return False,[f"{str(e)}"]
        except dns.exception.Timeout:
            return False,["DNS query timed out"]
        except dns.exception.DNSException as e:
            return False, [f"DNS query failed: {e}"]
        except Exception as e:
            return False,[f"An error occurred: {str(e)}"]
        

    def resolve_build_return(self, record, response):
        if record == 'CNAME':
            ret = [val.target.to_text() for val in response]
        else:
            ret = [val.to_text() for val in response]
        return ret





    def prepare_input_insertion_data(self,json_obj):
        time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        base_insertion_data = {
            'time_created': time_now,
            'time_modified': time_now,
            'scope_status': 'in',
        }
        result = []
        for ip in json_obj[1][3]:
            insertion_data = base_insertion_data.copy()
            insertion_data.update({'domain': json_obj[0]})
            insertion_data.update({'dns_type': json_obj[1][1]})
            insertion_data.update({'dns_value': ip})
            insertion_data.update({'resolver': self.dns_resolver_ip})
            result.append(insertion_data.copy())
        return result


