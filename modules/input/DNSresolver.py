import dns.resolver 
from  utils.applogger import app_logger
from  utils.applogger import func_call_logger
import datetime
import time
from utils.instancemanager import InstanceManager
from modules.input.basedatasources import BaseDataSources
from  utils.applogger import app_logger
from  utils.applogger import func_call_logger
from utils import datautils
import logging

####################################################################

#TODO fix collection and caching is off

####################################################################

#Using this class is not opsec safe and will result in DNS queries being actively resolved at target infrastructure.
class DNSresolver(BaseDataSources):
    """description of class"""
    def __init__(self,db, scope, name='dns_resolver', api_key="", timethreshold_refresh_in_days=365, dns_timeout=0, dns_resolver_ips=["8.8.8.8"]):
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
        super().__init__(db, scope, name, self.column_mapping)
        self.api = api_key
        #self.cache_folder_name = "DNS"
        self.dns_resolver_ip = "8.8.8.8" #multiple resolvers are currently not supported
        self.timethreshold_refresh_in_days = timethreshold_refresh_in_days  # not in use
        self.dns_timeout = dns_timeout


    def run(self):
        self.search_based_on_scope()
        self.search_based_on_collection()
        self.update_collection()

    def search_based_on_scope(self):
        cursor = self.db.conn.cursor()
        for scope_item in self.scope.get_scope("Domain"):
            cursor.execute('''
            SELECT domain, time_modified FROM dns_resolver WHERE domain =  ? 
            ''', (scope_item['scope_value'],))
            row = cursor.fetchall()
            if not row:
                self.resolve_handler(scope_item['scope_value'])
            self.db.conn.commit()
        cursor.close()
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
        cursor = self.db.conn.cursor()
        cursor.execute('''SELECT id, domain, dns_type, dns_value FROM dns_resolver''')
        rows = cursor.fetchall()
        scope_data = self.scope.get_scope()
        for row in rows:
            row_id = row[0]
            is_scope_active = False 
            for scope_item in scope_data:
                if scope_item['scope_type'] == 'Domain' and (str(row[1]).endswith(scope_item['scope_value']) or scope_item['scope_value'] in str(row[3])):
                    is_scope_active = True
                    break
                elif scope_item['scope_type'] == 'Subnet' and datautils.is_ip_in_subnet(row[3], scope_item['scope_value']):
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
        cursor = self.db.conn.cursor()
        cursor.execute('''UPDATE dns_resolver SET scope_status = 'in' WHERE id = ?''', (row_id,))
        self.db.conn.commit()
        cursor.close()

    def deactivate_scope(self, row_id):
        cursor = self.db.conn.cursor()
        cursor.execute('''UPDATE dns_resolver SET scope_status = 'out' WHERE id = ?''', (row_id,))
        self.db.conn.commit()
        cursor.close()

    '''Search'''
    def resolve_handler(self, domain):
        if datautils.AGGRESSIVE_SCANS:
            for ans in self.resolve_all_common_types(domain):
                print("DNS " + domain + " " + ', '.join(map(str,ans)))
                self.insert_input_data((domain,ans))
        else:
            status, response = self.resolve(domain)
            for ans in response:
                print("DNS " +domain + " "+ ', '.join(map(str,ans)))
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
                if 'does not exist' in response[0]:
                    domain_exists = False
            dns_answers.append((record_type, dns_type_mapping[record_type], status, response))
        return dns_answers
             

    def resolve(self, domain_name, record="A"):
        if datautils.DEBUG:
            file_name = domain_name + "_" + record + ".cache"
            response_text  = datautils.read_cache_dns(file_name,self.name)
            app_logger.log(logging.DEBUG,f"DNS request from cache: " + str(response_text))
            if response_text:
                if datautils.CACHE_UNSUCCESSFUL_CONTENT_PATTERN in response_text:
                     return False, response_text
                return True, response_text
        try:
            response = dns.resolver.query(domain_name, record)
            app_logger.log(logging.DEBUG,f"DNS request live: " + str(response_text))
            time.sleep(self.dns_timeout)
            response_text = self.resolve_build_return(record, response)
            if datautils.DEBUG:
                file_name = domain_name + "_" + record + ".cache"
                datautils.write_cache_dns(file_name,self.name, response_text)
            return True, response_text
        except dns.resolver.NoAnswer:
            if datautils.DEBUG:
                file_name = domain_name + "_" + record + ".cache"
                datautils.write_cache_dns(file_name,self.name, [{datautils.CACHE_UNSUCCESSFUL_CONTENT_PATTERN: 'No answer found'}])
            return False, ["No answer found"]
        except dns.resolver.NXDOMAIN as e:
            if datautils.DEBUG:
                file_name = domain_name + "_" + record + ".cache"
                datautils.write_cache_dns(file_name,self.name, [{datautils.CACHE_UNSUCCESSFUL_CONTENT_PATTERN: f"{str(e)}"}])
            return False,[f"Domain does not exist {str(e)}"]
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


