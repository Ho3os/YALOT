from src.utils import data_utils
from src.modules.instance_manager import InstanceManager
from  src.utils.app_logger import app_logger
from  src.utils.app_logger import func_call_logger
from src.utils import data_utils
import logging
from itertools import zip_longest
import sqlite3
from src.utils.config_controller import ConfigManager

class Collection(object):
    def __init__(self, general_handlers, name="collection"):
        self.name = name
        self.tablename = name
        self.link_strategy = None
        self.config = ConfigManager().get_config()
        self.input_modules = ConfigManager().get_input_modules_of_output_module(self.name)
        InstanceManager.register_output_instance(self.name, self, self.input_modules)
        self.column_mapping = {
            'id': ('INTEGER PRIMARY KEY', 'id', 'meta'),
            'OSINTsource': ('TEXT', 'secondary', 'meta'),
            'time_created': ('TEXT', 'secondary', 'meta'),
            'time_modified': ('TEXT', 'secondary', 'meta'),
            'scope_status': ('TEXT', 'secondary',''),
            'label': ('TEXT', 'secondary', 'meta'),
            'domain': ('TEXT','primary', ''),
            'hostnames': ('TEXT', 'secondary',''),
            'ip': ('TEXT','primary', ''),
            'port': ('TEXT','primary', ''),
            'transport': ('TEXT', 'secondary',''),
            'dns_type': ('TEXT','primary', ''),
            'dns_value': ('TEXT', 'secondary',''),
            'tags': ('TEXT','secondary', ''),
            'asn': ('TEXT', 'secondary',''),
            'isp': ('TEXT', 'secondary',''),
            'org': ('TEXT', 'secondary',''),
            'location_country': ('TEXT', 'secondary',''),
            'location_city': ('TEXT', 'secondary',''),
            'os': ('TEXT', 'secondary',''),
            'http_title': ('TEXT', 'secondary',''),
            'http_host': ('TEXT', 'secondary',''),
            'http_server': ('TEXT', 'secondary',''),
            'http_components': ('TEXT', 'secondary',''),
            'http_waf': ('TEXT', 'secondary',''),
            'http_redirects': ('TEXT', 'secondary',''),
            'cert_issuer': ('TEXT', 'secondary',''),
            'cert_subject': ('TEXT', 'secondary',''),
            'cert_issued': ('TEXT', 'secondary',''),
            'cert_expires': ('TEXT', 'secondary',''),
            'ssl_ja3s': ('TEXT', 'secondary',''),
            'ssl_jarm': ('TEXT', 'secondary',''),
            'vulns': ('TEXT','secondary', '')
        }
        self.db = general_handlers['osint_database']
        self._create_table()
        self.scope = general_handlers['scope']
        

    def set_strategy(self, strategy):
        self.link_strategy = strategy
        self.link_strategy.set_link_output_module(self)

    def get_column_mapping(self):
        return self.column_mapping.copy()


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
    
    


    @func_call_logger(log_level=logging.DEBUG)
    def batch_update_scope_status(self, row_ids, status, chunk_size=5000):
        for chunk_ids in zip_longest(*[iter(row_ids)] * chunk_size, fillvalue=None):
            chunk_ids = [id for id in chunk_ids if id is not None]
            placeholders = ",".join(["?" for _ in chunk_ids])
            update_query = f"UPDATE {self.tablename} SET scope_status = ? WHERE id IN ({placeholders})"
            parameters = [status] + chunk_ids
            try:
                self.db.execute_sql(update_query, parameters)
            except sqlite3.Error as e:
                app_logger.error(f"Error updating scope status: {e}")


    @func_call_logger(log_level=logging.INFO)
    def _create_table(self):
        table_columns = ', '.join([f"{col} {data_type}" for col, (data_type, _ , _) in self.column_mapping.items()])
        table_sql = f'CREATE TABLE IF NOT EXISTS {self.tablename} ({table_columns})'
        return self.db.execute_sql(table_sql)

    def activate_scope(self, row_id):
        return self.db.execute_sql(f'''UPDATE {self.tablename} SET scope_status = 'in' WHERE id = ?''', (row_id,))


    def deactivate_scope(self, row_id):
        return self.db.execute_sql(f'''UPDATE {self.tablename} SET scope_status = 'out' WHERE id = ?''', (row_id,))


    def get_column_names(self):
        column_list = []
        for col, (_, data_type, _) in self.column_mapping.items():
            if data_type in ('primary', 'secondary'):
                column_list.append(col)
        return column_list

    def get_column_names_without_meta_time(self):
        column_list = []
        for col, (_, data_type, _) in self.column_mapping.items():
            if data_type in ('primary', 'secondary') and col not in ('time_created', 'time_modified'):
                column_list.append(col)
        return column_list