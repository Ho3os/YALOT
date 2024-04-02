from utils import datautils
from utils.instancemanager import InstanceManager
from  utils.applogger import app_logger
from  utils.applogger import func_call_logger
from utils import datautils
import logging
from itertools import zip_longest
import sqlite3
from utils.configmanager import ConfigManager

class Collection(object):
    def __init__(self, general_handlers, name="collection"):
        self.name = name
        self.tablename = name
        InstanceManager.register_output_instance(self.name, self)
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
            'ssl_jarm': ('TEXT', 'secondary','')
        }
        self.db = general_handlers['osint_database']
        self._create_table()
        self.scope = general_handlers['scope']
        self.config = ConfigManager().get_config()

    def get_column_mapping(self):
        return self.column_mapping.copy()

        '''Scope'''
    @func_call_logger(log_level=logging.INFO)
    def scope_receive(self, message):
        column_fields = ['domain','ip','dns_type', 'dns_value']
        columns = ', '.join(f'{col}' for col in column_fields)
        rows = self.db.execute_sql(f'''SELECT id, {columns} FROM {self.tablename}''')
        scope_data = self.scope.get_scope()
        to_activate = []
        to_deactivate = []
        for row in rows:
            row_id = row[0]
            is_scope_active = any(self.is_row_in_scope(row, scope_item, column_fields) for scope_item in scope_data)
            if is_scope_active:
                to_activate.append(row_id)
            else:
                to_deactivate.append(row_id)
        if to_activate:
            self.batch_update_scope_status(to_activate, 'in')
        if to_deactivate:
            self.batch_update_scope_status(to_deactivate, 'out')

    @func_call_logger(log_level=logging.DEBUG)
    def is_row_in_scope(self, row, scope_item, columns):
        domain_index = self.is_in_list(columns,'domain') + 1
        if domain_index:
            if (scope_item['scope_type'] == 'Domain' and str(row[domain_index]).endswith(scope_item['scope_value'])):
                return True
        ip_index = self.is_in_list(columns,'ip') + 1
        if ip_index:
            if (scope_item['scope_type'] == 'Subnet' and datautils.is_ip_in_subnet(row[ip_index], scope_item['scope_value'])):
                return True
            if (scope_item['scope_type'] == 'IP' and scope_item['scope_value'] == row[ip_index]):
                return True
        dns_value_index = self.is_in_list(columns,'dns_value') + 1
        dns_type_index = self.is_in_list(columns,'dns_type') + 1
        if dns_type_index and dns_value_index and (row[dns_value_index] == '1' or  row[dns_value_index] == '28'):
            if (scope_item['scope_type'] == 'Subnet' and datautils.is_ip_in_subnet(row[dns_value_index], scope_item['scope_value'])):
                return True
            if (scope_item['scope_type'] == 'IP' and scope_item['scope_value'] == row[dns_value_index]):
                return True
        return False
    
    def is_in_list(self,element_list,element):
        try:
            return element_list.index(element)
        except ValueError:
            return False


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