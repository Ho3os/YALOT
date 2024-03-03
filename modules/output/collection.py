from utils import datautils
from utils.instancemanager import InstanceManager
from  utils.applogger import app_logger
from  utils.applogger import func_call_logger
from utils import datautils
import logging
from itertools import zip_longest
import sqlite3

class Collection(object):
    def __init__(self, db, scope,name="collection"):
        self.name = name
        self.tablename = name
        InstanceManager.register_output_instance(self.name, self)
        self.column_mapping = {
            'id': 'INTEGER PRIMARY KEY',
            'OSINTsource': 'TEXT',
            'time_created': 'TEXT',
            'time_modified': 'TEXT',
            'scope_status': 'TEXT',
            'domain': 'TEXT',
            'hostnames': 'TEXT',
            'ip': 'TEXT',
            'port': 'TEXT',
            'transport': 'TEXT',
            'dns_type': 'TEXT',
            'dns_value': 'TEXT',
            'label': 'TEXT',
            'shodan_module': 'TEXT',
            'asn': 'TEXT',
            'isp': 'TEXT',
            'org': 'TEXT',
            'location_country': 'TEXT',
            'location_city': 'TEXT',
            'os': 'TEXT',
            'http_title': 'TEXT',
            'http_host': 'TEXT',
            'http_server': 'TEXT',
            'http_components': 'TEXT',
            'http_waf': 'TEXT',
            'http_redirects': 'TEXT',
            'cert_issuer': 'TEXT',
            'cert_subject': 'TEXT',
            'cert_issued': 'TEXT',
            'cert_expires': 'TEXT',
            'ssl_ja3s': 'TEXT',
            'ssl_jarm': 'TEXT',
        }
        self.db = db
        self._create_table()
        self.scope = scope

    def get_column_mapping(self):
        return self.column_mapping.copy()

        '''Scope'''
    @func_call_logger(log_level=logging.INFO)
    def scope_receive(self, message):
        cursor = self.db.conn.cursor()
        column_fields = ['domain','ip','dns_type', 'dns_value']
        columns = ', '.join(f'{col}' for col in column_fields)
        cursor.execute(f'''SELECT id, {columns} FROM {self.tablename}''')
        rows = cursor.fetchall()
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
                self.execute_sql(update_query, parameters)
            except sqlite3.Error as e:
                app_logger.error(f"Error updating scope status: {e}")

    '''
    Wrapper for less code. Do not use if batching
    '''
    @func_call_logger(log_level=logging.DEBUG)
    def execute_sql(self, query, data=None):
        cursor = self.db.conn.cursor()
        try:
            if data is None:
                cursor.execute(query)
            else:
                if isinstance(data[0], (list, tuple)):
                    cursor.executemany(query, data)
                else:
                    cursor.execute(query, data)

            result = cursor.fetchall()
            self.db.conn.commit()
        except Exception as e:
            app_logger.error(f"Error executing SQL query: {query}")
            app_logger.error(f"Error details: {str(e)}")
            result = None
        finally:
            cursor.close()

        return result

    @func_call_logger(log_level=logging.INFO)
    def _create_table(self):
        cursor = self.db.conn.cursor()
        table_columns = ', '.join([f'{column} {data_type}' for column, data_type in self.column_mapping.items()])
        table_sql = f'CREATE TABLE IF NOT EXISTS {self.tablename} ({table_columns})'
        cursor.execute(table_sql)
        self.db.conn.commit()
        cursor.close()

    def activate_scope(self, row_id):
        cursor = self.db.conn.cursor()
        cursor.execute(f'''UPDATE {self.tablename} SET scope_status = 'in' WHERE id = ?''', (row_id,))
        self.db.conn.commit()

    def deactivate_scope(self, row_id):
        cursor = self.db.conn.cursor()
        cursor.execute(f'''UPDATE {self.tablename} SET scope_status = 'out' WHERE id = ?''', (row_id,))
        self.db.conn.commit()

    def get_table_columns(self):
        return self.column_mapping

