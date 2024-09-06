from abc import ABC, abstractmethod
from src.utils.config_controller import ConfigManager
from src.modules.instance_manager import InstanceManager
from  src.utils.app_logger import app_logger
from  src.utils.app_logger import func_call_logger
from src.utils import data_utils
import logging
import datetime
import sqlite3
from itertools import zip_longest
import sys

class BaseInputSources(ABC):
    def __init__(self, general_handlers, name, column_mapping, target_table='collection'):
        self.name = name
        self.tablename = name
        self.column_mapping = column_mapping
        self.db = general_handlers['osint_database']
        self.cache_db = general_handlers['cache_db']
        self.scope = general_handlers['scope']
        self.data_timeout_threshold = general_handlers['data_timeout_threshold']
        self.column_mappings_rated_lists = self.prepare_mappings_rated_lists()
        self.register()
        self._create_table()
        self.config = ConfigManager().get_config()


    @func_call_logger(log_level=logging.INFO)
    def _create_table(self):
        table_columns = ', '.join([f"{col} {data_type}" for col, (data_type, _ , _) in self.column_mapping.items()])
        table_sql = f'CREATE TABLE IF NOT EXISTS {self.tablename} ({table_columns})'
        self.db.execute_sql(table_sql)    
            
            
    '''Scope'''
    @func_call_logger(log_level=logging.INFO)
    def scope_receive(self, message):
        columns = ', '.join(f'{col}' for col in self.column_mappings_rated_lists["primary"])
        rows = self.db.execute_sql(f'''SELECT id, {columns} FROM {self.tablename}''')
        scope_data = self.scope.get_scope()

        to_activate = []
        to_deactivate = []

        for row in rows:
            row_id = row[0]
            is_scope_active = any(self.is_row_in_scope(row, scope_item, self.column_mappings_rated_lists["primary"]) for scope_item in scope_data)
            if is_scope_active:
                to_activate.append(row_id)
            else:
                to_deactivate.append(row_id)

        if to_activate:
            self.batch_update_scope_status(to_activate, 'in')
        if to_deactivate:
            self.batch_update_scope_status(to_deactivate, 'out')

    def _check_existing_domain(self,subdomain):
        domain, tld = data_utils.extract_domain_and_tld(subdomain)
        search_query = f'''
            SELECT 1
            FROM {self.tablename}
            WHERE domain = ?
            LIMIT 1
        '''
        result = self.db.execute_sql(search_query, domain + '.' + tld)
        if result:
            return True
        else:
            return False
            
    #policy is if it is an error requery
    #todo make it configerable 
    #todo create multiple policies.
    def enforce_cached_error_policy(self,response_value):
        if response_value == 'error':
            return False
        else:
            return True
    
    def prepare_mappings_without_meta(self):
        return list([key for key, (_, _, label) in  self.column_mapping.items() if label != 'meta'])
    
    def prepare_mappings_insertion_meta(self):
        return list([key for key, (_, label, label2) in  self.column_mapping.items() if label2 == 'meta' and label != 'id' ])


    def prepare_mappings_rated_lists(self):
        primary_keys = tuple(key for key, (_, label, _) in  self.column_mapping.items() if label == 'primary')
        secondary_keys = tuple(key for key, (_, label, _) in  self.column_mapping.items() if label == 'secondary')
        return {'primary': primary_keys, 'secondary': secondary_keys}


    def get_column_mapping(self):
        return self.column_mapping
    
    def register(self):
        InstanceManager.register_input_instance(self.name, self)


    @func_call_logger(log_level=logging.DEBUG)
    def search_based_on_output(self, target_table,timethreshold_refresh_in_days=365):
        time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        time_threshold = datetime.datetime.now() - datetime.timedelta(days=timethreshold_refresh_in_days)
        primary_csv = ','.join([f'{col}' for col in self.column_mappings_rated_lists['primary']])
        primary_csv_t1 = ','.join([f't1.{col}' for col in self.column_mappings_rated_lists['primary']])
        

        search_query = f''' SELECT {primary_csv_t1}
                FROM
                    (SELECT {primary_csv}, MAX(time_modified) as latest_time_modified
                    FROM {target_table}
                    WHERE scope_status = 'in' 
                    GROUP BY {primary_csv}) as t1
                LEFT JOIN {self.tablename} as t2
                ON {' AND '.join([f't1.{col} = t2.{col}' for col in self.column_mappings_rated_lists['primary']])}
                WHERE
                    {' AND '.join([f't2.{col} IS NULL' for col in self.column_mappings_rated_lists['primary']])}
                    {self.condiction_select_based_on_output() if self.condiction_select_based_on_output() else ''}
        '''
            
        # where in ... AND {' AND '.join([f'{col} IS NOT NULL' for col in self.current_column_mappings_rated_lists['primary']])}
        rows = self.db.execute_sql(search_query)
        if rows:
            self.receiver_search_by_primary_values(rows)


    '''
    Takes API data from the input source and inserts it in its own input table
    '''
    @func_call_logger(log_level=logging.DEBUG)
    def insert_input_data(self, input_objs):
        #TODO how do we want to safe errors to the database
        if input_objs == 'error':
            return
        self.column_mappings_rated_lists_without_meta = self.prepare_mappings_without_meta()
        columns_insertion_meta = self.prepare_mappings_insertion_meta()
        insertion_columns = columns_insertion_meta + self.column_mappings_rated_lists_without_meta
        columns = ', '.join(insertion_columns)
        placeholders = ', '.join(['?' for _ in range(len(insertion_columns))])
        where_clause = ' AND '.join([f"{col} = ?" for col in self.column_mappings_rated_lists_without_meta])

        # Generate the SELECT query to check for existence
        select_query = f'''
            SELECT id
            FROM {self.tablename}
            WHERE {where_clause};
        '''

        # Generate the INSERT query
        insert_query = f'''
            INSERT INTO {self.tablename} ({columns})
            VALUES ({placeholders});
        '''

        update_query = f'''
            UPDATE {self.tablename} 
            SET time_modified = ?, scope_status = ?
            WHERE id = ?
        '''

        try:
            # Start a transaction
            with self.db.conn:
                insertion_data_objs = self.prepare_input_insertion_data(input_objs)
                for obj in insertion_data_objs:
                    obj = {k: '' if v is None else v for k, v in obj.items()}
                    select_dict = self._get_insertion_data_subset_by_column_mapping_filter(obj, self.column_mappings_rated_lists_without_meta)
                    insert_dict = self._reorder_insertion_data(obj, insertion_columns)
                    result = self.db.conn.execute(select_query, list(select_dict.values())).fetchone()

                    if not result:
                        self.db.conn.execute(insert_query, list(insert_dict.values()))
                    else:
                        self.db.conn.execute(update_query, (obj.get("time_modified", ""), obj.get("scope_status", ""), result[0]))

        except sqlite3.Error as e:
            app_logger.error(f"Error: {e}")
            self.db.conn.rollback()

    def _reorder_insertion_data(self,insertion_data, insertion_columns):
        if len(insertion_data) != len(insertion_columns):
            raise ValueError(f"Number of insertion data {len(insertion_data)} doesn't match the number of columns {len(insertion_columns)}.")
        return self._get_insertion_data_subset_by_column_mapping_filter(insertion_data, insertion_columns)
    
    def _get_insertion_data_subset_by_column_mapping_filter(self, insertion_data, insertion_columns):
        data_subset = {}
        for key in insertion_columns:
            if key in insertion_data.keys():
                data_subset[key] = insertion_data[key]
        return data_subset

    @abstractmethod
    @func_call_logger(log_level=logging.INFO)
    def run(self):
        """
        Abstract method to define running the module.
        """
        pass

    @abstractmethod
    @func_call_logger(log_level=logging.INFO)
    def search_based_on_scope(self):
        """
        Abstract method to define running the module.
        """
        pass

    @abstractmethod
    def prepare_input_insertion_data():
        """
        Abstract method to get the likely API data in a finished preprocessed and unified structure.
        """
        pass


    @abstractmethod
    @func_call_logger(log_level=logging.DEBUG)
    def receiver_search_by_primary_values(self):
        """
        This receiver method handles 
        """
        pass

    def condiction_select_based_on_output(self):
        return None
    

