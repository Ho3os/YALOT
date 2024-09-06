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

class StrategyProviderOverwrite(ABC):
    def __init__(self, general_handlers, output_module, target_table='collection'):
        self.name = output_module["instance"].name
        self.tablename = output_module["instance"].name
        self.link_output_module = None
        self.db = general_handlers['osint_database']
        self.cache_db = general_handlers['cache_db']
        self.scope = general_handlers['scope']
        self.data_timeout_threshold = general_handlers['data_timeout_threshold']
        self.target_table = self.name
        self.config = ConfigManager().get_config()
        self.input_instances = output_module["input_instances"]
        self.reset_input_context()

    def set_link_output_module(self, output_instance):
        self.link_output_module = output_instance

    def get_data_update_from_all_inputs(self):
        for input_instance in self.input_instances:
            if input_instance:
                self.get_data_update_from_input(input_instance.name)

    def get_data_update_from_input(self, input_instance_name):
        for instance in self.input_instances:
            if instance and instance.name == input_instance_name:
                self.set_input_context(instance)
                self.update_collection()
                self.reset_input_context()
                return True
        return False


    def set_input_context(self, input_instance):
        self.current_input_instance = input_instance
        self.current_input_tablename = input_instance.tablename
        self.current_column_mapping = self.current_input_instance.get_column_mapping()
        self.current_column_mappings_rated_lists = self.prepare_mappings_rated_lists()
        self.current_column_mappings_rated_lists_primary_secondary_chained = \
            self.current_column_mappings_rated_lists['primary'] +\
            self.current_column_mappings_rated_lists['secondary']

    def reset_input_context(self):  
        self.current_input_instance = None
        self.current_input_tablename = None
        self.current_input_tablename = None
        self.current_column_mapping = None
        self.current_column_mappings_rated_lists = None
        self.current_column_mappings_rated_lists_primary_secondary_chained = None
                



    @func_call_logger(log_level=logging.DEBUG)
    def update_collection(self):
        time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        meta_data = {
            "time_created" : time_now,
            "time_modified" : time_now,
            'scope_status': 'TBD',
            }
        self.sync_input_into_output(meta_data)
        self._postprocessing_and_clenaup(self.target_table)


    """
    This function is used to keep a target_table clean and perform post processing tasks.
    """
    @func_call_logger(log_level=logging.DEBUG)
    def _postprocessing_and_clenaup(self,target_table):
        self._postprocessing_dns_types_and_ips(target_table)
        InstanceManager.get_output_instance(target_table).scope_receive('postprocessing scope reevalutation through: '+ self.current_input_tablename)

    @func_call_logger(log_level=logging.DEBUG)
    def _postprocessing_dns_types_and_ips(self, target_table):
        update_ip = f'''
            UPDATE {target_table} 
            SET ip = dns_value
            WHERE ip IS NULL 
            AND dns_value IS NOT NULL 
            AND ((dns_type = '1'  AND dns_value GLOB '[0-9]*.[0-9]*.[0-9]*.[0-9]*')
                OR (dns_type = '28' AND dns_value GLOB '[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*'))
        '''
        update_dns_value = f'''
            UPDATE {target_table} 
            SET dns_value = ip,
                dns_type = CASE 
                                WHEN ip GLOB '[0-9]*.[0-9]*.[0-9]*.[0-9]*' THEN '1' 
                                WHEN ip GLOB '[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*' THEN '28' 
                                ELSE dns_type 
                            END
            WHERE dns_value IS NULL 
            AND ip IS NOT NULL
            AND dns_type IS NULL
            '''

        self.db.execute_sql(update_ip)
        self.db.execute_sql(update_dns_value)


        

    """
    Generates the sql queries to insert and update and executes it. 
    For now this is inefficient and an update and insert takes place for every entry
    Bug checking for same data and skip update does not work
    """
    @func_call_logger(log_level=logging.DEBUG)
    def sync_input_into_output(self, meta_data, target_table='collection'):
        self._update_through_subset_input_into_output(target_table, meta_data)
        #todo a more role based approach
        #self._update_existing_records(target_table,meta_data)
        self._insert_input_into_output(target_table, meta_data)
        self._meta_data_update_input_into_output(target_table, meta_data)


    '''
    Checks for sufficient empty rows to insert data. Goal is to keep the dataset dense.
    First check for hard matching primary sets and empty secondaries and do a weak update.
    Then check for soft matching primary sets and overwrite secondaries anyway since primary are more percise. 
    TODO improve it if required by e.g. timestamps.
    '''
    def _update_through_subset_input_into_output(self,target_table, meta_data):  
        find_subset_query_weak = self._get_find_subset_weak_update_query_for_insert_into_output_data(target_table, meta_data)
        self.db.execute_sql(find_subset_query_weak)
        find_subset_query_strong = self._get_find_subset_strong_update_query_for_insert_into_output_data(target_table, meta_data)
        self.db.execute_sql(find_subset_query_strong)


    #TODO create a super duper primary. shodan it is IP, port
    def __update_to_complete_existing_records(self,target_table, meta_data):
        insert_columns = ', '.join(f'{col}=t3.{col}' for col in self.current_column_mappings_rated_lists_primary_secondary_chained)
        insert_columns_t1 = ', '.join(f't1.{col}' for col in self.current_column_mappings_rated_lists_primary_secondary_chained)

        query = f'''
            UPDATE {target_table} as t4
            SET {insert_columns}, OSINTsource = CASE
                     WHEN t4.OSINTsource NOT LIKE '%{self.current_input_tablename}%' THEN t4.OSINTsource || ',{self.current_input_tablename}'
                     ELSE t4.OSINTsource
                  END 
            FROM    
                (SELECT t2.id, {insert_columns_t1}, t1.RowNum
                FROM 
                    (SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join([f'{col}' for col in self.current_column_mappings_rated_lists['primary']])} ORDER BY time_modified DESC) AS RowNum
                    FROM {self.current_input_tablename})as t1
                LEFT JOIN 
                    (SELECT *
                    FROM {target_table}
                    ) as t2
                ON  {' AND '.join([f'(t1.{col} = t2.{col})' for col in ['ip','port']])} 
                ) as t3
            WHERE t4.id=t3.id and RowNum = 1;
        '''
        self.db.execute_sql(query)

    '''
    Regular insert for the target_table.
    '''
    def _insert_input_into_output(self,target_table, meta_data):
        insert_query = self._get_insert_query_for_insert_into_output_data(target_table,meta_data)
        self.db.execute_sql(insert_query)

    '''
    Is called to check if same data existis in output target_table but the meta_data like OSINTsource is not listed
    It checks on primary only.
    ''' 
    def _meta_data_update_input_into_output(self,target_table, meta_data):
        update_query = f""" UPDATE {target_table}
            SET time_modified = ?, OSINTsource = ?
            WHERE id = ?;
        """
        meta_data_update_query = self._get_update_meta_data_query_for_insert_into_output_data(target_table, meta_data)
        rows_same = self.db.execute_sql(meta_data_update_query)
        for row_id, input_time_modified, existing_time_modified, existing_osintsource in rows_same:
            module_name = self.current_input_tablename
            if existing_osintsource:
                existing_osintsource = existing_osintsource.split(",")
                if module_name not in existing_osintsource:
                    updated_osintsource = existing_osintsource + [module_name]
                else:
                    updated_osintsource = existing_osintsource
            else:
                updated_osintsource = [module_name]
            self.db.execute_sql(update_query, (input_time_modified, ",".join(updated_osintsource), row_id))

    def _get_update_meta_data_query_for_insert_into_output_data(self,target_table, meta_data):
        select_query_same = f"""SELECT id, t1tm, t2tm, OSINTsource             
            FROM ( 
                SELECT t2.id, t1.time_modified AS t1tm, t2.time_modified AS t2tm, t2.OSINTsource, t1.RowNum
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join([f'{col}' for col in self.current_column_mappings_rated_lists['primary']])} ORDER BY time_modified DESC) AS RowNum
                    FROM {self.current_input_tablename}
                ) AS t1
                LEFT JOIN {target_table} AS t2 
                ON {' AND '.join([f't1.{col} = t2.{col}' for col in self.current_column_mappings_rated_lists['primary']])}
                WHERE t2.id IS NOT NULL AND OSINTsource NOT LIKE '%{self.current_input_tablename}%'
            )
            WHERE RowNum = 1;
        """
        if hasattr(self.current_input_instance, "_get_update_meta_data_query_for_insert_into_output_data"):
                return self.current_input_instance._get_update_meta_data_query_for_insert_into_output_data(select_query_same)
        return select_query_same
    
    '''
    We only find subsets if they are 100 clean to use also from a secondary perspective
    '''
    def _get_find_subset_strong_update_query_for_insert_into_output_data(self,target_table, meta_data):  
        insert_columns = ', '.join(f'{col}=t3.{col}' for col in self.current_column_mappings_rated_lists_primary_secondary_chained)
        insert_columns_t1 = ', '.join(f't1.{col}' for col in self.current_column_mappings_rated_lists_primary_secondary_chained)

        condition = None
        if hasattr(self.current_input_instance, "condition_select_for_subset_updates_input_into_output"):
            condition = self.current_input_instance.condition_select_for_subset_updates_input_into_output()

        find_subset_query_strong = f'''
            UPDATE {target_table} as t4
            SET {insert_columns}, OSINTsource = CASE
                     WHEN t4.OSINTsource NOT LIKE '%{self.current_input_tablename}%' THEN t4.OSINTsource || ',{self.current_input_tablename}'
                     ELSE t4.OSINTsource
                  END 
            FROM    
                (SELECT t2.id, {insert_columns_t1}, t1.RowNum
                FROM 
                    (SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join([f'{col}' for col in self.current_column_mappings_rated_lists['primary']])} ORDER BY time_modified DESC) AS RowNum
                    FROM {self.current_input_tablename})as t1
                LEFT JOIN 
                    (SELECT *
                    FROM {target_table}
                    WHERE {' OR '.join([f'{col} IS NULL' for col in self.current_column_mappings_rated_lists['primary']])} 
                    ) as t2
                ON  {' AND '.join([f'(t1.{col} = t2.{col} OR t2.{col} IS NULL)' for col in self.current_column_mappings_rated_lists['primary']])} 
                WHERE  t2.id IS NOT NULL {condition if condition else ''}
                ) as t3
            WHERE t4.id=t3.id and RowNum = 1;
        '''
        if hasattr(self.current_input_instance, "_get_find_subset_strong_update_query_for_insert_into_output_data"):
                return self.current_input_instance._get_find_subset_strong_update_query_for_insert_into_output_data(find_subset_query_strong)
        return find_subset_query_strong
        
        
    
    '''
    We only find subsets if they are 100 clean to use also from a secondary perspective
    '''
    def _get_find_subset_weak_update_query_for_insert_into_output_data(self,target_table, meta_data):
        if self.current_column_mappings_rated_lists['secondary']:
            secondary_values_key_conditions_different = 'and (' + ' AND '.join([f't2.{col} IS NULL' for col in self.current_column_mappings_rated_lists['secondary']]) + ')'
        else:
            secondary_values_key_conditions_different = ''
        insert_columns = ', '.join(f'{col}=t3.{col}' for col in self.current_column_mappings_rated_lists_primary_secondary_chained)
        insert_columns_t1 = ', '.join(f't1.{col}' for col in self.current_column_mappings_rated_lists_primary_secondary_chained)

        select_query_different = f"""
            UPDATE {target_table} AS t4
            SET {insert_columns}, OSINTsource = CASE
                     WHEN t4.OSINTsource NOT LIKE '%{self.current_input_tablename}%' THEN t4.OSINTsource || ',{self.current_input_tablename}'
                     ELSE t4.OSINTsource
                  END 
            FROM ( 
                SELECT t2.id, {insert_columns_t1}, t1.RowNum
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join([f'{col}' for col in self.current_column_mappings_rated_lists['primary']])} ORDER BY time_modified DESC) AS RowNum
                    FROM {self.current_input_tablename}
                ) AS t1
                LEFT JOIN {target_table} AS t2 
                ON {' AND '.join([f't1.{col} = t2.{col}' for col in self.current_column_mappings_rated_lists['primary']])}
                WHERE t2.id is NOT NULL {secondary_values_key_conditions_different} 
            )  AS t3
            WHERE t4.id=t3.id and RowNum = 1;
        """
        if hasattr(self.current_input_instance, "_get_find_subset_weak_update_query_for_insert_into_output_data"):
                return self.current_input_instance._get_find_subset_weak_update_query_for_insert_into_output_data(select_query_different)
        return select_query_different
    
    def _get_insert_query_for_insert_into_output_data(self,target_table, meta_data):
        insert_columns = ', '.join(f'{col}' for col in self.current_column_mappings_rated_lists_primary_secondary_chained)
        insert_columns_t1 = ', '.join(f't1.{col}' for col in self.current_column_mappings_rated_lists_primary_secondary_chained)

        select_query_different = f"""
            INSERT INTO {target_table} ({insert_columns + ", OSINTsource, " + ', '.join([f"'{col}'" for col in list(meta_data.keys())])})
                SELECT {insert_columns + ", '" + self.current_input_tablename +"', " + ', '.join([f"'{meta_data[col]}'" for col in list(meta_data.keys())])}    
                FROM ( 
                    SELECT {insert_columns_t1}, t1.RowNum
                    FROM (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join([f'{col}' for col in self.current_column_mappings_rated_lists['primary']])} ORDER BY time_modified DESC) AS RowNum
                        FROM {self.current_input_tablename}
                    ) AS t1
                    LEFT JOIN {target_table} AS t2 
                    ON {' AND '.join([f't1.{col} = t2.{col}' for col in self.current_column_mappings_rated_lists['primary']])}
                    WHERE t2.id is NULL
                )
                WHERE RowNum = 1;
        """
        if hasattr(self.current_input_instance, "_get_insert_query_for_insert_into_output_data"):
                return self.current_input_instance._get_insert_query_for_insert_into_output_data(select_query_different)
        return select_query_different




        
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

    def prepare_mappings_without_meta(self):
        return list([key for key, (_, _, label) in  self.current_column_mapping.items() if label != 'meta'])
    
    def prepare_mappings_insertion_meta(self):
        return list([key for key, (_, label, label2) in  self.current_column_mapping.items() if label2 == 'meta' and label != 'id' ])


    def prepare_mappings_rated_lists(self):
        primary_keys = tuple(key for key, (_, label, _) in  self.current_column_mapping.items() if label == 'primary')
        secondary_keys = tuple(key for key, (_, label, _) in  self.current_column_mapping.items() if label == 'secondary')
        return {'primary': primary_keys, 'secondary': secondary_keys}

    def check_if_primary_record_exists(self, *args):
        if len(args) != len(self.self.current_column_mappings_rated_lists_rated_lists['primary']):
            raise ValueError("Number of arguments doesn't match the number of primary columns.")
        values = list(args)
        where_clause = ' AND '.join([f"{col} = ?" for col in self.self.current_column_mappings_rated_lists_rated_lists['primary']])
        rows = self.db.execute_sql(f'''SELECT id 
                    FROM {self.current_input_tablename} 
                    WHERE {where_clause}''', tuple(values))
        if rows:
            return rows[0][0]
        else:
            return False
        
    @func_call_logger(log_level=logging.DEBUG)
    def activate_scope(self, row_id):
        self.db.execute_sql(f'''UPDATE {self.tablename} SET scope_status = 'in' WHERE id = ?''', (row_id,))

    @func_call_logger(log_level=logging.DEBUG)
    def deactivate_scope(self, row_id):
        self.db.execute_sql(f'''UPDATE {self.tablename} SET scope_status = 'out' WHERE id = ?''', (row_id,))

    '''Scope'''
    @func_call_logger(log_level=logging.INFO)
    def scope_receive(self, message):
        columns = ', '.join(f'{col}' for col in self.current_column_mappings_rated_lists["primary"])
        rows = self.db.execute_sql(f'''SELECT id, {columns} FROM {self.tablename}''')
        scope_data = self.scope.get_scope()

        to_activate = []
        to_deactivate = []

        for row in rows:
            row_id = row[0]
            is_scope_active = any(self.is_row_in_scope(row, scope_item, self.current_column_mappings_rated_lists["primary"]) for scope_item in scope_data)
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
            if (scope_item['scope_type'] == 'Subnet' and data_utils.is_ip_in_subnet(row[ip_index], scope_item['scope_value'])):
                return True
            if (scope_item['scope_type'] == 'IP' and scope_item['scope_value'] == row[ip_index]):
                return True
        
        dns_value_index = self.is_in_list(columns,'dns_value') + 1
        dns_type_index = self.is_in_list(columns,'dns_type') + 1
        if dns_type_index and dns_value_index and (row[dns_value_index] == '1' or  row[dns_value_index] == '28'):
            if (scope_item['scope_type'] == 'Subnet' and data_utils.is_ip_in_subnet(row[dns_value_index], scope_item['scope_value'])):
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

    def condition_select_for_subset_updates_input_into_output(self):
        """
        Makes sure that besides the primary dataset a suited entry for completion is found
        """
        return None
    
    def create_db_metadata_analysis_struct(self):
        """
        Based on column_mapping and target_tables a struct for the decorator is created.
        This function should only be called by the decorator.
        """
        return {"module_table_name": self.tablename,
                "column_list": self.current_column_mappings_rated_lists['primary'],
                "target_table_name": self.target_table}


    #policy is if it is an error requery
    #todo make it configerable 
    #todo create multiple policies.
    def enforce_cached_error_policy(self,response_value):
        if response_value == 'error':
            return False
        else:
            return True



