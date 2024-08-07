from abc import ABC, abstractmethod
from utils.config_controller import ConfigManager
from utils.instance_manager import InstanceManager
from  utils.app_logger import app_logger
from  utils.app_logger import func_call_logger
from utils import data_utils
import logging
import datetime
import sqlite3
from itertools import zip_longest
import sys

class BaseDataSources(ABC):
    def __init__(self, general_handlers, name, column_mapping, target_table='collection'):
        self.name = name
        self.tablename = name
        InstanceManager.register_input_instance(self.name, self)
        self.db = general_handlers['osint_database']
        self.cache_db = general_handlers['cache_db']
        self.scope = general_handlers['scope']
        self.data_timeout_threshold = general_handlers['data_timeout_threshold']
        self.target_table = target_table
        self.column_mapping = column_mapping
        self.column_mappings_rated_lists = self.prepare_mappings_rated_lists()
        self.column_mappings_rated_lists_primary_secondary_chained = self.column_mappings_rated_lists['primary'] + self.column_mappings_rated_lists['secondary']
        self._create_table()
        self.config = ConfigManager().get_config()


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

    @func_call_logger(log_level=logging.DEBUG)
    def search_based_on_collection(self,timethreshold_refresh_in_days=365):
        time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        time_threshold = datetime.datetime.now() - datetime.timedelta(days=timethreshold_refresh_in_days)
        primary_csv = ','.join([f'{col}' for col in self.column_mappings_rated_lists['primary']])
        primary_csv_t1 = ','.join([f't1.{col}' for col in self.column_mappings_rated_lists['primary']])
        search_query = f''' SELECT {primary_csv_t1}
                FROM
                    (SELECT {primary_csv}, MAX(time_modified) as latest_time_modified
                    FROM {self.target_table}
                    WHERE scope_status = 'in' 
                    GROUP BY {primary_csv}) as t1
                LEFT JOIN {self.tablename} as t2
                ON {' AND '.join([f't1.{col} = t2.{col}' for col in self.column_mappings_rated_lists['primary']])}
                WHERE
                    {' AND '.join([f't2.{col} IS NULL' for col in self.column_mappings_rated_lists['primary']])}'''
            
        # where in ... AND {' AND '.join([f'{col} IS NOT NULL' for col in self.column_mappings_rated_lists['primary']])}
        rows = self.db.execute_sql(search_query)
        if rows:
            self.receiver_search_by_primary_values(rows,self.target_table)
        self.update_collection()

    """
    This function is used to keep a target_table clean and perform post processing tasks.
    """
    @func_call_logger(log_level=logging.DEBUG)
    def _postprocessing_and_clenaup(self,target_table):
        self._postprocessing_dns_types_and_ips(target_table)
        InstanceManager.get_output_instance(target_table).scope_receive('postprocessing scope reevalutation through: '+ self.name)

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
        insert_columns = ', '.join(f'{col}=t3.{col}' for col in self.column_mappings_rated_lists_primary_secondary_chained)
        insert_columns_t1 = ', '.join(f't1.{col}' for col in self.column_mappings_rated_lists_primary_secondary_chained)

        query = f'''
            UPDATE {target_table} as t4
            SET {insert_columns}, OSINTsource = CASE
                     WHEN t4.OSINTsource NOT LIKE '%{self.name}%' THEN t4.OSINTsource || ',{self.name}'
                     ELSE t4.OSINTsource
                  END 
            FROM    
                (SELECT t2.id, {insert_columns_t1}, t1.RowNum
                FROM 
                    (SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join([f'{col}' for col in self.column_mappings_rated_lists['primary']])} ORDER BY time_modified DESC) AS RowNum
                    FROM {self.tablename})as t1
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
            module_name = self.name
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
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join([f'{col}' for col in self.column_mappings_rated_lists['primary']])} ORDER BY time_modified DESC) AS RowNum
                    FROM {self.tablename}
                ) AS t1
                LEFT JOIN {target_table} AS t2 
                ON {' AND '.join([f't1.{col} = t2.{col}' for col in self.column_mappings_rated_lists['primary']])}
                WHERE t2.id IS NOT NULL AND OSINTsource NOT LIKE '%{self.name}%'
            )
            WHERE RowNum = 1;
        """
        return select_query_same
    
    '''
    We only find subsets if they are 100 clean to use also from a secondary perspective
    '''
    def _get_find_subset_strong_update_query_for_insert_into_output_data(self,target_table, meta_data):  
        insert_columns = ', '.join(f'{col}=t3.{col}' for col in self.column_mappings_rated_lists_primary_secondary_chained)
        insert_columns_t1 = ', '.join(f't1.{col}' for col in self.column_mappings_rated_lists_primary_secondary_chained)

        find_subset_query_strong = f'''
            UPDATE {target_table} as t4
            SET {insert_columns}, OSINTsource = CASE
                     WHEN t4.OSINTsource NOT LIKE '%{self.name}%' THEN t4.OSINTsource || ',{self.name}'
                     ELSE t4.OSINTsource
                  END 
            FROM    
                (SELECT t2.id, {insert_columns_t1}, t1.RowNum
                FROM 
                    (SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join([f'{col}' for col in self.column_mappings_rated_lists['primary']])} ORDER BY time_modified DESC) AS RowNum
                    FROM {self.tablename})as t1
                LEFT JOIN 
                    (SELECT *
                    FROM {target_table}
                    WHERE {' OR '.join([f'{col} IS NULL' for col in self.column_mappings_rated_lists['primary']])} 
                    ) as t2
                ON  {' AND '.join([f'(t1.{col} = t2.{col} OR t2.{col} IS NULL)' for col in self.column_mappings_rated_lists['primary']])} 
                WHERE  t2.id IS NOT NULL {self.condition_select_for_subset_updates_input_into_output() if self.condition_select_for_subset_updates_input_into_output() else ''}
                ) as t3
            WHERE t4.id=t3.id and RowNum = 1;
        '''
        return find_subset_query_strong
        
        
    
    '''
    We only find subsets if they are 100 clean to use also from a secondary perspective
    '''
    def _get_find_subset_weak_update_query_for_insert_into_output_data(self,target_table, meta_data):
        if self.column_mappings_rated_lists['secondary']:
            secondary_values_key_conditions_different = 'and (' + ' AND '.join([f't2.{col} IS NULL' for col in self.column_mappings_rated_lists['secondary']]) + ')'
        else:
            secondary_values_key_conditions_different = ''
        insert_columns = ', '.join(f'{col}=t3.{col}' for col in self.column_mappings_rated_lists_primary_secondary_chained)
        insert_columns_t1 = ', '.join(f't1.{col}' for col in self.column_mappings_rated_lists_primary_secondary_chained)

        select_query_different = f"""
            UPDATE {target_table} AS t4
            SET {insert_columns}, OSINTsource = CASE
                     WHEN t4.OSINTsource NOT LIKE '%{self.name}%' THEN t4.OSINTsource || ',{self.name}'
                     ELSE t4.OSINTsource
                  END 
            FROM ( 
                SELECT t2.id, {insert_columns_t1}, t1.RowNum
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join([f'{col}' for col in self.column_mappings_rated_lists['primary']])} ORDER BY time_modified DESC) AS RowNum
                    FROM {self.tablename}
                ) AS t1
                LEFT JOIN {target_table} AS t2 
                ON {' AND '.join([f't1.{col} = t2.{col}' for col in self.column_mappings_rated_lists['primary']])}
                WHERE t2.id is NOT NULL {secondary_values_key_conditions_different} 
            )  AS t3
            WHERE t4.id=t3.id and RowNum = 1;
        """
        return select_query_different
    
    def _get_insert_query_for_insert_into_output_data(self,target_table, meta_data):
        insert_columns = ', '.join(f'{col}' for col in self.column_mappings_rated_lists_primary_secondary_chained)
        insert_columns_t1 = ', '.join(f't1.{col}' for col in self.column_mappings_rated_lists_primary_secondary_chained)

        select_query_different = f"""
            INSERT INTO {target_table} ({insert_columns + ", OSINTsource, " + ', '.join([f"'{col}'" for col in list(meta_data.keys())])})
                SELECT {insert_columns + ", '" + self.name +"', " + ', '.join([f"'{meta_data[col]}'" for col in list(meta_data.keys())])}    
                FROM ( 
                    SELECT {insert_columns_t1}, t1.RowNum
                    FROM (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join([f'{col}' for col in self.column_mappings_rated_lists['primary']])} ORDER BY time_modified DESC) AS RowNum
                        FROM {self.tablename}
                    ) AS t1
                    LEFT JOIN {target_table} AS t2 
                    ON {' AND '.join([f't1.{col} = t2.{col}' for col in self.column_mappings_rated_lists['primary']])}
                    WHERE t2.id is NULL
                )
                WHERE RowNum = 1;
        """
        return select_query_different


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

    def prepare_mappings_without_meta(self):
        return list([key for key, (_, _, label) in  self.column_mapping.items() if label != 'meta'])
    
    def prepare_mappings_insertion_meta(self):
        return list([key for key, (_, label, label2) in  self.column_mapping.items() if label2 == 'meta' and label != 'id' ])


    def prepare_mappings_rated_lists(self):
        primary_keys = tuple(key for key, (_, label, _) in  self.column_mapping.items() if label == 'primary')
        secondary_keys = tuple(key for key, (_, label, _) in  self.column_mapping.items() if label == 'secondary')
        return {'primary': primary_keys, 'secondary': secondary_keys}

    def check_if_primary_record_exists(self, *args):
        if len(args) != len(self.self.column_mappings_rated_lists_rated_lists['primary']):
            raise ValueError("Number of arguments doesn't match the number of primary columns.")
        values = list(args)
        where_clause = ' AND '.join([f"{col} = ?" for col in self.self.column_mappings_rated_lists_rated_lists['primary']])
        rows = self.db.execute_sql(f'''SELECT id 
                    FROM {self.tablename} 
                    WHERE {where_clause}''', tuple(values))
        if rows:
            return rows[0][0]
        else:
            return False
        
    
    @func_call_logger(log_level=logging.INFO)
    def _create_table(self):
        table_columns = ', '.join([f"{col} {data_type}" for col, (data_type, _ , _) in self.column_mapping.items()])
        table_sql = f'CREATE TABLE IF NOT EXISTS {self.tablename} ({table_columns})'
        self.db.execute_sql(table_sql)

    @func_call_logger(log_level=logging.DEBUG)
    def activate_scope(self, row_id):
        self.db.execute_sql(f'''UPDATE {self.tablename} SET scope_status = 'in' WHERE id = ?''', (row_id,))

    @func_call_logger(log_level=logging.DEBUG)
    def deactivate_scope(self, row_id):
        self.db.execute_sql(f'''UPDATE {self.tablename} SET scope_status = 'out' WHERE id = ?''', (row_id,))

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

    

    @abstractmethod
    @func_call_logger(log_level=logging.INFO)
    
    def run(self):
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
                "column_list": self.column_mappings_rated_lists['primary'],
                "target_table_name": self.target_table}


    #policy is if it is an error requery
    #todo make it configerable 
    #todo create multiple policies.
    def enforce_cached_error_policy(self,response_value):
        if response_value == 'error':
            return False
        else:
            return True



