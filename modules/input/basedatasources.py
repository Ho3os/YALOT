from abc import ABC, abstractmethod
from utils.instancemanager import InstanceManager
from  utils.applogger import app_logger
from  utils.applogger import func_call_logger
from utils import datautils
import logging
import datetime
import sqlite3
from itertools import zip_longest



class BaseDataSources(ABC):
    def __init__(self, db, scope, name, column_mapping, target_table='collection'):
        self.name = name
        self.tablename = name
        InstanceManager.register_input_instance(self.name, self)
        self.db = db
        self.scope = scope
        self.target_table = target_table
        self.column_mapping = column_mapping
        self.column_mappings_rated_lists = self.prepare_mappings_rated_lists()
        self.column_mappings_rated_lists_primary_secondary_chained = self.column_mappings_rated_lists['primary'] + self.column_mappings_rated_lists['secondary']
        self._create_table()


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
        rows = self.execute_sql(search_query)
        if rows:
            self.receiver_search_by_primary_values(rows,self.target_table)
        self.update_collection()

    """
    This function is used to keep a target_table clean and perform post processing tasks.
    """
    @func_call_logger(log_level=logging.DEBUG)
    def _postprocessing_and_clenaup(self,target_table):
        self._postprocessing_dns_types_and_ips(target_table)
        #self.scope_receive('postprocessing scope reevalutation of: ' +self.name)
        InstanceManager.get_output_instance(target_table).scope_receive('postprocessing scope reevalutation through: '+ self.name)

    @func_call_logger(log_level=logging.DEBUG)
    def _postprocessing_dns_types_and_ips(self, target_table):
        update_query = f'''
            UPDATE {target_table} 
            SET ip = dns_value
            WHERE ip IS NULL 
            AND dns_value IS NOT NULL 
            AND ((dns_type = '1'  AND dns_value GLOB '[0-9]*.[0-9]*.[0-9]*.[0-9]*')
                OR (dns_type = '28' AND dns_value GLOB '[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*'))
        '''
        self.execute_sql(update_query)

        

    """
    Generates the sql queries to insert and update and executes it. 
    For now this is inefficient and an update and insert takes place for every entry
    Bug checking for same data and skip update does not work
    """
    @func_call_logger(log_level=logging.DEBUG)
    def sync_input_into_output(self, meta_data, target_table='collection'):
        self._update_through_subset_input_into_output(target_table, meta_data)
        self._insert_input_into_output(target_table, meta_data)
        self._meta_data_update_input_into_output(target_table, meta_data)


    '''
    Checks for sufficient empty rows to insert data. Goal is to keep the dataset dense.
    '''
    def _update_through_subset_input_into_output(self,target_table, meta_data):  
        find_subset_query = self._get_find_subset_update_query_for_insert_into_output_data(target_table, meta_data)
        rows = self.execute_sql(find_subset_query)
        for to_be_inserted in rows:
            conditions = []
            for field, value in zip(self.column_mappings_rated_lists['primary'], to_be_inserted[:len(self.column_mappings_rated_lists['primary'])]):
                    if value == None or value == '':
                        conditions.append(f"({field} = '' OR {field} IS NULL)")
                    else:
                        conditions.append(f"({field} = '{value}' OR {field} IS NULL)")
            additional_module_conditions = self.condition_select_for_subset_updates_input_into_output()
            query = f"SELECT id FROM {target_table} WHERE {' AND '.join([ '?' for _ in conditions])} {'?' if additional_module_conditions else ''};"
            if additional_module_conditions:
                conditions = conditions + additional_module_conditions 
            results_matches = self.execute_sql(query,conditions)
            if results_matches:
                update_columns = ''
                for field, value in zip(self.column_mappings_rated_lists['primary'] + self.column_mappings_rated_lists['secondary'] + list(meta_data.keys()), to_be_inserted):
                    if field in ['time_created']:
                        continue
                    if field == 'OSINTsource':
                        existing_osintsource = value.copy().split(",")
                        if self.name not in existing_osintsource:
                            value = existing_osintsource + self.name
                    update_columns += f"{field} = '{value}', "
                update_query = f""" UPDATE {target_table}
                    SET {update_columns.rstrip(', ')}
                    WHERE id = ?;
                """
                self.execute_sql(update_query,results_matches[0][0])


    '''
    Regular insert for the target_table.
    '''
    def _insert_input_into_output(self,target_table, meta_data):
        insert_query = self._get_insert_query_for_insert_into_output_data(target_table,meta_data)
        self.execute_sql(insert_query)



    '''
    Is called to check if same data existis in output target_table but the meta_data like OSINTsource is not listed
    ''' 
    def _meta_data_update_input_into_output(self,target_table, meta_data):
        update_query = f""" UPDATE {target_table}
            SET time_modified = ?, OSINTsource = ?
            WHERE id = ?;
        """
        meta_data_update_query = self._get_update_meta_data_query_for_insert_into_output_data(target_table, meta_data)
        rows_same = self.execute_sql(meta_data_update_query)
        for row_id, input_time_modified, existing_time_modified, existing_osintsource in rows_same:
            module_name = self.name
            if existing_osintsource:
                existing_osintsource = existing_osintsource.split(",")
                if module_name not in existing_osintsource:
                    updated_osintsource = existing_osintsource + [module_name]
                else:
                    updated_osintsource = existing_osintsource
            else:
                # If the row is new, set OSINTsource to the module name
                updated_osintsource = [module_name]
            # Execute the update query with timestamp, OSINTsource, and status
            self.execute_sql(update_query, (input_time_modified, ",".join(updated_osintsource), row_id))
    

    def _get_update_meta_data_query_for_insert_into_output_data(self,target_table, meta_data):
        select_query_same = f"""SELECT id, t1tm, t2tm, OSINTsource             
            FROM ( 
                SELECT t2.id, t1.time_modified AS t1tm, t2.time_modified AS t2tm, t2.OSINTsource,
                    ROW_NUMBER() OVER (PARTITION BY {', '.join([f't1.{col}' for col in self.column_mappings_rated_lists['primary']])} ORDER BY t1.time_modified DESC) AS RowNum
                FROM {self.tablename} AS t1
                LEFT JOIN {target_table} AS t2 ON {' AND '.join([f't1.{col} = t2.{col}' for col in self.column_mappings_rated_lists['primary']])}
                WHERE t2.id IS NOT NULL AND OSINTsource NOT LIKE '%{self.name}%'
            )
            WHERE RowNum = 1;
        """
        return select_query_same
    
    def _get_find_subset_update_query_for_insert_into_output_data(self,target_table, meta_data):
        secondary_values_key_conditions_different = ' OR '.join([f't2.{col} IS NULL' for col in self.column_mappings_rated_lists['secondary']])
        
        insert_columns = ', '.join(f'{col}' for col in self.column_mappings_rated_lists_primary_secondary_chained)
        insert_columns_t1 = ', '.join(f't1.{col}' for col in self.column_mappings_rated_lists_primary_secondary_chained)

        select_query_different = f"""
            SELECT {insert_columns}    
            FROM ( 
                SELECT {insert_columns_t1}, ROW_NUMBER() OVER (PARTITION BY {', '.join([f't1.{col}' for col in self.column_mappings_rated_lists['primary']])}  ORDER BY t1.time_modified DESC) AS RowNum
                FROM {self.tablename} AS t1
                LEFT JOIN {target_table} AS t2 ON {' AND '.join([f't1.{col} = t2.{col}' for col in self.column_mappings_rated_lists['primary']])}
                WHERE t2.id is NULL
            ) AS t1
            WHERE RowNum = 1;
        """
        return select_query_different
    
    def _get_insert_query_for_insert_into_output_data(self,target_table, meta_data):
        secondary_values_key_conditions_different = ' OR '.join([f't2.{col} IS NULL' for col in self.column_mappings_rated_lists['secondary']])
        insert_columns = ', '.join(f'{col}' for col in self.column_mappings_rated_lists_primary_secondary_chained)
        insert_columns_t1 = ', '.join(f't1.{col}' for col in self.column_mappings_rated_lists_primary_secondary_chained)

        select_query_different = f"""
            INSERT INTO {target_table} ({insert_columns + ", OSINTsource, " + ', '.join([f"'{col}'" for col in list(meta_data.keys())])})
                SELECT {insert_columns_t1 + ", '" + self.name +"', " + ', '.join([f"'{meta_data[col]}'" for col in list(meta_data.keys())])}    
                FROM ( 
                    SELECT {insert_columns_t1}, ROW_NUMBER() OVER (PARTITION BY {', '.join([f't1.{col}' for col in self.column_mappings_rated_lists['primary']])}  ORDER BY t1.time_modified DESC) AS RowNum
                    FROM {self.tablename} AS t1
                    LEFT JOIN {target_table} AS t2 ON {' AND '.join([f't1.{col} = t2.{col}' for col in self.column_mappings_rated_lists['primary']])}
                    WHERE t2.id is NULL
                ) AS t1
                WHERE RowNum = 1;
        """
        return select_query_different


    '''
    Takes API data from the input source and inserts it in its own input table
    '''
    @func_call_logger(log_level=logging.DEBUG)
    def insert_input_data(self, input_objs):
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
                    # Replace None values with ''
                    obj = {k: '' if v is None else v for k, v in obj.items()}
                    select_dict = self._get_insertion_data_subset_by_column_mapping_filter(obj, self.column_mappings_rated_lists_without_meta)
                    insert_dict = self._reorder_insertion_data(obj, insertion_columns)

                    # Check if the record exists
                    result = self.db.conn.execute(select_query, list(select_dict.values())).fetchone()

                    if not result:
                        # If the record does not exist, perform the INSERT
                        self.db.conn.execute(insert_query, list(insert_dict.values()))
                    else:
                        # If the record exists, perform the UPDATE
                        self.db.conn.execute(update_query, (obj.get("time_modified", ""), obj.get("scope_status", ""), result[0]))

        except sqlite3.Error as e:
            app_logger.error(f"Error: {e}")
            # Rollback in case of an error
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
        # Separate primary and secondary keys
        primary_keys = tuple(key for key, (_, label, _) in  self.column_mapping.items() if label == 'primary')
        secondary_keys = tuple(key for key, (_, label, _) in  self.column_mapping.items() if label == 'secondary')

        # Create the ColumnMappingObject
        return {'primary': primary_keys, 'secondary': secondary_keys}

    def check_if_primary_record_exists(self, *args):
       
        # Ensure that the number of arguments matches the number of primary columns
        if len(args) != len(self.self.column_mappings_rated_lists_rated_lists['primary']):
            raise ValueError("Number of arguments doesn't match the number of primary columns.")
        values = list(args)
        cursor = self.db.conn.cursor()
        where_clause = ' AND '.join([f"{col} = ?" for col in self.self.column_mappings_rated_lists_rated_lists['primary']])
        row = self.execute_sql(f'''SELECT id 
                    FROM {self.tablename} 
                    WHERE {where_clause}''', tuple(values))
        if row:
            return row[0][0]
        else:
            return False
        
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
            # Log the exception and any additional details
            app_logger.logging.error(f"Error executing SQL query: {query}")
            app_logger.logging.error(f"Error details: {str(e)}")
            result = None
        finally:
            cursor.close()

        return result

    @func_call_logger(log_level=logging.INFO)
    def _create_table(self):
        table_columns = ', '.join([f"{col} {data_type}" for col, (data_type, _ , _) in self.column_mapping.items()])
        table_sql = f'CREATE TABLE IF NOT EXISTS {self.tablename} ({table_columns})'
        self.execute_sql(table_sql)

    @func_call_logger(log_level=logging.DEBUG)
    def activate_scope(self, row_id):
        self.execute_sql(f'''UPDATE {self.tablename} SET scope_status = 'in' WHERE id = ?''', (row_id,))

    @func_call_logger(log_level=logging.DEBUG)
    def deactivate_scope(self, row_id):
        self.execute_sql(f'''UPDATE {self.tablename} SET scope_status = 'out' WHERE id = ?''', (row_id,))

    '''Scope'''
    @func_call_logger(log_level=logging.INFO)
    def scope_receive(self, message):
        cursor = self.db.conn.cursor()
        columns = ', '.join(f'{col}' for col in self.column_mappings_rated_lists["primary"])
        cursor.execute(f'''SELECT id, {columns} FROM {self.tablename}''')
        rows = cursor.fetchall()
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

            # Batch update for activation
        if to_activate:
            self.batch_update_scope_status(to_activate, 'in')

        # Batch update for deactivation
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
        # Perform a batch update of scope status for the provided row IDs
        for chunk_ids in zip_longest(*[iter(row_ids)] * chunk_size, fillvalue=None):
            # Remove any None values from the chunk
            chunk_ids = [id for id in chunk_ids if id is not None]

            placeholders = ",".join(["?" for _ in chunk_ids])
            update_query = f"UPDATE {self.tablename} SET scope_status = ? WHERE id IN ({placeholders})"
            
            # Append the status to the beginning of the chunk
            parameters = [status] + chunk_ids
            try:
                self.execute_sql(update_query, parameters)
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
        domain, tld = datautils.extract_domain_and_tld(subdomain)
        search_query = f'''
            SELECT 1
            FROM {self.tablename}
            WHERE domain = ?
            LIMIT 1
        '''
        result = self.execute_sql(search_query, domain + '.' + tld)
        if result:
            return True
        else:
            return False

    def condition_select_for_subset_updates_input_into_output(self):
        """
        Makes sure that besides the primary dataset a suited entry for completion is found
        """
        return None



