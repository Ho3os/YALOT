import requests
import datetime
from  utils.applogger import app_logger
from  utils.applogger import func_call_logger
from urllib.parse import urlparse
import os
from modules.input.basedatasources import BaseDataSources
from  utils.applogger import app_logger
from  utils.applogger import func_call_logger
from utils import datautils
import logging

#todo find a way to merge domains from both columns

class Certsh(BaseDataSources):
    """description of class"""
    def __init__(self,db,scope, name = "crtsh", api_key = '', timethreshold_refresh_in_days=365):
        self.column_mapping = {
            'id': ('INTEGER PRIMARY KEY', 'id','meta'),
            'time_created': ('TEXT', '','meta'),
            'time_modified': ('TEXT', '','meta'),
            'scope_status': ('TEXT', '','meta'),
            'domain': ('TEXT', 'primary',''),
            'cert_issuer_ca_id': ('INTEGER', '',''),
            'cert_issuer': ('TEXT', 'secondary',''),
            'name_value': ('TEXT', '',''),
            'entry_timestamp': ('TEXT', '',''),
            'cert_issued': ('TEXT', 'secondary',''),
            'cert_expires': ('TEXT', 'secondary',''),
            'serial_number': ('TEXT', '',''),
            'crtsh_id': ('TEXT', '',''),
        }
        super().__init__(db, scope, name, self.column_mapping)
        self.api_prot = "https://"
        self.BASE_URL = "crt.sh/"
        self.URL_IDENTITY = "?output=json&q="
        self.api = api_key
        self.timethreshold_refresh_in_days = timethreshold_refresh_in_days  # not in use
        

    @func_call_logger(log_level=logging.INFO)
    def run(self):
        self.search_based_on_scope()
        #self.search_based_on_collection()
        #self.update_collection()


    '''
    Receiver function from parent class, which is called if new data from an output table can be used to query new data. Make sure that the column corresponds to the correct primary value
    '''
    def receiver_search_by_primary_values(self,rows,originating_output_table_name):
        result_set = set(row[0] for row in rows)
        for result in result_set:
            if result and not self._check_existing_domain(result) and self.scope.check_domain_in_scope(result):
                resp = self.search_domain(result)
                if resp:
                    self.insert_input_data(resp)            

    #time is missing # AND time_modified < ?
    def search_based_on_scope(self,timethreshold_refresh_in_days=365):
        for scope_item in self.scope.get_scope("Domain"):
            cursor = self.db.conn.cursor()
            cursor.execute('''
                SELECT domain, time_modified FROM crtsh WHERE domain LIKE  ? 
            ''', ('%' + scope_item['scope_value'],))
            row = cursor.fetchone()
            if not row:
                resp = self.search_domain(scope_item['scope_value'])
                if resp:
                    self.insert_input_data(resp)
            self.db.conn.commit()
            cursor.close()
        self.update_collection()

   
    def search_domain(self,domain_name):
        url = self.api_prot + self.BASE_URL + self.URL_IDENTITY + domain_name
        resp = self.safe_request(url)
        return resp


    def safe_request(self,url):
        headers = {
            'User-Agent': 'YourCustomUserAgent/1.0'
        }
        if datautils.DEBUG:
            response = datautils.read_cache_url(url,self.name)
            if response:
                return response
        try:
            response = requests.get(url,headers=headers)
            response.raise_for_status()
            if datautils.DEBUG:
                parsed_url = urlparse(url)
                file_name = os.path.basename(parsed_url.query.split('=')[-1])
                datautils.write_cache_url(file_name, self.name,response)
            return response.json()
        except requests.exceptions.RequestException as e:
            app_logger.error(f"Request error: {e}")
            return None
        except Exception as e:
            app_logger.error(f"Error: {e}")
            return None

    def _get_json_api_values(self, record):
            return {
                'domain': record.get('common_name', ""),
                'cert_issuer_ca_id': record.get('issuer_ca_id', ""),
                'cert_issuer': record.get('issuer_name', ""),
                'name_value': record.get('name_value', ""),
                'entry_timestamp': record.get('entry_timestamp', ""),
                'cert_issued': record.get('not_before', ""),
                'cert_expires': record.get('not_after', ""),
                'serial_number': record.get('serial_number', ""),
                'crtsh_id': record.get('id', "")
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
            name_values = json_obj.get('name_value', '').split('\n')
            if len(name_values) == 0:
                result.append(insertion_data.copy())
            else:
                for domain in name_values:
                    insertion_data.update({'domain': str(domain)})
                    result.append(insertion_data.copy())
            if 'pasx-exta' in name_values:
                pass 
        return result
    






    '''
    Overwritten to address MAX(certsh_id) to keep only one entry in select input
    Overwrite if domain does not exists or if secoundary values of primary group have a NULL
    '''
    def _get_update_meta_data_query_for_insert_into_output_data(self,target_table, meta_data):
        select_query_same = f"""SELECT id, t1tm, t2tm, OSINTsource             
            FROM ( 
                SELECT t2.id, t1.time_modified AS t1tm, t2.time_modified AS t2tm, t2.OSINTsource,
                    ROW_NUMBER() OVER (PARTITION BY {', '.join([f't1.{col}' for col in self.column_mappings_rated_lists['primary']])} ORDER BY t1.crtsh_id DESC) AS RowNum
                FROM {self.tablename} AS t1
                LEFT JOIN {target_table} AS t2 ON {' AND '.join([f't1.{col} = t2.{col}' for col in self.column_mappings_rated_lists['primary']])}
                WHERE t2.id IS NOT NULL AND OSINTsource NOT LIKE '%{self.name}%'
            )
            WHERE RowNum = 1;
        """
        return select_query_same
    
    '''
    Overwritten to address MAX(certsh_id) to keep only one entry in select input
    Overwrite if domain does not exists or if secoundary values of primary group have a NULL
    '''
    def _get_find_subset_update_query_for_insert_into_output_data(self,target_table, meta_data):
        secondary_values_key_conditions_different = ' OR '.join([f't2.{col} IS NULL' for col in self.column_mappings_rated_lists['secondary']])
        insert_columns = ', '.join(f'{col}' for col in self.column_mappings_rated_lists_primary_secondary_chained)
        insert_columns_t1 = ', '.join(f't1.{col}' for col in self.column_mappings_rated_lists_primary_secondary_chained)

        select_query_different = f"""
            SELECT {insert_columns}       
            FROM ( 
                SELECT {insert_columns_t1}, ROW_NUMBER() OVER (PARTITION BY {', '.join([f't1.{col}' for col in self.column_mappings_rated_lists['primary']])}  ORDER BY t1.crtsh_id DESC) AS RowNum
                FROM {self.tablename} AS t1
                LEFT JOIN {target_table} AS t2 ON {' AND '.join([f't1.{col} = t2.{col}' for col in self.column_mappings_rated_lists['primary']])}
                WHERE {secondary_values_key_conditions_different}
            ) AS t1
            WHERE RowNum = 1;
        """
        return select_query_different
    
    '''
    Overwritten to address MAX(certsh_id) to keep only one entry in select input
    Overwrite if domain does not exists or if secoundary values of primary group have a NULL
    '''
    def _get_insert_query_for_insert_into_output_data(self,target_table, meta_data):
        secondary_values_key_conditions_different = ' OR '.join([f't2.{col} IS NULL' for col in self.column_mappings_rated_lists['secondary']])
        insert_columns = ', '.join(f'{col}' for col in self.column_mappings_rated_lists['primary'] + self.column_mappings_rated_lists['secondary'])
        insert_columns_t1 = ', '.join(f't1.{col}' for col in self.column_mappings_rated_lists_primary_secondary_chained)

        select_query_different = f"""
            INSERT INTO {target_table} ({insert_columns + ", OSINTsource, " + ', '.join([f"'{col}'" for col in list(meta_data.keys())])})
                SELECT {insert_columns_t1 +  ", '" + self.name +"', " + ', '.join([f"'{meta_data[col]}'" for col in list(meta_data.keys())])}    
                FROM ( 
                    SELECT {insert_columns_t1}, ROW_NUMBER() OVER (PARTITION BY {', '.join([f't1.{col}' for col in self.column_mappings_rated_lists['primary']])}  ORDER BY t1.crtsh_id DESC) AS RowNum
                    FROM {self.tablename} AS t1
                    LEFT JOIN {target_table} AS t2 ON {' AND '.join([f't1.{col} = t2.{col}' for col in self.column_mappings_rated_lists['primary']])}
                    WHERE {secondary_values_key_conditions_different}
                ) AS t1
                WHERE RowNum = 1;
        """
        return select_query_different



