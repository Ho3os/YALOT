import requests
import datetime
from  src.utils.app_logger import app_logger
from  src.utils.app_logger import func_call_logger
from urllib.parse import urlparse
import os
from src.modules.input.base_input_sources import BaseInputSources
from  src.utils.app_logger import app_logger
from  src.utils.app_logger import func_call_logger
from src.utils import data_utils
import logging
from src.utils.metadata_analysis import db_metadata_analysis_module
import json
import csv

'''
This class additionally need data_path and data_model_path to work.
'''
class ImportData(BaseInputSources):
    """description of class"""
    def __init__(self, general_handlers):
        self.data_path = general_handlers['data_path']
        self.data_model_path =  general_handlers['data_model_path']
        self.name = general_handlers['name']
        self.column_mapping = {}
        self.delimiter = ','
        self.check_availability_of_import_files()
        self._import_data_model()
        super().__init__(general_handlers, general_handlers['name'], self.column_mapping)
        

    @db_metadata_analysis_module()
    @func_call_logger(log_level=logging.INFO)
    def run(self):
        pass

    def check_availability_of_import_files(self):
        if not os.path.exists(self.data_model_path):
                raise FileNotFoundError(f"The file '{self.data_model_path}' does not exist.")
        if not os.path.exists(self.data_path):
                raise FileNotFoundError(f"The file '{self.data_path}' does not exist.")
        return True


    def _import_data_model(self):
        try:
            if not os.path.exists(self.data_model_path):
                raise FileNotFoundError(f"The file '{self.data_model_path}' does not exist.")
        
            with open(self.data_model_path, "r", encoding='utf-8') as file:
                data = json.load(file)
                if 'column_mapping' in data:
                    self.column_mapping = data['column_mapping']
                else:
                    app_logger.error("Import failed. No column mapping defined in config file")
                if 'name' in data:
                    self.name = self.name + '_' + data['name']
        except FileNotFoundError as e:
            app_logger.error(f"Error: {e}")
        except json.JSONDecodeError as e:
            app_logger.error(f"Error decoding JSON: {e}")
        except PermissionError as e:
            app_logger.error(f"Permission error: {e}")
        except Exception as e:
            app_logger.error(f"An unexpected error occurred: {e}")

    def import_data(self):
        try:
            if not os.path.exists(self.data_path):
                raise FileNotFoundError(f"The file '{self.data_path}' does not exist.")
            with open(self.data_path, "r", newline='', encoding='utf-8') as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=self.delimiter)
                header = next(csv_reader)
                for row in csv_reader: 
                    x = {header[i]: row[i] for i in range(len(header))}
                    self.insert_input_data(x)
        except FileNotFoundError as e:
            app_logger.error(f"Error: {e}")
        except csv.Error as e:
            app_logger.error(f"CSV error: {e}")
        except PermissionError as e:
            app_logger.error(f"Permission error: {e}")
        except Exception as e:
            app_logger.error(f"An unexpected error occurred: {e}")
       

    def prepare_input_insertion_data(self,json_objs):
         
        time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        base_insertion_data = {
            'time_created': time_now,
            'time_modified': time_now,
            'scope_status': 'TBD',
        }
        result = []
        for key,value in json_objs.items():
            if key in ('id','OSINTsource'):
                continue
            base_insertion_data.update({key: value})
        result.append(base_insertion_data)
        return result
    
    def receiver_search_by_primary_values(self):
        app_logger.error("Function receiver_search_by_primary_values should not have\
                          been called since import is not an iterative search.")
    




