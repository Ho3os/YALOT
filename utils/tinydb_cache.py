import os
from tinydb import TinyDB, Query
from datetime import datetime
import re
from typing import Any, Dict, Union, List, Tuple
from utils import data_utils

class TinyDBCache:
    def __init__(self, cache_folder: str) -> None:
        self.cache_folder: str = cache_folder
        data_utils.create_path_if_not_exists(cache_folder)
        self.config = data_utils.get_config()
        self.separator: str = '|'
        self.db = None
        self.history_db = None
        self.create_db()
    
    def __exit__(self):
        self.db.close()
        self.history_db.close()

    def create_db(self):
        if "CACHING" in self.config and "USE_CACHING" in self.config["CACHING"] and self.config["CACHING"]["USE_CACHING"] and "CACHE_DIRECTORY_PATH" in self.config["CACHING"]:
            file_name_cache = self.config["CACHING"]["CACHE_FILE_NAME_CACHE"]
            file_name_history = self.config["CACHING"]["CACHE_FILE_NAME_HISTORY"]
            
            if "PREFIX_PROJECT_NAME_TO_CACHE_NAME" in self.config["CACHING"] and "PROJECT_NAME" in self.config and self.config["CACHING"]["PREFIX_PROJECT_NAME_TO_CACHE_NAME"]:
                file_name_cache = self.config["PROJECT_NAME"] + '_' + file_name_cache
            if "PREFIX_PROJECT_NAME_TO_HISTORY_NAME" in self.config["CACHING"] and "PROJECT_NAME" in self.config and self.config["CACHING"]["PREFIX_PROJECT_NAME_TO_HISTORY_NAME"]:
                file_name_history = self.config["PROJECT_NAME"] + '_' + file_name_history

            self.db: TinyDB = TinyDB(os.path.join(self.config["CACHING"]["CACHE_DIRECTORY_PATH"],file_name_cache))
            self.history_db: TinyDB = TinyDB(os.path.join(self.config["CACHING"]["CACHE_DIRECTORY_PATH"],file_name_history))
        else:
            raise ValueError("Database is not properly defined in config. Currently it is required")


    def _generate_identifier(self, module_query: str, module_name: str) -> str:
        encoded_module_query, encoded_module_name = self._sanitize_identifier(module_query, module_name)
        return f"{encoded_module_name}{self.separator}{encoded_module_query}"

    def _generate_historic_identifier(self, identifier: str, timestamp: str) -> str:
        return f"{identifier}{self.separator}{timestamp}"

    def _sanitize_identifier(self, module_query: str, module_name: str) -> Tuple[str, str]:
        encoded_query: str = module_query.replace(self.separator, '\\' + self.separator)
        encoded_module_name: str = module_name.replace(self.separator, '\\' + self.separator)
        return encoded_query, encoded_module_name

    def _split_identifier(self, identifier: str) -> Tuple[str, str]:
        module_query, module_name = identifier.split(self.separator, 1)
        return module_query.replace('\\' + self.separator, self.separator), module_name.replace('\\' + self.separator, self.separator)

    def get(self, identifier: str) -> Union[Dict[str, Any], None]:
        return self.db.get(Query().key == identifier)

    def set(self, identifier: str, value: Any) -> None:
        timestamp: str = datetime.now().isoformat()
        data: Dict[str, Any] = {
            'key': identifier,
            'value': value,
            'time_created': None,
            'time_modified': None,
            'time_archived': None
        }
        existing_entry: Union[Dict[str, Any], None] = self.get(identifier)
        if existing_entry:
            new_data: bool = self.compare_data(existing_entry['value'], value)
            if new_data:
                # Update history entry based on identifier
                self.history_db.update({'value': value, 'time_archived': timestamp,'time_modified': existing_entry['time_modified']}, 
                                    (Query().key == identifier) & (Query().value == existing_entry['value']))
                data['time_modified'] = timestamp
                data['time_created'] = existing_entry['time_created']
                self.db.update(data, Query().key == identifier)
                self._set_historic(identifier, data, timestamp) 
            else:
                self.db.update({'time_modified': timestamp}, Query().key == identifier)
                self.history_db.update({'time_modified': timestamp}, (Query().key == identifier) & (Query().value == existing_entry['value']))
        else:
            data['time_created'] = timestamp
            data['time_modified'] = timestamp
            self.db.insert(data) 
            self._set_historic(identifier, data, timestamp) 
            
    
    def _set_historic(self, identifier: str, data: Dict[str, Any], timestamp: str) -> None:
        historic_identifier: str = self._generate_historic_identifier(identifier, timestamp)
        data['key'] = historic_identifier
        self.history_db.insert(data)

    def compare_data(self, old_data: Any, new_data: Any) -> bool:
        return old_data != new_data

    def delete(self, identifier: str) -> None:
        self.db.remove(Query().key == identifier)

    def delete_starting_with_string(self, module_name: str) -> None:
        self.db.remove(Query().key.matches(f'^{re.escape(module_name)}'))

    def clear(self) -> None:
        self.db.truncate()

    def search_current_data(self, module_query: str) -> List[Dict[str, Any]]:
        return self.db.search( module_query)

    def search_history_data(self, module_query: str) -> List[Dict[str, Any]]:
        return self.history_db.search(module_query)
    
    def search_historic_data_by_identifier(self, identifier: str) -> List[Dict[str, Any]]:
        return self.history_db.search(Query().key.matches(f'^{re.escape(identifier)}'))
    
    def search_historic_by_identifier_and_timestamp(self, identifier: str, timestamp_key: str, timestamp_value: str) -> Union[Dict[str, Any], None]:
        return self.history_db.get((Query().key.matches(f'^{re.escape(identifier)}')) 
                                   & (Query()[timestamp_key] == timestamp_value))

    def search_historic_by_identifier_and_value(self, identifier: str, value: Any) -> Union[Dict[str, Any], None]:
        return self.history_db.get((Query().key.matches(f'^{re.escape(identifier)}')) 
                                    & (Query().value == value))
