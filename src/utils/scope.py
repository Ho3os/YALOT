import datetime
import ipaddress
import logging
import sys
import os

from typing import Any, Dict, List, Optional, Union

from  src.utils.app_logger import app_logger
from  src.utils.app_logger import func_call_logger
from src.utils import data_utils
from src.utils import robtex
from src.modules.instance_manager import InstanceManager
from src.utils.config_controller import ConfigManager

class Scope:
    VALID_STATUSES = {'active', 'inactive'}
    """description of class"""
    def __init__(self, db: Any, name: str = "scope") -> None:
        self.db = db
        self.create_database()
        self.name = name
        self.config = ConfigManager().get_config()
        self.scope_type = ["IP", "Domain", "Subnet", "ASN"]

    @func_call_logger(log_level=logging.INFO)
    def publish_update_scope(self, message: str = "",  instance_name: Optional[str] = None) -> None:
        retrieved_instance = []
        if instance_name:
            retrieved_instance = InstanceManager.get_input_instance(instance_name)
            if not retrieved_instance:
                retrieved_instance = InstanceManager.get_output_instance(instance_name)
        else:
            retrieved_instance = InstanceManager.get_input_instances()
            retrieved_instance.extend(InstanceManager.get_output_instances())
            
        if retrieved_instance: 
            for instance in retrieved_instance:
                instance.scope_receive(message)

    def insert_from_file(self, file_path: str = "") -> None:
        try:
            if file_path == "":
                file_path = os.path.join(self.config["SCOPE"]["SCOPE_DIRECTORY_PATH"], self.config["SCOPE"]["SCOPE_FILE_NAME"])
        except KeyError:
              app_logger.error(f"Config file configuration contains an error. Key error: {e}")
              sys.exit(1)
        try:
            with open(file_path, 'r') as file:
                lines = file.readlines()
                for line in lines:
                    entry = line.strip()
                    try:
                        ip_obj = ipaddress.ip_address(entry)
                        self.put_new_scope(self.create_scope_item('IP', str(ip_obj), 'active'), False)
                    except ValueError:
                        try:
                            subnet_obj = ipaddress.ip_network(entry, strict=False)
                            self.put_new_scope(self.create_scope_item('Subnet', str(subnet_obj), 'active'), False)
                        except ValueError:
                            self.put_new_scope(self.create_scope_item('Domain', entry, 'active'), False)
            self.publish_update_scope("New Scope")
        except FileNotFoundError:
            app_logger.error(f"FileNotFoundError: Scope file {file_path} does not exist.")
            sys.exit(1)
        except OSError as e:
            app_logger.error(f"OS error: {e}")
        except Exception as e:
            app_logger.error(f"An error occurred: {e}")

    def create_database(self) -> None:
        self.db.execute_sql('''
            CREATE TABLE IF NOT EXISTS scope (
                id INTEGER PRIMARY KEY,
                time_created TEXT,
                time_modified TEXT,
                scope_type TEXT,
                scope_value TEXT,
                scope_status TEXT,
                scope_source TEXT,
                scope_description TEXT
                )
            ''')

    def create_scope_item(self, scope_type: str, scope_value: str, scope_status: str = 'inactive', scope_source: str = '', scope_description: str = '') -> Dict[str, str]:
        return {"scope_type": scope_type, "scope_value": scope_value, "scope_status": scope_status, "scope_source": scope_source, "scope_description": scope_description}

    def strip_ASN_to_int_string(self, asn: str) -> str:
        if asn.startswith('AS'):
            return asn[2:]
        else:
            return asn

    def put_new_scope(self, scope: Dict[str, str], publish: bool = True) -> int:
        flag_scope_added = False
        if scope['scope_type'] not in self.scope_type:
            app_logger.error(f"Scope values are not set correctly: scope_type {scope['scope_type']}, scope_value {scope['scope_value']}")
            return -1
        
        if scope['scope_type'] == 'ASN':
            rt = robtex.Robtex()
            asn_nrs = rt.queryAS(self.strip_ASN_to_int_string(scope['scope_value']))
            if asn_nrs:
                for item in asn_nrs:
                    self._insert_new_scope(self.create_scope_item('Subnet', item["n"],  scope['scope_status'],  scope['scope_value']))
                    flag_scope_added = True
        elif scope['scope_type'] in ['Subnet', 'IP', 'Domain']:
            self._insert_new_scope(scope)
            flag_scope_added = True
        else:
            app_logger.error(f"Scope is not a valid type. Not inserted: {scope['scope_type']}")
            return -1
        if publish:
            self.publish_update_scope("New Scope")
        return flag_scope_added

    def _insert_new_scope(self, scope: Dict[str, str]) -> int:
        try:
            scope_type = scope['scope_type']
            scope_value = scope['scope_value']
            scope_status = scope['scope_status']
        except KeyError as e:
            app_logger.error(f"Missing key in scope dictionary: {e}")
            return -1
        
        existing_scope = self.db.execute_sql('''
            SELECT * FROM scope
            WHERE scope_type = ? AND scope_value = ?
        ''', (scope.get("scope_type"), scope.get("scope_value")))

        if existing_scope:
            app_logger.debug(f"Scope already exists: {existing_scope}")
            return -1

        time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        scope_description = scope.get('scope_description', '')
        scope_source = scope.get('scope_source', '')
        result = self.db.execute_sql('''
            INSERT INTO scope (time_created, time_modified, scope_type, scope_value, scope_status, scope_description, scope_source)  VALUES (?, ?, ?, ?, ?, ?, ?)''', (
                time,
                time,
                scope['scope_type'],
                scope['scope_value'],
                scope['scope_status'],
                scope_description,
                scope_source
            )
        )
        app_logger.debug(f"New scope inserted successfully: {scope['scope_value']}")
        return result

    def get_scope(self, scope_type: Optional[str] = None) -> List[Dict[str, Union[int, str]]]:
        if scope_type == "Domain":
            records = self.db.execute_sql('''SELECT id, time_created, time_modified, scope_type, scope_value, scope_status, scope_source, scope_description FROM scope WHERE scope_type = "Domain"''')
        else:
           records = self.db.execute_sql('''SELECT id, time_created, time_modified, scope_type, scope_value, scope_status, scope_source, scope_description FROM scope''')
        return [{'id': record[0], 'time_created': record[1], 'time_modified': record[2], 'scope_type': record[3],\
            'scope_value': record[4], 'scope_status': record[5], "scope_source": record[6], "scope_description": record[7]} for record in records]

    def set_status(self, scope_id: int, status: str = "inactive") -> None:
        if not isinstance(scope_id, int):
            raise TypeError(f"scope_id must be an integer, got {type(scope_id).__name__}")
        if status not in self.VALID_STATUSES:
            raise ValueError(f"Invalid status: {status}. Valid statuses are: {self.VALID_STATUSES}")
        result = self.db.execute_sql('''UPDATE scope SET scope_status = ? WHERE id = ?''', (status, scope_id))
        return result

    def check_domain_in_scope(self, domain: str) -> bool:
        query = f'''SELECT 1 FROM scope WHERE scope_type = 'Domain' AND scope_value LIKE ? '''
        results = self.db.execute_sql(query, ('%'+domain,))
        if results:
            return True
        else:
            return False

    def print_scope(self, input_list: List[Dict[str, str]]) -> None:
        for input_dict in input_list:
            for key, value in input_dict.items():
                print(f"{key}: {value}")
