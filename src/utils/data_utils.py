import re
import sqlite3
import sys
import ipaddress
import tldextract
import os
import time
from urllib.parse import urlparse
import json
import logging
import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
from src.utils.app_logger import app_logger, func_call_logger
from src.utils.config_controller import ConfigManager
from src.utils.database import OSINTDatabase
import csv
from src.modules.instance_manager import InstanceManager

pattern: re.Pattern = re.compile(
    r'^(([a-zA-Z]{1})|([a-zA-Z]{1}[a-zA-Z]{1})|'
    r'([a-zA-Z]{1}[0-9]{1})|([0-9]{1}[a-zA-Z]{1})|'
    r'([a-zA-Z0-9][-_.a-zA-Z0-9]{0,61}[a-zA-Z0-9]))\.'
    r'([a-zA-Z]{2,13}|[a-zA-Z0-9-]{2,30}.[a-zA-Z]{2,3})$'
)



def check_ip_version(ip_address: str) -> str:
    try:
        ip: Union[ipaddress.IPv4Address, ipaddress.IPv6Address] = ipaddress.ip_address(ip_address)
        if isinstance(ip, ipaddress.IPv4Address):
            return "IPv4"
        elif isinstance(ip, ipaddress.IPv6Address):
            return "IPv6"
        else:
            return "Unknown"
    except ValueError:
        return "Invalid IP address"

def get_config() -> Dict[str, str]:
    return ConfigManager().get_config()

def is_in_list(element_list,element):
        try:
            return element_list.index(element)
        except ValueError:
            return False


def is_output_module_config_present(module_name: str) -> bool:
    config: Dict[str, Any] = get_config()
    return module_name in config.get("modules", {}).get("output", {})

def is_input_module_config_present(module_name: str) -> bool:
    config: Dict[str, Any] = get_config()
    return module_name in config.get("modules", {}).get("input", {})


def validate_domain(value: str) -> str | None:
    return pattern.match(value)

def is_ip_in_subnet(ip_address: str, subnet: str) -> bool:
    try:
        ip_address_obj: ipaddress.IPv4Address = ipaddress.ip_address(ip_address)
        subnet_obj: ipaddress.IPv4Network = ipaddress.ip_network(subnet, strict=False)
        return ip_address_obj in subnet_obj
    except ValueError:
        return False

def get_subnet_ips(subnet: str) -> List[str]:
    ips: List[str] = []
    try:
        network: ipaddress.IPv4Network = ipaddress.IPv4Network(subnet, strict=False)
        for ip in network:
            ips.append(str(ip))
    except ipaddress.AddressValueError as e:
        app_logger.error(f"Error: {e}")
    return ips

def extract_domain_and_tld(url: str) -> Tuple[Optional[str], Optional[str]]:
    extracted: tldextract.ExtractResult = tldextract.extract(url)
    return extracted.domain, extracted.suffix

def create_two_folder_dynamic_file_path(first_level: str, second_level: str, file_name: str = '') -> str:
    base_folder: str = os.path.join(os.getcwd(), first_level)
    full_folder_path: str = os.path.join(base_folder, second_level)
    if not os.path.exists(full_folder_path):
        os.makedirs(full_folder_path)
    return os.path.join(full_folder_path, file_name)



@func_call_logger(log_level=logging.INFO)
def dump_all_tables_to_csv(db: 'OSINTDatabase') -> None:
    cursor: sqlite3.Cursor = db.conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables: List[Tuple[str]] = cursor.fetchall()
    for table in tables:
        table_name: str = table[0]
        cursor.execute(f"SELECT * FROM {table_name}")
        rows: List[Tuple[str]] = remove_newlines_from_fields(cursor.fetchall())
        header = []
        for description in cursor.description:
            header.append(description[0])
        csv_path, _ = create_dump_path(table_name)
        write_csv(csv_path, header,rows)

def create_dump_path(table_name):
    csv_file: str = create_two_folder_dynamic_file_path('export', 'database_dumps', 
                f'{datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")}_{table_name}.csv')
    json_file: str = create_two_folder_dynamic_file_path('export', 'database_dumps', 
                f'{datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")}_{table_name}_mapping.json')
    return csv_file, json_file

def create_header_from_keys(column_mapping):
    return list(column_mapping)

@func_call_logger(log_level=logging.INFO) 
def dump_all_module_tables_to_csv(include_mapping_export=True) -> None:
    instances = InstanceManager.get_all_instances()
    for instance in instances.values():
        dump_table_to_csv(instance["instance"])

def dump_module_table(instance_name: Optional[str] = None,include_mapping_export=True) -> None:
    if instance_name:
        retrieved_instance = InstanceManager.get_input_instance(instance_name)
        if retrieved_instance: 
            dump_table_to_csv(retrieved_instance)
        if not retrieved_instance:
            retrieved_instance = InstanceManager.get_output_instance(instance_name)
            if retrieved_instance: 
                dump_table_to_csv(retrieved_instance)
    else:
        app_logger.error("Export error in dump_module_table: No instance {instance_name} found.")

@func_call_logger(log_level=logging.DEBUG)
def dump_table_to_csv(instance):
    cursor: sqlite3.Cursor = instance.db.conn.cursor()
    cursor.execute(f"SELECT * FROM {instance.tablename};")
    rows: List[Tuple[str]] = remove_newlines_from_fields(cursor.fetchall())
    csv_file_path, mapping_file_path = create_dump_path(instance.tablename)
    write_csv(csv_file_path,create_header_from_keys(instance.column_mapping),rows,instance)
    write_json_mapping(mapping_file_path, instance)
    


def write_json_mapping(mapping_file_path, instance):
    try:
        directory = os.path.dirname(mapping_file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)  
        with open(mapping_file_path, 'w') as json_file:
            dict_string = {
                "name": instance.name,
                "tablename": instance.tablename,
                "column_mapping": instance.column_mapping
            }
            json_file.write(json.dumps(dict_string, indent=4))
            app_logger.info(f"Data mapping specification of {instance.name} written to {mapping_file_path}.")
    except FileNotFoundError:
        app_logger.error(f"Error: The specified file or directory could not be found.")
    except PermissionError:
        app_logger.error(f"Error: Permission denied. Cannot write to the specified location.")
    except OSError as e:
        app_logger.error(f"OS error: {e}")
    except Exception as e:
        app_logger.error(f"An unexpected error occurred: {e}")

def remove_newlines_from_fields(data_rows):
    cleaned_data_rows = []
    for row in data_rows:
        cleaned_row = [str(field).replace('\n', ' ').replace('\r', ' ') for field in row]
        cleaned_data_rows.append(cleaned_row)
    return cleaned_data_rows

def write_csv(csv_file_path, header, data_rows, instance='table'):
    try:
        directory = os.path.dirname(csv_file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(csv_file_path, 'w', encoding='utf-8', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow(header)
            for row in data_rows:
                writer.writerow(row)  
        if  hasattr(instance, 'name'): 
            app_logger.info(f"Data from csv of {instance.name} written to {csv_file_path}.")
        elif isinstance(instance, str):
             app_logger.info(f"Data from csv of {instance} written to {csv_file_path}.")
        else:
            app_logger.error(f"Something was written to {csv_file_path}. This should not happen. Validate the output carefully!")
    except FileNotFoundError:
        app_logger.error(f"Error: The specified file or directory could not be found.")
    except PermissionError:
        app_logger.error(f"Error: Permission denied. Cannot write to the specified location.")
    except OSError as e:
        app_logger.error(f"OS error: {e}")
    except Exception as e:
        app_logger.error(f"An unexpected error occurred: {e}")

@func_call_logger(log_level=logging.INFO)
def create_path_if_not_exists(path):
    """
    Check if the given path exists and create it if it doesn't exist.

    Args:
    - path (str): The path to check and create if necessary.

    Returns:
    - bool: True if the path already exists or was successfully created, False otherwise.
    """
    if not os.path.exists(path):
        try:
            os.makedirs(path)
            app_logger.info(f"Created directory: {path}")
            return True
        except OSError as e:
            app_logger.error(f"Error creating directory: {path} - {e}")
            return False
    else:
        app_logger.info(f"Directory already exists: {path}")
        return True



"""
Depricated

def write_cache_dns(file_name: str, folder_name: str, response: Union[str, Dict[str, Any]]) -> None:
    config: Dict[str, Any] = get_config()
    file_path: str = create_two_folder_dynamic_file_path(config.get("CACHE_FOLDER_NAME", ""), folder_name, file_name.replace('*', '_wildcard_'))
    with open(file_path, 'w') as file:
        json.dump(response, file, indent=2)
        app_logger.info(f"DNS Cache: {file_path} created successfully.")

def read_cache_dns(file_name: str, folder_name: str) -> Union[bool, Dict[str, Any]]:
    config: Dict[str, Any] = get_config()
    file_path: str = create_two_folder_dynamic_file_path(config.get("CACHE_FOLDER_NAME", ""), folder_name, file_name.replace('*', '_wildcard_'))
    try:
        with open(file_path, 'r') as file:
            cache_response: Union[str, Dict[str, Any]] = json.load(file)
            if isinstance(cache_response, dict) and config.get("CACHE_UNSUCCESSFUL_CONTENT_PATTERN") in cache_response:
                app_logger.info(f"URL Cache: {file_name} exists. Cache hit {config.get('CACHE_UNSUCCESSFUL_CONTENT_PATTERN')}.")
                return cache_response
            app_logger.info(f"DNS Cache: {file_name} exists.")
            return cache_response
    except FileNotFoundError:
        app_logger.error(f"FileNotFoundError: DNS File {file_path} does not exist.")
        return False

def write_cache_url(file_name: str, folder_name: str, response: Union[str, bytes]) -> None:
    config: Dict[str, Any] = get_config()
    file_path: str = create_two_folder_dynamic_file_path(config.get("CACHE_FOLDER_NAME", ""), folder_name, file_name)
    try:
        with open(file_path, 'wb') as file:
            if response == config.get("CACHE_UNSUCCESSFUL_CONTENT_PATTERN", "error"):
                file.write(config.get("CACHE_UNSUCCESSFUL_CONTENT_PATTERN", "error").encode('utf-8'))
            else:
                if hasattr(response, 'content'):
                    file.write(response.content)
                elif hasattr(response, '_content'): # this is for censys
                    file.write(response._content)
                else:
                    app_logger.error(f"Cannot handle {response} since content seems to be missing")
                    sys.exit(1)
            app_logger.info(f"URL Cache: {file_path} created successfully.")
    except FileNotFoundError:
        app_logger.error(f"FileNotFoundError: URL File {file_path} does not exist.")

def read_cache_url(url: str, folder_name: str) -> Union[bool, str, Dict[str, Any]]:
    config: Dict[str, Any] = get_config()
    try:
        parsed_url: urlparse = urlparse(url)
        if parsed_url.hostname == 'crt.sh':
            file_name: str = os.path.basename(parsed_url.query.split('=')[-1])
        else:
            file_name: str = os.path.basename(parsed_url.path.replace('/', '_').replace(':', '-'))
        file_path: str = create_two_folder_dynamic_file_path(config.get("CACHE_FOLDER_NAME", ""), folder_name, file_name)
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, 'r') as file:
                content: str = file.read().strip()
                if content.lower() == config.get("CACHE_UNSUCCESSFUL_CONTENT_PATTERN", "error"):
                    app_logger.info(f"URL Cache: {file_name} exists. Cache hit {config.get('CACHE_UNSUCCESSFUL_CONTENT_PATTERN', 'error')}.")
                    return config.get("CACHE_UNSUCCESSFUL_CONTENT_PATTERN", "error")
                else:
                    response: Union[str, Dict[str, Any]] = json.loads(content)
                    app_logger.info(f"URL Cache: {file_name} exists.")
                    return response
        else:
            app_logger.error(f"FileNotFoundError: URL File {file_path} does not exist.")
            return False
    except FileNotFoundError:
        app_logger.error(f"FileNotFoundError: URL File {file_path} does not exist.")
        return False

"""



__all__: List[str] = [name for name in globals() if not name.startswith("__")]
