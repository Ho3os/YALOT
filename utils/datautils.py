import re
import sqlite3
import sys
import ipaddress
import tldextract
import os
from urllib.parse import urlparse
import json
import logging
import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
from utils.applogger import app_logger, func_call_logger
from utils.configmanager import ConfigManager
from utils.database import OSINTDatabase

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

def is_output_module_config_present(module_name: str) -> bool:
    config: Dict[str, Any] = get_config()
    return module_name in config.get("modules", {}).get("output", {})

def is_input_module_config_present(module_name: str) -> bool:
    config: Dict[str, Any] = get_config()
    return module_name in config.get("modules", {}).get("input", {})

def set_aggression(level: int) -> None:
    config: Dict[str, Any] = ConfigManager.get_config()
    config["AGGRESSIVE_SCANS"] = (level != 1)
    ConfigManager.set_config(config)

def set_cache(flag: str, foldername: str = 'cache') -> None:
    config: Dict[str, Any] = ConfigManager.get_config()
    config["CACHE_FOLDER_NAME"] = foldername
    config["CACHING"] = flag
    ConfigManager.set_config(config)

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

@func_call_logger(log_level=logging.INFO)
def dump_all_tables_to_csv(db: 'OSINTDatabase') -> None:
    cursor: sqlite3.Cursor = db.conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables: List[Tuple[str]] = cursor.fetchall()
    for table in tables:
        table_name: str = table[0]
        csv_file: str = create_two_folder_dynamic_file_path('export', 'database_dumps', f'{datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")}_{table_name}.csv')
        cursor.execute(f"SELECT * FROM {table_name}")
        rows: List[Tuple[Any]] = cursor.fetchall()
        with open(csv_file, 'w', encoding='utf-8', newline='') as csvfile:
            # Write header
            header: str = '-_-_-'.join([description[0] for description in cursor.description]) + '\n'
            csvfile.write(header)
            for row in rows:
                row_str: str = '-_-_-'.join(map(str, row)).replace('\n', '<newl>') + '\n'
                csvfile.write(row_str)
    db.conn.close()

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


__all__: List[str] = [name for name in globals() if not name.startswith("__")]
