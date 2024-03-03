import re
import ipaddress
import os
from urllib.parse import urlparse, urlsplit
import json
import logging
import datetime
from  utils.applogger import app_logger
from  utils.applogger import func_call_logger
import logging

DEBUG = True
AGGRESSIVE_SCANS = True
CACHE_FOLDER_NAME = 'cache'
CACHE_UNSUCCESSFUL_CONTENT_PATTERN = 'error' # do not change it since shodan web requests have a hardcoded error field when failing which logic bases on
CENSYS_SEARCH_QUOTA = 'strict' # only strict is implemented,unlimited searches for everything, #limited searches result orienteid #strict only searches in scope items.

pattern = re.compile(
    r'^(([a-zA-Z]{1})|([a-zA-Z]{1}[a-zA-Z]{1})|'
    r'([a-zA-Z]{1}[0-9]{1})|([0-9]{1}[a-zA-Z]{1})|'
    r'([a-zA-Z0-9][-_.a-zA-Z0-9]{0,61}[a-zA-Z0-9]))\.'
    r'([a-zA-Z]{2,13}|[a-zA-Z0-9-]{2,30}.[a-zA-Z]{2,3})$'
)

def set_aggression(level):
    if level == 1:
        AGGRESSIVE_SCANS = False
    else:
        AGGRESSIVE_SCANS = True
    return True

def set_cache(flag, foldername = 'cache'):
    CACHE_FOLDER_NAME = foldername
    DEBUG = flag


def validate_domain(value):
    return pattern.match(value)

def is_ip_in_subnet(ip_address, subnet):
    try:
        ip_address_obj = ipaddress.ip_address(ip_address)
        subnet_obj = ipaddress.ip_network(subnet, strict=False)
        return ip_address_obj in subnet_obj
    except ValueError as e:
        return False

def get_subnet_ips(subnet):
    ips = []
    try:
        network = ipaddress.IPv4Network(subnet, strict=False)
        for ip in network:
            ips.append(ip)
    except ipaddress.AddressValueError as e:
      app_logger.error(f"Error: {e}")
    return ips

def extract_domain_and_tld(url):
    domain_parts = url.split('.')
    tld = domain_parts[-1]
    sld = domain_parts[-2]
    return sld, tld


def create_two_folder_dynamic_file_path( first_level, second_level, file_name=''):
    base_folder = os.path.join(os.getcwd(), first_level)
    full_folder_path = os.path.join(base_folder, second_level)
    if not os.path.exists(full_folder_path):
        os.makedirs(full_folder_path)
    return os.path.join(full_folder_path, file_name)

def write_cache_dns(file_name, folder_name, response):
    file_path = create_two_folder_dynamic_file_path(CACHE_FOLDER_NAME, folder_name, file_name.replace('*', '_'))
    with open(file_path, 'w') as file:
        json.dump(response, file, indent=2)
        app_logger.info(f"DNS Cache: {file_path} created successfully.")


def read_cache_dns(file_name, folder_name):
    file_path = create_two_folder_dynamic_file_path(CACHE_FOLDER_NAME, folder_name, file_name.replace('*', '_'))
    try:
        with open(file_path, 'r') as file:
            cache_response = json.load(file)
            if isinstance(cache_response[0], dict): 
                if cache_response[0].get(CACHE_UNSUCCESSFUL_CONTENT_PATTERN) is not None :
                    app_logger.info("URL Cache: {file_name} exists. Cash hit {CACHE_UNSUCCESSFUL_CONTENT_PATTERN}.")
                    return  [cache_response[0].get(CACHE_UNSUCCESSFUL_CONTENT_PATTERN)]
            app_logger.info(f"DNS Cache: {file_name} exists.")
            return cache_response
    except FileNotFoundError:
        app_logger.error(f"FileNotFoundError: DNS File {file_path} does not exist.")
        return False

def write_cache_url(file_name,folder_name,response):
    file_path = create_two_folder_dynamic_file_path(CACHE_FOLDER_NAME, folder_name, file_name)
    try:
        with open(file_path, 'wb') as file:
            if response == CACHE_UNSUCCESSFUL_CONTENT_PATTERN:
                file.write(CACHE_UNSUCCESSFUL_CONTENT_PATTERN.encode('utf-8'))
            else:
                if '_content' in response: #this is for censys
                    file.write(response._content)
                else:
                    file.write(response.content)
            app_logger.info(f"URL Cache: {file_path} created successfully.")
    except FileNotFoundError:
        app_logger.error(f"FileNotFoundError: URL File {file_path} does not exist.")
        return False
    
def read_cache_url(url,folder_name):
    try:
        parsed_url = urlparse(url)
        if parsed_url.hostname == 'crt.sh':
            file_name = os.path.basename(parsed_url.query.split('=')[-1])
        else:
            file_name =  os.path.basename(parsed_url.path.replace('/', '_').replace(':', '-'))
        file_path = create_two_folder_dynamic_file_path(CACHE_FOLDER_NAME, folder_name, file_name)
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, 'r') as file:
                content = file.read().strip()
                if content.lower() == CACHE_UNSUCCESSFUL_CONTENT_PATTERN:
                    app_logger.info("URL Cache: {file_name} exists. Cash hit {CACHE_UNSUCCESSFUL_CONTENT_PATTERN}.")
                    return CACHE_UNSUCCESSFUL_CONTENT_PATTERN
                else:
                    response = json.loads(content)
                    app_logger.info(f"URL Cache: {file_name} exists.")
                    return response
        else:
            app_logger.error(f"FileNotFoundError: URL File {file_path} does not exist.")
            return False
    except FileNotFoundError:
        app_logger.error(f"FileNotFoundError: URL File {file_path} does not exist.")
        return False


@func_call_logger(log_level=logging.INFO)
def dump_all_tables_to_csv(db):
    cursor = db.conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for table in tables:
        table_name = table[0]
        csv_file = create_two_folder_dynamic_file_path('export','database_dumps',f'{datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")}_{table_name}.csv')
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        with open(csv_file, 'w', encoding='utf-8',newline='') as csvfile:
            # Write header
            header = '-_-_-'.join([description[0] for description in cursor.description]) + '\n'
            csvfile.write(header)
            for row in rows:
                row_str = '-_-_-'.join(map(str, row)).replace('\n', '<newl>') + '\n'
                csvfile.write(row_str)
    db.conn.close()


