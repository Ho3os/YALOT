import datetime
import ipaddress
import logging
from  utils.applogger import app_logger
from  utils.applogger import func_call_logger
from utils import robtex
from utils.instancemanager import InstanceManager

class Scope(object):
    """description of class"""
    def __init__(self, db, name = "scope"):
        self.db = db
        self.create_database()
        self.name = name


    @func_call_logger(log_level=logging.INFO)
    def publish_update_scope(self,message=""):
            # Access instances from other classes
        retrieved_instance = InstanceManager.get_input_instance()
        retrieved_instance.update(InstanceManager.get_output_instance())
        for instance in retrieved_instance.values():
            instance.scope_receive(message)

    @func_call_logger(log_level=logging.INFO)
    def publish_update_scope(self,instance_name,message=""):
            # Access instances from other classes
        retrieved_instance = InstanceManager.get_input_instance(instance_name)
        retrieved_instance.update(InstanceManager.get_output_instance(instance_name))
        for instance in retrieved_instance.values():
            instance.scope_receive(message)


    def insert_from_file(self,file_path):
        try:
            with open(file_path, 'r') as file:
                lines = file.readlines()
                for line in lines:
                    entry = line.strip()
                    try:
                        ip_obj = ipaddress.ip_address(entry)
                        # If it's an IP address, add it to the 'ips' list
                        #ips.append((str(ip_obj), 'IPv4' if ip_obj.version == 4 else 'IPv6'))
                        self.put_new_scope(self.create_scope_item('IP',str(ip_obj),'active'),False)
                    except ValueError:
                        # If it's not a valid IP, check if it's a subnet
                        try:
                            subnet_obj = ipaddress.ip_network(entry, strict=False)
                            # If it's a subnet, add it to the 'subnets' list
                            self.put_new_scope(self.create_scope_item('Subnet',str(subnet_obj),'active'),False)
                        except ValueError:
                            # If it's neither an IP nor a subnet, assume it's a domain
                            self.put_new_scope(self.create_scope_item('Domain',entry,'active'),False)
                        # Process each line as needed
            self.publish_update_scope("New Scope")
        except FileNotFoundError:
           app_logger.error(f"FileNotFoundError: Scope file {file_path} does not exist.")
        except Exception as e:
           app_logger.error(f"An error occurred: {e}")


    '''DB Columbus'''
    def create_database(self):
        cursor = self.db.conn.cursor()
        cursor.execute('''
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
        self.db.conn.commit()
        cursor.close()

    def create_scope_item(self,scope_type,scope_value,scope_status='inactive', scope_source = '', scope_description = ''):
        return {"scope_type": scope_type,"scope_value": scope_value,"scope_status": scope_status, "scope_source":scope_source, "scope_description":scope_description}

    def strip_ASN_to_int_string(self, asn):
        if asn.startswith('AS'):
            return asn[2:]
        else:
            return asn

    def put_new_scope(self,scope,publish=True):
        if scope['scope_type'] is None or scope['scope_value'] is None:
            app_logger.error("Scope values are not set correctly: scope_type " +  scope['scope_type'] + ", scope_value" +  scope['scope_value'])
            return -1
        
        if scope['scope_type'] == 'ASN':
            rt = robtex.Robtex()
            asn_nrs = rt.queryAS(self.strip_ASN_to_int_string(scope['scope_value']))
            if asn_nrs:
                for item in asn_nrs:
                    self._insert_new_scope(self.create_scope_item('Subnet', item["n"],  scope['scope_status'],  scope['scope_value']))
        elif scope['scope_type'] == 'Subnet':
            self._insert_new_scope(scope)
        elif scope['scope_type'] == 'IP':
            self._insert_new_scope(scope)
        elif scope['scope_type'] == 'Domain':
            self._insert_new_scope(scope)
        else:
            app_logger.error("Scope is not a valid type. Not inserted: " + scope['scope_type'])
            return -1
        if publish:
            self.publish_update_scope("New Scope")

    def _insert_new_scope(self,scope):
        cursor = self.db.conn.cursor()
        cursor.execute('''
            SELECT * FROM scope
            WHERE scope_type = ? AND scope_value = ?
        ''', (scope.get("scope_type"), scope.get("scope_value")))
        existing_scope = cursor.fetchone()

        if existing_scope:
            app_logger.debug("Scope already exists: " + existing_scope)
            return -1

        existing_scope = cursor.fetchone()
        time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''
            INSERT INTO scope (time_created, time_modified, scope_type, scope_value, scope_status, scope_description, scope_source)  VALUES (?, ?, ?, ?, ?, ?, ?)''', (
                time,
                time,
                scope['scope_type'],
                scope['scope_value'],
                scope['scope_status'],
                scope['scope_description'],
                scope['scope_source']
            )
        )
        self.db.conn.commit()
        cursor.close()
        app_logger.debug("New scope inserted successfully: " + scope['scope_value'])

    def get_scope(self, scope_type=None):
        cursor = self.db.conn.cursor()
        if scope_type == "Domain":
            cursor.execute('''SELECT id, time_created, time_modified, scope_type, scope_value, scope_status, scope_source, scope_description FROM scope WHERE scope_type = "Domain"''')
        else:
            cursor.execute('''SELECT id, time_created, time_modified, scope_type, scope_value, scope_status, scope_source, scope_description FROM scope''')
        records = cursor.fetchall()
        self.db.conn.commit()
        cursor.close()
        return [{'id': record[0], 'time_created': record[1], 'time_modified': record[2], 'scope_type': record[3],\
            'scope_value': record[4], 'scope_status': record[5], "scope_source":record[6], "scope_description":record[7]} for record in records]

    def set_status(self,scope_id,status="inactive"):
        if (status == "inactive" or status == "active"):
            app_logger.error("Status is not inactive or active: " + str(status))
            return -1
        if isinstance(scope_id, int) and scope_id > 0:
            app_logger.error("scope_id is not a int or above 0: " + str(scope_id))
            return -1
        cursor = self.cursor()
        cursor.execute('''UPDATE scope SET scope_status = ? WHERE id = ?''', (
            status,
            scope_id
            )
        )
        self.db.conn.commit()
        cursor.close()

    def check_domain_in_scope(self,type,domain):
        cursor = self.db.conn.cursor()
        query = f'''SELECT 1 FROM scope WHERE scope_type = 'Domain' AND scope_value LIKE ? '''
        result = cursor.execute(query, '%'+domain)
        if result:
            return True
        else:
            return False

    def print_scope(self,input_list):
        for input_dict in input_list:
            for key, value in input_dict.items():
                print(f"{key}: {value}")
