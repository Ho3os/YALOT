import sqlite3
import os
import datetime
from  utils.applogger import app_logger
from  utils.applogger import func_call_logger
import logging

class OSINTDatabase:
    def __init__(self, db_name='Database.db'):
        self.conn = sqlite3.connect(db_name)

    @func_call_logger(log_level=logging.INFO)
    def delete_existing_database(self, db_name):
        if os.path.exists(db_name):
            os.remove(db_name)
            app_logger.info(f"Deleted existing database: {db_name}")
        else:
            app_logger.info(f"Database does not exist: {db_name}")

    @func_call_logger(log_level=logging.DEBUG)
    def replace_none_with_null(self,json_obj):
        if isinstance(json_obj, dict):
            return {key: self.replace_none_with_null(value) if value is not None else '' for key, value in json_obj.items()}
        elif isinstance(json_obj, list):
            return [self.replace_none_with_null(item) if item is not None else '' for item in json_obj]
        else:
            return json_obj




