import sqlite3
import os
import sys
import logging
from typing import Any, Dict, List, Union, Optional
from utils.applogger import app_logger, func_call_logger
from utils.configmanager import ConfigManager



class OSINTDatabase:
    def __init__(self, db_name: str = 'Database.db') -> None:
        self.db_name = db_name
        self.config: Dict[str, str] = ConfigManager().get_config()
        if "DB_PATH" in self.config:
            if not os.path.exists(self.config.get("DB_PATH", "")):
                os.makedirs(self.config.get("DB_PATH", ""))
        self.conn: Optional[sqlite3.Connection] = None
        self.connect()
        
    def connect(self):
        self.conn = sqlite3.connect(os.path.join(self.config.get("DB_PATH", ""), self.db_name))

    def check_and_open_conn(self):
        if not self.conn:
            self.connect()

    '''
    Wrapper for less code. Do not use if batching
    '''
    @func_call_logger(log_level=logging.DEBUG)
    def execute_sql(self, query, data=None):
        self.check_and_open_conn()
        cursor = self.conn.cursor()
        try:
            if data is None:
                cursor.execute(query)
            else:
                if isinstance(data[0], (list, tuple)):
                    cursor.executemany(query, data)
                else:
                    cursor.execute(query, data)

            result = cursor.fetchall()
            self.conn.commit()
        except Exception as e:
            app_logger.error(f"Error executing SQL query: {query}")
            app_logger.error(f"Error details: {str(e)}")
            sys.exit(1)
        finally:
            cursor.close()
        return result

    '''
    Wrapper for less code. Do not use if batching
    '''
    @func_call_logger(log_level=logging.DEBUG)
    def execute_sql_fetchone(self, query, data=None):
        cursor = self.conn.cursor()
        try:
            if data is None:
                cursor.execute(query)
            else:
                if isinstance(data[0], (list, tuple)):
                    cursor.executemany(query, data)
                else:
                    cursor.execute(query, data)

            result = cursor.fetchone()
            self.conn.commit()
        except Exception as e:
            app_logger.error(f"Error executing SQL query: {query}")
            app_logger.error(f"Error details: {str(e)}")
            sys.exit(1)
        finally:
            cursor.close()
        return result



    @func_call_logger(log_level=logging.DEBUG)
    def delete_existing_database(self, db_name: str) -> None:
        if os.path.exists(db_name):
            os.remove(db_name)
            app_logger.info(f"Deleted existing database: {db_name}")
        else:
            app_logger.info(f"Database does not exist: {db_name}")

    @func_call_logger(log_level=logging.DEBUG)
    def replace_none_with_null(self, json_obj: Any) -> Any:
        if isinstance(json_obj, dict):
            return {key: self.replace_none_with_null(value) if value is not None else '' for key, value in json_obj.items()}
        elif isinstance(json_obj, list):
            return [self.replace_none_with_null(item) if item is not None else '' for item in json_obj]
        else:
            return json_obj

    @func_call_logger(log_level=logging.DEBUG)
    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            app_logger.info("Database connection closed.")