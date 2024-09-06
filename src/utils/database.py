import sqlite3
import os
import sys
import logging
from typing import Any, Dict, List, Union, Optional
from src.utils.app_logger import app_logger, func_call_logger
from src.utils.config_controller import ConfigManager



class OSINTDatabase:
    def __init__(self, db_name: str = 'Database.db') -> None:
        self.db_name = db_name
        self.db_path = ""
        self.config: Dict[str, str] = ConfigManager().get_config()
        if "DATABASE" in self.config and "DATABASE_DIRECTORY_PATH" in self.config["DATABASE"]:
            self.db_path = self.config["DATABASE"].get("DATABASE_DIRECTORY_PATH", "")
            if not os.path.exists(self.db_path):
                os.makedirs(self.db_path)
        self.conn: Optional[sqlite3.Connection] = None
        self.abs_path_db = os.path.abspath(os.path.join(self.db_path, self.db_name))
        self.connect()

    def __exit__(self):
        self.conn.close()

    def delete_database_file(self):
        self.disconnect()
        self.delete_file(self.abs_path_db)

        
    def connect(self):
        self.conn = sqlite3.connect(self.abs_path_db)

    def disconnect(self):
        if self.conn:
            self.conn.close()

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
                elif isinstance(data, str):
                    cursor.execute(query, (data,))
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
    def delete_file(self,file_path):
        if os.path.exists(file_path):
            os.remove(file_path)
            app_logger.info(f"Deleted existing database: {file_path}")
        else:
            app_logger.info(f"Database does not exist: {file_path}")

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