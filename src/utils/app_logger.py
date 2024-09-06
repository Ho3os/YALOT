import logging
import logging.handlers
from functools import wraps
import sys
import os
from typing import Callable
from src.utils.config_controller import ConfigManager
#from src.utils.config_controller import ConfigManager


log_level_basic: int = logging.INFO
log_level_log_file: int = logging.DEBUG
log_level_console: int = logging.INFO
config = ConfigManager().get_config()
#if "PROJECT_NAME" in config:
#     log_file_name: str = config["PROJECT_NAME"]+'.log'
#else:
log_file_name: str = 'YALOT.log'
log_directory_name: str = 'log'

logging.getLogger().setLevel(logging.NOTSET)

console_handler: logging.StreamHandler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(log_level_console)
console_formatter: logging.Formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
console_handler.propagate = False 
logging.getLogger().addHandler(console_handler)

log_directory: str = os.path.join(os.getcwd(), log_directory_name)
if not os.path.exists(log_directory):
        os.makedirs(log_directory)
file_handler: logging.handlers.RotatingFileHandler = logging.handlers.RotatingFileHandler(filename=os.path.join(log_directory, log_file_name), maxBytes=1024 * 1024 * 1024, backupCount=10)
file_handler.setLevel(log_level_log_file)
formatter: logging.Formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
file_handler.propagate = False
logging.getLogger().addHandler(file_handler)

app_logger: logging.Logger = logging.getLogger(__name__)

def func_call_logger(log_level: int = logging.DEBUG) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            app_logger.log(log_level, f"Calling function {func.__name__} with args: {args}, kwargs: {kwargs}")
            result = func(*args, **kwargs)
            app_logger.log(log_level, f"Function {func.__name__} returned: {result}")
            return result
        return wrapper
    return decorator

def set_log_level_file(log_level: int) -> None:
    global log_level_log_file
    log_level_log_file = log_level
    file_handler.setLevel(log_level_log_file)
    app_logger.info(f"Log level for file handler set to {logging.getLevelName(log_level_log_file)}")

def set_log_level_console(log_level: int) -> None:
    global log_level_console
    log_level_console = log_level
    console_handler.setLevel(log_level_console)
    app_logger.info(f"Log level for console handler set to {logging.getLevelName(log_level_console)}")