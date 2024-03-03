import logging
import logging.handlers
import functools
import sys
import os


log_level_basic = logging.INFO
log_level_log_file = logging.DEBUG
log_level_console = logging.INFO
log_file_name = 'YALOT.log'
log_directory_name = 'log'



logging.getLogger().setLevel(logging.NOTSET)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(log_level_console)
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
console_handler.propagate = False 
logging.getLogger().addHandler(console_handler)


log_directory = os.path.join(os.getcwd(), log_directory_name)
if not os.path.exists(log_directory):
        os.makedirs(log_directory)
file_handler = logging.handlers.RotatingFileHandler(filename=os.path.join(log_directory, log_file_name), maxBytes=1024 * 1024 * 1024, backupCount=10)
file_handler.setLevel(log_level_log_file)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
file_handler.propagate = False
logging.getLogger().addHandler(file_handler)

app_logger = logging.getLogger(__name__)



def func_call_logger(log_level=logging.DEBUG):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            app_logger.log(log_level, f"Calling function {func.__name__} with args: {args}, kwargs: {kwargs}")
            result = func(*args, **kwargs)
            app_logger.log(log_level, f"Function {func.__name__} returned: {result}")
            return result

        return wrapper
    return decorator

def set_log_level_file(log_level):
    global log_level_log_file
    log_level_log_file = log_level
    file_handler.setLevel(log_level_log_file)
    app_logger.info(f"Log level for file handler set to {logging.getLevelName(log_level_log_file)}")

def set_log_level_console(log_level):
    global log_level_console
    log_level_console = log_level
    console_handler.setLevel(log_level_console)
    app_logger.info(f"Log level for console handler set to {logging.getLevelName(log_level_console)}")
