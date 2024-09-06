import time
from functools import wraps
from  src.utils.app_logger import app_logger
from  src.utils.app_logger import func_call_logger

start_time = time.time()


# Define the debug flag
debug = True  # Set this to False to disable elapsed time logging

def time_logger(msg=''):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            func_start_time = time.time()
            result = func(*args, **kwargs)
            func_end_time = time.time()
            total_elapsed_time = func_end_time - start_time 
            last_elapsed_time = func_end_time - func_start_time
            app_logger.info("========================================================================================================")
            app_logger.info(f"Execution time in total: {total_elapsed_time:.2f} seconds ---  Module {msg}: {last_elapsed_time:.2f} seconds")
            app_logger.info("========================================================================================================")
            return result
        return wrapper
    return decorator