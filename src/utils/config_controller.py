import json
from typing import Dict, Optional
import os
import sys
from src.utils.init_logger import init_logger



class SingletonMeta(type):
    _instances: Dict[type, object] = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

class ConfigManager(metaclass=SingletonMeta):
    _config: Dict[str, object] = None
    _filename: str = None

    @classmethod
    def _load_config(cls) -> None:
        if cls._filename:
            try:
                with open(cls._filename, 'r') as file:
                    cls._config = json.load(file)
            except (FileNotFoundError, json.JSONDecodeError):
                # Handle file not found or invalid JSON gracefully
                cls._config = {}

    @classmethod
    def load_config_from_file(cls, file_path: str) -> Dict[str, object]:
        cls._filename = file_path
        return cls.get_config()

    @classmethod  
    def get_config(cls) -> Dict[str, object]:
        if cls._config is None:
            cls._load_config()
        return cls._config

    @classmethod 
    def set_config(cls, new_config: Dict[str, object]) -> None:
        if not isinstance(new_config, dict):
            raise ValueError("new_config must be a dictionary")
        if cls._config is None:
            cls._config = {}
        cls._config.update(new_config)


    @classmethod
    def set_key(cls,  new_keys: Dict[str, object]) -> None:
        if not isinstance(new_keys, dict):
            raise ValueError("new_keys must be a dictionary")
        if cls._config is None:
            cls._config = {}
        deep_merge_dicts(cls._config, new_keys)

    @classmethod
    def is_config_loaded(cls) -> bool:
        return cls._config is not None
    
    def get_input_modules_of_output_module(cls, output_name):
        config = cls.get_config()
        input_modules = []
        sources = config["modules"]["output"][output_name]["sources"] 
        if isinstance(sources, str) and sources == "all":
            for input_module in config["modules"]["input"]:
                input_modules.append(input_module)
        elif isinstance(sources, list):
            for input_module in sources:
                if input_module in config["modules"]["input"]:
                    input_modules.append(input_module.key)
        return input_modules
    
def deep_merge_dicts(target, source):
    """
    Recursively merge source dictionary into target dictionary.
    """
    for key, value in source.items():
        if isinstance(value, dict) and key in target and isinstance(target[key], dict):
            deep_merge_dicts(target[key], value)
        else:
            target[key] = value

def setup_config(args):
    if args.config:
            config_path = args.config
    else:
        init_logger.info("Config not specified. Default config path config/config.json used.")
        config_path = "config/default.json"
    ConfigManager().load_config_from_file(config_path)
    config = ConfigManager().get_config()
    translate_args_to_config(args)
    config = ConfigManager().get_config()
    try:
        validate_config(config)
        init_logger.info("Configuration is valid.")
    except ValueError as e:
        init_logger.error(f"Validation failed: {e}")
        sys.exit(1)



def check_non_ascii_in_string(value, key_path):
    """Check if the given string contains non-ASCII characters."""
    if not isinstance(value, str):
        return
    if not all(ord(char) < 128 for char in value):
        raise ValueError(f"Non-ASCII value found at '{key_path}': {value}")

def recursively_ascii_check_config(config, key_path=''):
    """Recursively check the config for non-ASCII strings."""
    if isinstance(config, dict):
        for key, value in config.items():
            if not isinstance(key, str) or not all(ord(char) < 128 for char in key):
                raise ValueError(f"Non-ASCII key found: {key_path + key}")
            recursively_ascii_check_config(value, key_path + key + '.')
    elif isinstance(config, list):
        for index, item in enumerate(config):
            recursively_ascii_check_config(item, key_path + f'[{index}]')
    else:
        check_non_ascii_in_string(config, key_path)

def validate_config(config):
    mandatory_keys = {"SCOPE", "PROJECT_NAME", "DATABASE"}
    valid_log_levels = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
    missing_keys = mandatory_keys - set(config.keys())
    if missing_keys:
        raise ValueError(f"Missing mandatory keys: {missing_keys}")
    for key, value in config.items():
        if key == "LOGGING" and isinstance(value, dict):
            file_log_level = value.get("FILE_LOG_LEVEL", "")
            console_log_level = value.get("CONSOLE_LOG_LEVEL", "")
            if file_log_level not in valid_log_levels:
                raise ValueError(f"Invalid file log level found: {file_log_level}. Valid log levels are: {valid_log_levels}")
            if console_log_level not in valid_log_levels:
                raise ValueError(f"Invalid console log level found: {console_log_level}. Valid log levels are: {valid_log_levels}")

        if key == "SCOPE" and isinstance(value, dict):
            scope_directory_path = value.get("SCOPE_DIRECTORY_PATH", "")
            scope_file_name = value.get("SCOPE_FILE_NAME", "")
            if not os.path.exists(scope_directory_path):
                raise ValueError(f"Directory does not exist: {scope_directory_path}")
            file_path = os.path.join(scope_directory_path, scope_file_name)
            if not os.path.isfile(file_path):
                raise ValueError(f"File does not exist: {file_path}")
                 
        recursively_ascii_check_config(value)
    return True


def translate_args_to_config(args):
    new_keys = {}
    
    if args.scope:
        directory, filename = os.path.split(args.scope)
        new_keys.update({
            "SCOPE": {
                "SCOPE_DIRECTORY_PATH": directory,
                "SCOPE_FILE_NAME": filename  # Fixing the duplicate key issue
            }
        })

    if args.aggression:
        new_keys["AGGRESSIVE_SCANS"] = args.aggression

    if args.cache_directory:
        if "CACHING" not in new_keys:
            new_keys["CACHING"] = {}
        new_keys["CACHING"]["CACHE_DIRECTORY_PATH"] = args.cache_directory

    if args.run:
        new_keys["RUN"] = args.run

    if args.dump:
        if "EXPORT" not in new_keys:
            new_keys["EXPORT"] = {}
        new_keys["EXPORT"]["DUMP_ALL_MODULES"] = args.dump

    if args.projectname:
        new_keys["PROJECT_NAME"] = args.projectname

    if args.file_log_level or args.console_log_level:
        if "LOGGING" not in new_keys:
            new_keys["LOGGING"] = {}
        if args.file_log_level:
            new_keys["LOGGING"]["FILE_LOG_LEVEL"] = args.file_log_level
        if args.console_log_level:
            new_keys["LOGGING"]["CONSOLE_LOG_LEVEL"] = args.console_log_level

    ConfigManager().set_key(new_keys)
