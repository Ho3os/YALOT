import json
from typing import Dict, Optional

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
        """Load configuration from the file."""
        if cls._filename:
            try:
                with open(cls._filename, 'r') as file:
                    cls._config = json.load(file)
            except (FileNotFoundError, json.JSONDecodeError):
                # Handle file not found or invalid JSON gracefully
                cls._config = {}

    @classmethod
    def load_config_from_file(cls, file_path: str) -> Dict[str, object]:
        """Load configuration from a specific file."""
        cls._filename = file_path
        return cls.get_config()

    @classmethod  
    def get_config(cls) -> Dict[str, object]:
        """Get the current configuration."""
        if cls._config is None:
            cls._load_config()
        return cls._config

    @classmethod 
    def set_config(cls, new_config: Dict[str, object]) -> None:
        """Set the configuration."""
        if not isinstance(new_config, dict):
            raise ValueError("new_config must be a dictionary")
        if cls._config is None:
            cls._config = {}
        cls._config.update(new_config)

    @classmethod
    def is_config_loaded(cls) -> bool:
        """Check if the configuration is loaded."""
        return cls._config is not None