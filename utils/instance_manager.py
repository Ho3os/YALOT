from typing import Any, Dict, Optional
from utils.app_logger import func_call_logger
import logging

class InstanceManager:
    input_instances: Dict[str, Any] = {}
    output_instances: Dict[str, Any] = {}

    @classmethod
    @func_call_logger(log_level=logging.INFO)
    def register_input_instance(cls, name: str, instance: Any) -> None:
        cls.input_instances[name] = instance

    @classmethod
    @func_call_logger(log_level=logging.INFO)
    def register_output_instance(cls, name: str, instance: Any) -> None:
        cls.output_instances[name] = instance

    @classmethod
    def get_input_instance(cls, name: str) -> Optional[Any]:
        return cls.input_instances.copy().get(name, None)

    @classmethod
    def get_output_instance(cls, name: str) -> Optional[Any]:
        return cls.output_instances.copy().get(name, None)

    @classmethod
    def get_input_instances(cls) -> Dict[str, Any]:
        return cls.input_instances.copy()

    @classmethod
    def get_output_instances(cls) -> Dict[str, Any]:
        return cls.output_instances.copy()
    
    @classmethod
    def get_all_instances(cls) -> Dict[str, Any]:
        return cls.output_instances.copy() | cls.input_instances.copy()
