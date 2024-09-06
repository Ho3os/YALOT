from typing import Any, Dict, Optional, List
from src.utils.app_logger import func_call_logger
import logging

class InstanceManager:
    input_instances: Dict[str, Any] = {}
    output_instances: Dict[str, Any] = {}

    @classmethod
    @func_call_logger(log_level=logging.INFO)
    def register_input_instance(cls, name: str, instance: Any) -> None:
        cls.input_instances[name] = {
            "instance": instance,
            "TBD": None
        }

    @classmethod
    @func_call_logger(log_level=logging.INFO)
    def register_output_instance(cls, name: str, instance: Any, input_instance_names: List[Any]) -> None:
        input_instances = []
        for input_name in input_instance_names:
            input_instances.append(cls.get_input_instance(input_name))
        cls.output_instances[name] = {
            "instance": instance,
            "input_instances": input_instances
        }

    @classmethod
    def get_input_instance(cls, name: str) -> Optional[Any]:
        return cls.simplify_instance(cls.input_instances.copy().get(name, None))

    @classmethod
    def get_output_instance(cls, name: str) -> Optional[Any]:
        return cls.simplify_instance(cls.output_instances.copy().get(name, None))

    @classmethod
    def get_input_instances(cls) -> Dict[str, Any]:
        return cls.simplify_instances(cls.input_instances.copy())

    @classmethod
    def get_output_instances(cls) -> Dict[str, Any]:
        return cls.simplify_instances(cls.output_instances.copy())
       
    @classmethod
    def get_full_output_instance(cls, name: str) -> Optional[Any]:
        return cls.output_instances.copy().get(name, None)
    
    @classmethod
    def get_full_output_instances(cls) -> Dict[str, Any]:
        return cls.output_instances.copy()

    @classmethod
    def get_all_instances(cls) -> Dict[str, Any]:
        return cls.output_instances.copy() | cls.input_instances.copy()
    
    @classmethod
    def flush_instances(cls):
        cls.flush_input_instances()
        cls.flush_output_instances()

    @classmethod
    def flush_input_instances(cls):
        cls.input_instances = {}

    @classmethod
    def flush_output_instances(cls):
        cls.output_instances = {}

    @classmethod
    def simplify_instances(cls, instances):
        instance = []
        if instances:
            for _, data in instances.items():
                for inner_name, inner_data in data.items():
                    if inner_name == "instance":
                        instance.append(inner_data)
        return instance
    
    @classmethod
    def simplify_instance(cls, instance):
        if instance and "instance" in instance:
            return instance["instance"]