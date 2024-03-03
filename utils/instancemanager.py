from  utils.applogger import app_logger
from  utils.applogger import func_call_logger
import logging

class InstanceManager:
    input_instances = {}
    output_instances = {}

    @classmethod
    @func_call_logger(log_level=logging.INFO)
    def register_input_instance(cls, name, instance):
        cls.input_instances[name] = instance

    @classmethod
    @func_call_logger(log_level=logging.INFO)
    def register_output_instance(cls, name, instance):
        cls.output_instances[name] = instance

    @classmethod
    def get_input_instance(cls, name):
        return cls.input_instances.copy().get(name, None)

    @classmethod
    def get_output_instance(cls, name):
        return cls.output_instances.copy().get(name, None)

    @classmethod
    def get_input_instances(cls):
        return cls.input_instances.copy()

    @classmethod
    def get_output_instances(cls):
        return cls.output_instances.copy()
