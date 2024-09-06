import logging
import logging.handlers
import functools
import sys
import os
from typing import Callable


init_logger = logging.getLogger(__name__)
init_logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
init_logger.addHandler(console_handler)