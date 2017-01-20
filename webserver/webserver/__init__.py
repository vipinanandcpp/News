import os, inspect, sys
current_module = sys.modules[__name__]
module_directory = os.path.dirname(inspect.getabsfile(current_module))
project_directory = os.path.abspath(os.path.join(module_directory, os.pardir))
