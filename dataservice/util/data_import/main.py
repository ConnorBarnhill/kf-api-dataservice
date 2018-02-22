from importlib import import_module

from dataservice.util.data_import.utils import time_it
"""
Dynamically imports the etl module
"""


@time_it
def run(module_name):
    prefix_path = 'dataservice.util.data_import'
    module_path = "{}.{}.{}".format(prefix_path, module_name, 'etl')
    try:
        etl_module = import_module(module_path)
    except ModuleNotFoundError as e:
        print('ETL module {} does not exist'.format(module_path))
    else:
        etl_module.run()
