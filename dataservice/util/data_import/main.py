from importlib import import_module

from dataservice.util.data_import.utils import time_it
"""
Dynamically imports the etl module
"""


def _load_module(module_name):
    prefix_path = 'dataservice.util.data_import'
    module_path = "{}.{}.{}".format(prefix_path, module_name, 'etl')
    try:
        etl_module = import_module(module_path)
    except ModuleNotFoundError as e:
        print('ModuleNotFoundError: {}'.format(e.msg))
    else:
        return etl_module


@time_it
def run(module_name):
    etl_module = _load_module(module_name)
    if etl_module:
        etl_module.run()


@time_it
def drop_data(module_name):
    etl_module = _load_module(module_name)
    if etl_module:
        try:
            etl_module.drop_data()
        except AttributeError:
            print('Aborting! Method "drop_data" not implemented in {}'
                  .format(etl_module))
