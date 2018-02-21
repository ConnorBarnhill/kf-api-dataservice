from importlib import import_module


def run(module_name):
    prefix_path = 'dataservice.util.data_import'
    etl_module = import_module("{}.{}.{}".format(prefix_path, module_name,
                                                 'etl'))
    etl_module.run()
