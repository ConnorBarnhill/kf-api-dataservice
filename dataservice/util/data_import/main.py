import os
from importlib import import_module
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from dataservice.util.data_import.utils import time_it, read_yaml

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
CONFIG_SCHEMA_FILE = os.path.join(ROOT_DIR, 'etl', 'config_schema.yaml')


def _load_etl_module_cls(module_name):
    """
    Dynamically import etl module and init ETLModule class
    """
    prefix_path = 'dataservice.util.data_import'
    module_path = "{}.{}.{}".format(prefix_path, module_name, 'etl')
    try:
        etl_module = import_module(module_path)
    except ModuleNotFoundError as e:
        print('ModuleNotFoundError: {}'.format(e.msg))
    else:
        try:
            cls = getattr(etl_module, 'ETLModule')
        except AttributeError as e:
            print('This ETL module {} has no ETLModule class implemented'
                  .format(module_name))
        return cls


def _load_config(config_file):
    """
    Validate and load config dict from yaml file
    """
    config = None

    # Read config file if exists
    try:
        config = read_yaml(config_file)
    except FileNotFoundError as e:
        print('Aborting! {} does not exist.'
              ' No config file found for etl module'
              .format(config_file))

    # Validate config file against schema
    else:
        schema = read_yaml(CONFIG_SCHEMA_FILE)
        try:
            validate(config, schema)
        except ValidationError as e:
            config = None
            print(e)

    return config


def _init(etl_module_name, config_file):
    # If no config file supplied try loading default
    if not config_file:
        config_file = os.path.join(ROOT_DIR, etl_module_name, 'config.yaml')

    # Import ETL config
    config = _load_config(config_file)

    # Import ETL module
    etl_module = _load_etl_module_cls(etl_module_name)

    return config, etl_module


@time_it
def run(etl_module_name, config_file):
    """
    Run ETL module given the configuration obj
    """
    # Load config and etl module
    config, etl_module = _init(etl_module_name, config_file)

    # Run ETL
    if config and etl_module:
        etl_module(config).run()


@time_it
def drop_data(etl_module_name, config_file):
    """
    Drop all data related to the study

    Study is idenfied by its external id which is sourced from the etl config
    """
    # Load config and etl module
    config, etl_module = _init(etl_module_name, config_file)

    # Drop data for study
    if config and etl_module:
        etl_module(config).drop_data()
