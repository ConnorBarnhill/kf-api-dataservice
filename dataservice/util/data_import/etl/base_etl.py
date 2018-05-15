import sys
import requests
from dataservice.util.data_import.config import (
    DEFAULT_ENTITY_TYPES,
    IMPORT_DATA_OP,
    SCHEMA_URL,
    ENTITY_MODEL_MAP
)


class BaseETLModule(object):

    def __init__(self, config, extractor=None, transformer=None, loader=None):
        self.config = config

        validate_on = config['transform'].get('validate_on', True)
        schemas = self.load_schemas(validate_on)

        self.extractor = extractor or self.create_default(
            'extract.extract', 'BaseExtractor')
        self.transformer = transformer or self.create_default(
            'transform.transform', 'BaseTransformer',
            schemas=schemas, validate_on=validate_on)
        self.loader = loader or self.create_loader(schemas=schemas)

    def create_default(self, module_name, cls_name, **kwargs):
        """
        Import and load default ETL class

        :param: module_name: name of etl module
        :param: cls_name: name of etl class
        """
        from importlib import import_module
        prefix_path = 'dataservice.util.data_import.etl'
        module_path = "{0}.{1}".format(prefix_path, module_name)
        mod = import_module(module_path)
        cls = getattr(mod, cls_name)
        return cls(self.config, **kwargs)

    def create_loader(self, **kwargs):
        module_name = self.config['load']['use']
        module_name = '{}.{}'.format('load', module_name)
        return self.create_default(module_name, 'Loader', **kwargs)

    def load_schemas(self, validate_on):
        """
        Load schemas from dataservice swagger json into a dict

        :param validate_on: If true, ensure that schemas can be loaded
        """
        if validate_on:
            response = requests.get(SCHEMA_URL)
            if response.status_code > 300:
                print('Aborting! Could not load enumerations: {}'
                      .format(response.text))
                sys.exit()
        else:
            return {}

        return {ENTITY_MODEL_MAP[k]: v
                for k, v in response.json()['definitions'].items()
                if k in ENTITY_MODEL_MAP}

    def run(self, operation=IMPORT_DATA_OP):
        """
        Run extract, transfrom, load modules for a particular dataset
        """
        entity_types = self.config.get('entities', DEFAULT_ENTITY_TYPES)

        # Extract and combine data
        print('Begin extraction ...')
        df_dict = self.extractor.run()
        print('Completed extraction\n')

        # Transform from dataframe to dicts
        content = None
        if not self.transformer.mapper:
            print('Aborting transformation! No mappings found.')
        else:
            print('Begin transformation ...')
            content = self.transformer.run(df_dict, entity_types=entity_types)
            print('Completed transformation\n')

        # Load into db via sqlalchemy
        if content:
            print('Begin loading ...')
            self.loader.run(content, entity_types=entity_types,
                            operation=operation)
            print('Completed loading\n')

    def drop_data(self):
        """
        Delete all data associated with a study identified by the study's
        external_id or kf_id
        """
        kf_id = self.config['drop_data']['study']
        print('Deleting study {}...'.format(kf_id))
        self.loader.drop_all(kf_id)
