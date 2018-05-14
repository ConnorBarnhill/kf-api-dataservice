from dataservice.util.data_import.config import (
    DEFAULT_ENTITY_TYPES,
    IMPORT_DATA_OP
)


class BaseETLModule(object):

    def __init__(self, config, extractor=None, transformer=None, loader=None):
        self.config = config
        self.extractor = extractor or self.create_default(
            'extract.extract', 'BaseExtractor')
        self.transformer = transformer or self.create_default(
            'transform.transform', 'BaseTransformer')
        self.loader = loader or self.create_loader()

    def create_default(self, module_name, cls_name):
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
        return cls(self.config)

    def create_loader(self):
        module_name = self.config['load']['use']
        module_name = '{}.{}'.format('load', module_name)
        return self.create_default(module_name, 'Loader')

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
        study_params = self.config['drop_data']['study']
        kwargs = {study_params['attribute']: study_params['value']}
        print('Deleting study {}...'.format(kwargs))
        self.loader.drop_all(**kwargs)
