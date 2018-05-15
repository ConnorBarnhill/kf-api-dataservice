import sys
from dataservice.util.data_import.etl.base_etl import BaseETLModule
from dataservice.util.data_import.seq_centers.extract import Extractor


class ETLModule(BaseETLModule):

    def __init__(self, config):
        super().__init__(config, extractor=Extractor(config),
                         loader=self.create_loader(config))

    def create_loader(self, config):
        """
        Select loader based on config
        """
        module_name = config['load']['use']
        loader = None
        if module_name == 'db_load':
            from dataservice.util.data_import.seq_centers.load import DbLoader
            loader = DbLoader(config)
        elif module_name == 'kf_dataservice_load':
            from dataservice.util.data_import.seq_centers.load import (
                DataserviceLoader
            )
            loader = DataserviceLoader(config)
        else:
            print('Aborting! Loader type {} not recognized'.format(
                module_name))
            sys.exit()
        return loader

    def drop_data(self):
        """
        Delete seq centers
        """
        print('Deleting all sequencing_centers ...')
        self.loader.drop_all()
