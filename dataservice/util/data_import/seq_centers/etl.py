from dataservice.util.data_import.etl.base_etl import BaseETLModule
from dataservice.util.data_import.seq_centers.extract import Extractor
from dataservice.util.data_import.seq_centers.load import SeqCenterLoader


class ETLModule(BaseETLModule):

    def __init__(self, config):
        super().__init__(config, extractor=Extractor(config),
                         loader=SeqCenterLoader(config))

    def drop_data(self):
        print('Deleting all sequencing_centers ...')
        SeqCenterLoader.drop_all()
