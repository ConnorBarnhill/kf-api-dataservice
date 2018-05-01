from dataservice.util.data_import.etl.base_etl import BaseETLModule
from dataservice.util.data_import.seq_centers.extract import Extractor
from dataservice.util.data_import.seq_centers.load import Loader


class ETLModule(BaseETLModule):

    def __init__(self, config):
        super().__init__(config, extractor=Extractor(config),
                         loader=Loader(config))
