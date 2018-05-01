from dataservice.util.data_import.etl.base_etl import BaseETLModule
from dataservice.util.data_import.schiffman.extract import Extractor


class ETLModule(BaseETLModule):

    def __init__(self, config):
        super().__init__(config, Extractor(config))
