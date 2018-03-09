from dataservice.util.data_import.seidman.extract import Extractor
from dataservice.util.data_import.seidman.transform import Transformer
from dataservice.util.data_import.seidman.load import Loader
from dataservice.util.data_import.etl import etl

STUDY_EXT_ID = 'phs001138'

ex = Extractor()
tm = Transformer()
ld = Loader()


def run():
    """
    Run extract, transfrom, load modules for a particular dataset
    """
    etl.run(ex, tm, ld)


def drop_data(study_external_id=STUDY_EXT_ID):
    """
    Delete all data associated with a study identified by the study's
    external_id
    """
    etl.drop_data(ld, study_external_id)
