from dataservice.util.data_import.seq_centers.extract import Extractor
from dataservice.util.data_import.seq_centers.transform import Transformer
from dataservice.util.data_import.seq_centers.load import Loader
from dataservice.util.data_import.etl import etl

ex = Extractor()
tm = Transformer()
ld = Loader()


def run():
    """
    Run extract, transfrom, load modules for a particular dataset
    """
    etl.run(ex, tm, ld)


def drop_data():
    """
    Delete all sequencing centers
    """
    from dataservice.api.sequencing_center.models import SequencingCenter
    from dataservice.extensions import db

    SequencingCenter.query.delete()
    db.session.commit()
