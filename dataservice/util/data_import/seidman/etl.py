from pprint import pprint

from dataservice.util.data_import.seidman.extract import Extractor
from dataservice.util.data_import.seidman.transform import Transformer
from dataservice.util.data_import.seidman.load import Loader

ENTITY_TYPES = [
    'study',
    'participant',
    'family',
    'demographic',
    'diagnosis',
    'sample',
    'aliquot',
    'sequencing_experiment'
]


def run():
    # Extract and combine data
    e = Extractor()
    df = e.run()

    # Transform from dataframe to dicts
    t = Transformer()
    content = t.run(df, ENTITY_TYPES)
    # pprint(content)

    # Load into db via sqlalchemy
    l = Loader()
    l.run(content)
