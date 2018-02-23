from pprint import pprint

from dataservice.util.data_import.schiffman.extract import Extractor
from dataservice.util.data_import.schiffman.transform import Transformer
from dataservice.util.data_import.schiffman.load import Loader

ENTITY_TYPES = [
    'investigator',
    'study',
    'study_file',
    'participant',
    'family_relationship',
    'demographic',
    'diagnosis',
    'sample',
    'aliquot',
    'sequencing_experiment',
    'genomic_file',
    'phenotype'
]


def run():
    # Extract and combine data
    print('Begin extraction ...')
    e = Extractor()
    df_dict = e.run()
    print('Completed extraction\n')

    # Transform from dataframe to dicts
    print('Begin transformation ...')
    t = Transformer()
    content = t.run(df_dict, ENTITY_TYPES)
    print('Completed transformation\n')

    # Load into db via sqlalchemy
    print('Begin loading ...')

    l = Loader()
    l.run(content, entity_types=ENTITY_TYPES)
    print('Completed loading\n')
