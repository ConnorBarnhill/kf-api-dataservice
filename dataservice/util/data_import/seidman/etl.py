from pprint import pprint

from dataservice.util.data_import.seidman.extract import Extractor
from dataservice.util.data_import.seidman.transform import Transformer
from dataservice.util.data_import.seidman.load import Loader


def run():

    # Extract and combine data
    print('Begin extraction ...')
    e = Extractor()
    df_dict = e.run()
    print('Completed extraction\n')

    # Transform from dataframe to dicts
    print('Begin transformation ...')
    t = Transformer()
    content = t.run(df_dict)
    print('Completed transformation\n')

    # Load into db via sqlalchemy
    print('Begin loading ...')

    l = Loader()
    l.run(content)
    print('Completed loading\n')
