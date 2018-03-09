
def run(extractor=None, transformer=None, loader=None):
    """
    Run extract, transfrom, load modules for a particular dataset
    """
    # Extract and combine data
    if extractor:
        print('Begin extraction ...')
        df_dict = extractor.run()
        print('Completed extraction\n')

    # Transform from dataframe to dicts
    if transformer:
        print('Begin transformation ...')
        content = transformer.run(df_dict)
        print('Completed transformation\n')

    if loader:
        # Load into db via sqlalchemy
        print('Begin loading ...')
        loader.run(content)
        print('Completed loading\n')


def drop_data(loader, study_external_id):
    """
    Delete all data associated with a study identified by the study's
    external_id
    """
    print('Begin deleting data for study {} ...'.format(study_external_id))
    loader.drop_all(study_external_id)
    print('Completed deleting of data\n')
