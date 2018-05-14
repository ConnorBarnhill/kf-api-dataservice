from dataservice.util.data_import.etl.load.load import BaseLoader


class Loader(BaseLoader):

    def run(self, *args, **kwargs):
        kwargs['entity_types'] = ['sequencing_center']
        return super().run(*args, **kwargs)
