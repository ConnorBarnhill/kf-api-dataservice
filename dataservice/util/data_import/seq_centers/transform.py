from dataservice.util.data_import.etl.transform import BaseTransformer


class Transformer(BaseTransformer):

    def __init__(self, mappings_dict=None):
        if not mappings_dict:
            from .mappings import mappings_dict
        super(Transformer, self).__init__(mappings_dict)

    def run(self, *args, **kwargs):
        kwargs['entity_types'] = ['sequencing_center']
        return super().run(*args, **kwargs)