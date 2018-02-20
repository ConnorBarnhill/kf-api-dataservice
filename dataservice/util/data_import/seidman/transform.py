from dataservice.util.data_import.etl.transform import BaseTransformer


class Transformer(BaseTransformer):

    def __init__(self):
        from .mappings import mappings_dict
        super(Transformer, self).__init__(mappings_dict)
