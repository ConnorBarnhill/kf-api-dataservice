from pprint import pprint
from pandas import DataFrame

from dataservice.util.data_import.etl.defaults import DEFAULT_ENTITY_TYPES


class BaseTransformer(object):

    def __init__(self, mappings_dict, mapper=None):
        if not mapper:
            from dataservice.util.data_import.etl.mapper import Mapper
            mapper = Mapper(mappings_dict)
        self.mapper = mapper

    def run(self, entity_dfs, **kwargs):
        """
        For each entity_type:
        Transform the entity dataframe into a collection of dicts

        Each row will be transformed into a dict
        Column names are mapped to dict keys
        Column values are filled in to dict values
        """
        nrows = kwargs.get('nrows')
        entity_type_list = kwargs.get('entity_types', DEFAULT_ENTITY_TYPES)
        entity_dict = {}

        # For each entity type
        for entity_type in entity_type_list:
            print('Begin transformation for {}'.format(entity_type))
            # Get unique entities
            df = self._get_entity_df(entity_type, entity_dfs)
            # None or empty df
            try:
                if df.empty:
                    print('Extraction failed: no {}s in dataframe'.
                          format(entity_type))
                    continue
            except AttributeError:
                print('Extraction failed for entity {}, dataframe was {}'
                      .format(entity_type, df))
                continue

            print("Entity {} shape {}".format(entity_type, df.shape))

            # For each row in df
            entities = []
            # Use a count var since idx will not always be in
            # chronological order
            count = 0
            for idx, row in df.iterrows():
                # Only process nrows (for debugging purposes)
                if (nrows is not None) and (count >= nrows):
                    break
                # Transform row to entity
                entity = self.mapper.build_entity(entity_type, row)
                # Add to output
                entities.append(entity)
                count += 1

            entity_dict[entity_type] = entities

        return entity_dict

    def _get_entity_df(self, entity_type, entity_dfs):
        """
        Extract subset of dataframe by entity_type and unique id of the entity
        """
        # Get original entity dataframe
        data_df = entity_dfs.get(entity_type, entity_dfs.get('default'))
        if not isinstance(data_df, DataFrame) and data_df is None:
            return None

        # Get the unique id column for this dataframe
        _id = self.mapper.get_id_col(entity_type)

        # Filter dataframe down to unique rows using unique id
        if _id and _id in data_df.columns:
            df = data_df.drop_duplicates(_id)
        else:
            df = None
        return df
