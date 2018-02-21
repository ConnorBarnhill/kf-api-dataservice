from pprint import pprint


class BaseTransformer(object):

    def __init__(self, mappings_dict, mapper=None):
        if not mapper:
            from dataservice.util.data_import.etl.mapper import Mapper
            mapper = Mapper(mappings_dict)
        self.mapper = mapper

    def run(self, entity_dfs, entity_type_list, nrows=None):
        """
        Transform dataframe into a collection of dicts
        """
        entity_dict = {}

        # For each entity type
        for entity_type in entity_type_list:
            # Get unique entities
            df = self._get_entity_df(entity_type, entity_dfs)
            # None or empty df
            try:
                if df.empty:
                    continue
            except AttributeError:
                print('Could not extract df for entity {}'.format(entity_type))
                continue

            print("Entity {} shape {}".format(entity_type, df.shape))

            # For each row in df
            entities = []
            count = 0
            for idx, row in df.iterrows():
                # print(row)
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
        data_df = entity_dfs.get(entity_type, entity_dfs['default'])
        _id = self.mapper.get_id_col(entity_type)

        if _id and _id in data_df.columns:
            df = data_df.drop_duplicates(_id)
        else:
            df = None
        return df
