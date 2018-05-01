import importlib.util
import os
from pandas import DataFrame

from dataservice.util.data_import.etl.mapper import Mapper


class BaseTransformer(object):

    def __init__(self, config):
        self.config = config
        self.mapper = Mapper(self.import_mappings(config))

    def import_mappings(self, config):
        """
        Import mappings module
        """
        # Return empty mappings dict if file not found
        fp = config['transform']['mappings_file']
        if not os.path.isfile(fp):
            print('Could not import mappings, file {} not found'.format(fp))
            return {}

        # Import mappings module from file
        module_name = os.path.basename(fp).split(".")[0]
        spec = importlib.util.spec_from_file_location(module_name, fp)
        mappings_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mappings_module)

        return mappings_module.mappings_dict

    def run(self, entity_dfs, **kwargs):
        """
        For each entity_type:
        Transform the entity dataframe into a collection of dicts

        Each row will be transformed into a dict
        Column names are mapped to dict keys
        Column values are filled in to dict values
        """
        nrows = kwargs.get('nrows')
        entity_type_list = kwargs.get('entity_types')
        entity_dict = {}

        # For each entity type
        for entity_type in entity_type_list:
            print('\nBegin transformation for {}'.format(entity_type))
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
