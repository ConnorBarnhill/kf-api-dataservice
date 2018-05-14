from copy import deepcopy
from pprint import pprint

"""
Map columns names in a csv row to keys in a template dict
Fill values in dict with mapped column values in csv row
"""


COL_NAME = "$col_name"
COL_VALUE = "$col_value"
COL_TYPE = "$col_type"
NULL_VALUES = ["unknown", "not applicable", "not reported", "other"]


class Mapper(object):
    def __init__(self, mappings_dict):
        self.mappings = mappings_dict

    def build_entity(self, entity_type, row):
        """
        Transform a dataframe row into a dict using the mappings dict
        """
        mapping = self.mappings.get(entity_type)

        # Map col names and values to dict keys and values
        entity = deepcopy(mapping)
        self._map(row, None, None, entity)

        # Add unique id col (needed later for loading)
        unique_id_col = self.get_id_col(entity_type)
        entity['_unique_id_val'] = row[unique_id_col]

        return entity

    def _map(self, row, parent, key, value):
        """
        Traverse the input value dict and populate with mapped values
        """
        if isinstance(value, dict):
            keys = value.keys()

            # Leaf nodes
            if (COL_NAME in keys) or (COL_VALUE in keys):
                parent[key] = self._map_value(row, key, value)
            # Recurse
            else:
                for k, v in value.items():
                    self._map(row, value, k, v)
        elif value is None:
            parent[key] = self.get_allowable_value(key, value)

    def _map_value(self, row, property_name, mapping):
        """
        Given an input value, get the mapped value for the given property
        """
        mapped_value = None

        # Get col name
        col_name = mapping.get(COL_NAME)

        # Get value
        if col_name:
            if col_name not in row:
                print('\tWarning - column "{}" not found in source data.'
                      ' Filling in None for property "{}"'.
                      format(col_name, property_name))
            # From csv row
            mapped_value = row.get(col_name)

            # Use mapping func in mapping file to map the value
            if mapped_value and isinstance(mapping.get(COL_VALUE), dict):
                value_dict = mapping.get(COL_VALUE)
                val = value_dict.get(mapped_value)
                if val:
                    mapped_value = val
        else:
            # From specified value in mapping file
            mapped_value = mapping.get(COL_VALUE)

        # Get type
        col_type = mapping.get(COL_TYPE)
        if col_type and mapped_value:
            mapped_value = self._type_cast(col_type, mapped_value)

        # Map value to allowable value for this property
        mapped_value = self.get_allowable_value(property_name,
                                                mapped_value)

        return mapped_value

    def get_allowable_value(self, property_name, value):
        """
        Map the value to an allowable value for this property
        """
        # TODO
        return value

    def _type_cast(self, in_type, in_value):
        """
        Convert value to specified type
        """
        if in_type == 'integer':
            return int(in_value),
        elif in_type == 'float':
            return float(in_value)
        elif in_type == 'string':
            return str(in_value),
        elif in_type == 'boolean':
            return bool(in_value)
        else:
            return in_value

    def get_id_col(self, entity_type):
        """
        Get the unique id column specified in mappings dict for this
        entity_type
        """
        _id = None
        entity_mapping = self.mappings.get(entity_type)
        if entity_mapping:
            col = entity_mapping.get('_unique_id_col')
            if col:
                _id = col[COL_VALUE]

        return _id
