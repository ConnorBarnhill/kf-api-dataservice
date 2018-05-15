from copy import deepcopy

from dataservice.util.data_import.config import (
    COL_NAME_KEY,
    COL_VALUE_KEY,
    COL_TYPE_KEY,
    NOT_REPORTED
)
"""
Map columns names in a csv row to keys in a template dict
Fill values in dict with mapped column values in csv row
"""


class Mapper(object):
    def __init__(self, mappings_dict, **kwargs):
        self.mappings = mappings_dict
        self.schemas = (kwargs.get('schemas')
                        if kwargs.get('validate_on') else None)

    def build_entity(self, entity_type, row):
        """
        Transform a dataframe row into a dict using the mappings dict
        """
        mapping = self.mappings.get(entity_type)

        # Map col names and values to dict keys and values
        entity = deepcopy(mapping)
        self._map(entity_type, row, None, None, entity)

        # Add unique id col (needed later for loading)
        unique_id_col = self.get_id_col(entity_type)
        entity['_unique_id_val'] = row[unique_id_col]

        return entity

    def _map(self, entity_type, row, parent, key, value):
        """
        Traverse the input value dict and populate with mapped values
        """
        if isinstance(value, dict):
            keys = value.keys()

            # Leaf nodes
            if (COL_NAME_KEY in keys) or (COL_VALUE_KEY in keys):
                parent[key] = self._map_value(row, key, value)
            # Recurse
            else:
                for k, v in value.items():
                    self._map(entity_type, row, value, k, v)

        # Map null value to allowable value for this property
        elif value is None:
            parent[key] = self.get_allowable_value(entity_type, key, value)

    def _map_value(self, row, property_name, mapping):
        """
        Given an input value, get the mapped value for the given property
        """
        mapped_value = None

        # Get col name
        col_name = mapping.get(COL_NAME_KEY)

        # Get value
        if col_name:
            if col_name not in row:
                print('\tWarning - column "{}" not found in source data.'
                      ' Filling in None for property "{}"'.
                      format(col_name, property_name))
            # From csv row
            mapped_value = row.get(col_name)

            # Use mapping func in mapping file to map the value
            if mapped_value and isinstance(mapping.get(COL_VALUE_KEY), dict):
                value_dict = mapping.get(COL_VALUE_KEY)
                val = value_dict.get(mapped_value)
                if val:
                    mapped_value = val
        else:
            # From specified value in mapping file
            mapped_value = mapping.get(COL_VALUE_KEY)

        # Get type
        col_type = mapping.get(COL_TYPE_KEY)
        if col_type and mapped_value:
            mapped_value = self._type_cast(col_type, mapped_value)

        return mapped_value

    def get_allowable_value(self, entity_type, property_name, value):
        """
        Map the null value to an allowable value for this property
        """
        if not self.schemas:
            return value

        # Entity schema
        schema = self.schemas.get(entity_type)
        if not schema:
            print('Warning - Could not load schema for {}'.format(entity_type))
            return value

        # Property schema
        properties = schema.get('properties')
        property_schema = properties.get(property_name)
        if not property_schema:
            print('Warning - Could not find schema '
                  'for property {} in {} schema'.format(
                      property_name, entity_type))
            return value

        # String types that are None -_> use Not Reported
        # Enums types that are None --> use Not Reported
        if (('enum' in property_schema) or
                (property_schema['type'] == 'string') and
                not property_name.endswith('date')):
            value = NOT_REPORTED

        # Ints/floats/boolean that are None leave as is
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
                _id = col[COL_VALUE_KEY]

        return _id
