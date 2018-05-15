import os
from pprint import pprint

from dataservice.util.data_import.utils import (
    read_json,
    write_json
)
from dataservice.util.data_import.config import (
    ETL_PACKAGE_NAME_KEY,
    KF_ID_CACHE_FNAME
)


class BaseLoader(object):

    def __init__(self, config, **kwargs):
        self.config = config
        self.schemas = kwargs.get('schemas')
        self.kf_id_cache = {}

    def _build_entities(self, entity_type, entity_dict):
        """
        Build entity payloads of a particular entity_type to db
        """

        print('\nLoading {}s ...'.format(entity_type))

        # For all entities of entity_type
        entities_to_load = entity_dict.get(entity_type)
        if not entities_to_load:
            print('\nExpected to load {0} but 0 {0}s were found to load.\n'.
                  format(entity_type))
            return (None, None)

        _ids = []
        for i, params in enumerate(entities_to_load):
            # Save ids
            _ids.append(params['_unique_id_val'])

            # Add foreign keys
            self._build_links(entity_type, params)
            if params:
                # Remove the private keys which were only needed for linking
                self._remove_extra_keys(params)

        return _ids, entities_to_load

    def _build_links(self, entity_type, params):
        """
        Add foreign keys to input params if this entity is linked to any others
        """
        # Get required foreign keys
        required = set(self.schemas[entity_type]['required']
                       if (self.schemas and
                           'required' in self.schemas[entity_type]) else [])

        linked_entities = params.get('_links')
        if linked_entities:
            for linked_entity, _params in linked_entities.items():
                # The required keys don't exist in mapping
                if not (_params.get('source_fk_col') and
                        _params.get('target_fk_col')):
                    print('Error loading {0}. Missing foreign key info.'
                          ' Check mappings for {0}'.format(entity_type))
                    continue
                source_fk_val = str(_params['source_fk_col'])
                target_fk_col = _params['target_fk_col']

                # A required linked/foreign entity is not found
                try:
                    target_fk_value = self.kf_id_cache[
                        linked_entity][source_fk_val]
                except KeyError as e:
                    # Foreign key not required
                    if target_fk_col not in required:
                        target_fk_value = None
                    # Foreign key is required but no corresponding external id
                    # found in source data
                    else:
                        pprint('Error loading {}, required foreign entity {} '
                               'not found'.format(params['_unique_id_col'],
                                                  source_fk_val))
                        params = None
                        continue
                if params:
                    params[target_fk_col] = target_fk_value

            if params:
                params.pop('_links', None)

    def _build_family_relationships(self, entities_to_load,
                                    relation_keys=None):
        """
        Create and save family relationships for a proband
        Relationships are: mother - proband and father - proband
        """
        print('\nLoading {}s ...'.format('family_relationship'))

        if not entities_to_load:
            print('\nExpected to load {0} but 0 {0}s were found to load.\n'.
                  format('family_relationship'))
            return (None, None)

        if not relation_keys:
            relation_keys = ['mother', 'father']

        entities = []
        _ids = []
        for family_rel in entities_to_load:
            for r in relation_keys:
                params = self._build_family_relationship(r, family_rel)
                if params:
                    entities.append(params)
                    _ids.append('{}'.format(
                        '_'.join(list(params.values()))))
        return _ids, entities

    def _build_family_relationship(self, relation_key, family_rel):
        """
        Create a family relationship
        """
        params = {}
        if family_rel.get(relation_key) and family_rel.get('proband'):
            p_id = self._get_kf_id(
                'participant', family_rel[relation_key])
            r_id = self._get_kf_id(
                'participant', family_rel['proband'])
            if p_id and r_id:
                params = {'participant_id': p_id,
                          'relative_id': r_id,
                          'participant_to_relative_relation': relation_key}
        return params

    def _get_kf_id(self, entity_type, key):
        return self.kf_id_cache[entity_type].get(str(key))

    def _save_kf_ids(self, _ids, kf_ids, entity_type):
        """
        Add to entity id map which maps original unique id in entity table
        to kf_id
        """
        # Save kf ids
        if self.kf_id_cache.get(entity_type) is None:
            self.kf_id_cache[entity_type] = {}

        for i, kf_id in enumerate(kf_ids):
            self.kf_id_cache[entity_type][str(_ids[i])] = kf_id

        # Write to file
        self._write_kf_id_cache()

    def _update_kf_id(self, _id, kf_id, entity_type):
        if self.kf_id_cache.get(entity_type) is None:
            self.kf_id_cache[entity_type] = {}

        self.kf_id_cache[entity_type][str(_id)] = kf_id

    def _remove_extra_keys(self, params):
        """
        Remove private keys from kwargs dict.

        Private keys were only needed for linking entities and should
        not stay in the model's kwargs (params)
        """
        keys = ['_unique_id_val', '_unique_id_col', '_links']
        for k in keys:
            params.pop(k, None)

    def _load_kf_id_cache(self):
        """
        Read file containing cache of kf_ids for objects already created
        """
        filepath = self._get_kf_id_cache_path()
        if os.path.isfile(filepath):
            self.kf_id_cache = read_json(filepath)

    def _write_kf_id_cache(self):
        """
        Write file containing cache of kf_ids for objects created
        """
        filepath = self._get_kf_id_cache_path()

        write_json(self.kf_id_cache, filepath)

    def _get_kf_id_cache_path(self):
        root_dir = os.path.abspath(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        etl_package_name = self.config[ETL_PACKAGE_NAME_KEY]
        filepath = os.path.join(root_dir, etl_package_name,
                                KF_ID_CACHE_FNAME)
        return filepath
