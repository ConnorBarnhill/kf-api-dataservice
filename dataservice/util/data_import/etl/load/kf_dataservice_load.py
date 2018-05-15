import sys
import requests

from dataservice.util.data_import.etl.load.base_load import BaseLoader
from dataservice.util.data_import.config import (
    IMPORT_DATA_OP,
    UPDATE_DATA_OP,
    ENTITY_ENDPOINT_MAP
)


class Loader(BaseLoader):

    def __init__(self, config,  **kwargs):
        super().__init__(config, **kwargs)
        self.setup()

    def setup(self):
        """
        Extract params from config and save
        """
        # Get params
        load_config = self.config['load']
        load_module_name = load_config['use']
        if load_module_name not in load_config:
            print('Aborting! Could not find load parameters for {} in config'
                  .format(load_module_name))
            sys.exit()

        # Get base url
        web_config = load_config[load_module_name]
        self.base_url = web_config.get('base_url')

        if not self.base_url:
            print('Aborting! No base url specified in {} config'
                  .format(load_module_name))
            sys.exit()

    def drop_all(self, kf_id):
        """
        Delete all data related to a study
        """
        # Get investigator
        url = self.base_url + '/investigators' + '?study_id={}'.format(kf_id)
        headers = {'Content-Type': 'application/json'}
        response = requests.get(url, headers=headers)
        if response.status_code > 300:
            print('Aborting! {}'.format(response.text))
            return

        # Get investigator id
        results = response.json()['results']
        if not results:
            investigator_id = None
        else:
            investigator_id = results[0]['kf_id']

        # Delete study
        url = self.base_url + '/studies/{}'.format(kf_id)
        headers = {'Content-Type': 'application/json'}
        response = requests.delete(url, headers=headers)
        if response.status_code > 300:
            print('Failed! {}'.format(response.text))
            return

        # Delete investigator
        if investigator_id:
            url = self.base_url + '/investigators/{}'.format(investigator_id)
            headers = {'Content-Type': 'application/json'}
            response = requests.delete(url, headers=headers)
            if response.status_code > 300:
                print('Failed! {}'.format(response.text))
                return

    def run(self, entity_dict, **kwargs):
        """
        Load all entities into db
        """

        # Check service status
        is_down = self._check_service_status()
        if is_down:
            print('Aborting! Kids First Dataservice is down ...')
            return

        operation = kwargs.get('operation', None)
        entity_types = kwargs.get('entity_types')
        skip_entities = {'family_relationship'}

        # Get entity id map
        if operation == UPDATE_DATA_OP:
            self._load_kf_id_cache()

        # For each entity type
        for entity_type in entity_types:
            if entity_type in skip_entities:
                continue

            # Create all entity objects and save to db
            _ids, entities_to_load = self._build_entities(
                entity_type,
                entity_dict)
            if not entities_to_load:
                continue

            # Create or update through dataservice api
            self._create_or_update_all(operation, entity_type, _ids,
                                       entities_to_load)

        # Process family relationships
        if 'family_relationship' in entity_types:
            self._load_fr(operation,
                          entity_dict.get('family_relationship'))

        print('Completed loading {}\n'.format(entity_type))

    def _create_or_update_all(self, operation, entity_type, _ids,
                              entities_params):
        """
        Create or update entities
        """
        endpoint = ENTITY_ENDPOINT_MAP.get(entity_type)
        success_ids = []
        kf_ids = []

        print('Loading {} {}s ...'.format(len(entities_params), entity_type))

        for i, params in enumerate(entities_params):
            headers = {'Content-Type': 'application/json'}

            # Build request based on operation
            url = self.base_url + endpoint
            method_str = 'post'
            if operation == UPDATE_DATA_OP:
                if entity_type not in self.kf_id_cache:
                    print('Error updating - No existing {}s found'.format(
                        entity_type))
                    continue
                kf_id = self.kf_id_cache[entity_type][_ids[i]]
                url = url + '/' + kf_id
                method_str = 'patch'

            # Send request
            method = getattr(requests, method_str)
            response = method(url, json=params, headers=headers)

            # Failure
            if response.status_code > 300:
                print('Error loading {}, request: {} response: {}'
                      .format(entity_type, params, response.text))
            # Success
            else:
                kf_id = response.json()['results']['kf_id']
                kf_ids.append(kf_id)
                success_ids.append(_ids[i])

        if IMPORT_DATA_OP:
            self._save_kf_ids(success_ids, kf_ids, entity_type)

    def _load_fr(self, operation, entities):
        """
        Load family relationships
        """
        _ids_fr, entities_to_load_fr = self._build_family_relationships(
            entities)

        if not entities_to_load_fr:
            return

        self._create_or_update_all(operation, 'family_relationship', _ids_fr,
                                   entities_to_load_fr)

    def _check_service_status(self):
        """
        Check dataservice status
        """
        url = '/status'
        headers = {'Content-Type': 'application/json'}
        response = requests.get(self.base_url + url, headers=headers)
        is_down = response.status_code > 200
        if is_down:
            print(response.json()['_status'])

        return is_down
