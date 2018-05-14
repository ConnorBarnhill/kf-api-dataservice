import sys
import json
import requests

ENTITY_ENDPOINT_MAP = {
    'study': '/studies',
    'investigator': '/investigators',
    'study_file': '/study-files',
    'family': '/families',
    'family_relationship': '/family-relationships',
    'cavatica_app': '/cavatica-apps',
    'sequencing_center': '/sequencing-centers',
    'participant': '/participants',
    'diagnosis': '/diagnoses',
    'phenotype': '/phenotypes',
    'outcome': '/outcomes',
    'biospecimen': '/biospecimens',
    'genomic_file': '/genomic-files',
    'sequencing_experiment': '/sequencing-experiments',
    'cavatica_task': '/cavatica-tasks',
    'cavatica_task_genomic_file': '/cavatica-task-genomic-files'
}


class Loader(object):

    def __init__(self, config):
        self.config = config
        self.setup()

    def setup(self):
        load_config = self.config['load']
        load_module_name = load_config['use']
        if load_module_name not in load_config:
            print('Aborting! Could not find load parameters for {} in config'
                  .format(load_module_name))
            sys.exit()
        web_config = load_config[load_module_name]
        self.base_url = web_config.get('base_url')
        if not self.base_url:
            print('Aborting! No base url specified in {} config'
                  .format(load_module_name))
            sys.exit()

    def _check_status(self):
        pass

    def run(self, entity_dict, **kwargs):
        print('In run')
        pass
