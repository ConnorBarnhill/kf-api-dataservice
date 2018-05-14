
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
        web_config = self.config.get('load').get('web')
        self.base_url = web_config.get('base_url')

    def _check_status(self):
        pass

    def run(self, entity_dict, **kwargs):
        pass
