import os
from importlib import import_module
from pprint import pprint

from dataservice.extensions import db
from dataservice import create_app

from dataservice.util.data_import.utils import to_camel_case
from dataservice.api.study.models import Study
from dataservice.api.participant.models import Participant
from dataservice.api.family_relationship.models import FamilyRelationship
from dataservice.api.demographic.models import Demographic
from dataservice.api.diagnosis.models import Diagnosis
from dataservice.api.outcome.models import Outcome
from dataservice.api.phenotype.models import Phenotype
from dataservice.api.sample.models import Sample
from dataservice.api.aliquot.models import Aliquot
from dataservice.api.sequencing_experiment.models import SequencingExperiment
from dataservice.api.genomic_file.models import GenomicFile
from dataservice.api.workflow.models import (
    Workflow,
    WorkflowGenomicFile
)

ENTITY_TYPES = [
    'study',
    'participant',
    'family_relationship',
    'demographic',
    'diagnosis',
    'sample',
    'aliquot',
    'sequencing_experiment'
]


class BaseLoader(object):

    def __init__(self, config_name=None):
        if not config_name:
            config_name = os.environ.get('FLASK_CONFIG', 'default')
        self.setup(config_name)
        self.entity_id_map = {}

    def setup(self, config_name):
        """
        Creates tables in database
        """
        self.app = create_app(config_name)
        self.app_context = self.app.app_context()
        self.app_context.push()
        # TODO - remove
        db.drop_all()
        db.create_all()

    def drop_all(self):
        """
        Drop all tables in database
        """
        db.session.remove()
        db.drop_all()
        self.app_context.pop()

    def run(self, entity_dict, entity_types=ENTITY_TYPES):
        """
        Load all entities into db
        """
        skip_entities = ['family_relationship']
        # For each entity type
        for entity_type in entity_types:
            # Skip some entities
            if entity_type in skip_entities:
                continue
            # Dynamically import entity model class
            model_name = to_camel_case(entity_type)
            model_module_path = 'dataservice.api.{}.models'.format(
                entity_type)
            models_module = import_module(model_module_path)
            model = getattr(models_module, model_name)

            # Create all entity objects and save to db
            self._create_entities(model, entity_dict)
            print('Completed loading {}'.format(entity_type))

        # Create family relationships
        if 'family_relationship' not in skip_entities:
            self._create_family_relationships(entity_dict)

    def _create_entities(self, entity_model, entity_dict):
        """
        Create and save entities of a particular type to db
        """
        # Get entity type from model class name
        entity_type = entity_model.__tablename__

        print('Loading {}s ...'.format(entity_type))

        # For all entities of entity_type
        _ids = []
        entities = []
        for i, params in enumerate(entity_dict[entity_type]):
            print('\tLoading {} # {}'.format(entity_type, i))

            # Save ids
            _ids.append(params['_unique_id_val'])

            # Add foreign keys to input params
            # if this entity is linked to any others
            if '_links' in params:
                linked_entities = params['_links']
                for linked_entity, _params in linked_entities.items():
                    link_key = _params['link_key']
                    fk_col = _params['fk_col']
                    fk_value = self.entity_id_map[linked_entity][link_key]
                    params[fk_col] = fk_value
                    del params['_links']
            # Remove the private keys which were only needed for linking
            self._remove_extra_keys(params)

            # Add to list
            entities.append(entity_model(**params))

        # Add to session, save to database
        if entities:
            print('Adding {}s to the session'.format(entity_type))
            db.session.add_all(entities)
            print('Begin commit of {} {}s to db'.format(len(entities),
                                                        entity_type))
            db.session.commit()

            # Save kids first ids
            self._save_kf_ids(_ids, entity_type, entities)

    def _create_family_relationships(self, entity_dict):
        """
        Create and save family relationships for a proband
        Relationships are: mother - proband and father - proband
        """
        print('Loading {}s ...'.format('family_relationship'))

        families = entity_dict['family_relationship']
        for family in families:
            # Mother
            mother_id = self._get_kf_id('participant', family['mother'])
            # Father
            father_id = self._get_kf_id('participant', family['father'])
            # Proband
            proband_id = self._get_kf_id('participant', family['proband'])

            r1 = FamilyRelationship(participant_id=mother_id,
                                    relative_id=proband_id,
                                    participant_to_relative_relation='mother')

            r2 = FamilyRelationship(participant_id=father_id,
                                    relative_id=proband_id,
                                    participant_to_relative_relation='father')
            db.session.add_all([r1, r2])
        db.session.commit()

    def _get_kf_id(self, entity_type, key):
        return self.entity_id_map[entity_type].get(str(key))

    def _save_kf_ids(self, _ids, entity_type, entities):
        """
        Add to entity id map which maps original unique id in entity table
        to kf_id
        """
        # Save kf ids
        if self.entity_id_map.get(entity_type) is None:
            self.entity_id_map[entity_type] = {}

        for i, entity_obj in enumerate(entities):
            self.entity_id_map[entity_type][str(_ids[i])] = entity_obj.kf_id

    def _remove_extra_keys(self, params):
        """
        Remove private keys from kwargs dict.

        Private keys were only needed for linking entities and should
        not stay in the model's kwargs (params)
        """
        keys = ['_unique_id_val', '_unique_id_col', '_links']
        for k in keys:
            params.pop(k, None)
