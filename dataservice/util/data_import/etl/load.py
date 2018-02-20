import os
from pprint import pprint

from dataservice.extensions import db
from dataservice import create_app

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

    def run(self, entity_dict):
        # Create study
        self._create_study()
        # Create participants
        self._create_participants(entity_dict)
        # Create demographics
        self._create_entities(Demographic, entity_dict)
        # Create diagnoses
        self._create_entities(Diagnosis, entity_dict)
        # Create samples
        self._create_entities(Sample, entity_dict)
        # Create aliquot
        self._create_entities(Aliquot, entity_dict)
        # Create sequencing_experiment
        self._create_entities(SequencingExperiment, entity_dict)

    def _create_study(self, **kwargs):
        """
        Create study
        """
        # Study
        # https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id=phs001138.v1.p2
        kwargs = {
            'data_access_authority': 'dbGaP',
            'external_id': 'phs001138',
            'version': 'v1.p2',
            'name': 'National Heart, Lung, and Blood Institute (NHLBI)'
            'Bench to Bassinet Program: The Gabriella Miller Kids First'
            'Pediatric Research Program of the Pediatric Cardiac Genetics'
            'Consortium (PCGC)',
            'attribution': 'https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/'
            'GetAcknowledgementStatement.cgi?study_id=phs001138.v1.p2'
        }
        study = Study(**kwargs)

        # Save to db
        db.session.add(study)
        db.session.commit()

        self.entity_id_map['study'] = {kwargs['external_id']: study.kf_id}

    def _create_participants(self, entity_dict):
        """
        Create and save participants to db
        """
        # Get study id
        study_id = self.entity_id_map['study']['phs001138']

        # Create and save participants
        _ids = []
        participants = []
        for params in entity_dict['participant']:
            # Save ids
            _ids.append(params['_unique_id_val'])
            # Remove the private keys which were only needed for linking
            self._remove_extra_keys(params)

            # Add study id
            params['study_id'] = study_id
            participants.append(Participant(**params))

        # Save to db
        db.session.add_all(participants)
        db.session.commit()

        self._save_kf_ids(_ids, 'participant', participants)

    def _create_entities(self, entity_model, entity_dict):
        """
        Create and save entities of a particular type to db
        """
        # Get entity type from model class name
        entity_type = entity_model.__tablename__

        # For all entities of entity_type
        _ids = []
        entities = []
        for params in entity_dict[entity_type]:
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
        db.session.add_all(entities)
        db.session.commit()

        # Save kids first ids
        self._save_kf_ids(_ids, entity_type, entities)

    def _create_family_relationships(self, entity_dict):
        """
        Create family relationships for a proband, mother, and father
        """
        pass

    def _save_kf_ids(self, _ids, entity_type, entities):
        # Save kf ids
        if self.entity_id_map.get(entity_type) is None:
            self.entity_id_map[entity_type] = {}

        for i, entity_obj in enumerate(entities):
            self.entity_id_map[entity_type][_ids[i]] = entity_obj.kf_id

    def _remove_extra_keys(self, params):
        keys = ['_unique_id_val', '_unique_id_col', '_links']
        for k in keys:
            params.pop(k, None)
