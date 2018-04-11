import os
from importlib import import_module
from pprint import pprint

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

from dataservice.extensions import db
from dataservice import create_app
from dataservice.util.data_import.utils import to_camel_case
from dataservice.util.data_import.etl.defaults import DEFAULT_ENTITY_TYPES
from dataservice.api.family_relationship.models import FamilyRelationship


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
        db.create_all()

    def teardown(self):
        """
        Clean up
        """
        db.session.remove()
        self.app_context.pop()

    def drop_all(self, study_external_id):
        """
        Delete all data related to a study
        """
        from dataservice.api.study.models import Study
        from dataservice.api.investigator.models import Investigator

        studies = Study.query.filter_by(external_id=study_external_id).all()
        for study in studies:
            investigator_id = study.investigator_id

            # Delete study
            db.session.delete(study)

            # Delete investigator
            if investigator_id:
                investigator = Investigator.query.get(investigator_id)
                db.session.delete(investigator)

        db.session.commit()

    def run(self, entity_dict, **kwargs):
        """
        Load all entities into db
        """
        entity_types = kwargs.get('entity_types', DEFAULT_ENTITY_TYPES)
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
            print('Completed loading {}\n'.format(entity_type))

        # Create family relationships
        if 'family_relationship' in entity_types:
            self._create_family_relationships(entity_dict)

    def _create_entities(self, entity_model, entity_dict):
        """
        Create and save entities of a particular type to db
        """
        # Get entity type from model class name
        entity_type = entity_model.__tablename__

        print('Loading {}s ...'.format(entity_type))

        # For all entities of entity_type
        entities_to_load = entity_dict.get(entity_type)
        if not entities_to_load:
            print('\nExpected to load {0} but 0 {0}s were found to load.\n'.
                  format(entity_type))
            return

        _ids = []
        entities = []
        for i, params in enumerate(entities_to_load):
            # print('\tCreating {} # {}'.format(entity_type, i))

            # Save ids
            _ids.append(params['_unique_id_val'])

            # Add foreign keys to input params
            # if this entity is linked to any others
            if '_links' in params:
                linked_entities = params['_links']
                for linked_entity, _params in linked_entities.items():
                    if (('source_fk_col' not in _params) and
                            ('target_fk_col' not in params)):
                        print('Error loading {0}. Missing foreign key info.'
                              ' Check mappings for {0}'
                              .format(entity_type))
                        continue
                    source_fk_col = _params['source_fk_col']
                    target_fk_col = _params['target_fk_col']
                    try:
                        fk_value = self.entity_id_map[
                            linked_entity][source_fk_col]
                    except KeyError as e:
                        pprint('Error loading {}, linked entity {} not found'
                               .format(params, source_fk_col))
                        params = None
                        continue
                    params[target_fk_col] = fk_value
                    del params['_links']

            if params:
                # Remove the private keys which were only needed for linking
                self._remove_extra_keys(params)
                # Add to list
                entities.append(entity_model(**params))

        # Save to db
        if entities:
            # Add to session and commit
            # self.load_entities(entity_type, entities)
            self.load_all(entity_type, entities)
            # Save kids first ids
            self._save_kf_ids(_ids, entity_type, entities)

    def load_entities(self, entity_type, entities):
        """
        Save entities to the database
        """
        chunk_size = 1000
        if len(entities) > chunk_size:
            self.batch_load(entity_type, entities, chunk_size)
        else:
            self.load_all(entity_type, entities)
        # db.session.bulk_save_objects(entities, return_defaults=True)

    def load_all(self, entity_type, entities):
        """
        Add all entities into single session, and commit in single transaction
        """
        print('Adding {} {}s to the session'.format(len(entities),
                                                    entity_type))
        db.session.add_all(entities)
        print('Begin commit of {} {}s to db'.format(len(entities),
                                                    entity_type))
        # Save to db
        self._db_commit(len(entities), entity_type)

    def batch_load(self, entity_type, entities, chunk_size=1000):
        """
        Add chunks of entities into a session, flush chunk, commit once
        """
        n = len(entities)
        if chunk_size > n:
            chunk_size = max(n, chunk_size)

        for i in range(0, n, chunk_size):
            chunk = entities[i - chunk_size:i]
            if chunk:
                start = i - chunk_size + 1
                print('Adding {}:{} {}s to session'.format(start, i,
                                                           entity_type))
                db.session.add_all(entities[start:i])
                print('Flushing {} {}s'.format(chunk_size, entity_type))
                self._db_commit(chunk_size, entity_type)

        # Save remainder entities
        remaining = entities[0:1] + entities[i:]
        print('Adding remaining {} {}s to session'.format(len(remaining),
                                                          entity_type))
        db.session.add_all(remaining)
        # Save to db
        self._db_commit(len(remaining), entity_type)

    def _create_family_relationships(self, entity_dict, relation_keys=None):
        """
        Create and save family relationships for a proband
        Relationships are: mother - proband and father - proband
        """
        print('Loading {}s ...'.format('family_relationship'))

        entities_to_load = entity_dict.get('family_relationship')
        if not entities_to_load:
            print('\nExpected to load {0} but 0 {0}s were found to load.\n'.
                  format('family_relationship'))
            return

        if not relation_keys:
            relation_keys = ['mother', 'father']

        entities = []
        for family in entities_to_load:
            for r in relation_keys:
                rel = self._create_family_relationship(r, family)
                if rel:
                    entities.append(rel)

        self.load_all('family_relationship', entities)

    def _create_family_relationship(self, relation_key, family):
        """
        Create a family relationship
        """
        if family.get(relation_key) and family.get('proband'):
            p_id = self._get_kf_id(
                'participant', family[relation_key])
            r_id = self._get_kf_id(
                'participant', family['proband'])
            if p_id and r_id:
                return FamilyRelationship(
                    participant_id=p_id,
                    relative_id=r_id,
                    participant_to_relative_relation=relation_key)

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

    def _db_commit(self, count, entity_type):
        print('Committing {} {}s'.format(count, entity_type))
        try:
            db.session.commit()
        except IntegrityError as e:
            print('Failed loading of {}'.format(entity_type))
            print(e)
            db.session.rollback()
