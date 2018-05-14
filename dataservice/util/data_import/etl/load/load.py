import os
from importlib import import_module
from pprint import pprint

from sqlalchemy.exc import IntegrityError

from dataservice.extensions import db
from dataservice import create_app
from dataservice.util.data_import.utils import (
    to_camel_case,
    read_json,
    write_json
)
from dataservice.util.data_import.config import (
    ETL_PACKAGE_NAME_KEY,
    KF_ID_CACHE_FNAME,
    IMPORT_DATA_OP,
    UPDATE_DATA_OP
)
from dataservice.api.family_relationship.models import FamilyRelationship


class BaseLoader(object):

    def __init__(self, config, config_name=None):
        self.config = config
        if not config_name:
            config_name = os.environ.get('FLASK_CONFIG', 'default')
        self.setup(config_name)
        self.kf_id_cache = {}

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

    def drop_all(self, **kwargs):
        """
        Delete all data related to a study
        """
        from dataservice.api.study.models import Study
        from dataservice.api.investigator.models import Investigator

        studies = Study.query.filter_by(**kwargs).all()
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
            # Import ORM model class
            model_cls = self._import_model_cls(entity_type)

            # Create all entity objects and save to db
            _ids, entities_to_load = self._build_entities(model_cls,
                                                          entity_dict)
            if not entities_to_load:
                continue

            # Create new
            if operation == IMPORT_DATA_OP:
                self._create_all(model_cls, _ids, entities_to_load)
            # Update existing
            elif operation == UPDATE_DATA_OP:
                self._update_all(model_cls, _ids, entities_to_load)

        if 'family_relationship' in entity_types:
            _ids_fr, entities_to_load_fr = self._build_family_relationships(
                entity_dict)

            if not entities_to_load_fr:
                return

            # Create new
            if operation == IMPORT_DATA_OP:
                self._create_all_fr(_ids_fr,
                                    entities_to_load_fr)
            # Update existing
            elif operation == UPDATE_DATA_OP:
                self._update_all(FamilyRelationship, _ids_fr,
                                 entities_to_load_fr)

        print('Completed loading {}\n'.format(entity_type))

    def _create_all(self, model_cls, _ids, entities_params):
        """
        Create new entities in db
        """
        entity_type = model_cls.__tablename__
        entities = [model_cls(**params) for params in entities_params]
        # Add to session and commit
        self.load_all(entity_type, entities)
        # Save kids first ids
        self._save_kf_ids(_ids, entity_type, entities)

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

    def _update_all(self, model_cls, _ids, entities_params):
        """
        Update existing entities in db
        """
        count = len(entities_params)
        entity_type = model_cls.__tablename__
        print('Begin update of {} {}s in db'.format(count,
                                                    entity_type))

        kf_ids = self.kf_id_cache.get(entity_type)
        if not kf_ids:
            print('No {}s found in db'
                  ' not able to update entities of this type'.format(
                      entity_type
                  ))
            return

        for i, params in enumerate(entities_params):
            _id = str(_ids[i])

            kf_id = kf_ids.get(_id)
            if not kf_id:
                print('{} for {} not found in kf_id cache,'
                      ' not able to update entity'.format(_id,
                                                          entity_type))
                continue

            # Query for existing object
            obj = model_cls.query.get(kf_id)
            if not obj:
                count += -1
                print('{} {} not found in db'
                      ' not able to update entity'.format(
                          entity_type, kf_id
                      ))
                continue

            # Update properties
            for k, v in params.items():
                if hasattr(obj, k):
                    setattr(obj, k, v)

        self._db_commit(count, entity_type)

    def _build_entities(self, model_cls, entity_dict):
        """
        Build entity payloads of a particular entity_type to db
        """
        # Get entity type from model class name
        entity_type = model_cls.__tablename__

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
        linked_entities = params.get('_links')
        if linked_entities:
            for linked_entity, _params in linked_entities.items():
                if not (_params.get('source_fk_col') and
                        _params.get('target_fk_col')):
                    print('Error loading {0}. Missing foreign key info.'
                          ' Check mappings for {0}'.format(entity_type))
                    continue
                source_fk_val = str(_params['source_fk_col'])
                target_fk_col = _params['target_fk_col']
                try:
                    target_fk_value = self.kf_id_cache[
                        linked_entity][source_fk_val]
                except KeyError as e:
                    pprint('Error loading {}, linked entity {} not found'
                           .format(params, source_fk_val))
                    params = None
                    continue
                if params:
                    params[target_fk_col] = target_fk_value

            if params:
                params.pop('_links', None)

    def _create_all_fr(self, _ids, entities_params):
        self._create_all(FamilyRelationship, _ids, entities_params)

    def _build_family_relationships(self, entity_dict, relation_keys=None):
        """
        Create and save family relationships for a proband
        Relationships are: mother - proband and father - proband
        """
        print('\nLoading {}s ...'.format('family_relationship'))

        entities_to_load = entity_dict.get('family_relationship')
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

    def _save_kf_ids(self, _ids, entity_type, entities):
        """
        Add to entity id map which maps original unique id in entity table
        to kf_id
        """
        # Save kf ids
        if self.kf_id_cache.get(entity_type) is None:
            self.kf_id_cache[entity_type] = {}

        for i, entity_obj in enumerate(entities):
            self.kf_id_cache[entity_type][str(_ids[i])] = entity_obj.kf_id

        # Write to file
        self._write_kf_id_cache()

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
        """
        Commit transaction to db
        """
        print('Committing {} {}s'.format(count, entity_type))
        try:
            db.session.commit()
        except IntegrityError as e:
            print('Failed loading of {}'.format(entity_type))
            print(e)
            db.session.rollback()

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

    def _import_model_cls(self, entity_type):
        """
        Dynamically import entity model class
        """
        model_name = to_camel_case(entity_type)
        model_module_path = 'dataservice.api.{}.models'.format(
            entity_type)
        models_module = import_module(model_module_path)
        return getattr(models_module, model_name)

    def load_entities(self, entity_type, entities):
        """
        Save entities to the database
        """
        chunk_size = 1000
        if len(entities) > chunk_size:
            self.batch_load(entity_type, entities, chunk_size)
        else:
            self.load_all(entity_type, entities)

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
