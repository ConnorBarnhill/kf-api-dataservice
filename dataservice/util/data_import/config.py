from dataservice.util.data_import.utils import to_camel_case


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

DEFAULT_ENTITY_TYPES = list(ENTITY_ENDPOINT_MAP.keys())

ENTITY_MODEL_MAP = {to_camel_case(k): k
                    for k in ENTITY_ENDPOINT_MAP}


IMPORT_DATA_OP = 'import_data'
UPDATE_DATA_OP = 'update_data'
ETL_PACKAGE_NAME_KEY = '__etl_package_name'
KF_ID_CACHE_FNAME = 'cache_kf_id.json'
SCHEMA_URL = 'http://localhost:5000/swagger'

COL_NAME_KEY = "$col_name"
COL_VALUE_KEY = "$col_value"
COL_TYPE_KEY = "$col_type"
NOT_REPORTED = 'Not Reported'
