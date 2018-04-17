"""
A config file used to drive the transformation from csv to dict
Each csv row in the csv file will be transformed into a single dict

Each key in mappings_dict must match the name of the table_name of
entity to be loaded.

Each entity dict in mappings_dict has a set of keys that must match
the attributes of the entity's SQLAlchemy model.

The values of these keys can be set to the following:
    - a dict specifying which column in the csv row to lookup the value
        Use {COL_NAME: <name of col in input csv>}
    - a constant value
        Use {COL_VALUE: <constant value>}
    - a constant value that will be mapped to another value via the provided
    dict (see participant gender as an example)
        Use {COL_NAME: <name of col in input csv>,
            COL_VALUE: {<a potential value in the input csv>: <mapped value>,
                        <a potential value in the input csv>: <mapped value>}}

Every entity in mappings_dict must specify which column should be used to
uniquely identify the records in the table. Specify the unique columns like
this:
    "_unique_id_col": {COL_VALUE: <name of column in original table>}

Links - Every entity must specify its parent entities using links.
For example, participant has two parent entities: study and family. Thus,
in the participant mapping, two links must be specified.

A link is defined with the following format:

"_links": {
    '<name of Kids First Entity>': {
        'target_fk_col': {COL_VALUE: '<name of foreign key in KF entity>'},
        'source_fk_col': {COL_NAME: '<name of col in source csv>'}
    }
}
"""

COL_NAME = "$col_name"
COL_VALUE = "$col_value"
COL_TYPE = "$col_type"

mappings_dict = {
    "study": {
        'data_access_authority': {COL_NAME: 'data_access_authority'},
        'external_id': {COL_NAME: 'study_id'},
        'version': {COL_NAME: 'study_version'},
        'name': {COL_NAME: 'study_name'},
        'attribution': {COL_NAME: 'attribution'},
        "_links": {
            'investigator': {
                'target_fk_col': {COL_VALUE: 'investigator_id'},
                'source_fk_col': {COL_NAME: "investigator_name"}
            }
        },
        "_unique_id_col": {COL_VALUE: "study_id"}
    },
    "investigator": {
        'name': {COL_NAME: 'investigator_name'},
        'institution': {COL_NAME: 'institution'},
        "_unique_id_col": {COL_VALUE: "investigator_name"}
    },
    "study_file": {
        "file_name": {COL_NAME: "study_file_name"},
        "_unique_id_col": {COL_VALUE: "study_file_name"},
        "_links": {
            'study': {
                'target_fk_col': {COL_VALUE: 'study_id'},
                'source_fk_col': {COL_NAME: "study_id"}
            }
        }
    },
    'family': {
        "external_id": {COL_NAME: "family_id"},
        "_unique_id_col": {COL_VALUE: "family_id"}
    },
    "participant": {
        "external_id": {COL_NAME: "individual_name"},
        "is_proband": {COL_NAME: "relationship_to_proband"},
        "ethnicity": "unknown",
        "gender": {COL_NAME: "gender",
                   COL_VALUE: {"Female": "female",
                               "Male": "male"}},
        "race": "unknown",
        "_unique_id_col": {COL_VALUE: "individual_name"},
        "_links": {
            'family': {
                'target_fk_col': {COL_VALUE: 'family_id'},
                'source_fk_col': {COL_NAME: "family_id"}
            },
            'study': {
                'target_fk_col': {COL_VALUE: 'study_id'},
                'source_fk_col': {COL_NAME: "study_id"}
            }
        }
    },
    "family_relationship": {
        "mother": {COL_NAME: "Mother"},
        "father": {COL_NAME: "Father"},
        "proband": {COL_NAME: "Self/Case"},
        "_unique_id_col": {COL_VALUE: "rel_id"}
    },
    "phenotype": {
        "age_at_event_days": {COL_NAME: "age_at_diagnosis_(days)"},
        "phenotype": {COL_NAME: "phenotype"},
        "hpo_id": {COL_NAME: "hpo_id"},
        "observed": {COL_NAME: "observed"},
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "individual_name"}
            }
        },
        "_unique_id_col": {COL_VALUE: "individual_name"}
    },
    "diagnosis": {
        "age_at_event_days": {COL_NAME: "age_at_diagnosis_(days)"},
        "diagnosis": {COL_NAME: "morphology"},
        "tumor_location": None,
        "diagnosis_category": {COL_VALUE: 'cancer'},
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "individual_name"}
            }
        },
        "_unique_id_col": {COL_VALUE: "individual_name"}
    },
    "biospecimen": {
        "external_sample_id": {COL_NAME: "sample_name"},
        "external_aliquot_id": None,
        "tissue_type": None,
        "composition": {COL_NAME: "sample_type"},
        "anatomical_site": {COL_NAME: "topography"},
        "age_at_event_days": None,
        "tumor_descriptor": None,
        "shipment_origin": None,
        "shipment_date": None,
        "shipment_destination": {COL_VALUE: "Washington University"},
        "analyte_type": {COL_VALUE: "DNA"},
        "concentration_mg_per_ml": None,
        "volume_ml": None,
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "individual_name"}
            }
        },
        "_unique_id_col": {COL_VALUE: "sample_name"}
    },
    "sequencing_experiment": {
        "external_id": {COL_NAME: "build_id"},
        "experiment_date": None,
        "experiment_strategy": {COL_VALUE: "WGS"},
        "center": {COL_VALUE: "Washington University"},
        "library_name": None,
        "library_strand": None,
        "is_paired_end": {COL_VALUE: "True", COL_TYPE: "boolean"},
        "platform": {COL_VALUE: "Illumina"},
        "instrument_model": None,
        "max_insert_size": None,
        "mean_insert_size": {COL_NAME: "mean_insert_size"},
        "mean_depth": None,
        "total_reads": {COL_NAME: "pf_reads"},
        "mean_read_length": None,
        "_unique_id_col": {COL_VALUE: "build_id"}
    },
    "genomic_file": {
        "latest_did": {COL_NAME: "latest_did"},
        # "file_name": {COL_NAME: "file_name"},
        # "file_size": {COL_NAME: "file_size"},
        "file_format": {COL_NAME: "file_format"},
        "is_harmonized": {COL_NAME: "is_harmonized"},
        "reference_genome": None,
        # "file_url": {COL_NAME: "file_url"},
        "data_type": {COL_NAME: "data_type"},
        # "md5sum": {COL_NAME: "md5sum"},
        "controlled_access": {COL_VALUE: "True", COL_TYPE: "boolean"},
        "_links": {
            'biospecimen': {
                'target_fk_col': {COL_VALUE: 'biospecimen_id'},
                'source_fk_col': {COL_NAME: "sample_name"}
            },
            'sequencing_experiment': {
                'target_fk_col': {COL_VALUE: 'sequencing_experiment_id'},
                'source_fk_col': {COL_NAME: "build_id"}
            }
        },
        "_unique_id_col": {COL_VALUE: "file_name"}
    }
}
