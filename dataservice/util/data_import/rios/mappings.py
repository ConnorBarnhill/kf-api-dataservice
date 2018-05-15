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
        'kf_id': {COL_VALUE: 'SD_VMK01DZK'},
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
        "external_id": None,
        'name': {COL_NAME: 'investigator_name'},
        'institution': {COL_NAME: 'institution'},
        "_unique_id_col": {COL_VALUE: "investigator_name"},
    },
    "study_file": {
        "external_id": None,
        "latest_did": {COL_NAME: "latest_did"},
        "file_name": {COL_NAME: "study_file_name"},
        "data_type": None,
        "file_format": None,
        'size': {COL_NAME: 'size'},
        'urls': {COL_NAME: 'urls'},
        'hashes': {COL_NAME: 'hashes'},
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
        "external_id": {COL_NAME: "subject_id"},
        "is_proband": {COL_NAME: "proband"},
        "consent_type": {COL_NAME: "consent"},
        "_unique_id_col": {COL_VALUE: "subject_id"},
        "ethnicity": {COL_NAME: "ethnicity"},
        "gender": {COL_NAME: "sex"},
        "race": {COL_NAME: "race",
                 COL_VALUE: {"More than one race": "Other",
                             "Caucasian": "White",
                             "African American": "Black or African American",
                             "Hispanic": "American Indian or Alaska Native",
                             }},
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
        "external_id": None,
        "proband": {COL_NAME: "subject_id"},
        "mother": {COL_NAME: "mother"},
        "father": {COL_NAME: "father"},
        "_unique_id_col": {COL_VALUE: "subject_id"}
    },
    "phenotype": {
        "age_at_event_days": None,
        "source_text_phenotype": {COL_NAME: "phenotype"},
        "hpo_id_phenotype": {COL_NAME: "hpo_id"},
        "snomed_id_phenotype": None,
        "observed": {COL_NAME: "observed"},
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "subject_id"}
            }
        },
        "_unique_id_col": {COL_VALUE: "subject_id"}
    },
    "outcome": {
        "age_at_event_days": None,
        "external_id": None,
        "vital_status": None,
        "disease_related": None,
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "subject_id"}
            }
        },
        "_unique_id_col": None
    },
    "diagnosis": {
        "age_at_event_days": None,
        "source_text_diagnosis": {COL_NAME: "diagnosis"},
        "source_text_tumor_location": None,
        "mondo_id_diagnosis": None,
        "icd_id_diagnosis": None,
        "uberon_id_tumor_location": None,
        "ncit_id_diagnosis": None,
        "spatial_descriptor": None,
        "diagnosis_category": {COL_VALUE: 'Structural Birth Defect'},
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "subject_id"}
            }
        },
        "_unique_id_col": {COL_VALUE: "subject_id"}
    },
    "biospecimen": {
        "external_sample_id": {COL_NAME: "sample_id"},
        "external_aliquot_id": None,
        "source_text_tissue_type": {COL_NAME: "is_tumor",
                                    COL_VALUE: {"Y": "Tumor",
                                                "N": "Normal"}},
        "composition": {COL_NAME: "histological_type"},
        "source_text_anatomical_site": {COL_NAME: "body_site"},
        "age_at_event_days": None,
        "source_text_tumor_descriptor": None,
        "ncit_id_tissue_type": None,
        "ncit_id_anatomical_site": None,
        "spatial_descriptor": None,
        "shipment_origin": {COL_NAME: "sample_source"},
        "shipment_date": None,
        "sequencing_center_id": {COL_VALUE: "SC_X1N69WJM"},
        "analyte_type": {COL_NAME: "analyte_type"},
        "concentration_mg_per_ml": None,
        "volume_ml": None,
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "subject_id"}
            }
        },
        "_unique_id_col": {COL_VALUE: "sample_id"}
    },
    "sequencing_experiment": {
        "external_id": {COL_NAME: "seq_exp_id"},
        "experiment_date": None,
        "experiment_strategy": {COL_VALUE: "WGS"},
        "sequencing_center_id": {COL_VALUE: "SC_X1N69WJM"},
        "library_name": {COL_NAME: "library"},
        "library_strand": None,
        "is_paired_end": {COL_VALUE: "True", COL_TYPE: "boolean"},
        "platform": {COL_VALUE: "Illumina"},
        "instrument_model": {COL_VALUE: "HiSeq X"},
        "max_insert_size": None,
        "mean_insert_size": None,
        "mean_depth": None,
        "total_reads": None,
        "mean_read_length": None,
        "_unique_id_col": {COL_VALUE: "seq_exp_id"}
    },
    "genomic_file": {
        "external_id": None,
        "availability": None,
        "latest_did": {COL_NAME: "latest_did"},
        "file_name": {COL_NAME: "file_name"},
        "size": {COL_NAME: "size"},
        "file_format": {COL_NAME: "file_format"},
        "is_harmonized": {COL_NAME: "is_harmonized"},
        "reference_genome": None,
        "urls": {COL_NAME: "urls"},
        "data_type": {COL_NAME: "data_type"},
        "hashes": {COL_NAME: "hashes"},
        "controlled_access": {COL_VALUE: "True", COL_TYPE: "boolean"},
        "_links": {
            'biospecimen': {
                'target_fk_col': {COL_VALUE: 'biospecimen_id'},
                'source_fk_col': {COL_NAME: "sample_id"}
            },
            'sequencing_experiment': {
                'target_fk_col': {COL_VALUE: 'sequencing_experiment_id'},
                'source_fk_col': {COL_NAME: "seq_exp_id"}
            }
        },
        "_unique_id_col": {COL_VALUE: "file_url"}
    }
}
