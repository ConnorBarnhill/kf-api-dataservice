
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
        "_unique_id_col": {COL_VALUE: "investigator_name"},
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
        "external_id": {COL_NAME: "subject_id"},
        "is_proband": {COL_NAME: "is_proband"},
        "consent_type": {COL_NAME: "consent"},
        "ethnicity": {COL_NAME: "ethnicity"},
        "gender": {COL_NAME: "sex",
                   COL_VALUE: {"F": "female",
                               "M": "male"}},
        "race": {COL_NAME: "race",
                 COL_VALUE: {"mulitiple_races": "other"}},
        "_unique_id_col": {COL_VALUE: "subject_id"},
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
        "proband": {COL_NAME: "subject_id"},
        "mother": {COL_NAME: "mother"},
        "father": {COL_NAME: "father"},
        "_unique_id_col": {COL_VALUE: "subject_id"}
    },
    "phenotype": {
        "age_at_event_days": None,
        "phenotype": {COL_NAME: "phenotype"},
        "hpo_id": {COL_NAME: "hpo_id"},
        "observed": {COL_NAME: "observed"},
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "subject_id"}
            }
        },
        "_unique_id_col": {COL_VALUE: "phenotype_id"}
    },
    "outcome": {
        "age_at_event_days": None,
        "vital_status": {COL_NAME: "discharge_status"},
        "disease_related": None,
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "subject_id"}
            }
        },
        "_unique_id_col": {COL_VALUE: "outcome_id"}
    },
    "diagnosis": {
        "age_at_event_days": None,
        "diagnosis": {COL_NAME: "phenotype"},
        "tumor_location": None,
        "diagnosis_category": None,
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "subject_id"}
            }
        },
        "_unique_id_col": {COL_VALUE: "phenotype_id"}
    },
    "biospecimen": {
        "external_sample_id": {COL_NAME: "sample_id"},
        'external_aliquot_id': None,
        "tissue_type": {COL_NAME: "is_tumor",
                        COL_VALUE: {"Y": "Tumor",
                                    "N": "Normal"}},
        "composition": None,
        "anatomical_site": {COL_NAME: "body_site"},
        "age_at_event_days": None,
        "tumor_descriptor": None,
        "shipment_origin": None,
        "shipment_date": None,
        "shipment_destination": {COL_VALUE: "Broad Institute"},
        "analyte_type": {COL_NAME: "analyte_type"},
        "concentration_mg_per_ml": {COL_NAME: "concentration"},
        "volume_ml": {COL_NAME: "volume"},
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "subject_id"}
            }
        },
        "_unique_id_col": {COL_VALUE: "sample_id"}
    },
    "sequencing_experiment": {
        "external_id": {COL_NAME: "library-1_name"},
        "experiment_date": None,
        "experiment_strategy": {COL_VALUE: 'WGS'},
        "center": {COL_VALUE: "Broad Institute"},
        "library_name": {COL_NAME: "library-1_name"},
        "library_strand": None,
        "is_paired_end": {COL_VALUE: "True", COL_TYPE: "boolean"},
        "platform": {COL_VALUE: "Illumina"},
        "instrument_model": {COL_VALUE: "HiSeq X"},
        "max_insert_size": {COL_NAME: "max_insert_size"},
        "mean_insert_size": {COL_NAME: "mean_insert_size"},
        "mean_depth": {COL_NAME: "mean_depth"},
        "total_reads": {COL_NAME: "total_reads"},
        "mean_read_length": {COL_NAME: "mean_read_length"},
        "_unique_id_col": {COL_VALUE: "library-1_name"}
    },
    "genomic_file": {
        "uuid": {COL_NAME: "uuid"},
        "file_name": {COL_NAME: "file_name"},
        "file_size": {COL_NAME: "file_size"},
        "file_format": {COL_NAME: "file_format"},
        "file_url": {COL_NAME: "file_url"},
        "data_type": {COL_NAME: "data_type"},
        "md5sum": {COL_NAME: "md5sum"},
        "controlled_access": {COL_VALUE: "True", COL_TYPE: "boolean"},
        "_links": {
            'biospecimen': {
                'target_fk_col': {COL_VALUE: 'biospecimen_id'},
                'source_fk_col': {COL_NAME: "sample_id"}
            },
            'sequencing_experiment': {
                'target_fk_col': {COL_VALUE: 'sequencing_experiment_id'},
                'source_fk_col': {COL_NAME: "library-1_name"}
            }
        },
        "_unique_id_col": {COL_VALUE: "file_url"}
    }
}
