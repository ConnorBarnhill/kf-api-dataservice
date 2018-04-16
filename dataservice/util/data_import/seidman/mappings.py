
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
        "external_id": {COL_NAME: "famid"},
        "_unique_id_col": {COL_VALUE: "famid"}
    },
    "participant": {
        "external_id": {COL_NAME: "subjid"},
        "is_proband": {COL_NAME: "is_proband"},
        "_unique_id_col": {COL_VALUE: "subjid"},
        "ethnicity": {COL_NAME: "ethnicity",
                      COL_VALUE: {"Yes": "hispanic or latino",
                                  "No": "not hispanic or latino"}},

        "gender": {COL_NAME: "sex",
                   COL_VALUE: {"F": "female",
                               "M": "male"}},

        "race": {COL_NAME: "race",
                 COL_VALUE: {"More than one race": "other"}},
        "_links": {
            'family': {
                'target_fk_col': {COL_VALUE: 'family_id'},
                'source_fk_col': {COL_NAME: "famid"}
            },
            'study': {
                'target_fk_col': {COL_VALUE: 'study_id'},
                'source_fk_col': {COL_NAME: "study_id"}
            }
        }
    },
    "family_relationship": {
        "proband": {COL_NAME: "subjid"},
        "mother": {COL_NAME: "mother"},
        "father": {COL_NAME: "father"},
        "_unique_id_col": {COL_VALUE: "famid"}
    },
    "phenotype": {
        "age_at_event_days": {COL_NAME: "latest_exam_age"},
        "phenotype": {COL_NAME: "phenotype"},
        "hpo_id": {COL_NAME: "hpo_id"},
        "observed": {COL_NAME: "observed"},
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "subjid"}
            }
        },
        "_unique_id_col": {COL_VALUE: "phenotype_id"}
    },
    "diagnosis": {
        "age_at_event_days": None,
        "diagnosis": {COL_NAME: "diagnosis"},
        "tumor_location": None,
        "diagnosis_category": None,
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "subjid"}
            }
        },
        "_unique_id_col": {COL_VALUE: "diagnosis_id"}
    },
    "biospecimen": {
        "external_sample_id": {COL_NAME: "sampid"},
        "external_aliquot_id": {COL_NAME: "barcode"},
        "tissue_type": {COL_VALUE: "Normal"},
        "composition": {COL_VALUE: "Peripheral Whole Blood"},
        "anatomical_site": None,
        "age_at_event_days": None,
        "tumor_descriptor": None,
        "shipment_origin": None,
        "shipment_date": None,
        "shipment_destination": {COL_VALUE: "Baylor College of Medicine"},
        "analyte_type": {COL_VALUE: "DNA"},
        "concentration_mg_per_ml": {COL_NAME: "concentration_ng_per_ul"},
        "volume_ml": {COL_NAME: "initial_volume_microliters"},
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "subjid"}
            }
        },
        "_unique_id_col": {COL_VALUE: "sampid"}
    },
    "sequencing_experiment": {
        "external_id": {COL_NAME: "rg_barcode"},
        "experiment_date": {COL_NAME: "date"},
        "experiment_strategy": {COL_NAME: "library_strategy"},
        "center": {COL_VALUE: "Baylor College of Medicine"},
        "library_name": {COL_NAME: "library_name"},
        "library_strand": None,
        "is_paired_end": {COL_VALUE: "True", COL_TYPE: "boolean"},
        "platform": {COL_VALUE: "Illumina"},
        "instrument_model": {COL_NAME: "instrument"},
        "max_insert_size": {COL_NAME: "max_insert_size"},
        "mean_insert_size": {COL_NAME: "mean_insert_size"},
        "mean_depth": None,
        "total_reads": None,
        "mean_read_length": {COL_NAME: "mean_read_length"},
        "_unique_id_col": {COL_VALUE: "rg_barcode"}
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
            'sequencing_experiment': {
                'target_fk_col': {COL_VALUE: 'sequencing_experiment_id'},
                'source_fk_col': {COL_NAME: "rg_barcode"}
            }
        },
        "_unique_id_col": {COL_VALUE: "uuid"}
    }
}
