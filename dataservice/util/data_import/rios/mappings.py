
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
    "participant": {
        "external_id": {COL_NAME: "subject_id"},
        "is_proband": {COL_NAME: "proband"},
        "consent_type": {COL_NAME: "consent"},
        "family_id": {COL_NAME: "family_id"},
        "_unique_id_col": {COL_VALUE: "subject_id"},
        "_links": {
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
        "_unique_id_col": {COL_VALUE: "subject_id"}
    },
    "demographic": {
        "ethnicity": {COL_NAME: "ethnicity",
                      COL_VALUE: {"Yes": "hispanic or latino",
                                  "No": "not hispanic or latino"}},

        "gender": {COL_NAME: "sex"},
        "race": {COL_NAME: "race",
                 COL_VALUE: {"More than one race": "other"}},
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "subject_id"}
            }
        },
        "_unique_id_col": {COL_VALUE: "subject_id"}
    },
    "diagnosis": {
        "age_at_event_days": None,
        "diagnosis": {COL_NAME: "diagnosis"},
        "tumor_location": None,
        "diagnosis_category": None,
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "subject_id"}
            }
        },
        "_unique_id_col": {COL_VALUE: "subject_id"}
    },
    "sample": {
        "external_id": {COL_NAME: "sample_id"},
        "composition": {COL_NAME: "histological_type"},
        "age_at_event_days": None,
        "tumor_descriptor": None,
        "anatomical_site": {COL_NAME: "body_site"},
        "tissue_type": {COL_NAME: "is_tumor",
                        COL_VALUE: {"Y": "Tumor",
                                    "N": "Normal"}},
        "_links": {
            'participant': {
                'target_fk_col': {COL_VALUE: 'participant_id'},
                'source_fk_col': {COL_NAME: "subject_id"}
            }
        },
        "_unique_id_col": {COL_VALUE: "sample_id"}
    },
    "aliquot": {
        "external_id": {COL_NAME: "sample_id"},
        "analyte_type": {COL_NAME: "analyte_type"},
        "volume": None,
        "concentration": None,
        "shipment_destination": {COL_VALUE: "HudsonAlpha Institute"
                                 " for Biotechnology"},
        "shipment_origin": {COL_NAME: "sample_source"},
        "shipment_date": None,
        "_links": {
            'sample': {
                'target_fk_col': {COL_VALUE: 'sample_id'},
                'source_fk_col': {COL_NAME: "sample_id"}
            }
        },
        "_unique_id_col": {COL_VALUE: "sample_id"}
    },
    "sequencing_experiment": {
        "external_id": {COL_NAME: "sample_description"},
        "experiment_date": None,
        "experiment_strategy": {COL_NAME: "sample_use",
                                COL_VALUE: {"Seq_DNA_WholeGenome": "WGS"}},
        "center": {COL_VALUE: "HudsonAlpha Institute for Biotechnology"},
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
        "_links": {
            'aliquot': {
                'target_fk_col': {COL_VALUE: 'aliquot_id'},
                'source_fk_col': {COL_NAME: "sample_description"}
            }
        },
        "_unique_id_col": {COL_VALUE: "seq_exp_id"}
    },
    "genomic_file": {
        "uuid": {COL_NAME: "uuid"},
        "file_name": {COL_NAME: "file_name"},
        "file_size": {COL_NAME: "file_size"},
        "file_format": {COL_NAME: "file_format"},
        "file_url": {COL_NAME: "file_url"},
        "data_type": {COL_NAME: "data_type"},
        "md5sum": {COL_NAME: "md5sum"},
        "controlled_access": None,
        "_links": {
            'sequencing_experiment': {
                'target_fk_col': {COL_VALUE: 'sequencing_experiment_id'},
                'source_fk_col': {COL_NAME: "seq_exp_id"}
            }
        },
        "_unique_id_col": {COL_VALUE: "file_url"}
    }
}