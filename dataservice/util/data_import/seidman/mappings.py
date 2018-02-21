
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
        "_unique_id_col": {COL_VALUE: "study_id"},
    },
    "participant": {
        "external_id": {COL_NAME: "subjid"},
        "is_proband": {COL_NAME: "is_proband"},
        "family_id": {COL_NAME: "famid"},
        "_unique_id_col": {COL_VALUE: "subjid"},
        "_links": {
            'study': {
                'fk_col': {COL_VALUE: 'study_id'},
                'link_key': {COL_NAME: "study_id"}
            }
        }
    },
    "family_relationship": {
        "proband": {COL_NAME: "subjid"},
        "mother": {COL_NAME: "mother"},
        "father": {COL_NAME: "father"},
        "_unique_id_col": {COL_VALUE: "famid"}
    },
    "demographic": {
        "ethnicity": {COL_NAME: "ethnicity",
                      COL_VALUE: {"Yes": "hispanic or latino",
                                  "No": "not hispanic or latino"}},

        "gender": {COL_NAME: "sex",
                   COL_VALUE: {"F": "female",
                               "M": "male"}},

        "race": {COL_NAME: "race",
                 COL_VALUE: {"More than one race": "other"}},
        "_links": {
            'participant': {
                'fk_col': {COL_VALUE: 'participant_id'},
                'link_key': {COL_NAME: "subjid"}
            }
        },
        "_unique_id_col": {COL_VALUE: "demographic_id"}
    },
    "diagnosis": {
        "age_at_event_days": None,
        "diagnosis": {COL_NAME: "diagnosis"},
        "tumor_location": None,
        "diagnosis_category": None,
        "_links": {
            'participant': {
                'fk_col': {COL_VALUE: 'participant_id'},
                'link_key': {COL_NAME: "subjid"}
            }
        },
        "_unique_id_col": {COL_VALUE: "diagnosis_id"}
    },
    "sample": {
        "external_id": {COL_NAME: "sample_name"},
        "composition": {COL_VALUE: "Peripheral Whole Blood"},
        "age_at_event_days": None,
        "tumor_descriptor": None,
        "anatomical_site": None,
        "tissue_type": {COL_VALUE: "Normal"},
        "_links": {
            'participant': {
                'fk_col': {COL_VALUE: 'participant_id'},
                'link_key': {COL_NAME: "subjid"}
            }
        },
        "_unique_id_col": {COL_VALUE: "sample_name"}
    },
    "aliquot": {
        "external_id": {COL_NAME: "barcode"},
        "analyte_type": {COL_VALUE: "DNA"},
        "volume": {COL_NAME: "initial_volume_microliters"},
        "concentration": {COL_NAME: "concentration_ng_per_ul"},
        "shipment_destination": {COL_VALUE: "Baylor College of Medicine"},
        "shipment_origin": None,
        "shipment_date": None,
        "_links": {
            'sample': {
                'fk_col': {COL_VALUE: 'sample_id'},
                'link_key': {COL_NAME: "sample_name"}
            }
        },
        "_unique_id_col": {COL_VALUE: "barcode"}
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
        "instrument_model": {COL_VALUE: "instrument"},
        "max_insert_size": {COL_NAME: "max_insert_size"},
        "mean_insert_size": {COL_NAME: "mean_insert_size"},
        "mean_depth": None,
        "total_reads": None,
        "mean_read_length": {COL_NAME: "mean_read_length"},
        "_links": {
            'aliquot': {
                'fk_col': {COL_VALUE: 'aliquot_id'},
                'link_key': {COL_NAME: "barcode"}
            }
        },
        "_unique_id_col": {COL_VALUE: "rg_barcode"}
    }
}
