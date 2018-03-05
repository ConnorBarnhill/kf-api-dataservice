"""
Map phenotype strings to HPO IDs
"""

import os
import obonet
import pandas as pd
from pprint import pprint

from dataservice.util.data_import.utils import (
    read_json,
    write_json
)
HPO_URL = 'http://purl.obolibrary.org/obo/hp.obo'
DATA_DIR = '/Users/singhn4/Projects/kids_first/data'


def read_or_create_hpo_map(filepath=None):
    """
    Read HPO graph from cached file or download from HPO URL
    """
    if not filepath:
        filepath = os.path.join(DATA_DIR, 'hpo_map.json')

    # Download and cache the hpo map if it doesn't exist
    if not os.path.exists(filepath):
        graph = obonet.read_obo(HPO_URL)
        name_to_id = {data['name'].lower(): id_
                      for id_, data in graph.nodes(data=True)}
        write_json(name_to_id, filepath)
    # Read the HPO map from file
    else:
        name_to_id = read_json(filepath)

    return name_to_id


# Read or create hpo map
name_to_id = read_or_create_hpo_map()


def get_term(phenotype_str):
    """
    Try to resolve an hpo term from a string
    """
    # Look up HPO Id
    formatted_str = phenotype_str.lower().replace('_', ' ')
    if formatted_str in name_to_id:
        return name_to_id[formatted_str]
    return None


def get_mapping(df, phenotype_col):
    """
    Given raw data, try to extract an initial mapping
    """
    mapping = {}

    if phenotype_col not in df.columns.tolist():
        return mapping

    for idx, row in df.iterrows():
        phenotype = row[phenotype_col]
        hpo_term = get_term(phenotype)
        mapping[phenotype] = hpo_term
    return mapping


def apply_mapping(df, join_col, mapping):
    """
    Given a dataframe, join it with the HPO ID dataframe
    """
    mapped_df = pd.DataFrame.from_dict(mapping, orient='index')
    mapped_df.rename(columns={0: 'hpo_id'}, inplace=True)

    # Merge original df with mapped df
    df = pd.merge(df, mapped_df, left_on=join_col, right_index=True)
    return df


def add_hpo_id_col(df, phenotype_col='phenotype'):
    """
    """
    mapping = get_mapping(df, phenotype_col)
    if mapping:
        df = apply_mapping(df, phenotype_col, mapping)
    return df
