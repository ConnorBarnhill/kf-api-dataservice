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


class HPOMapper(object):

    def __init__(self, cache_dir):
        fp = os.path.join(cache_dir, 'hpo_graph.json')
        self.hpo_mappings = self._read_or_create_hpo_graph(fp)

    def _read_or_create_hpo_graph(self, filepath):
        """
        Read HPO graph from cached file or download from HPO URL
        """
        # Download and cache the hpo map if it doesn't exist
        if not os.path.exists(filepath):
            graph = obonet.read_obo(HPO_URL)
            self.hpo_mappings = {data['name'].lower(): id_
                                 for id_, data in graph.nodes(data=True)}
            write_json(self.hpo_mappings, filepath)
        # Read the HPO map from file
        else:
            self.hpo_mappings = read_json(filepath)

        return self.hpo_mappings

    def get_hpo_term(self, phenotype_str):
        """
        Try to resolve an hpo term from a string
        """
        # Look up HPO Id
        formatted_str = phenotype_str.lower().replace('_', ' ')
        if formatted_str in self.hpo_mappings:
            return self.hpo_mappings[formatted_str]
        return None

    def get_mapping(self, df, phenotype_col):
        """
        Given raw data, try to map the strings in phenotype_col to an HPO ID
        """
        mapping = {}

        if phenotype_col not in df.columns.tolist():
            return mapping

        for idx, row in df.iterrows():
            phenotype = row[phenotype_col]
            hpo_term = self.get_hpo_term(phenotype)
            mapping[phenotype] = hpo_term
        return mapping

    def add_hpo_id_col(self, df, phenotype_col='phenotype'):
        """
        Map the strings in phenotype_col to HPO_IDs and insert as new col
        """
        # Get phenotype - HPO ID mapping dict
        mapping = self.get_mapping(df, phenotype_col)
        if mapping:
            # Merge original df with mapped df
            mapped_df = pd.DataFrame.from_dict(mapping, orient='index')
            mapped_df.rename(columns={0: 'hpo_id'}, inplace=True)
            df = pd.merge(df, mapped_df, left_on=phenotype_col,
                          right_index=True)
        return df
