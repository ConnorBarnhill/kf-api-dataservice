import os
import pandas as pd

from dataservice.util.data_import.utils import (
    dropna_rows_cols,
    reformat_column_names
)
from dataservice.util.data_import.etl.extract import BaseExtractor

DATA_DIR = '/Users/singhn4/Projects/kids_first/data'


class Extractor(BaseExtractor):

    @reformat_column_names
    @dropna_rows_cols
    def read_sequencing_centers(self, filepath=None):
        """
        Read in raw study files
        """
        if not filepath:
            filepath = os.path.join(DATA_DIR,
                                    'seq-centers.txt')
        if not os.path.exists(filepath):
            seq_centers = [{'name': 'Broad Institute',
                            'kf_id': 'SC_DGDDQVYR'},
                           {'name': 'HudsonAlpha Institute for Biotechnology',
                            'kf_id': 'SC_X1N69WJM'},
                           {'name': 'Washington University',
                            'kf_id': 'SC_K52V7463'},
                           {'name': 'Baylor College of Medicine',
                            'kf_id': 'SC_A1JNZAZH'}]
            df = pd.DataFrame(seq_centers)
            df.to_csv(filepath, index=False)
        else:
            df = pd.read_csv(filepath)

        return df

    def build_dfs(self):
        """
        Read in all entities and join into a single table
        representing all participant data
        """
        df = self.read_sequencing_centers()

        # Dict to store dfs for each entity
        entity_dfs = {
            'sequencing_center': df
        }
        return entity_dfs

    def run(self):
        """
        Run extraction and return a Pandas DataFrame
        """
        return self.build_dfs()
