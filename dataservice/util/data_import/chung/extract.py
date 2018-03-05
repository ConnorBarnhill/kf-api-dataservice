import os
import json
import pandas as pd

from dataservice.util.data_import.utils import (
    read_json,
    write_json,
    cols_to_lower,
    dropna_rows_cols,
    reformat_column_names
)

DATA_DIR = '/Users/singhn4/Projects/kids_first/data/Chung'
DBGAP_DIR = os.path.join(DATA_DIR, 'dbgap')
MANIFESTS_DIR = os.path.join(DATA_DIR, 'manifests')


class Extractor(object):

    @reformat_column_names
    @dropna_rows_cols
    def read_study_file_data(self, filepaths=None):
        """
        Read in raw study files
        """
        if not filepaths:
            filepaths = os.listdir(DBGAP_DIR)
            filepaths.extend(os.listdir(MANIFESTS_DIR))

        study_files = [{"study_file_name": f}
                       for f in filepaths]
        return pd.DataFrame(study_files)

    @reformat_column_names
    @dropna_rows_cols
    def read_study_data(self, filepath=None):
        """
        Read study data
        """
        if not filepath:
            filepath = os.path.join(DATA_DIR,
                                    'study.txt')
        df = pd.read_csv(filepath)

        return df

    @reformat_column_names
    @dropna_rows_cols
    def read_investigator_data(self, filepath=None):
        """
        Read investigator data
        """
        if not filepath:
            filepath = os.path.join(DATA_DIR,
                                    'investigator.txt')
        df = pd.read_csv(filepath)

        return df

    @reformat_column_names
    @dropna_rows_cols
    def read_subject_data(self, filepath=None):
        """
        Read subject data file
        """
        if not filepath:
            filepath = os.path.join(DBGAP_DIR,
                                    '4a_dbGaP_SubjectDS_corrected_7-16.xlsx')
        df = pd.read_excel(filepath, dtype={'SUBJECT_ID': str})

        # Decode consent ints to consent strings
        def func(row):
            _map = {
                1: "Disease-Specific (Congenital Diaphragmatic Hernia"
                ", COL, GSO, RD) (DS-CDH-COL-GSO-RD)"}
            return _map.get(row['CONSENT'])
        df['CONSENT'] = df.apply(func, axis=1)

        # Decode affected status ints to strings
        def func(row):
            _map = {0: 'unknown', 1: "affected", 2: "unaffected"}
            return _map.get(row['AFFECTED_STATUS'])
        df['AFFECTED_STATUS'] = df.apply(func, axis=1)
        return df

    @reformat_column_names
    @dropna_rows_cols
    def read_subject_attr_data(self, filepath=None):
        """
        Read subject attributes file
        """
        if not filepath:
            filepath = os.path.join(
                DBGAP_DIR,
                '3a_dbGaP_SubjectAttributesDS_corrected.6.12.xlsx')
        df = pd.read_excel(filepath, dtype={'SUBJECT_ID': str})

        # Decode body_site chars to strings
        def func(row):
            _map = {'B': 'blood', 'SK': 'skin', 'D': 'diaphragm',
                    'SV': 'saliva', 'A': 'amniocytes', 'M': 'amniocytes'}
            return _map.get(row['body_site'])
        df['body_site'] = df.apply(func, axis=1)
        return df

    @reformat_column_names
    @dropna_rows_cols
    def read_family_data(self, filepath=None):
        """
        Read pedigree data
        """
        if not filepath:
            filepath = os.path.join(DBGAP_DIR,
                                    '6a_dbGaP_PedigreeDS_corrected.6.12.xlsx')
        df = pd.read_excel(filepath)
        del df['SEX']

        return df

    @reformat_column_names
    @dropna_rows_cols
    def read_sample_manifests(self, manifest_dir):
        """
        Read and combine all sample manifest sheets
        """
        if not manifest_dir:
            manifest_dir = MANIFESTS_DIR

        # Sample manifests
        # Combine all sample manifest sheets
        dfs = [pd.read_excel(os.path.join(manifest_dir, filename))

               for filename in os.listdir(manifest_dir)

               ]
        df = pd.concat(dfs)
        df = df[df['Sample ID'].notnull()]

        df.rename(columns={'Alias.2': 'is_proband'}, inplace=True)

        return df[['Concentration', 'Volume', 'Sample ID', 'Sample Type',
                   'is_proband']]

    @reformat_column_names
    @dropna_rows_cols
    def read_subject_sample_data(self, filepath=None):
        if not filepath:
            filepath = os.path.join(
                DBGAP_DIR,
                '5a_dbGaP_SubjectSampleMappingDS cumulative.xlsx')
        return pd.read_excel(filepath, delimiter='\t')

    @reformat_column_names
    @dropna_rows_cols
    def read_demographic_data(self, filepath=None):
        """
        Read demographic data from phenotype file
        """
        if not filepath:
            filepath = os.path.join(DBGAP_DIR,
                                    '2a_dbGaP_SubjectPhenotypesDS.xlsx')
        df = pd.read_excel(filepath)
        # Make all values lower case
        for col in ['Ethnicity', 'Race']:
            df[col] = df[col].apply(lambda x: str(x).lower().strip())
        return df[['SUBJECT_ID', 'SEX', 'Ethnicity', 'Race']]

    @reformat_column_names
    @dropna_rows_cols
    def read_phenotype_data(self, filepath=None):
        """
        Read phenotype file and insert HPO IDs
        """
        if not filepath:
            filepath = os.path.join(
                DBGAP_DIR, '2a_dbGaP_SubjectPhenotypesDS.xlsx')

        df = pd.read_excel(filepath)
        df.drop(['Ethnicity', 'Race', 'SEX', 'discharge_status', 'ISOLATED'],
                inplace=True, axis=1)
        # Reshape to build the phenotypes df
        cols = df.columns.tolist()[1:]
        phenotype_df = pd.melt(df, id_vars='SUBJECT_ID', value_vars=cols,
                               var_name='phenotype', value_name='value')

        # Drop rows where value is NaN
        phenotype_df = phenotype_df[pd.notnull(phenotype_df['value'])]

        # Decode phenotypes to descriptive strings
        def func(row):
            _map = {0: 'no', 1: 'yes'}
            return _map.get(row['value'], row['value'])
        phenotype_df['value'] = phenotype_df.apply(func, axis=1)

        # Decode phenotypes to descriptive strings
        def func(row):
            # Always take most specific value
            if row['value'] not in ['yes', 'no']:
                val = row['value']
            else:
                _map = {'CHD': 'congenital heart defect',
                        'CNS': 'central nervous system defect',
                        'GI': 'gastrointestinal defect'}
                val = _map.get(row['phenotype'], 'congenital birth defect')
            return val
        phenotype_df['phenotype'] = phenotype_df.apply(func, axis=1)

        # Set observed
        phenotype_df['observed'] = phenotype_df['value'].apply(
            lambda x: 'positive' if x != 'no' else 'negative')
        del phenotype_df['value']

        # Add HPOs
        from dataservice.util.data_import.etl.hpo import mapper
        hpo_mapper = mapper.HPOMapper(DATA_DIR)
        phenotype_df = hpo_mapper.add_hpo_id_col(phenotype_df)
        return phenotype_df

    @reformat_column_names
    @dropna_rows_cols
    def read_outcome_data(self, filepath=None):
        """
        Read outcome data from phenotype file
        """
        if not filepath:
            filepath = os.path.join(DBGAP_DIR,
                                    '2a_dbGaP_SubjectPhenotypesDS.xlsx')
        df = pd.read_excel(filepath)

        # Replace NaN values with None
        df['discharge_status'] = df['discharge_status'].where(
            (pd.notnull(df['discharge_status'])), 999)

        # Map discharge status
        # 1=Alive 4=Deceased 0=Fetal sample 8=unknown NA=Not applicable
        def func(row):
            _map = {0: 'alive', 1: 'deceased', 4: 'fetal sample', 8: 'unknown'}
            return _map.get(int(row['discharge_status']), 'not applicable')
        df['discharge_status'] = df.apply(func, axis=1)
        return df[['SUBJECT_ID', 'discharge_status']]

    def build_dfs(self):
        """
        Read in all entities and join into a single table
        representing all participant data
        """
        # Investigator
        investigator_df = self.read_investigator_data()

        # Study
        study_df = self.read_study_data()

        # Study files
        study_files_df = self.read_study_file_data()

        # Add study to investigator df
        study_investigator_df = self._add_study_cols(study_df, investigator_df)

        # Add study to study files df
        study_study_files_df = self._add_study_cols(study_df, study_files_df)

        # Dict to store dfs for each entity
        entity_dfs = {
            'study': study_investigator_df,
            'study_file': study_study_files_df,
            'investigator': investigator_df
            # 'default': participant_df
        }
        return entity_dfs

    def _add_study_cols(self, study_df, df):
        # Add study cols to a df
        cols = study_df.columns.tolist()
        row = study_df.iloc[0]
        for col in cols:
            df[col] = row[col]
        return df

    def run(self):
        """
        Run extraction and return a Pandas DataFrame
        """
        return self.build_dfs()
