import os
import pandas as pd

from dataservice.util.data_import.utils import (
    reformat_column_names,
    dropna_rows_cols
)
from dataservice.util.data_import.etl.extract import BaseExtractor

DATA_DIR = '/Users/singhn4/Projects/kids_first/data/Rios_Wise_2016'
DBGAP_DIR = os.path.join(DATA_DIR, 'dbgap')
MANIFESTS_DIR = os.path.join(DATA_DIR, 'manifests')


class Extractor(BaseExtractor):

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
        if not filepath:
            filepath = os.path.join(DBGAP_DIR,
                                    'HL13237501A1_V3_SubjectDS.txt')
        df = pd.read_csv(filepath, delimiter='\t', dtype={'SUBJID': str})
        df = df[['SUBJECT_ID', 'CONSENT']]

        # Decode consent ints to consent strings
        def func(row):
            _map = {0: None,
                    1: "Health/Medical/Biomedical (IRB)",
                    2: "Disease-Specific (Musculoskeletal Diseases,"
                    " IRB)(DS-MUS-SKEL-IRB)"}
            return _map[row['CONSENT']]
        df['CONSENT'] = df.apply(func, axis=1)

        return df

    @reformat_column_names
    @dropna_rows_cols
    def read_sample_attr_data(self, filepath=None):
        """
        Read sample attributes file
        """
        if not filepath:
            filepath = os.path.join(DBGAP_DIR,
                                    'HL132375-01A1_V2_SampleAttributesDS.txt')
        return pd.read_csv(filepath, delimiter='\t')

    @reformat_column_names
    @dropna_rows_cols
    def read_subject_sample_data(self, filepath=None):
        """
        Read subject sample mapping file
        """
        if not filepath:
            filepath = os.path.join(
                DBGAP_DIR,
                'HL132375-01A1_V2_SubjectSampleMappingDS.txt')
        return pd.read_csv(filepath, delimiter='\t')

    @reformat_column_names
    @dropna_rows_cols
    def read_phenotype_data(self, filepath=None):
        if not filepath:
            filepath = os.path.join(DBGAP_DIR,
                                    'HL132375-01A1_V2_SubjectPhenotypesDS.txt')
        df = pd.read_csv(filepath,
                         delimiter='\t',
                         dtype={'SUBJID': str})

        # Decode sex ints to gender strings
        def func(row):
            _map = {1: "male", 2: "female"}
            return _map[row['Sex']]
        df['Sex'] = df.apply(func, axis=1)

        # Decode affected status ints to strings
        def func(row):
            _map = {0: 'unknown', 1: "not affected", 2: "affected"}
            return _map[row['AFFSTAT']]
        df['AFFSTAT'] = df.apply(func, axis=1)

        # Decode proband ints to booleans
        def func(row):
            _map = {1: True, 2: False}
            return _map[row['Proband']]
        df['Proband'] = df.apply(func, axis=1)

        # Create ethnicity column
        _map = {'Hispanic': 'hispanic or latino'}
        df['ethnicity'] = df['Race'].apply(
            lambda x: _map.get(x, 'not hispanic or latino'))

        return df

    def create_diagnosis_df(self, phenotype_df):
        """
        Create diagnosis df from phenotype df
        """
        def func(row):
            _map = {'affected': 'adolescent idiopathic scoliosis',
                    'not affected': None}
            return _map.get(row['affstat'], row['affstat'])
        phenotype_df['diagnosis'] = phenotype_df.apply(func, axis=1)
        return phenotype_df[['subject_id', 'diagnosis']]

    def create_phenotype_df(self, phenotype_df):
        """
        Create phenotype df from original phenotype_df
        """
        # Extract columns
        phenotype_df = phenotype_df[['subject_id', 'affstat']]
        # Drop unknowns
        phenotype_df = phenotype_df[phenotype_df.affstat != 'unknown']

        # Add columns
        def func(row):
            _map = {'affected': 'positive',
                    'not affected': 'negative'}
            return _map.get(row['affstat'], row['affstat'])

        phenotype_df['observed'] = phenotype_df.apply(func, axis=1)
        phenotype_df['hpo_id'] = 'HP:0002650'
        phenotype_df['phenotype'] = 'adolescent idiopathic scoliosis'
        return phenotype_df

    @reformat_column_names
    @dropna_rows_cols
    def read_family_data(self, filepath=None):
        if not filepath:
            filepath = os.path.join(DBGAP_DIR,
                                    'HL132375-01A1_V2_PedgreeDS.txt')
        df = pd.read_csv(filepath, delimiter='\t', dtype={'SUBJID': str})
        del df['SEX']
        return df

    @reformat_column_names
    @dropna_rows_cols
    def read_seq_exp_data(self, filepath=None):
        """
        Read sequencing experiment data
        """
        if not filepath:
            filepath = os.path.join(MANIFESTS_DIR, 'manifest_171210.csv')

        df = pd.read_csv(filepath)
        df['Sample Description'] = df['Sample Description'].apply(
            lambda x: x.split(':')[-1].strip())
        df.describe(include=['O']).T.sort_values('unique', ascending=False)

        # Subject sample mapping
        filepath = os.path.join(DBGAP_DIR,
                                'HL132375-01A1_V2_SubjectSampleMappingDS.txt')
        subject_sample_df = pd.read_csv(filepath, delimiter='\t')

        # Merge with subject samples
        df = pd.merge(subject_sample_df, df, left_on='SAMPLE_ID',
                      right_on='Sample Description')

        # Add unique col
        def func(row): return "_".join(['seq_exp', str(row.name)])
        df['seq_exp_id'] = df.apply(func, axis=1)

        return df

    @reformat_column_names
    @dropna_rows_cols
    def create_genomic_file_df(self, seq_exp_df):
        """
        Create genomic file df
        """
        # Seq Exp Data
        seq_exp_df = self.read_seq_exp_data()
        seq_exp_df = seq_exp_df[['subject_id', 'library',
                                 'sample_description', 'seq_exp_id']]

        # Genomic file info
        filepath = os.path.join(DATA_DIR, 'genomic_files_by_uuid.json')
        gf_df = super(Extractor, self).read_genomic_files_info(filepath)

        # Add library
        gf_df['library'] = gf_df['file_url'].apply(
            lambda file_url: os.path.dirname(file_url).split('/')[-1])

        # Merge
        df = pd.merge(seq_exp_df, gf_df, on='library')

        return df

    def build_dfs(self):
        """
        Read in all entities and join into a single table
        representing all participant data
        """
        # Investigator
        investigator_df = self.read_investigator_data()

        # Study
        study_df = self.read_study_data()

        # Study
        study_files_df = self.read_study_file_data()

        # Subject data
        subject_df = self.read_subject_data()

        # Sample attributes file
        sample_attr_df = self.read_sample_attr_data()

        # Subject sample file
        subject_sample_df = self.read_subject_sample_data()

        # Family
        family_df = self.read_family_data()

        # Phenotype file
        phenotype_df = self.read_phenotype_data()

        # Diagnosis
        diagnosis_df = self.create_diagnosis_df(phenotype_df)

        # Mapped Phenotype df
        phenotype_df1 = self.create_phenotype_df(phenotype_df)

        # Sequencing Experiment
        seq_exp_df = self.read_seq_exp_data()

        # Genomic file
        genomic_file_df = self.create_genomic_file_df(seq_exp_df)

        # Basic participant
        # Merge subject + phenotype
        df1 = pd.merge(subject_df, phenotype_df, on='subject_id')
        # Merge family
        participant_df = pd.merge(df1, family_df, on='subject_id')

        # Add study to basic participant df
        participant_df = self._add_study_cols(study_df, participant_df)

        # Add study to investigator df
        study_investigator_df = self._add_study_cols(study_df, investigator_df)

        # Add study to study files df
        study_study_files_df = self._add_study_cols(study_df, study_files_df)

        # Sample
        # Merge sample attributes w subject sample
        df1 = pd.merge(sample_attr_df, subject_sample_df, on='sample_id')
        # Merge sample with subject
        sample_df = pd.merge(df1, subject_df, on='subject_id')

        # Dict to store dfs for each entity
        entity_dfs = {
            'study': study_investigator_df,
            'study_file': study_study_files_df,
            'investigator': investigator_df,
            'participant': participant_df,
            'demographic': participant_df,
            'diagnosis': diagnosis_df,
            'phenotype': phenotype_df1,
            'sample': sample_df,
            'aliquot': sample_df,
            'sequencing_experiment': seq_exp_df,
            'family_relationship': family_df,
            'genomic_file': genomic_file_df,
            'default': participant_df
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
