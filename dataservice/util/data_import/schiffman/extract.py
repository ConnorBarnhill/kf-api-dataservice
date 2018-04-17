import os
import pandas as pd

from dataservice.util.data_import.utils import (
    reformat_column_names,
    dropna_rows_cols,
    cols_to_lower
)
from dataservice.util.data_import.etl.extract import BaseExtractor

DATA_DIR = '/Users/singhn4/Projects/kids_first/data/Schiffman'


class Extractor(BaseExtractor):

    @reformat_column_names
    @dropna_rows_cols
    def read_study_file_data(self, filepaths=None):
        """
        Read in raw study files
        """
        if not filepaths:
            filepaths = os.listdir(DATA_DIR)

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
    def read_data(self, filepath=None):
        """
        Read all the data into a dataframe
        """
        if not filepath:
            filepath = os.path.join(DATA_DIR, 'Schiffman_X01 Sample List.xlsx')

        df = pd.read_excel(filepath)

        return df

    def create_participant_df(self, df):
        """
        Extract participant data from full data set
        """
        cols_to_lower(df)
        # Extract participant columns
        participant_df = df[['individual_name', 'ewing_trio_number',
                             'relationship_to_proband', 'gender']]

        # Create is_proband col
        def func(row): return row.relationship_to_proband == 'Self/Case'
        participant_df['relationship_to_proband'] = participant_df.apply(
            func, axis=1)

        # Create family_id column
        participant_df.rename(columns={'ewing_trio_number': 'family_id'},
                              inplace=True)

        return participant_df

    def create_family_relationship_df(self, df):
        """
        Create family relationship df from all_data_df
        """
        df = df[['individual_name', 'relationship_to_proband',
                 'ewing_trio_number']]
        family_dict = {}
        for idx, row in df.iterrows():
            fam_id = row['ewing_trio_number']
            if fam_id not in family_dict:
                family_dict[fam_id] = {}
            family_dict[fam_id][
                row['relationship_to_proband']] = row['individual_name']
        df = pd.DataFrame(list(family_dict.values()))

        def func(row): return "_".join(['rel', str(row.name)])
        df['rel_id'] = df.apply(func, axis=1)

        return df

    def create_diagnosis_df(self, df):
        """
        Extract diagnosis df from all data df
        """
        # Handle NaN
        df = df.where((pd.notnull(df)), None)

        # Remove carriage returns and new lines from diagnosis
        def func(diagnosis):
            if diagnosis:
                diagnosis = ' '.join(diagnosis.splitlines())
            return diagnosis
        df['morphology'] = df['morphology'].apply(func)

        # Extract columns needed
        df = df[['individual_name', 'age_at_diagnosis_(days)', 'morphology']]

        return df

    def create_phenotype_df(self, diagnosis_df):
        """
        Create phenotype dataframe using diagnosis_df
        """
        diagnosis_df['phenotype'] = "Ewing's Sarcoma"
        diagnosis_df['hpo_id'] = "HP:0012254"
        _map = {True: 'positive', False: 'negative'}
        diagnosis_df['observed'] = (pd.notnull(diagnosis_df['morphology']).
                                    apply(lambda has_morph: _map[has_morph]))

        return diagnosis_df

    @reformat_column_names
    @dropna_rows_cols
    def read_genomic_data(self, filepath=None):
        """
        Read genomic data
        """
        if not filepath:
            filepath = os.path.join(
                DATA_DIR,
                'Schiffman_EwingSarcoma_QC_vs_Phenotype.xlsx')

        df = pd.read_excel(filepath)

        return df

    def create_seq_exp_data(self, df):
        df = df[['build_id', 'mean_insert_size', 'pf_reads',
                 'phenotype_sheet_sample_name']]
        return df

    def create_genomic_file_df(self, genomic_df, biospecimen_df):
        filepath = os.path.join(DATA_DIR, 'genomic_file_uuid.json')
        gf_info_df = super(Extractor, self).read_genomic_files_info(filepath)
        genomic_df = genomic_df[['build_id',
                                 'phenotype_sheet_sample_name',
                                 'bam_path']]
        genomic_df['file_name'] = genomic_df['bam_path'].apply(
            lambda p: os.path.basename(p))

        # Merge sequencing experiment data
        df1 = pd.merge(genomic_df, gf_info_df, on='file_name')
        # Merge biospecimen data
        genomic_file_df = pd.merge(df1, biospecimen_df,
                                   left_on='phenotype_sheet_sample_name',
                                   right_on='sample_name')
        genomic_file_df = genomic_file_df[['build_id',
                                           'sample_name',
                                           'file_name',
                                           'file_format',
                                           'uuid',
                                           'form',
                                           'hashes',
                                           'file_size',
                                           'file_url',
                                           'data_type',
                                           'md5sum']]
        return genomic_file_df

    def build_dfs(self):
        """
        Read in all entities into data frames
        """
        # Investigator
        investigator_df = self.read_investigator_data()

        # Study
        study_df = self.read_study_data()

        # Study
        study_files_df = self.read_study_file_data()

        # All participant data
        all_data_df = self.read_data()

        # Family relationships
        family_df = self.create_family_relationship_df(all_data_df)

        # Participant df
        participant_df = self.create_participant_df(all_data_df)

        # Diagnosis df
        diagnosis_df = self.create_diagnosis_df(all_data_df)

        # Phenotype df
        phenotype_df = self.create_phenotype_df(diagnosis_df)

        # Sequencing Experiment
        genomic_df = self.read_genomic_data()
        seq_exp_df = self.create_seq_exp_data(genomic_df)

        # Genomic File
        genomic_file_df = self.create_genomic_file_df(genomic_df, all_data_df)

        # Add study to investigator df
        study_investigator_df = self._add_study_cols(study_df, investigator_df)

        # Add study to study files df
        study_study_files_df = self._add_study_cols(study_df, study_files_df)

        # Add study to basic participant df
        study_participant_df = self._add_study_cols(study_df, participant_df)

        # Dict to store dfs for each entity
        entity_dfs = {
            'study': study_investigator_df,
            'study_file': study_study_files_df,
            'investigator': investigator_df,
            'family': participant_df,
            'participant': study_participant_df,
            'family_relationship': family_df,
            'diagnosis': diagnosis_df,
            'phenotype': phenotype_df,
            'biospecimen': all_data_df,
            'sequencing_experiment': seq_exp_df,
            'genomic_file': genomic_file_df,
            'default': all_data_df
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
