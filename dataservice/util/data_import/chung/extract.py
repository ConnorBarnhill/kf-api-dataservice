import os
import pandas as pd
from numpy import NaN

from dataservice.util.data_import.utils import (
    dropna_rows_cols,
    reformat_column_names
)
from dataservice.util.data_import.etl.extract import BaseExtractor

DATA_DIR = '/Users/singhn4/Projects/kids_first/data/Chung'
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

        # Add unique col
        def func(row): return "_".join(['phenotype', str(row.name)])
        phenotype_df['phenotype_id'] = phenotype_df.apply(func, axis=1)

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

        # Add unique col
        def func(row): return "_".join(['outcome', str(row.name)])
        df['outcome_id'] = df.apply(func, axis=1)

        return df[['SUBJECT_ID', 'discharge_status', 'outcome_id']]

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
    def read_sample_manifests(self, manifest_dir=None):
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
        df['is_proband'] = df['is_proband'].apply(
            lambda x: True if x == 'Proband' else False)

        # Clean up volume and concentration cols
        def func(row):
            val = str(row['Volume']).strip("uL")
            try:
                val = int(val)
            except ValueError:
                val = NaN
            return val
        df['Volume'] = df.apply(func, axis=1)

        def func(row):
            val = str(row['Concentration']).strip("ng/uL")
            try:
                val = int(val)
            except ValueError:
                val = NaN
            return val
        df['Concentration'] = df.apply(func, axis=1)

        df = df[pd.notnull(df.Concentration)]
        df = df[pd.notnull(df.Volume)]
        df.Concentration = df.Concentration.astype('int')
        df.Volume = df.Volume.astype('int')

        return df[['Concentration', 'Volume', 'Sample ID', 'Sample Type',
                   'is_proband']]

    @reformat_column_names
    @dropna_rows_cols
    def read_genomic_file_manifest(self, filepath=None):
        """
        Read genomic file manifest (ties subjects to genomic files)
        """
        if not filepath:
            filepath = os.path.join(DATA_DIR, 'sample.txt')

        df = pd.read_csv(filepath, delimiter='\t')
        return df[['entity:sample_id', 'aligned_reads', 'crai_or_bai_path',
                   'cram_or_bam_path', 'library-1_name',
                   'library-2_name', 'max_insert_size', 'mean_depth',
                   'mean_insert_size', 'mean_read_length', 'min_insert_size',
                   'sample_alias', 'total_reads']]

    def read_genomic_files_info(self, filepath=None):
        """
        Read genomic file info
        """
        if not filepath:
            filepath = os.path.join(DATA_DIR, 'genomic_files_by_uuid.json')
        df = super(Extractor, self).read_genomic_files_info(filepath)

        df['subject_id'] = df['file_name'].apply(
            lambda file_name: file_name.split('.')[0])
        return df

    def create_seq_exp_df(self, aliquot_df, gf_manifest_df):
        """
        Create sequencing_experiment df from aliquots and genomic_file manifest
        """
        seq_exp_df = pd.merge(gf_manifest_df,
                              aliquot_df[['subject_id', 'sample_id',
                                          'sample_use']],
                              left_on='sample_alias',
                              right_on='subject_id')

        # Add unique col
        def func(row): return "_".join(['seq_exp_id', str(row.name)])
        seq_exp_df['seq_exp_id'] = seq_exp_df.apply(func, axis=1)

        return seq_exp_df

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

        # Subject data
        subject_df = self.read_subject_data()
        subject_attr_df = self.read_subject_attr_data()

        # Demographic data
        demographic_df = self.read_demographic_data()

        # Family data
        family_df = self.read_family_data()

        # Sample manifest data
        sample_manifest_df = self.read_sample_manifests()

        # Subject sample mapping data
        subject_sample_df = self.read_subject_sample_data()

        # Genomic file manifests
        gf_manifest_df = self.read_genomic_file_manifest()

        # Genomic files info
        gf_file_info_df = self.read_genomic_files_info()

        # Phenotype data
        phenotype_df = self.read_phenotype_data()

        # Outcome data
        outcome_df = self.read_outcome_data()

        # Participant df
        # Merge subject + subject attributes
        df1 = pd.merge(subject_df, subject_attr_df, on='subject_id')

        # Merge family
        df2 = pd.merge(df1, family_df, on='subject_id')
        # Merge proband from sample manifests
        participant_df = pd.merge(
            df2,
            sample_manifest_df[['sample_id', 'is_proband']], on='sample_id')
        # Merge demographics
        participant_df = pd.merge(demographic_df, participant_df,
                                  on='subject_id')

        # Add study to basic participant df
        participant_df = self._add_study_cols(study_df, participant_df)

        # Sample df
        # Merge with subject data
        df3 = pd.merge(participant_df, subject_sample_df[[
                       'subject_id', 'sample_use']], on='subject_id')
        # Merge with sample manifests
        sample_df = pd.merge(df3, sample_manifest_df, on='sample_id')

        # Sequencing experiment df
        seq_exp_df = self.create_seq_exp_df(sample_df, gf_manifest_df)

        # Genomic file df
        genomic_file_df = pd.merge(gf_file_info_df, seq_exp_df,
                                   on='subject_id')

        # Phenotype df
        # Merge with participant df
        phenotype_df = pd.merge(phenotype_df, participant_df, on='subject_id')
        phenotype_df.head()

        # Outcome df
        outcome_df = pd.merge(outcome_df, participant_df, on='subject_id')

        # Add study to investigator df
        study_investigator_df = self._add_study_cols(study_df, investigator_df)

        # Add study to study files df
        study_study_files_df = self._add_study_cols(study_df, study_files_df)

        # Dict to store dfs for each entity
        entity_dfs = {
            'study': study_investigator_df,
            'study_file': study_study_files_df,
            'investigator': investigator_df,
            'participant': participant_df,
            'diagnosis': phenotype_df,
            'phenotype': phenotype_df,
            'outcome': outcome_df,
            'sample': sample_df,
            'aliquot': sample_df,
            'sequencing_experiment': seq_exp_df,
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
