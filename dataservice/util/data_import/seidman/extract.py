import os
import pandas as pd

from dataservice.util.data_import.utils import (
    reformat_column_names,
    dropna_rows_cols
)

DATA_DIR = '/Users/singhn4/Projects/kids_first/data/Seidman_2015'
DBGAP_DIR = os.path.join(DATA_DIR, 'dbgap')
ALIQUOT_SHIP_DIR = os.path.join(DATA_DIR, 'manifests', 'shipping')


class Extractor(object):

    @reformat_column_names
    @dropna_rows_cols
    def read_study_data(self, filepath=None):
        """
        Read study data
        """
        if not filepath:
            filepath = os.path.join(DBGAP_DIR,
                                    'study.txt')
        df = pd.read_csv(filepath)

        return df

    @reformat_column_names
    @dropna_rows_cols
    def read_family_data(self, filepath=None):
        """
        Read family data for all participants
        """
        if not filepath:
            filepath = os.path.join(DBGAP_DIR,
                                    '7a_dbGaP_PedigreeDS.txt')
        df = pd.read_csv(filepath,
                         delimiter='\t',
                         dtype={'SUBJID': str})
        # Subset of columns
        df.drop(['SEX'], axis=1, inplace=True)

        # Add proband column
        def func(row): return bool(row['MOTHER'] and row['FATHER'])
        df['is_proband'] = df.apply(func, axis=1)

        return df

    @reformat_column_names
    @dropna_rows_cols
    def create_phenotype_data(self, filepath=None):
        """
        Read phenotype data
        """
        if not filepath:
            filepath = os.path.join(
                DBGAP_DIR,
                '3a_dbGaP_SubjectPhenotypes_ExtracardiacFindingsDS.txt')

        # Read csv
        df = pd.read_csv(filepath,
                         delimiter='\t',
                         dtype={'SUBJID': str})

        # Convert age years to days
        df["LATEST_EXAM_AGE"] = df["LATEST_EXAM_AGE"].apply(
            lambda x: float(x) * 365)

        # Delete extra column
        del df['AGE_AT_FORM_COMPLETION']

        # Make all values lower case
        for col in df.columns.tolist():
            df[col] = df[col].apply(lambda x: str(x).lower())

        # Reshape to build the phenotypes df
        phenotype_cols = df.columns.tolist()[2:]
        phenotype_df = pd.melt(df, id_vars='SUBJID', value_vars=phenotype_cols,
                               var_name='phenotype', value_name='value')
        # Create observed column

        def func(row):
            observed = 'positive'
            negative_values = ['nan', 'none', 'not reported',
                               'not applicable', 'unknown', 'no/not checked']
            if row['value'] in negative_values:
                observed = 'negative'
            return observed

        phenotype_df['observed'] = phenotype_df.apply(func, axis=1)

        def func(row): return "_".join(['phenotype', str(row.name)])
        phenotype_df['phenotype_id'] = phenotype_df.apply(func, axis=1)

        return phenotype_df

    def read_phenotype_data(self, filepath=None):
        if not filepath:
            filepath = os.path.join(DBGAP_DIR, 'phenotypes.txt')

        if not os.path.isfile(filepath):
            df = self.create_phenotype_data()
            # Write to file
            df.to_csv(filepath)
        else:
            df = pd.read_csv(filepath, dtype={'subjid': str})

        return df

    @reformat_column_names
    @dropna_rows_cols
    def read_gender_data(self, filepath=None):
        """
        Read gender data for all participants
        """
        if not filepath:
            filepath = os.path.join(DBGAP_DIR,
                                    '3a_dbGaP_SubjectPhenotypes_GenderDS.txt')
        df = pd.read_csv(filepath,
                         delimiter='\t',
                         dtype={'SUBJID': str})

        return df

    @reformat_column_names
    @dropna_rows_cols
    def read_demographic_data(self, filepaths=None):
        """
        Read demographic data for all participants (child, mother, father)
        """
        if not filepaths:
            filenames = [
                '3a_dbGaP_SubjectPhenotypes_DemographicsDS.txt',
                '3a_dbGaP_SubjectPhenotypes_MaternalDemographicsDS.txt',
                '3a_dbGaP_SubjectPhenotypes_PaternalDemographicsDS.txt']

            filepaths = [os.path.join(DBGAP_DIR, filename)
                         for filename in filenames
                         ]

        child_demo_df = pd.read_csv(os.path.join(filepaths[0]),
                                    delimiter='\t',
                                    dtype={'SUBJID': str})

        mother_demo_df = pd.read_csv(os.path.join(filepaths[1]),
                                     delimiter='\t',
                                     dtype={'SUBJID': str})

        father_demo_df = pd.read_csv(os.path.join(filepaths[2]),
                                     delimiter='\t',
                                     dtype={'SUBJID': str})

        # Combine demographics of all participants
        participant_demo_df = pd.concat(
            [child_demo_df, mother_demo_df, father_demo_df])

        participant_demo_df.drop_duplicates('SUBJID', inplace=True)

        # Subset of columns
        participant_demo_df = participant_demo_df[['RACE',
                                                   'ETHNICITY',
                                                   'SUBJID']]

        # Add unique id col (needed for transformation + loading)
        def func(row): return "_".join(['demographic', str(row.name)])
        participant_demo_df['demographic_id'] = participant_demo_df.apply(
            func, axis=1)

        return participant_demo_df

    @reformat_column_names
    @dropna_rows_cols
    def read_diagnosis_data(self, filepath=None):
        """
        Read diagnoses data for all participants
        """
        if not filepath:
            filename = '3a_dbGaP_SubjectPhenotypes_PatientDiagnosisDS.txt'
            filepath = os.path.join(DBGAP_DIR, filename)

        diagnosis_df = pd.read_csv(filepath,
                                   delimiter='\t',
                                   dtype={'SUBJID': str})

        # Add unique id col (needed for transformation + loading)
        def func(row): return "_".join(['diagnosis', str(row.name)])
        diagnosis_df['diagnosis_id'] = diagnosis_df.apply(
            func, axis=1)

        return diagnosis_df

    @reformat_column_names
    @dropna_rows_cols
    def read_participant_sample_data(self, filepath=None):
        """
        Read sample metadata for all participants
        """
        if not filepath:
            filename = '6a_dbGaP_SubjectSampleMappingDS.txt'
            filepath = os.path.join(DBGAP_DIR, filename)

        participant_sample_df = pd.read_csv(filepath,
                                            delimiter='\t',
                                            dtype={'SUBJID': str})
        participant_sample_df.drop_duplicates('SUBJID', inplace=True)

        return participant_sample_df

    @reformat_column_names
    @dropna_rows_cols
    def read_sample_shipping_manifest_data(self, *filepaths):
        """
        Read aliquot data (from PI/sample source center)
        """
        if not filepaths:
            filepaths = [os.path.join(ALIQUOT_SHIP_DIR, filename)

                         for filename in os.listdir(ALIQUOT_SHIP_DIR)
                         ]

        # Combine all manifest files
        dfs = [pd.read_excel(filepath,
                             delimiter='/t',
                             dtype={'*barcode': str},
                             skiprows=[0, 1],
                             header=[6])

               for filepath in filepaths

               if os.path.basename(filepath).startswith("PCGC")

               ]
        df = pd.concat(dfs)

        # Rename columns
        df.columns = map((lambda x: x.lower().lstrip("*")), df.columns)

        # Subset of columns
        df = df[['barcode',
                 'external_id',
                 'sample_collection_site',
                 'sample_role',
                 'concentration_ng_per_ul',
                 'initial_volume_microliters']]

        # Drop rows where id cols are nan
        id_cols = [col for col in df.columns if "id" in col]
        df.dropna(subset=id_cols, inplace=True)

        return df

    @reformat_column_names
    @dropna_rows_cols
    def read_seq_experiment_data(self, filepath=None):
        if not filepath:
            filepath = os.path.join(DATA_DIR, "seidman_metadata.xlsx")

        df = pd.read_excel(filepath, dtype={"date": str})
        # Rename some columns
        df.rename(columns={"library_name (in original BAM header)":
                           "library_name",
                           "barcode": "rg_barcode"}, inplace=True)
        df["read_length"] = df["read_length"].apply(
            lambda x: int(x.split("x")[0]))

        # Create new columns
        df['max_insert_size'] = df['insert_size'].max()
        df['mean_insert_size'] = df['insert_size'].mean()
        df['mean_read_length'] = df['read_length'].mean()
        df['total_reads'] = df['read_length'].count()

        # Subset of columns
        df = df[['sample_name',
                 'library_name',
                 'rg_barcode',
                 'run_name',
                 'read_length',
                 'date',
                 'library_strategy',
                 'library_source',
                 'library_selection',
                 'insert_size',
                 'instrument',
                 'library_layout',
                 'max_insert_size',
                 'mean_insert_size',
                 'mean_read_length',
                 'total_reads']]

        return df

    def build_dfs(self):
        """
        Read in all entities and join into a single table
        representing all participant data
        """
        # Study
        study_df = self.read_study_data()

        # Family
        family_df = self.read_family_data()

        # Gender
        gender_df = self.read_gender_data()

        # Phenotype
        phenotype_df = self.read_phenotype_data()

        # Demographic
        demographic_df = self.read_demographic_data()

        # Diagnosis data
        diagnosis_df = self.read_diagnosis_data()

        # Sample data
        participant_sample_df = self.read_participant_sample_data()

        # Aliquot/Sample Shipping data
        aliquot_df = self.read_sample_shipping_manifest_data()

        # Sequencing experiments
        seq_exp_df = self.read_seq_experiment_data()

        # Create full participant df
        # Merge Gender + Demographics
        gender_demo_df = pd.merge(gender_df, demographic_df, on='subjid')

        # Merge Family
        df1 = pd.merge(gender_demo_df, family_df, on='subjid')

        # Merge Diagnosis
        df2 = pd.merge(df1, diagnosis_df, on='subjid')

        # Merge Sample
        df3 = pd.merge(df2, participant_sample_df, on='subjid')

        # Merge Aliquot
        df4 = pd.merge(df3, aliquot_df,
                       left_on='sampid',
                       right_on='external_id')

        # Merge Sequencing Experiment
        full_participant_df = pd.merge(df4, seq_exp_df,
                                       left_on='external_id',
                                       right_on='sample_name')
        # Add study to full participant df
        self._add_study_cols(study_df, full_participant_df)

        # Basic participant df
        participant_df = self._add_study_cols(study_df, family_df)

        # Phenotype df
        phenotype_participant_df = pd.merge(phenotype_df, participant_df,
                                            on='subjid')

        # Dict to store dfs for each entity
        entity_dfs = {
            'participant': participant_df,
            'phenotype': phenotype_participant_df,
            'default': full_participant_df
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
