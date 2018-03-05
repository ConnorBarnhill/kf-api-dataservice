import os
import json
import pandas as pd

from dataservice.util.data_import.utils import (
    reformat_column_names,
    dropna_rows_cols,
    cols_to_lower
)

DATA_DIR = '/Users/singhn4/Projects/kids_first/data/Seidman_2015'
DBGAP_DIR = os.path.join(DATA_DIR, 'dbgap')
ALIQUOT_SHIP_DIR = os.path.join(DATA_DIR, 'manifests', 'shipping')


class Extractor(object):

    @reformat_column_names
    @dropna_rows_cols
    def read_study_file_data(self, filepaths=None):
        """
        Read in raw study files
        """
        if not filepaths:
            filepaths = os.listdir(DBGAP_DIR)

        study_files = [{"study_file_name": f}
                       for f in filepaths if 'dbGaP' in f]
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
    def read_phenotype_data(self, filepath=None):
        """
        Read phenotype data
        """
        # Read in cached phenotypes or create if they don't exist
        hpo_fp = os.path.join(DATA_DIR, 'phenotype_hpo_mapping.txt')
        if os.path.exists(hpo_fp):
            return pd.read_csv(hpo_fp, dtype={'SUBJID': str})

        filepath = os.path.join(
            DBGAP_DIR,
            '3a_dbGaP_SubjectPhenotypes_ExtracardiacFindingsDS.txt')

        # Read csv
        df = pd.read_csv(filepath,
                         delimiter='\t',
                         dtype={'SUBJID': str})

        # Convert age years to days
        df['LATEST_EXAM_AGE'] = df["LATEST_EXAM_AGE"].apply(
            lambda x: int(x) * 365)
        age_at_event_days = df[['LATEST_EXAM_AGE', 'SUBJID']]

        # Select string based phenotypes
        df = df.select_dtypes(include='object')

        # Make all values lower case
        for col in df.columns.tolist():
            df[col] = df[col].apply(lambda x: str(x).lower())

        # Reshape to build the phenotypes df
        cols = df.columns.tolist()[2:]
        phenotype_cols = [col for col in cols if not col.startswith('OTHER')]
        phenotype_df = pd.melt(df, id_vars='SUBJID', value_vars=phenotype_cols,
                               var_name='phenotype', value_name='observed')

        # Remove unkonwns
        unknown_values = ['none', 'unknown', 'no/not checked',
                          'not applicable', 'absent']
        phenotype_df = phenotype_df[phenotype_df['observed'].apply(
            lambda x: x not in unknown_values)]

        # Add HPOs
        from dataservice.util.data_import.etl.hpo_mapper import mapper
        hpo_mapper = mapper.HPOMapper(DATA_DIR)
        phenotype_df = hpo_mapper.add_hpo_id_col(phenotype_df)

        # Map to positive/negative
        def func(row):
            return 'negative' if row['observed'] == 'no' else 'positive'
        phenotype_df['observed'] = phenotype_df.apply(func, axis=1)

        # Merge back in age at event in days
        phenotype_df = pd.merge(phenotype_df, age_at_event_days, on='SUBJID')

        # Add unique col
        def func(row): return "_".join(['phenotype', str(row.name)])
        phenotype_df['phenotype_id'] = phenotype_df.apply(func, axis=1)

        # Write to file
        phenotype_df.to_csv(hpo_fp, index=False)

        return phenotype_df

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

    def read_genomic_file_info(self, filepath=None):
        """
        Read genomic file info
        """
        if not filepath:
            filepath = os.path.join(DATA_DIR, 'genomic_file_uuid.json')

        def get_ext(fp):
            filename = os.path.basename(fp)
            parts = filename.split('.')
            if len(parts) > 2:
                ext = '.'.join(parts[1:])
            else:
                ext = parts[-1]
            return ext

        with open(filepath, 'r') as json_file:
            uuid_dict = json.load(json_file)

        gf_dicts = []
        for k, v in uuid_dict.items():
            file_info = {
                'uuid': v['did'],
                'file_size': v['size'],
                'md5sum': v['hashes']['md5'],
                'file_url': v['urls'][0],
                'data_type': 'submitted aligned reads',
                'file_format': get_ext(v['urls'][0]),
                'file_name': os.path.basename(v['urls'][0])
            }
            gf_dicts.append(file_info)

        return pd.DataFrame(gf_dicts)

    def read_sample_gf_data(self, filepath=None):
        """
        Read sample to genomic file mapping
        """
        if not filepath:
            filepath = os.path.join(DATA_DIR, 'manifests',
                                    'GMKF_BAMsampleIDs.xlsx')
        df = pd.read_excel(filepath)
        df = df.loc[df['Cohort'] == 'GMKF-Seidman']
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

        # Genomic files
        # Read genomic file info
        gf_file_info_df = self.read_genomic_file_info()
        # Sample and BAM File df
        sample_gf_df = self.read_sample_gf_data()

        # Create full participant df
        # Merge Gender + Demographics
        gender_demo_df = pd.merge(gender_df, demographic_df, on='subjid')

        # Merge Family
        demographic_df = pd.merge(gender_demo_df, family_df, on='subjid')

        # Merge Diagnosis
        diagnosis_df = pd.merge(family_df, diagnosis_df, on='subjid')

        # Merge Sample
        df3 = pd.merge(family_df, participant_sample_df, on='subjid')

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

        # Add study to basic participant df
        participant_df = self._add_study_cols(study_df, family_df)

        # Add study to investigator df
        study_investigator_df = self._add_study_cols(study_df, investigator_df)

        # Add study to study files df
        study_study_files_df = self._add_study_cols(study_df, study_files_df)

        # Phenotype df
        phenotype_participant_df = pd.merge(phenotype_df, participant_df,
                                            on='subjid')

        # Merge with sequencing experiment df
        df = pd.merge(sample_gf_df, full_participant_df,
                      left_on='dbgap_subject_id',
                      right_on='sample_name')
        # Merge with genomic file info df
        genomic_file_df = pd.merge(df, gf_file_info_df,
                                   left_on='BAM sample ID',
                                   right_on='file_name')

        # Dict to store dfs for each entity
        entity_dfs = {
            'study': study_investigator_df,
            'study_file': study_study_files_df,
            'investigator': investigator_df,
            'participant': participant_df,
            'family_relationship': family_df,
            'demographic': demographic_df,
            'diagnosis': diagnosis_df,
            'phenotype': phenotype_participant_df,
            'genomic_file': genomic_file_df,
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
