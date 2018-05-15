from os.path import basename
import pandas as pd
import uuid

from dataservice.util.data_import.utils import (
    read_json,
    extract_uncompressed_file_ext
)

HARMONIZED_TYPES = {'aligned reads'}


class BaseExtractor(object):

    def __init__(self, config, **kwargs):
        self.config = config

    def create_study_file_df(self, filepaths):
        from os import stat

        study_files = [{"study_file_name": basename(f),
                        'latest_did': str(uuid.uuid4()),
                        'size': stat(f).st_size,
                        'urls': ['s3://bucket/key'],
                        'hashes': {'md5': str(uuid.uuid4()).replace('-', '')}
                        }
                       for f in filepaths]
        return pd.DataFrame(study_files)

    def read_genomic_files_info(self, filepath):
        """
        Read genomic file info json produced by Gen3 registration
        and convert into genomic file table for dataservice
        """
        data = read_json(filepath)
        df = pd.DataFrame(list(data.values()))

        # Reformat
        df['file_url'] = df['urls'].apply(lambda x: x[0])
        df['file_name'] = df['urls'].apply(lambda x: basename(x[0]))
        df['file_format'] = df['file_name'].apply(
            extract_uncompressed_file_ext)
        df.rename(columns={'did': 'latest_did'},
                  inplace=True)

        # Data type
        def func(x):
            x = x.strip()
            if x.endswith('cram') or x.endswith('bam'):
                val = 'Aligned Reads'
            elif x.endswith('crai'):
                val = 'Aligned Reads Index'
            elif 'fastq' in x:
                val = 'Unaligned Reads'
            elif 'vcf' in x:
                val = 'Simple Nucleotide Variation'
            else:
                val = 'Other'
            return val
        df['data_type'] = df['file_name'].apply(func)

        # Is harmonized
        df['is_harmonized'] = df['data_type'].apply(
            lambda data_type: True if data_type.lower()
            in HARMONIZED_TYPES else False)

        return df
