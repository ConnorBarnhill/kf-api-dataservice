import requests
import os
# pip install pandas
import pandas as pd
import json
import math
import config


from dataservice.util.data_import.utils import (
    reformat_column_names,
    dropna_rows_cols,
    cols_to_lower
)
from dataservice.util.data_import.etl.extract import BaseExtractor


DATA_DIR = '/Users/lgraglia/TEST/GMKF_service/data/Schiffman'
baseUrl = 'http://127.0.0.1:5000'
# baseUrl = 'http://kf-api-dataservice-qa.kids-first.io'
# baseUrlSafe = 'https://127.0.0.1:5000'


headers = {'Content-Type': 'application/json'}



class Extractor(BaseExtractor):

	def __init__(self, config):
		super().__init__(config)
        # self.data_dir = config['extract']['data_dir']

	@reformat_column_names
	@dropna_rows_cols
	def read_study_file_data(self):
		"""
		Read in raw study files
		"""
		filepaths = [os.path.join(DATA_DIR, f)
					for f in os.listdir(DATA_DIR)]

		return self.create_study_file_df(filepaths)

	@reformat_column_names
	@dropna_rows_cols
	def read_study_data(self, filepath=None):
		"""
		Read study data
		"""
		if not filepath:
			filepath = os.path.join(DATA_DIR,'study.txt')
		try:
			df = pd.read_csv(filepath)
			return df
		except OSError as err:
			print("OS error: {0}".format(err))

		return None

	@reformat_column_names
	@dropna_rows_cols
	def read_investigator_data(self, filepath=None):
		"""
		Read investigator data
		"""
        
		if not filepath:
			filepath = os.path.join(DATA_DIR,
                                    'investigator.txt')

		try:
			df = pd.read_csv(filepath)
			return df
		except OSError as err:
			print("OS error: {0}".format(err))

		return None

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
		df['topography'] = df['topography'].apply(func)

		# Extract columns needed
		df = df[['individual_name', 'age_at_diagnosis_(days)', 'morphology', 'topography']]

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
		return genomic_file_df

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

                   



	def add_investigator(self):
		investigator_df = self.read_investigator_data()

		if investigator_df is not None:
			for index, row in investigator_df.iterrows():
				institution = row["institution"]
				name = row["investigator_name"]

				#TODO add this information
				# external_id =
				
				url = '/investigators'
				data = '{ "institution": "' + institution + '", "name": "' + name + '" }'
				investigatorsResponse = requests.post(baseUrl + url, data=data, headers=headers)

				if investigatorsResponse.json()['_status']['code'] < 300:
					investigator = investigatorsResponse.json()['results']
					return investigator['kf_id']
				else:
					print (investigatorsResponse.json())
					return None
		else:
			print("Missing Investigator data\n")
			return None

	def add_study(self,df, investigator_id):
		study_df = df
		ret = {}

		if study_df is not None:
			for index, row in study_df.iterrows():
				data_access_authority = row["data_access_authority"]
				attribution = row["attribution"]
				name = row["study_name"]
				version = row["study_version"]
				external_id = row["study_id"]	

				#TODO add this information
				# release_status =
				# short_name =	
				
				url = '/studies'
				data = '{"data_access_authority": "' + data_access_authority \
						+ '", "attribution": "' + attribution \
						+ '", "name": "' + name \
						+ '", "version": "' + version \
						+ '", "external_id": "' + external_id \
						+ '", "investigator_id": "' + investigator_id + '" }'
				studyResponse = requests.post(baseUrl + url, data=data, headers=headers)

				if studyResponse.json()['_status']['code'] < 300:
					study = studyResponse.json()['results']
					ret[external_id] = study['kf_id']
					# print(study)
				else:
					print (studyResponse.json())
		else:
			print("missing Study data\n")

		return ret

	def add_study_file(self, df, study_dct):
		study_df = df

		if study_df is not None:
			for index, row in study_df.iterrows():

				latest_did = row['latest_did']
				file_name = row['study_file_name']	
				size = str(row['size'])
				hashes = json.dumps(row['hashes']) 
				study_id = study_dct[row['study_id']]
				urls = json.dumps(['s3://bucket/key'])

				#TODO add this information
				# data_type
				# file_format
				# external_id
				# metadata

				url = '/study-files'
				data = '{"latest_did": "' + latest_did \
						+ '", "file_name": "' + file_name \
						+ '", "size": "' + size \
						+ '", "study_id": "' + study_id \
						+ '", "urls": ' + urls \
						+ ', "hashes": ' + hashes + ' }'

				print(data)
				print(baseUrl + url)

				studyFileResponse = requests.post(baseUrl + url, data=data, headers=headers)

				print(studyFileResponse)

				if studyFileResponse.json()['_status']['code'] < 300:
					studyFile = studyFileResponse.json()['results']
				else:
					print (studyFileResponse.json())
		else:
			print("missing Study File data\n")

	def add_family(self, df):
		ret = {}
		if df is not None:
			for index, row in df.iterrows():
				family_id = str(row["family_id"])

				if family_id in ret:
					continue
				else:
					url = '/families'
					data = '{ "external_id": "' + family_id + '" }'
					familyResponse = requests.post(baseUrl + url, data=data, headers=headers)

					if familyResponse.json()['_status']['code'] < 300:
						family = familyResponse.json()['results']
						ret[family_id] = family['kf_id']
					else:
						print (familyResponse.json())
		else:
			print("missing Family data\n")

		return ret

	def add_participant(self, df, family_ids, study_ids):
		participant_df = df
		ret = {}

		if participant_df is not None:
			for index, row in participant_df.iterrows():
				

				external_id = row["individual_name"]
				is_proband = str(row["relationship_to_proband"]) 
				ethnicity = "unknown"
				race = "unknown"
				study_id = study_ids[row["study_id"]]	
				family_id = family_ids[str(row["family_id"])]
				gender = row['gender'].lower()	

				#TODO add this information
				# alias_group
				# consent_type
				
				url = '/participants'
				data = '{"external_id": "' + external_id \
						+ '", "is_proband": "' + is_proband \
						+ '", "ethnicity": "' + ethnicity \
						+ '", "race": "' + race \
						+ '", "study_id": "' + study_id \
						+ '", "family_id": "' + family_id \
						+ '", "gender": "' + gender + '" }'

				participantResponse = requests.post(baseUrl + url, data=data, headers=headers)

				if participantResponse.json()['_status']['code'] < 300:
					participant = participantResponse.json()['results']
					ret[external_id] = participant['kf_id']
				else:
					print (participantResponse.json())
		else:
			print("missing Participant data\n")

		return ret

	def add_family_relationship(self, df, participant_dct):
		fam_rel_df = df

		# Get data from API
		relation_keys = ['Mother', 'Father']

		if fam_rel_df is not None:
			for index, row in fam_rel_df.iterrows():
				family = row

				for r in relation_keys:
					rel = self._create_family_relationship(r, family, participant_dct)
					if rel is not None:
						
						participant_id = rel["participant_id"]
						relative_id = rel["relative_id"]
						participant_to_relative_relation = rel['participant_to_relative_relation']

						#TODO add this information
						# external_id = 
						# relative_to_participant_relation

						url = '/family-relationships'
						data = '{"participant_id": "' + participant_id \
								+ '", "relative_id": "' + relative_id \
								+ '", "participant_to_relative_relation": "' + participant_to_relative_relation \
								+ '" }'

						famRelResponse = requests.post(baseUrl + url, data=data, headers=headers)

						if famRelResponse.json()['_status']['code'] < 300:
							famRel = famRelResponse.json()['results']
						else:
							print (famRelResponse.json())
		else:
			print("missing Family Relationship data\n")

	def add_diagnoses(self, df, participant_dct):
		diagnosis_df = df

		if diagnosis_df is not None:
			for index, row in diagnosis_df.iterrows():

				age_at_event_days = str(row['age_at_diagnosis_(days)'])[:-2] if row['age_at_diagnosis_(days)'] is not None else ''
				diagnosis_category = "cancer"
				participant_id = participant_dct[row['individual_name']]
				source_text_diagnosis = row['morphology'] if row['morphology'] is not None else ''
				source_text_tumor_location = row['topography'] if row['topography'] is not None else ''


        		#TODO add this information
        		# uberon_id_tumor_location =
        		# icd_id_diagnosis = 
				# mondo_id_diagnosis =
				# ncit_id_diagnosis =
				# external_id
				# spatial_descriptor
				

				url = '/diagnoses'
				data = '{"diagnosis_category": "' + diagnosis_category \
						+ '", "source_text_diagnosis": "' + source_text_diagnosis \
						+ '", "source_text_tumor_location": "' + source_text_tumor_location \
						+ '", "participant_id": "' + participant_id + '"'

				if age_at_event_days != '':
					data += ', "age_at_event_days": "' + age_at_event_days + '"' 

				data += ' }'

				diagnosisResponse = requests.post(baseUrl + url, data=data, headers=headers)

				if diagnosisResponse.json()['_status']['code'] < 300:
					diagnosis = diagnosisResponse.json()['results']
				else:
					print (diagnosisResponse.json())
		else:
			print("missing Diagnosis data\n")

	def add_biospecimen(self, df, participant_dct):
		biospecimen_df = df
		ret = {}

		if biospecimen_df is not None:
			for index, row in biospecimen_df.iterrows():

				age_at_event_days = str(row['age_at_diagnosis_(days)'])[:-2] if row['age_at_diagnosis_(days)'] is not None else ''
				analyte_type = "DNA"
				composition = row['sample_type']
				external_sample_id = row['sample_name']
				participant_id = participant_dct[row['individual_name']]

				# TODO set from dct
				sequencing_center_id = "SC_K52V7463"

				source_text_anatomical_site = row['topography'] if row['topography'] is not None else ''

				#TODO add this information
				# concentration_mg_per_ml
				# external_aliquot_id
				# ncit_id_anatomical_site
				# ncit_id_tissue_type
				# shipment_date =
				# shipment_origin
				# source_text_tissue_type
				# source_text_tumor_descriptor
				# spatial_descriptor
				# uberon_id_anatomical_site
				# volume_ml

				url = '/biospecimens'
				data = '{"analyte_type": "' + analyte_type \
						+ '", "composition": "' + composition \
						+ '", "external_sample_id": "' + external_sample_id \
						+ '", "participant_id": "' + participant_id \
						+ '", "sequencing_center_id": "' + sequencing_center_id \
						+ '", "source_text_anatomical_site": "' + source_text_anatomical_site + '"'

				if age_at_event_days != '':
					data += ', "age_at_event_days": "' + age_at_event_days + '"' 

				data += ' }'

				biospecimenResponse = requests.post(baseUrl + url, data=data, headers=headers)

				if biospecimenResponse.json()['_status']['code'] < 300:
					biospecimen = biospecimenResponse.json()['results']
					ret[external_sample_id] = biospecimen['kf_id']
				else:
					print (biospecimenResponse.json())
		else:
			print("missing Biospecimen data\n")

		return ret

	def add_sequencing_experiment(self, df):
		seq_center_df = df
		ret = {}

		if seq_center_df is not None:
			for index, row in seq_center_df.iterrows():

				
				experiment_strategy = "WGS"
				external_id = row['build_id']
				is_paired_end = str(True)
				mean_insert_size = str(row['mean_insert_size'])
				platform = "Illumina"
				sequencing_center_id = "SC_K52V7463"
				total_reads = str(row['pf_reads'])

				#TODO add this information
				# experiment_date
				# instrument_model
				# library_name
				# library_strand
				# max_insert_size
				# mean_depth
				# mean_read_length

				url = '/sequencing-experiments'
				data = '{"experiment_strategy": "' + experiment_strategy \
						+ '", "external_id": "' + external_id \
						+ '", "is_paired_end": "' + is_paired_end \
						+ '", "mean_insert_size": "' + mean_insert_size \
						+ '", "platform": "' + platform \
						+ '", "sequencing_center_id": "' + sequencing_center_id \
						+ '", "total_reads": "' + total_reads + '"' 

				data += ' }'

				seqExpResponse = requests.post(baseUrl + url, data=data, headers=headers)

				if seqExpResponse.json()['_status']['code'] < 300:
					seqExp = seqExpResponse.json()['results']
					ret[external_id] = seqExp['kf_id']
				else:
					print (seqExpResponse.json())
		else:
			print("missing Sequencing Experiment data\n")

		return ret

	def add_genomic_file(self, df, biospecimen_dct, sequencing_experiment_dct):
		genomic_df = df

		if genomic_df is not None:
			for index, row in genomic_df.iterrows():

				biospecimen_id = biospecimen_dct[row['sample_name']]
				controlled_access = str(True)
				data_type = row['data_type']
				file_format = row['file_format']
				file_name = row['file_name']
				hashes = json.dumps(row['hashes'])
				is_harmonized = str(row['is_harmonized'])
				sequencing_experiment_id = sequencing_experiment_dct[row['build_id']]
				size = str(row['size'])

				#TODO add this information
				# external_id = 
				# availability
				# metadata
				# reference_genome = 

				url = '/genomic-files'
				data = '{"biospecimen_id": "' + biospecimen_id \
						+ '", "controlled_access": "' + controlled_access \
						+ '", "data_type": "' + data_type \
						+ '", "file_format": "' + file_format \
						+ '", "file_name": "' + file_name \
						+ '", "is_harmonized": "' + is_harmonized \
						+ '", "sequencing_experiment_id": "' + sequencing_experiment_id \
						+ '", "size": "' + size + '", "hashes": ' + hashes  

				data += ' }'

				genFileResponse = requests.post(baseUrl + url, data=data, headers=headers)

				if genFileResponse.json()['_status']['code'] < 300:
					genFile = genFileResponse.json()['results']
				else:
					print (genFileResponse.json())
		else:
			print("missing Genomic File data\n")

	def add_phenotype(self, df, participant_dct):
		phenotype_df = df

		if phenotype_df is not None:
			for index, row in phenotype_df.iterrows():

				age_at_event_days = str(row['age_at_diagnosis_(days)'])[:-2] if row['age_at_diagnosis_(days)'] is not None else ''
				source_text_phenotype = row['phenotype'] if row['phenotype'] is not None else ''
				# hpo_id_phenotype = row['hpo_id'] if row['hpo_id'] is not None else ''
				observed = row['observed']
				participant_id = participant_dct[row['individual_name']]

				#TODO add this information
				# external_id
				# snomed_id_phenotype

				url = '/phenotypes'
				data = '{"source_text_phenotype": "' + source_text_phenotype \
						+ '", "observed": "' + observed \
						+ '", "participant_id": "' + participant_id + '"' 
				
				if age_at_event_days != '':
					data += ', "age_at_event_days": "' + age_at_event_days + '"' 

				data += ' }'

				genFileResponse = requests.post(baseUrl + url, data=data, headers=headers)

				if genFileResponse.json()['_status']['code'] < 300:
					genFile = genFileResponse.json()['results']
				else:
					print (genFileResponse.json())
		else:
			print("missing Phenotype data\n")



	def build_dfs(self):

		# Data Frame
		all_data_df = self.read_data()
		study_df = self.read_study_data()
		study_files_df = self.read_study_file_data()
		study_study_files_df = self._add_study_cols(study_df, study_files_df)
		participant_df = self.create_participant_df(all_data_df)
		family_df = self.create_family_relationship_df(all_data_df)
		study_participant_df = self._add_study_cols(study_df, participant_df)
		diagnosis_df = self.create_diagnosis_df(all_data_df)
		phenotype_df = self.create_phenotype_df(diagnosis_df)
		genomic_df = self.read_genomic_data()
		seq_exp_df = self.create_seq_exp_data(genomic_df)
		genomic_file_df = self.create_genomic_file_df(genomic_df, all_data_df)



		# Investigator
		print("adding investigator")
		investigator_id = self.add_investigator()

		# Study
		print("adding study")
		study_dct = self.add_study(study_df, investigator_id)

		# Study File
		print("adding study file")
		self.add_study_file(study_study_files_df, study_dct)

		#Family
		print("adding family")
		families_dct = self.add_family(participant_df)

		#Participant
		print("adding participant")
		participant_dct = self.add_participant(study_participant_df, families_dct, study_dct)

		# Family relationships
		print("adding family relationships")
		self.add_family_relationship(family_df, participant_dct)

		# Diagnosis 
		print("adding diagnosis")
		self.add_diagnoses(diagnosis_df, participant_dct)

		# Phenotype
		print("adding phenotype")
		self.add_phenotype(phenotype_df, participant_dct)

      	# Biospecimen
		# print("adding biospecimen")
		# biospecimen_dct = self.add_biospecimen(all_data_df, participant_dct)
      	# TODO add in add_biospeciment the reference to the sequencing center

		# Sequencing Experiment
		# print("adding sequencing experiment")
		# sequencing_experiment_dct = self.add_sequencing_experiment(seq_exp_df)

		# Genomic File 
		# print("adding genomic file")
		# self.add_genomic_file(genomic_file_df, biospecimen_dct, sequencing_experiment_dct)



	def _add_study_cols(self, study_df, df):
		# Add study cols to a df
		cols = study_df.columns.tolist()
		row = study_df.iloc[0]
		for col in cols:
			df[col] = row[col]
		return df

	def _create_family_relationship(self, relation_key, family, participant_dct):
		"""
		Create a family relationship
		"""
		if family.get(relation_key) and family.get('Self/Case'):	

			if not pd.isnull(family[relation_key]):		
				p_id = participant_dct[family[relation_key]] 
				r_id = participant_dct[family['Self/Case']] 

				if p_id and r_id:
					return {'participant_id': p_id, 'relative_id': r_id, 'participant_to_relative_relation': relation_key} 
			else:
				return None
		else:
			return None  


	def run(self):
		"""
		Run extraction and return a Pandas DataFrame
		"""
		return self.build_dfs()


ex = Extractor(config)
ex.run()


