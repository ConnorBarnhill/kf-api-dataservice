from marshmallow_sqlalchemy import field_for
from marshmallow import (
    fields,
    validates
)

from dataservice.api.common.custom_fields import PatchedURLFor
from dataservice.extensions import ma
from dataservice.api.common.validation import (
    enum_validation_generator,
    validate_kf_id
)
from dataservice.api.genomic_file.models import GenomicFile
from dataservice.api.common.schemas import (
    BaseSchema,
    IndexdFileSchema,
    AVAILABILITY_ENUM
)

DATA_TYPE_ENUM = {'Aligned Reads',
                  'Aligned Reads Index',
                  'Unaligned Reads',
                  'Simple Nucleotide Variation',
                  'Variant Calls',
                  'Variant Calls Index',
                  'gVCF',
                  'gVCF Index',
                  'Other',
                  'Expression',
                  'Histology Images', 'Radiology Images', 'Pathology Reports',
                  'Operation Reports', 'Radiology Reports'}

PAIRED_END_ENUM = {1, 2}


class GenomicFileSchema(BaseSchema, IndexdFileSchema):
    class Meta(BaseSchema.Meta, IndexdFileSchema.Meta):
        model = GenomicFile
        resource_url = 'api.genomic_files'
        collection_url = 'api.genomic_files_list'

        exclude = (BaseSchema.Meta.exclude +
                   ('biospecimen', 'sequencing_experiment',) +
                   ('cavatica_task_genomic_files',
                    'biospecimen_genomic_files',))

    paired_end = field_for(GenomicFile, 'paired_end',
                           validate=enum_validation_generator(
                               PAIRED_END_ENUM))

    data_type = field_for(GenomicFile, 'data_type',
                          validate=enum_validation_generator(
                              DATA_TYPE_ENUM))
    availability = field_for(GenomicFile, 'availability',
                             validate=enum_validation_generator(
                                 AVAILABILITY_ENUM))

    sequencing_experiment_id = field_for(GenomicFile,
                                         'sequencing_experiment_id',
                                         load_only=True)

    latest_did = field_for(GenomicFile,
                           'latest_did',
                           required=False,
                           dump_only=True)

    _links = ma.Hyperlinks({
        'self': ma.URLFor(Meta.resource_url, kf_id='<kf_id>'),
        'collection': ma.URLFor(Meta.collection_url),
        'sequencing_experiment': PatchedURLFor(
            'api.sequencing_experiments',
            kf_id='<sequencing_experiment_id>'),
        'cavatica_task_genomic_files': ma.URLFor(
            'api.cavatica_task_genomic_files_list', genomic_file_id='<kf_id>'),
        'biospecimen_genomic_files': ma.URLFor(
            'api.biospecimen_genomic_files_list', genomic_file_id='<kf_id>'),
        'read_group_genomic_files': ma.URLFor(
            'api.read_group_genomic_files_list', genomic_file_id='<kf_id>'),
        'read_groups': ma.URLFor('api.read_groups_list',
                                 genomic_file_id='<kf_id>')
    }, description='Resource links and pagination')


class GenomicFileFilterSchema(GenomicFileSchema):

    read_group_id = fields.Str()

    @validates('read_group_id')
    def valid_read_group_id(self, value):
        validate_kf_id('RG', value)
