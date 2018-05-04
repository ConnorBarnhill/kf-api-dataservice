from marshmallow_sqlalchemy import field_for

from dataservice.api.study.models import Study, ReleaseStatusEnum
from dataservice.api.common.schemas import BaseSchema
from dataservice.api.common.custom_fields import PatchedURLFor, EnumField
from dataservice.extensions import ma


class StudySchema(BaseSchema):

    investigator_id = field_for(Study, 'investigator_id',
                                required=False, example='IG_ABB2C104')
    release_status = EnumField(enum=[s.value for s in ReleaseStatusEnum])

    class Meta(BaseSchema.Meta):
        model = Study
        resource_url = 'api.studies'
        collection_url = 'api.studies_list'

    _links = ma.Hyperlinks({
        'self': ma.URLFor(Meta.resource_url, kf_id='<kf_id>'),
        'collection': ma.URLFor(Meta.collection_url),
        'investigator': PatchedURLFor('api.investigators',
                                      kf_id='<investigator_id>')
    })
