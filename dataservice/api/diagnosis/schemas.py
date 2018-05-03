from marshmallow_sqlalchemy import field_for

from dataservice.api.diagnosis.models import Diagnosis, DiagnosisCategoryEnum
from dataservice.api.common.schemas import BaseSchema
from dataservice.api.common.validation import validate_age
from dataservice.extensions import ma
from dataservice.api.common.custom_fields import EnumColumn


class DiagnosisSchema(BaseSchema):
    participant_id = field_for(Diagnosis, 'participant_id', required=True,
                               load_only=True, example='DZB048J5')
    age_at_event_days = field_for(Diagnosis, 'age_at_event_days',
                                  validate=validate_age, example=232)
    diagnosis_categoty = EnumColumn(
        enum=[s.value for s in DiagnosisCategoryEnum])

    class Meta(BaseSchema.Meta):
        model = Diagnosis
        resource_url = 'api.diagnoses'
        collection_url = 'api.diagnoses_list'

    _links = ma.Hyperlinks({
        'self': ma.URLFor(Meta.resource_url, kf_id='<kf_id>'),
        'collection': ma.URLFor(Meta.collection_url),
        'participant': ma.URLFor('api.participants', kf_id='<participant_id>')
    })
